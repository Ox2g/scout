# Scout Backend MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Scout backend vertical slice — `curl POST /ingest → worker extracts claims → novelty judged → inbox page lists articles with novel/supplement/duplicate counts → article page shows extracted novel claims with original quotes`.

**Architecture:** Rust workspace, single binary with `web` / `worker` / `migrate` / `reembed` modes; Axum + Maud SSR + HTMX; PostgreSQL + pgvector for business data, vectors, and `SKIP LOCKED` job queue; traits in `scout-core` enable mocked unit tests and future multi-pass / LLM-verifier upgrades.

**Tech Stack:** Rust (stable), Axum 0.7, Maud, tokio, sqlx (with `pgvector` crate), serde, reqwest, clap, tracing, testcontainers, wiremock, PostgreSQL 16 + pgvector.

**Methodology:** TDD + Tidy First — structural and behavioral commits split. Commit prefixes: `tidy:` / `feat:` / `fix:` / `test:` / `docs:` / `chore:`. After each task, both compliance review (spec adherence) and code-quality review must pass before the next task starts.

**Spec:** [`docs/superpowers/specs/2026-05-17-scout-design.md`](../specs/2026-05-17-scout-design.md)

---

## File Map

```
Cargo.toml                                # workspace manifest
rust-toolchain.toml                       # pinned stable
rustfmt.toml
.gitignore
.env.example
config.example.toml
docker-compose.yml                        # Postgres+pgvector for dev/prod
.github/workflows/ci.yml
migrations/
  20260517000001_init.sql                 # all MVP tables + indexes
crates/
  scout-core/                             # types, traits, errors (NO IO)
    src/{lib,types,traits,error,novelty,kind}.rs
  scout-storage/                          # sqlx + PgJobQueue
    src/{lib,pool,article_repo,claim_repo,entity_repo,
         claim_entity_repo,feedback_repo,job_queue}.rs
    tests/                                # testcontainers integration tests
  scout-llm/                              # LlmClient + Embedder impls
    src/{lib,anthropic,openai,openai_embedder,mock}.rs
  scout-pipeline/                         # extractor / judge / resolver impls
    src/{lib,claim_extractor,novelty_judge,entity_resolver,prompts}.rs
  scout-web/                              # Axum + Maud + HTMX
    src/{lib,app,auth,config,error}.rs
    src/templates/{layout,login,inbox,article}.rs
    src/routes/{login,ingest,inbox,article,health}.rs
    static/{htmx.min.js,pico.min.css,app.css}
  scout-cli/                              # binary entry; clap subcommands
    src/{main,mode_web,mode_worker,mode_migrate,mode_reembed}.rs
README.md
```

Each file has one responsibility. Repos are one-per-table. Templates are one-per-page. Routes one-per-page handler.

---

## Sequencing & Conventions

- Tasks are numbered. **Do not reorder** — later tasks depend on earlier types and traits.
- Every TDD task is: (1) failing test, (2) verify red, (3) minimal impl, (4) verify green, (5) commit. Tidy refactors get their own `tidy:` commit between steps when needed.
- Database tests use `testcontainers` to bring up `pgvector/pgvector:pg16` per test module. Set `TESTCONTAINERS_RYUK_DISABLED=true` on macOS if Docker socket access misbehaves.
- For vector queries we use sqlx's **runtime API** (`sqlx::query`/`query_as`) because `query!` macros don't know the `vector` type. Non-vector queries use compile-time `query!`/`query_as!` and are checked in CI via `cargo sqlx prepare --check`.
- All async traits use `async-trait` until Rust's native dyn-compatible `async fn` lands cleanly.

---


## Task 1: Workspace scaffolding + tooling baseline

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `rustfmt.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `config.example.toml`
- Create: `crates/scout-core/Cargo.toml`
- Create: `crates/scout-core/src/lib.rs`
- Create: `.github/workflows/ci.yml`

This task has no behavior test — it's pure scaffolding. We verify it works by `cargo check` and CI passing.

- [ ] **Step 1: Write workspace `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "crates/scout-core",
]

[workspace.package]
edition = "2021"
rust-version = "1.78"
license = "MIT"

[workspace.dependencies]
async-trait = "0.1"
anyhow = "1"
thiserror = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
```

- [ ] **Step 2: Write `rust-toolchain.toml`**

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Write `rustfmt.toml`**

```toml
edition = "2021"
max_width = 100
```

- [ ] **Step 4: Write `.gitignore`**

```
/target
/.env
/.idea
/.vscode
*.swp
.DS_Store
/.sqlx
```

- [ ] **Step 5: Write `.env.example`**

```
DATABASE_URL=postgres://scout:scout@localhost:5432/scout
SCOUT_TOKEN=change-me-to-a-long-random-string
SCOUT_BIND=0.0.0.0:8080
SCOUT_COOKIE_SECRET=change-me-to-64-random-bytes-hex
ANTHROPIC_API_KEY=
OPENAI_API_KEY=
RUST_LOG=info,scout=debug
```

- [ ] **Step 6: Write `config.example.toml`**

```toml
# Scout configuration. Copy to config.toml and edit.

[llm]
# Which LlmClient implementation to wire in at startup.
provider = "anthropic"   # anthropic | openai
model = "claude-sonnet-4-6"

[embedder]
provider = "openai"
model = "text-embedding-3-small"
dim = 1536

[novelty]
# Per-kind thresholds. Initial placeholders; calibrate after ~200 articles.
fact_duplicate_threshold = 0.92
fact_supplement_low = 0.85
opinion_duplicate_threshold = 0.96
opinion_supplement_low = 0.88
methodology_duplicate_threshold = 0.93
methodology_supplement_low = 0.86

[worker]
poll_interval_ms = 500
max_concurrent_jobs = 4
```

- [ ] **Step 7: Write `crates/scout-core/Cargo.toml`**

```toml
[package]
name = "scout-core"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
async-trait.workspace = true
anyhow.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_json.workspace = true
uuid.workspace = true
chrono.workspace = true
```

- [ ] **Step 8: Write `crates/scout-core/src/lib.rs`**

```rust
//! Scout domain types and traits — no IO, no transitive runtime deps.
```

- [ ] **Step 9: Write `.github/workflows/ci.yml`**

```yaml
name: ci
on:
  push:
    branches: [master]
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [ ] **Step 10: Verify everything compiles**

Run: `cargo check --workspace`
Expected: `Finished` with no errors.

Run: `cargo fmt --all -- --check`
Expected: no diff.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 11: Commit**

```bash
git add Cargo.toml rust-toolchain.toml rustfmt.toml .gitignore .env.example config.example.toml crates .github
git commit -m "chore: scaffold workspace + tooling baseline"
```

---

## Task 2: scout-core domain types

**Files:**
- Create: `crates/scout-core/src/kind.rs`
- Create: `crates/scout-core/src/novelty.rs`
- Create: `crates/scout-core/src/types.rs`
- Create: `crates/scout-core/src/error.rs`
- Modify: `crates/scout-core/src/lib.rs`

- [ ] **Step 1: Write the failing test for `ClaimKind` serde round-trip**

Append to `crates/scout-core/src/kind.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_kind_serializes_to_lowercase_strings() {
        let json = serde_json::to_string(&ClaimKind::Fact).unwrap();
        assert_eq!(json, "\"fact\"");
        let back: ClaimKind = serde_json::from_str("\"opinion\"").unwrap();
        assert_eq!(back, ClaimKind::Opinion);
    }

    #[test]
    fn claim_kind_db_value_matches_serde() {
        for k in ClaimKind::ALL {
            assert_eq!(k.as_db_str(), serde_json::to_value(k).unwrap().as_str().unwrap());
        }
    }
}
```

Stub the type so the file compiles but the test fails:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimKind {}

impl ClaimKind {
    pub const ALL: &'static [ClaimKind] = &[];
    pub fn as_db_str(&self) -> &'static str { unimplemented!() }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p scout-core kind::tests`
Expected: FAIL — `ClaimKind::Fact` does not exist.

- [ ] **Step 3: Implement `ClaimKind`**

Replace `crates/scout-core/src/kind.rs` body (keep tests at bottom):

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClaimKind {
    Fact,
    Opinion,
    Data,
    Prediction,
    Methodology,
}

impl ClaimKind {
    pub const ALL: &'static [ClaimKind] = &[
        ClaimKind::Fact,
        ClaimKind::Opinion,
        ClaimKind::Data,
        ClaimKind::Prediction,
        ClaimKind::Methodology,
    ];

    pub fn as_db_str(&self) -> &'static str {
        match self {
            ClaimKind::Fact => "fact",
            ClaimKind::Opinion => "opinion",
            ClaimKind::Data => "data",
            ClaimKind::Prediction => "prediction",
            ClaimKind::Methodology => "methodology",
        }
    }

    pub fn parse_db(s: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|k| k.as_db_str() == s)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p scout-core kind::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Write the failing test for `Novelty`**

Create `crates/scout-core/src/novelty.rs`:

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoveltyTag {
    Pending,
    Novel,
    Duplicate,
    Supplement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Novelty {
    Novel,
    Duplicate { dup_of: Uuid },
    Supplement { supplement_of: Uuid },
}

impl Novelty {
    pub fn tag(&self) -> NoveltyTag {
        match self {
            Novelty::Novel => NoveltyTag::Novel,
            Novelty::Duplicate { .. } => NoveltyTag::Duplicate,
            Novelty::Supplement { .. } => NoveltyTag::Supplement,
        }
    }

    pub fn dup_of(&self) -> Option<Uuid> {
        match self { Novelty::Duplicate { dup_of } => Some(*dup_of), _ => None }
    }

    pub fn supplement_of(&self) -> Option<Uuid> {
        match self { Novelty::Supplement { supplement_of } => Some(*supplement_of), _ => None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn novelty_tag_is_consistent_with_variant() {
        let id = Uuid::new_v4();
        assert_eq!(Novelty::Novel.tag(), NoveltyTag::Novel);
        assert_eq!(Novelty::Duplicate { dup_of: id }.tag(), NoveltyTag::Duplicate);
        assert_eq!(Novelty::Supplement { supplement_of: id }.tag(), NoveltyTag::Supplement);
    }

    #[test]
    fn dup_and_supplement_id_accessors() {
        let id = Uuid::new_v4();
        assert_eq!(Novelty::Duplicate { dup_of: id }.dup_of(), Some(id));
        assert_eq!(Novelty::Supplement { supplement_of: id }.supplement_of(), Some(id));
        assert_eq!(Novelty::Novel.dup_of(), None);
    }
}
```

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p scout-core novelty::tests`
Expected: PASS (2 tests).

- [ ] **Step 7: Write `error.rs`**

Create `crates/scout-core/src/error.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("provider error: {0}")]
    Provider(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type CoreResult<T> = Result<T, CoreError>;
```

- [ ] **Step 8: Write `types.rs`**

Create `crates/scout-core/src/types.rs`:

```rust
use crate::kind::ClaimKind;
use crate::novelty::NoveltyTag;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    pub id: Uuid,
    pub url: String,
    pub title: String,
    pub author: Option<String>,
    pub source: String,
    pub language: String,
    pub content_md: String,
    pub content_hash: Vec<u8>,
    pub ingested_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct NewArticle {
    pub url: String,
    pub title: String,
    pub author: Option<String>,
    pub source: String,
    pub language: String,
    pub raw_html: Option<String>,
    pub content_md: String,
    pub content_hash: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityHint {
    pub name: String,
    pub kind: String,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRef {
    pub entity_id: Uuid,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawClaim {
    pub text: String,
    pub original_quote: String,
    pub quote_offset_start: i32,
    pub quote_offset_end: i32,
    pub kind: ClaimKind,
    pub language: String,
    pub entity_hints: Vec<EntityHint>,
}

#[derive(Debug, Clone)]
pub struct ClaimDraft {
    pub id: Uuid,
    pub article_id: Uuid,
    pub text: String,
    pub kind: ClaimKind,
    pub language: String,
    pub embedding: Option<Vec<f32>>,
}

#[derive(Debug, Clone)]
pub struct Candidate {
    pub claim_id: Uuid,
    pub text: String,
    pub kind: ClaimKind,
    pub similarity: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claim {
    pub id: Uuid,
    pub article_id: Uuid,
    pub text: String,
    pub original_quote: String,
    pub quote_offset_start: i32,
    pub quote_offset_end: i32,
    pub kind: ClaimKind,
    pub language: String,
    pub novelty: NoveltyTag,
    pub dup_of: Option<Uuid>,
    pub supplement_of: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}
```

- [ ] **Step 9: Update `lib.rs`**

Replace `crates/scout-core/src/lib.rs`:

```rust
//! Scout domain types and traits — no IO, no transitive runtime deps.

pub mod error;
pub mod kind;
pub mod novelty;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use kind::ClaimKind;
pub use novelty::{Novelty, NoveltyTag};
pub use types::{
    Article, Candidate, Claim, ClaimDraft, EntityHint, EntityRef, NewArticle, RawClaim,
};
```

- [ ] **Step 10: Verify full crate compiles + tests pass**

Run: `cargo test -p scout-core`
Expected: PASS — at least 4 tests (kind ×2, novelty ×2).

Run: `cargo clippy -p scout-core --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 11: Commit**

```bash
git add crates/scout-core
git commit -m "feat(core): add Claim/Article domain types and Novelty enum"
```

---

## Task 3: scout-core traits

**Files:**
- Create: `crates/scout-core/src/traits.rs`
- Modify: `crates/scout-core/src/lib.rs`

- [ ] **Step 1: Write the failing compile-only test**

Create `crates/scout-core/src/traits.rs`:

```rust
use crate::error::CoreResult;
use crate::types::{Article, Candidate, ClaimDraft, EntityHint, EntityRef, RawClaim};
use crate::novelty::Novelty;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub system: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub max_tokens: u32,
    pub temperature: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub text: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, req: ChatRequest) -> CoreResult<ChatResponse>;
}

#[async_trait]
pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;
    async fn embed(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>>;
}

#[async_trait]
pub trait ClaimExtractor: Send + Sync {
    async fn extract(&self, article: &Article) -> CoreResult<Vec<RawClaim>>;
}

pub trait NoveltyJudge: Send + Sync {
    fn judge(&self, claim: &ClaimDraft, candidates: &[Candidate]) -> Novelty;
}

#[async_trait]
pub trait EntityResolver: Send + Sync {
    async fn resolve(
        &self,
        claim: &ClaimDraft,
        hints: &[EntityHint],
    ) -> CoreResult<Vec<EntityRef>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_object_safe(_: &dyn LlmClient, _: &dyn Embedder, _: &dyn ClaimExtractor,
                          _: &dyn NoveltyJudge, _: &dyn EntityResolver) {}

    #[test]
    fn traits_are_object_safe() {
        // Compile-time check; bodies unused.
        let _ = assert_object_safe;
    }
}
```

- [ ] **Step 2: Update `lib.rs`**

Replace `crates/scout-core/src/lib.rs`:

```rust
//! Scout domain types and traits — no IO, no transitive runtime deps.

pub mod error;
pub mod kind;
pub mod novelty;
pub mod traits;
pub mod types;

pub use error::{CoreError, CoreResult};
pub use kind::ClaimKind;
pub use novelty::{Novelty, NoveltyTag};
pub use traits::{
    ChatMessage, ChatRequest, ChatResponse, ClaimExtractor, Embedder, EntityResolver,
    LlmClient, NoveltyJudge,
};
pub use types::{
    Article, Candidate, Claim, ClaimDraft, EntityHint, EntityRef, NewArticle, RawClaim,
};
```

- [ ] **Step 3: Verify compile**

Run: `cargo test -p scout-core`
Expected: PASS.

Run: `cargo clippy -p scout-core --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/scout-core/src/traits.rs crates/scout-core/src/lib.rs
git commit -m "feat(core): add LlmClient/Embedder/ClaimExtractor/NoveltyJudge/EntityResolver traits"
```

---

## Task 4: Postgres setup + scout-storage pool

**Files:**
- Create: `docker-compose.yml`
- Create: `crates/scout-storage/Cargo.toml`
- Create: `crates/scout-storage/src/lib.rs`
- Create: `crates/scout-storage/src/pool.rs`
- Create: `crates/scout-storage/tests/pool_test.rs`
- Modify: `Cargo.toml` (workspace `members`, add `sqlx` and `pgvector` to `workspace.dependencies`)

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: pgvector/pgvector:pg16
    container_name: scout-postgres
    environment:
      POSTGRES_DB: scout
      POSTGRES_USER: scout
      POSTGRES_PASSWORD: scout
    ports:
      - "5432:5432"
    volumes:
      - scout_pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U scout -d scout"]
      interval: 5s
      timeout: 3s
      retries: 10
volumes:
  scout_pgdata:
```

- [ ] **Step 2: Add workspace deps**

Modify `Cargo.toml` — add to `workspace.members` `"crates/scout-storage",` and append to `workspace.dependencies`:

```toml
sqlx = { version = "0.8", default-features = false, features = [
    "runtime-tokio", "tls-rustls", "postgres", "macros", "uuid", "chrono", "json", "migrate"
] }
pgvector = { version = "0.4", features = ["sqlx"] }
testcontainers = "0.20"
testcontainers-modules = { version = "0.8", features = ["postgres"] }
```

- [ ] **Step 3: Write `crates/scout-storage/Cargo.toml`**

```toml
[package]
name = "scout-storage"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
scout-core = { path = "../scout-core" }
async-trait.workspace = true
anyhow.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_json.workspace = true
sqlx.workspace = true
pgvector.workspace = true
tokio.workspace = true
tracing.workspace = true
uuid.workspace = true
chrono.workspace = true

[dev-dependencies]
testcontainers.workspace = true
testcontainers-modules.workspace = true
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 4: Write `lib.rs` and `pool.rs`**

`crates/scout-storage/src/lib.rs`:

```rust
//! Scout Postgres storage layer: repos, job queue, migrations runner.

pub mod pool;

pub use pool::{connect, PgPool};
```

`crates/scout-storage/src/pool.rs`:

```rust
use anyhow::Context;
use sqlx::postgres::PgPoolOptions;

pub type PgPool = sqlx::PgPool;

pub async fn connect(database_url: &str, max_conns: u32) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect(database_url)
        .await
        .with_context(|| format!("connecting to {database_url}"))
}
```

- [ ] **Step 5: Write the failing integration test**

Create `crates/scout-storage/tests/pool_test.rs`:

```rust
use scout_storage::connect;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn pool_can_select_one() {
    let container = Postgres::default()
        .with_tag("pg16")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = connect(&url, 4).await.expect("connect");
    let (one,): (i32,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.unwrap();
    assert_eq!(one, 1);
}
```

> Note: `testcontainers_modules::postgres::Postgres` defaults to the official `postgres` image (no pgvector). For this connectivity test, the default image is fine; later tasks override the image to `pgvector/pgvector:pg16` for migrations.

- [ ] **Step 6: Run and confirm pass**

Run: `cargo test -p scout-storage --test pool_test`
Expected: PASS (Docker daemon must be running locally).

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml Cargo.toml crates/scout-storage
git commit -m "feat(storage): add PgPool factory + testcontainers connectivity test"
```

---

## Task 5: Initial migration

**Files:**
- Create: `migrations/20260517000001_init.sql`
- Create: `crates/scout-storage/src/migrate.rs`
- Modify: `crates/scout-storage/src/lib.rs`
- Create: `crates/scout-storage/tests/migrate_test.rs`
- Create: `crates/scout-storage/tests/common/mod.rs`

- [ ] **Step 1: Write the migration**

Create `migrations/20260517000001_init.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE articles (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    url             TEXT        NOT NULL,
    title           TEXT        NOT NULL,
    author          TEXT,
    source          TEXT        NOT NULL,
    language        TEXT        NOT NULL,
    raw_html        TEXT,
    content_md      TEXT        NOT NULL,
    content_hash    BYTEA       NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL,
    error           TEXT
);

CREATE UNIQUE INDEX articles_url_unique         ON articles(url)          WHERE status <> 'exact_duplicate';
CREATE UNIQUE INDEX articles_content_hash_unique ON articles(content_hash) WHERE status <> 'exact_duplicate';
CREATE INDEX        articles_status_ingested    ON articles(status, ingested_at DESC);

CREATE TABLE claims (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id          UUID        NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    text                TEXT        NOT NULL,
    original_quote      TEXT        NOT NULL,
    quote_offset_start  INT         NOT NULL,
    quote_offset_end    INT         NOT NULL,
    kind                TEXT        NOT NULL,
    language            TEXT        NOT NULL,
    embedding           vector(1536),
    novelty             TEXT        NOT NULL DEFAULT 'pending',
    dup_of              UUID        REFERENCES claims(id),
    supplement_of       UUID        REFERENCES claims(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX claims_article_id            ON claims(article_id);
CREATE INDEX claims_novelty_kind_created  ON claims(novelty, kind, created_at DESC);
CREATE INDEX claims_embedding_hnsw        ON claims USING hnsw (embedding vector_cosine_ops);

CREATE TABLE entities (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name  TEXT        NOT NULL,
    kind            TEXT        NOT NULL,
    aliases         TEXT[]      NOT NULL DEFAULT '{}',
    embedding       vector(1536),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (canonical_name, kind)
);

CREATE INDEX entities_embedding_hnsw ON entities USING hnsw (embedding vector_cosine_ops);

CREATE TABLE claim_entities (
    claim_id    UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    entity_id   UUID NOT NULL REFERENCES entities(id),
    role        TEXT NOT NULL,
    PRIMARY KEY (claim_id, entity_id, role)
);

CREATE TABLE jobs (
    id            BIGSERIAL    PRIMARY KEY,
    kind          TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    status        TEXT         NOT NULL DEFAULT 'pending',
    attempts      INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 5,
    scheduled_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    picked_at     TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    error         TEXT
);

CREATE INDEX jobs_pending ON jobs(scheduled_at) WHERE status = 'pending';

CREATE TABLE feedback (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id    UUID        REFERENCES claims(id),
    entity_id   UUID        REFERENCES entities(id),
    action      TEXT        NOT NULL,
    target_id   UUID,
    note        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Write `migrate.rs`**

Create `crates/scout-storage/src/migrate.rs`:

```rust
use crate::pool::PgPool;
use anyhow::Context;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

pub async fn run(pool: &PgPool) -> anyhow::Result<()> {
    MIGRATOR.run(pool).await.context("running migrations")
}
```

- [ ] **Step 3: Update `lib.rs`**

Modify `crates/scout-storage/src/lib.rs`:

```rust
//! Scout Postgres storage layer: repos, job queue, migrations runner.

pub mod migrate;
pub mod pool;

pub use migrate::{run as run_migrations, MIGRATOR};
pub use pool::{connect, PgPool};
```

- [ ] **Step 4: Write a shared test helper**

Create `crates/scout-storage/tests/common/mod.rs`:

```rust
use scout_storage::{connect, run_migrations, PgPool};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

pub struct TestDb {
    pub pool: PgPool,
    _container: ContainerAsync<Postgres>,
}

pub async fn fresh_db() -> TestDb {
    let container = Postgres::default()
        .with_name("pgvector/pgvector")
        .with_tag("pg16")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect(&url, 4).await.expect("connect");
    run_migrations(&pool).await.expect("migrate");
    TestDb { pool, _container: container }
}
```

- [ ] **Step 5: Write the failing migration test**

Create `crates/scout-storage/tests/migrate_test.rs`:

```rust
mod common;

#[tokio::test]
async fn migration_creates_all_tables() {
    let db = common::fresh_db().await;
    let expected = [
        "articles", "claims", "entities", "claim_entities", "jobs", "feedback",
    ];
    for table in expected {
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(exists.0, "table {table} should exist after migrations");
    }
}

#[tokio::test]
async fn vector_extension_loaded() {
    let db = common::fresh_db().await;
    let (installed,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(installed);
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p scout-storage --test migrate_test`
Expected: PASS — 2 tests.

- [ ] **Step 7: Commit**

```bash
git add migrations crates/scout-storage
git commit -m "feat(storage): initial migration with all MVP tables"
```

---

## Task 6: ArticleRepo

**Files:**
- Create: `crates/scout-storage/src/article_repo.rs`
- Create: `crates/scout-storage/tests/article_repo_test.rs`
- Modify: `crates/scout-storage/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/scout-storage/tests/article_repo_test.rs`:

```rust
mod common;

use scout_core::NewArticle;
use scout_storage::ArticleRepo;

fn new_article(url: &str, body: &str) -> NewArticle {
    NewArticle {
        url: url.to_string(),
        title: "T".into(),
        author: None,
        source: "browser-extension".into(),
        language: "en".into(),
        raw_html: None,
        content_md: body.into(),
        content_hash: sha2::Sha256::digest(body.as_bytes()).to_vec(),
    }
}

#[tokio::test]
async fn insert_returns_new_article_with_pending_status() {
    let db = common::fresh_db().await;
    let repo = ArticleRepo::new(db.pool.clone());
    let id = repo.insert(new_article("https://a.test/1", "hello")).await.unwrap();
    let got = repo.get(id).await.unwrap().unwrap();
    assert_eq!(got.status, "pending");
    assert_eq!(got.url, "https://a.test/1");
}

#[tokio::test]
async fn insert_same_url_returns_exact_duplicate_id() {
    let db = common::fresh_db().await;
    let repo = ArticleRepo::new(db.pool.clone());
    let _ = repo.insert(new_article("https://a.test/dup", "x")).await.unwrap();
    let r = repo.insert(new_article("https://a.test/dup", "different")).await.unwrap_err();
    assert!(matches!(r, scout_storage::StorageError::Duplicate(_)));
}

#[tokio::test]
async fn set_status_persists() {
    let db = common::fresh_db().await;
    let repo = ArticleRepo::new(db.pool.clone());
    let id = repo.insert(new_article("https://a.test/2", "y")).await.unwrap();
    repo.set_status(id, "processed").await.unwrap();
    assert_eq!(repo.get(id).await.unwrap().unwrap().status, "processed");
}
```

Add to `crates/scout-storage/Cargo.toml`:

```toml
[dev-dependencies]
sha2 = "0.10"
```

- [ ] **Step 2: Write the minimal implementation**

Create `crates/scout-storage/src/article_repo.rs`:

```rust
use crate::pool::PgPool;
use scout_core::{Article, NewArticle};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("duplicate: {0}")]
    Duplicate(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Clone)]
pub struct ArticleRepo {
    pool: PgPool,
}

impl ArticleRepo {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn insert(&self, a: NewArticle) -> StorageResult<Uuid> {
        let row = sqlx::query!(
            r#"INSERT INTO articles
               (url, title, author, source, language, raw_html, content_md, content_hash, status)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending')
               RETURNING id"#,
            a.url, a.title, a.author, a.source, a.language, a.raw_html, a.content_md, a.content_hash
        )
        .fetch_one(&self.pool)
        .await;
        match row {
            Ok(r) => Ok(r.id),
            Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("23505") => {
                Err(StorageError::Duplicate(db.constraint().unwrap_or("?").into()))
            }
            Err(e) => Err(StorageError::Db(e)),
        }
    }

    pub async fn get(&self, id: Uuid) -> StorageResult<Option<Article>> {
        let row = sqlx::query!(
            r#"SELECT id, url, title, author, source, language, content_md, content_hash,
                      ingested_at, processed_at, status
               FROM articles WHERE id = $1"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| Article {
            id: r.id,
            url: r.url,
            title: r.title,
            author: r.author,
            source: r.source,
            language: r.language,
            content_md: r.content_md,
            content_hash: r.content_hash,
            ingested_at: r.ingested_at,
            processed_at: r.processed_at,
            status: r.status,
        }))
    }

    pub async fn set_status(&self, id: Uuid, status: &str) -> StorageResult<()> {
        sqlx::query!(
            "UPDATE articles SET status = $2,
                processed_at = CASE WHEN $2 = 'processed' THEN now() ELSE processed_at END
             WHERE id = $1",
            id, status
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_recent(&self, limit: i64) -> StorageResult<Vec<Article>> {
        let rows = sqlx::query!(
            r#"SELECT id, url, title, author, source, language, content_md, content_hash,
                      ingested_at, processed_at, status
               FROM articles
               ORDER BY ingested_at DESC LIMIT $1"#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| Article {
            id: r.id, url: r.url, title: r.title, author: r.author, source: r.source,
            language: r.language, content_md: r.content_md, content_hash: r.content_hash,
            ingested_at: r.ingested_at, processed_at: r.processed_at, status: r.status,
        }).collect())
    }
}
```

> **Note on sqlx compile-time checking:** `query!` macros require either `DATABASE_URL` pointing at a live DB at build time, or `.sqlx/` query metadata committed via `cargo sqlx prepare`. For local development, run `docker compose up -d postgres && cargo sqlx prepare --workspace -- --tests` once after each new query, and commit `.sqlx/`. CI uses `--check` mode against committed metadata.
>
> If you prefer to defer `.sqlx` discipline until later, swap each `query!` for `sqlx::query_as::<_, RowStruct>("…")` runtime queries. The plan keeps `query!` to preserve compile-time SQL checks for the non-vector paths.

- [ ] **Step 3: Update `lib.rs`**

Append to `crates/scout-storage/src/lib.rs`:

```rust
pub mod article_repo;
pub use article_repo::{ArticleRepo, StorageError, StorageResult};
```

- [ ] **Step 4: Prepare sqlx metadata**

```bash
docker compose up -d postgres
sleep 3  # wait for healthy
DATABASE_URL=postgres://scout:scout@localhost:5432/scout \
    sqlx migrate run --source migrations
DATABASE_URL=postgres://scout:scout@localhost:5432/scout \
    cargo sqlx prepare --workspace -- --tests
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p scout-storage --test article_repo_test`
Expected: PASS — 3 tests.

- [ ] **Step 6: Commit (tidy + feat split)**

```bash
git add .sqlx
git commit -m "tidy(storage): commit sqlx offline metadata"

git add crates/scout-storage/src/article_repo.rs \
        crates/scout-storage/src/lib.rs \
        crates/scout-storage/Cargo.toml \
        crates/scout-storage/tests/article_repo_test.rs
git commit -m "feat(storage): ArticleRepo with insert/get/set_status/list_recent"
```

---

## Task 7: ClaimRepo with embedding + ANN search

**Files:**
- Create: `crates/scout-storage/src/claim_repo.rs`
- Create: `crates/scout-storage/tests/claim_repo_test.rs`
- Modify: `crates/scout-storage/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/scout-storage/tests/claim_repo_test.rs`:

```rust
mod common;

use scout_core::{ClaimKind, NewArticle};
use scout_storage::{ArticleRepo, ClaimRepo, NewClaim};
use sha2::Digest;
use uuid::Uuid;

async fn seed_article(pool: &scout_storage::PgPool) -> Uuid {
    let repo = ArticleRepo::new(pool.clone());
    repo.insert(NewArticle {
        url: format!("https://a.test/{}", Uuid::new_v4()),
        title: "T".into(),
        author: None,
        source: "test".into(),
        language: "en".into(),
        raw_html: None,
        content_md: "body".into(),
        content_hash: sha2::Sha256::digest(b"body").to_vec(),
    })
    .await
    .unwrap()
}

fn vec1536(seed: f32) -> Vec<f32> {
    (0..1536).map(|i| (i as f32 * 0.001) + seed).collect()
}

#[tokio::test]
async fn insert_and_set_embedding_and_set_novelty() {
    let db = common::fresh_db().await;
    let repo = ClaimRepo::new(db.pool.clone());
    let article_id = seed_article(&db.pool).await;
    let id = repo.insert(NewClaim {
        article_id,
        text: "GPT-5 scores 87 on ARC-AGI".into(),
        original_quote: "GPT-5 scored 87% on ARC-AGI".into(),
        quote_offset_start: 0,
        quote_offset_end: 28,
        kind: ClaimKind::Fact,
        language: "en".into(),
    }).await.unwrap();

    repo.set_embedding(id, &vec1536(0.0)).await.unwrap();
    repo.set_novelty_novel(id).await.unwrap();

    let pending = repo.pending_for_article(article_id).await.unwrap();
    assert!(pending.is_empty(), "no longer pending after set_novelty_novel");
}

#[tokio::test]
async fn ann_search_returns_nearest_first() {
    let db = common::fresh_db().await;
    let repo = ClaimRepo::new(db.pool.clone());
    let article_id = seed_article(&db.pool).await;

    let near = repo.insert(NewClaim {
        article_id, text: "near".into(), original_quote: "near".into(),
        quote_offset_start: 0, quote_offset_end: 4,
        kind: ClaimKind::Fact, language: "en".into(),
    }).await.unwrap();
    let far  = repo.insert(NewClaim {
        article_id, text: "far".into(), original_quote: "far".into(),
        quote_offset_start: 4, quote_offset_end: 7,
        kind: ClaimKind::Fact, language: "en".into(),
    }).await.unwrap();
    repo.set_embedding(near, &vec1536(0.0)).await.unwrap();
    repo.set_embedding(far,  &vec1536(1.0)).await.unwrap();
    repo.set_novelty_novel(near).await.unwrap();
    repo.set_novelty_novel(far).await.unwrap();

    let cands = repo.ann_search(&vec1536(0.0), ClaimKind::Fact, 5).await.unwrap();
    assert!(cands.len() >= 2);
    assert_eq!(cands[0].claim_id, near);
}
```

- [ ] **Step 2: Implement `ClaimRepo`**

Create `crates/scout-storage/src/claim_repo.rs`:

```rust
use crate::pool::PgPool;
use pgvector::Vector;
use scout_core::{Candidate, ClaimKind};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewClaim {
    pub article_id: Uuid,
    pub text: String,
    pub original_quote: String,
    pub quote_offset_start: i32,
    pub quote_offset_end: i32,
    pub kind: ClaimKind,
    pub language: String,
}

#[derive(Debug, Clone)]
pub struct PendingClaim {
    pub id: Uuid,
    pub text: String,
    pub kind: ClaimKind,
    pub language: String,
}

#[derive(Clone)]
pub struct ClaimRepo {
    pool: PgPool,
}

impl ClaimRepo {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn insert(&self, c: NewClaim) -> Result<Uuid, sqlx::Error> {
        let row = sqlx::query!(
            r#"INSERT INTO claims
               (article_id, text, original_quote, quote_offset_start, quote_offset_end, kind, language)
               VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id"#,
            c.article_id, c.text, c.original_quote,
            c.quote_offset_start, c.quote_offset_end, c.kind.as_db_str(), c.language
        ).fetch_one(&self.pool).await?;
        Ok(row.id)
    }

    pub async fn set_embedding(&self, id: Uuid, v: &[f32]) -> Result<(), sqlx::Error> {
        let vector = Vector::from(v.to_vec());
        sqlx::query("UPDATE claims SET embedding = $2 WHERE id = $1")
            .bind(id).bind(vector).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_novelty_novel(&self, id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query!("UPDATE claims SET novelty = 'novel' WHERE id = $1", id)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_novelty_duplicate(&self, id: Uuid, dup_of: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query!("UPDATE claims SET novelty = 'duplicate', dup_of = $2 WHERE id = $1", id, dup_of)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_novelty_supplement(&self, id: Uuid, supp: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query!("UPDATE claims SET novelty = 'supplement', supplement_of = $2 WHERE id = $1", id, supp)
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn pending_for_article(&self, article_id: Uuid) -> Result<Vec<PendingClaim>, sqlx::Error> {
        let rows = sqlx::query!(
            "SELECT id, text, kind, language FROM claims
             WHERE article_id = $1 AND novelty = 'pending'",
            article_id
        ).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().filter_map(|r| {
            ClaimKind::parse_db(&r.kind).map(|kind| PendingClaim {
                id: r.id, text: r.text, kind, language: r.language,
            })
        }).collect())
    }

    pub async fn ann_search(&self, vec: &[f32], kind: ClaimKind, k: i64) -> Result<Vec<Candidate>, sqlx::Error> {
        let query = Vector::from(vec.to_vec());
        let rows: Vec<(Uuid, String, String, f64)> = sqlx::query_as(
            "SELECT id, text, kind, 1.0 - (embedding <=> $1)::float8 AS similarity
             FROM claims
             WHERE kind = $2 AND novelty IN ('novel','supplement') AND embedding IS NOT NULL
             ORDER BY embedding <=> $1
             LIMIT $3"
        ).bind(query).bind(kind.as_db_str()).bind(k).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().filter_map(|(id, text, kind_s, sim)| {
            ClaimKind::parse_db(&kind_s).map(|kind| Candidate {
                claim_id: id, text, kind, similarity: sim as f32,
            })
        }).collect())
    }
}
```

- [ ] **Step 3: Update `lib.rs`**

Append:

```rust
pub mod claim_repo;
pub use claim_repo::{ClaimRepo, NewClaim, PendingClaim};
```

- [ ] **Step 4: Refresh sqlx metadata and run**

```bash
DATABASE_URL=postgres://scout:scout@localhost:5432/scout \
    cargo sqlx prepare --workspace -- --tests
cargo test -p scout-storage --test claim_repo_test
```

Expected: PASS — 2 tests.

- [ ] **Step 5: Commit**

```bash
git add .sqlx crates/scout-storage/src/claim_repo.rs crates/scout-storage/src/lib.rs crates/scout-storage/tests/claim_repo_test.rs
git commit -m "feat(storage): ClaimRepo with embedding write + ANN search"
```

---

## Task 8: EntityRepo + ClaimEntityRepo + FeedbackRepo

**Files:**
- Create: `crates/scout-storage/src/entity_repo.rs`
- Create: `crates/scout-storage/src/claim_entity_repo.rs`
- Create: `crates/scout-storage/src/feedback_repo.rs`
- Create: `crates/scout-storage/tests/entity_repo_test.rs`
- Create: `crates/scout-storage/tests/feedback_repo_test.rs`
- Modify: `crates/scout-storage/src/lib.rs`

- [ ] **Step 1: Write the failing entity test**

Create `crates/scout-storage/tests/entity_repo_test.rs`:

```rust
mod common;

use scout_storage::{ClaimEntityRepo, EntityRepo, NewEntity};
use uuid::Uuid;

#[tokio::test]
async fn upsert_by_canonical_returns_same_id_second_time() {
    let db = common::fresh_db().await;
    let repo = EntityRepo::new(db.pool.clone());
    let id1 = repo.upsert(NewEntity {
        canonical_name: "GPT-5".into(), kind: "product".into(),
        aliases: vec!["gpt5".into()],
    }).await.unwrap();
    let id2 = repo.upsert(NewEntity {
        canonical_name: "GPT-5".into(), kind: "product".into(),
        aliases: vec!["GPT 5".into()],
    }).await.unwrap();
    assert_eq!(id1, id2);
}

#[tokio::test]
async fn link_claim_to_entity_idempotent() {
    let db = common::fresh_db().await;
    let er = EntityRepo::new(db.pool.clone());
    let cer = ClaimEntityRepo::new(db.pool.clone());
    let eid = er.upsert(NewEntity {
        canonical_name: "Anthropic".into(), kind: "company".into(), aliases: vec![],
    }).await.unwrap();

    // Need a real claim to FK into. Seed minimally:
    let article_id: Uuid = sqlx::query_scalar(
        "INSERT INTO articles (url,title,source,language,content_md,content_hash,status)
         VALUES ('https://a.test/e','T','t','en','x',$1,'pending') RETURNING id"
    ).bind(b"hash".to_vec()).fetch_one(&db.pool).await.unwrap();
    let claim_id: Uuid = sqlx::query_scalar(
        "INSERT INTO claims (article_id,text,original_quote,quote_offset_start,quote_offset_end,kind,language)
         VALUES ($1,'t','t',0,1,'fact','en') RETURNING id"
    ).bind(article_id).fetch_one(&db.pool).await.unwrap();

    cer.link(claim_id, eid, "subject").await.unwrap();
    cer.link(claim_id, eid, "subject").await.unwrap();  // idempotent
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM claim_entities WHERE claim_id=$1 AND entity_id=$2"
    ).bind(claim_id).bind(eid).fetch_one(&db.pool).await.unwrap();
    assert_eq!(count, 1);
}
```

- [ ] **Step 2: Implement EntityRepo**

Create `crates/scout-storage/src/entity_repo.rs`:

```rust
use crate::pool::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewEntity {
    pub canonical_name: String,
    pub kind: String,
    pub aliases: Vec<String>,
}

#[derive(Clone)]
pub struct EntityRepo {
    pool: PgPool,
}

impl EntityRepo {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn upsert(&self, e: NewEntity) -> Result<Uuid, sqlx::Error> {
        let row = sqlx::query!(
            r#"INSERT INTO entities (canonical_name, kind, aliases)
               VALUES ($1, $2, $3)
               ON CONFLICT (canonical_name, kind) DO UPDATE
                 SET aliases = (
                   SELECT array_agg(DISTINCT a) FROM unnest(entities.aliases || EXCLUDED.aliases) a
                 )
               RETURNING id"#,
            e.canonical_name, e.kind, &e.aliases
        ).fetch_one(&self.pool).await?;
        Ok(row.id)
    }

    pub async fn find_by_alias(&self, name: &str) -> Result<Option<Uuid>, sqlx::Error> {
        let row = sqlx::query!(
            r#"SELECT id FROM entities
               WHERE canonical_name = $1 OR $1 = ANY(aliases)
               LIMIT 1"#,
            name
        ).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| r.id))
    }
}
```

- [ ] **Step 3: Implement ClaimEntityRepo**

Create `crates/scout-storage/src/claim_entity_repo.rs`:

```rust
use crate::pool::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct ClaimEntityRepo {
    pool: PgPool,
}

impl ClaimEntityRepo {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn link(&self, claim_id: Uuid, entity_id: Uuid, role: &str) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO claim_entities (claim_id, entity_id, role)
             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            claim_id, entity_id, role
        ).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn entities_for_claim(&self, claim_id: Uuid) -> Result<Vec<(Uuid, String)>, sqlx::Error> {
        let rows = sqlx::query!(
            "SELECT entity_id, role FROM claim_entities WHERE claim_id = $1",
            claim_id
        ).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| (r.entity_id, r.role)).collect())
    }
}
```

- [ ] **Step 4: Implement FeedbackRepo + test**

Create `crates/scout-storage/src/feedback_repo.rs`:

```rust
use crate::pool::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NewFeedback {
    pub claim_id: Option<Uuid>,
    pub entity_id: Option<Uuid>,
    pub action: String,
    pub target_id: Option<Uuid>,
    pub note: Option<String>,
}

#[derive(Clone)]
pub struct FeedbackRepo {
    pool: PgPool,
}

impl FeedbackRepo {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn insert(&self, f: NewFeedback) -> Result<Uuid, sqlx::Error> {
        let row = sqlx::query!(
            r#"INSERT INTO feedback (claim_id, entity_id, action, target_id, note)
               VALUES ($1,$2,$3,$4,$5) RETURNING id"#,
            f.claim_id, f.entity_id, f.action, f.target_id, f.note
        ).fetch_one(&self.pool).await?;
        Ok(row.id)
    }
}
```

Create `crates/scout-storage/tests/feedback_repo_test.rs`:

```rust
mod common;

use scout_storage::{FeedbackRepo, NewFeedback};

#[tokio::test]
async fn insert_feedback_returns_id() {
    let db = common::fresh_db().await;
    let repo = FeedbackRepo::new(db.pool.clone());
    let id = repo.insert(NewFeedback {
        claim_id: None, entity_id: None,
        action: "mark_duplicate".into(),
        target_id: None,
        note: Some("test note".into()),
    }).await.unwrap();
    assert!(!id.is_nil());
}
```

- [ ] **Step 5: Update `lib.rs`**

Append:

```rust
pub mod claim_entity_repo;
pub mod entity_repo;
pub mod feedback_repo;

pub use claim_entity_repo::ClaimEntityRepo;
pub use entity_repo::{EntityRepo, NewEntity};
pub use feedback_repo::{FeedbackRepo, NewFeedback};
```

- [ ] **Step 6: Refresh sqlx + test**

```bash
DATABASE_URL=postgres://scout:scout@localhost:5432/scout \
    cargo sqlx prepare --workspace -- --tests
cargo test -p scout-storage --test entity_repo_test --test feedback_repo_test
```

Expected: PASS — 3 tests across the two files.

- [ ] **Step 7: Commit**

```bash
git add .sqlx crates/scout-storage
git commit -m "feat(storage): EntityRepo, ClaimEntityRepo, FeedbackRepo"
```

---

## Task 9: PgJobQueue

**Files:**
- Create: `crates/scout-storage/src/job_queue.rs`
- Create: `crates/scout-storage/tests/job_queue_test.rs`
- Modify: `crates/scout-storage/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/scout-storage/tests/job_queue_test.rs`:

```rust
mod common;

use scout_storage::{NewJob, PgJobQueue};
use serde_json::json;

#[tokio::test]
async fn enqueue_then_dequeue_returns_same_payload() {
    let db = common::fresh_db().await;
    let q = PgJobQueue::new(db.pool.clone());
    let id = q.enqueue(NewJob {
        kind: "extract".into(),
        payload: json!({"article_id": "abc"}),
    }).await.unwrap();
    let job = q.dequeue(&["extract"]).await.unwrap().expect("a job");
    assert_eq!(job.id, id);
    assert_eq!(job.kind, "extract");
    assert_eq!(job.payload["article_id"], "abc");
}

#[tokio::test]
async fn second_concurrent_dequeue_returns_none() {
    let db = common::fresh_db().await;
    let q = PgJobQueue::new(db.pool.clone());
    q.enqueue(NewJob { kind: "extract".into(), payload: json!({}) }).await.unwrap();
    let _first = q.dequeue(&["extract"]).await.unwrap().unwrap();
    let second = q.dequeue(&["extract"]).await.unwrap();
    assert!(second.is_none(), "second dequeue should see no pending job");
}

#[tokio::test]
async fn fail_with_attempts_left_reschedules() {
    let db = common::fresh_db().await;
    let q = PgJobQueue::new(db.pool.clone());
    let id = q.enqueue(NewJob { kind: "extract".into(), payload: json!({}) }).await.unwrap();
    let job = q.dequeue(&["extract"]).await.unwrap().unwrap();
    q.fail(job.id, "boom", true).await.unwrap();
    let row: (String, i32) = sqlx::query_as(
        "SELECT status, attempts FROM jobs WHERE id = $1"
    ).bind(id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(row.0, "pending");
    assert_eq!(row.1, 1);
}
```

- [ ] **Step 2: Implement `PgJobQueue`**

Create `crates/scout-storage/src/job_queue.rs`:

```rust
use crate::pool::PgPool;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct NewJob {
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub kind: String,
    pub payload: Value,
    pub attempts: i32,
}

#[derive(Clone)]
pub struct PgJobQueue {
    pool: PgPool,
}

impl PgJobQueue {
    pub fn new(pool: PgPool) -> Self { Self { pool } }

    pub async fn enqueue(&self, j: NewJob) -> Result<i64, sqlx::Error> {
        let row = sqlx::query!(
            "INSERT INTO jobs (kind, payload) VALUES ($1, $2) RETURNING id",
            j.kind, j.payload
        ).fetch_one(&self.pool).await?;
        Ok(row.id)
    }

    pub async fn dequeue(&self, kinds: &[&str]) -> Result<Option<Job>, sqlx::Error> {
        let kinds_vec: Vec<String> = kinds.iter().map(|s| s.to_string()).collect();
        let row = sqlx::query!(
            r#"UPDATE jobs SET status = 'running', picked_at = now()
               WHERE id = (
                 SELECT id FROM jobs
                 WHERE status = 'pending' AND scheduled_at <= now() AND kind = ANY($1)
                 ORDER BY scheduled_at
                 FOR UPDATE SKIP LOCKED LIMIT 1
               )
               RETURNING id, kind, payload, attempts"#,
            &kinds_vec
        ).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| Job { id: r.id, kind: r.kind, payload: r.payload, attempts: r.attempts }))
    }

    pub async fn ack(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE jobs SET status = 'done', finished_at = now() WHERE id = $1", id
        ).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn fail(&self, id: i64, err: &str, retry: bool) -> Result<(), sqlx::Error> {
        if retry {
            sqlx::query!(
                r#"UPDATE jobs SET
                     attempts = attempts + 1,
                     status = CASE
                       WHEN attempts + 1 >= max_attempts THEN 'failed'
                       ELSE 'pending' END,
                     scheduled_at = now() + (power(2, attempts) * interval '1 minute'),
                     error = $2
                   WHERE id = $1"#,
                id, err
            ).execute(&self.pool).await?;
        } else {
            sqlx::query!(
                "UPDATE jobs SET status = 'failed', finished_at = now(), error = $2 WHERE id = $1",
                id, err
            ).execute(&self.pool).await?;
        }
        Ok(())
    }
}
```

- [ ] **Step 3: Update `lib.rs`**

Append:

```rust
pub mod job_queue;
pub use job_queue::{Job, NewJob, PgJobQueue};
```

- [ ] **Step 4: Refresh sqlx + run**

```bash
DATABASE_URL=postgres://scout:scout@localhost:5432/scout \
    cargo sqlx prepare --workspace -- --tests
cargo test -p scout-storage --test job_queue_test
```

Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add .sqlx crates/scout-storage
git commit -m "feat(storage): PgJobQueue with SKIP LOCKED + exponential-backoff retry"
```

---

## Task 10: ThresholdJudge (NoveltyJudge MVP)

**Files:**
- Create: `crates/scout-pipeline/Cargo.toml`
- Create: `crates/scout-pipeline/src/lib.rs`
- Create: `crates/scout-pipeline/src/novelty_judge.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Add crate to workspace**

Modify root `Cargo.toml` — add `"crates/scout-pipeline",` to `workspace.members`.

- [ ] **Step 2: Write `crates/scout-pipeline/Cargo.toml`**

```toml
[package]
name = "scout-pipeline"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
scout-core = { path = "../scout-core" }
anyhow.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_json.workspace = true
async-trait.workspace = true
tracing.workspace = true
uuid.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt"] }
```

- [ ] **Step 3: Write failing tests for ThresholdJudge**

Create `crates/scout-pipeline/src/novelty_judge.rs`:

```rust
use scout_core::{Candidate, ClaimDraft, ClaimKind, Novelty, NoveltyJudge};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Thresholds {
    pub fact_duplicate_threshold: f32,
    pub fact_supplement_low: f32,
    pub opinion_duplicate_threshold: f32,
    pub opinion_supplement_low: f32,
    pub methodology_duplicate_threshold: f32,
    pub methodology_supplement_low: f32,
}

impl Thresholds {
    pub fn defaults() -> Self {
        Self {
            fact_duplicate_threshold: 0.92,
            fact_supplement_low: 0.85,
            opinion_duplicate_threshold: 0.96,
            opinion_supplement_low: 0.88,
            methodology_duplicate_threshold: 0.93,
            methodology_supplement_low: 0.86,
        }
    }

    fn dup_threshold(&self, k: ClaimKind) -> f32 {
        match k {
            ClaimKind::Fact | ClaimKind::Data => self.fact_duplicate_threshold,
            ClaimKind::Opinion | ClaimKind::Prediction => self.opinion_duplicate_threshold,
            ClaimKind::Methodology => self.methodology_duplicate_threshold,
        }
    }

    fn supp_low(&self, k: ClaimKind) -> f32 {
        match k {
            ClaimKind::Fact | ClaimKind::Data => self.fact_supplement_low,
            ClaimKind::Opinion | ClaimKind::Prediction => self.opinion_supplement_low,
            ClaimKind::Methodology => self.methodology_supplement_low,
        }
    }
}

pub struct ThresholdJudge {
    thresholds: Thresholds,
}

impl ThresholdJudge {
    pub fn new(thresholds: Thresholds) -> Self { Self { thresholds } }
}

impl NoveltyJudge for ThresholdJudge {
    fn judge(&self, claim: &ClaimDraft, candidates: &[Candidate]) -> Novelty {
        let dup_t  = self.thresholds.dup_threshold(claim.kind);
        let supp_l = self.thresholds.supp_low(claim.kind);

        if let Some(best) = candidates.iter().max_by(|a, b| a.similarity.total_cmp(&b.similarity)) {
            if best.similarity >= dup_t {
                return Novelty::Duplicate { dup_of: best.claim_id };
            }
            if best.similarity >= supp_l && has_new_entity_or_number(&claim.text, &best.text) {
                return Novelty::Supplement { supplement_of: best.claim_id };
            }
        }
        Novelty::Novel
    }
}

/// Heuristic: are there capitalized tokens or numeric tokens in `new` that aren't in `existing`?
fn has_new_entity_or_number(new: &str, existing: &str) -> bool {
    let existing_lower = existing.to_lowercase();
    new.split(|c: char| !c.is_alphanumeric() && c != '%')
        .any(|tok| {
            if tok.is_empty() { return false; }
            let is_entity = tok.chars().next().map(|c| c.is_ascii_uppercase()).unwrap_or(false);
            let is_number = tok.chars().any(|c| c.is_ascii_digit());
            (is_entity || is_number) && !existing_lower.contains(&tok.to_lowercase())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn draft(text: &str, kind: ClaimKind) -> ClaimDraft {
        ClaimDraft {
            id: Uuid::new_v4(),
            article_id: Uuid::new_v4(),
            text: text.into(),
            kind,
            language: "en".into(),
            embedding: None,
        }
    }

    fn cand(text: &str, sim: f32, kind: ClaimKind) -> Candidate {
        Candidate {
            claim_id: Uuid::new_v4(),
            text: text.into(),
            kind,
            similarity: sim,
        }
    }

    #[test]
    fn no_candidates_means_novel() {
        let j = ThresholdJudge::new(Thresholds::defaults());
        let n = j.judge(&draft("hello", ClaimKind::Fact), &[]);
        assert!(matches!(n, Novelty::Novel));
    }

    #[test]
    fn fact_above_dup_threshold_is_duplicate() {
        let j = ThresholdJudge::new(Thresholds::defaults());
        let c = cand("same idea", 0.95, ClaimKind::Fact);
        let id = c.claim_id;
        let n = j.judge(&draft("same idea", ClaimKind::Fact), &[c]);
        assert_eq!(n.dup_of(), Some(id));
    }

    #[test]
    fn opinion_at_0_94_is_not_duplicate() {
        let j = ThresholdJudge::new(Thresholds::defaults());
        let c = cand("opinion x", 0.94, ClaimKind::Opinion);
        let n = j.judge(&draft("opinion x", ClaimKind::Opinion), &[c]);
        assert!(matches!(n, Novelty::Novel | Novelty::Supplement { .. }));
        assert!(n.dup_of().is_none());
    }

    #[test]
    fn supplement_when_in_window_with_new_entity_or_number() {
        let j = ThresholdJudge::new(Thresholds::defaults());
        let c = cand("GPT-4 hits 80 on bench", 0.88, ClaimKind::Fact);
        let n = j.judge(&draft("GPT-5 hits 87 on bench", ClaimKind::Fact), &[c]);
        assert!(matches!(n, Novelty::Supplement { .. }));
    }

    #[test]
    fn no_supplement_when_window_but_no_new_tokens() {
        let j = ThresholdJudge::new(Thresholds::defaults());
        let c = cand("same boring claim", 0.88, ClaimKind::Fact);
        let n = j.judge(&draft("same boring claim restated", ClaimKind::Fact), &[c]);
        assert!(matches!(n, Novelty::Novel));
    }
}
```

- [ ] **Step 4: Write `lib.rs`**

Create `crates/scout-pipeline/src/lib.rs`:

```rust
//! Pipeline implementations: ClaimExtractor, NoveltyJudge, EntityResolver.

pub mod novelty_judge;

pub use novelty_judge::{ThresholdJudge, Thresholds};
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-pipeline`
Expected: PASS — 5 tests.

```bash
git add Cargo.toml crates/scout-pipeline
git commit -m "feat(pipeline): ThresholdJudge with per-kind cosine thresholds + supplement heuristic"
```

---

## Task 11: scout-llm crate + Mock implementations

**Files:**
- Create: `crates/scout-llm/Cargo.toml`
- Create: `crates/scout-llm/src/lib.rs`
- Create: `crates/scout-llm/src/mock.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Add to workspace**

Modify root `Cargo.toml` — append `"crates/scout-llm",` to `workspace.members` and add:

```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
wiremock = "0.6"
```

- [ ] **Step 2: Write `crates/scout-llm/Cargo.toml`**

```toml
[package]
name = "scout-llm"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
scout-core = { path = "../scout-core" }
async-trait.workspace = true
anyhow.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_json.workspace = true
reqwest.workspace = true
tracing.workspace = true
tokio = { workspace = true, features = ["sync"] }

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt"] }
wiremock.workspace = true
```

- [ ] **Step 3: Write failing tests for MockLlmClient and MockEmbedder**

Create `crates/scout-llm/src/mock.rs`:

```rust
use async_trait::async_trait;
use scout_core::{
    ChatRequest, ChatResponse, CoreError, CoreResult, Embedder, LlmClient,
};
use std::sync::Mutex;

/// Pattern-matching mock: maps a substring in the joined-message text to a canned response.
pub struct MockLlmClient {
    rules: Vec<(String, String)>,
    calls: Mutex<Vec<ChatRequest>>,
}

impl MockLlmClient {
    pub fn new() -> Self { Self { rules: Vec::new(), calls: Mutex::new(Vec::new()) } }
    pub fn with_rule(mut self, contains: &str, response: &str) -> Self {
        self.rules.push((contains.to_string(), response.to_string()));
        self
    }
    pub fn calls(&self) -> Vec<ChatRequest> { self.calls.lock().unwrap().clone() }
}

#[async_trait]
impl LlmClient for MockLlmClient {
    async fn complete(&self, req: ChatRequest) -> CoreResult<ChatResponse> {
        self.calls.lock().unwrap().push(req.clone());
        let body: String = req.messages.iter().map(|m| m.content.as_str()).collect::<Vec<_>>().join("\n");
        for (needle, resp) in &self.rules {
            if body.contains(needle) {
                return Ok(ChatResponse { text: resp.clone(), input_tokens: 0, output_tokens: 0 });
            }
        }
        Err(CoreError::Provider(format!(
            "MockLlmClient: no rule matched. Body was:\n{body}"
        )))
    }
}

/// Deterministic embedder: hashes text into a vector seeded by char codes.
pub struct MockEmbedder { pub dim: usize }

#[async_trait]
impl Embedder for MockEmbedder {
    fn dim(&self) -> usize { self.dim }
    async fn embed(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| hash_to_vec(t, self.dim)).collect())
    }
}

fn hash_to_vec(s: &str, dim: usize) -> Vec<f32> {
    let mut acc: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        acc ^= b as u64;
        acc = acc.wrapping_mul(0x100000001b3);
    }
    let seed = (acc as f64 / u64::MAX as f64) as f32;
    (0..dim).map(|i| ((i as f32 * 0.0001).sin() + seed) * 0.01).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use scout_core::ChatMessage;

    #[tokio::test]
    async fn mock_llm_returns_matching_response() {
        let m = MockLlmClient::new().with_rule("extract", "OK");
        let r = m.complete(ChatRequest {
            system: None,
            messages: vec![ChatMessage { role: "user".into(), content: "please extract".into() }],
            max_tokens: 100, temperature: 0.0,
        }).await.unwrap();
        assert_eq!(r.text, "OK");
    }

    #[tokio::test]
    async fn mock_llm_records_calls() {
        let m = MockLlmClient::new().with_rule("hi", "yo");
        m.complete(ChatRequest {
            system: None,
            messages: vec![ChatMessage { role: "user".into(), content: "hi".into() }],
            max_tokens: 1, temperature: 0.0,
        }).await.unwrap();
        assert_eq!(m.calls().len(), 1);
    }

    #[tokio::test]
    async fn mock_embedder_is_deterministic() {
        let e = MockEmbedder { dim: 1536 };
        let a = e.embed(&["hello"]).await.unwrap();
        let b = e.embed(&["hello"]).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(a[0].len(), 1536);
    }
}
```

- [ ] **Step 4: Write `lib.rs`**

Create `crates/scout-llm/src/lib.rs`:

```rust
//! LLM and embedder implementations.

pub mod mock;
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-llm`
Expected: PASS — 3 tests.

```bash
git add Cargo.toml crates/scout-llm
git commit -m "feat(llm): MockLlmClient (pattern-matched) + MockEmbedder (deterministic) for tests"
```

---

## Task 12: LlmClaimExtractor

**Files:**
- Create: `crates/scout-pipeline/src/prompts.rs`
- Create: `crates/scout-pipeline/src/claim_extractor.rs`
- Modify: `crates/scout-pipeline/src/lib.rs`
- Modify: `crates/scout-pipeline/Cargo.toml` (add `scout-llm` to dev-deps)

- [ ] **Step 1: Add dev-dep**

Modify `crates/scout-pipeline/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt"] }
scout-llm = { path = "../scout-llm" }
```

- [ ] **Step 2: Write the prompt module**

Create `crates/scout-pipeline/src/prompts.rs`:

```rust
//! Prompt templates. Keep flat — one function per task.

pub const EXTRACT_SYSTEM: &str = include_str!("../prompts/extract_system.md");

pub fn extract_user(title: &str, language: &str, content_md: &str) -> String {
    format!(
        "Article title: {title}\n\
         Detected language: {language}\n\n\
         Article body (markdown):\n---\n{content_md}\n---\n\n\
         Return JSON only, no prose."
    )
}

pub const RESOLVE_ENTITIES_SYSTEM: &str = include_str!("../prompts/resolve_entities_system.md");

pub fn resolve_entities_user(claim_text: &str, hints_json: &str) -> String {
    format!(
        "Claim: {claim_text}\n\
         Existing entity hints (JSON): {hints_json}\n\n\
         Return JSON only."
    )
}
```

Create `crates/scout-pipeline/prompts/extract_system.md`:

```markdown
You extract atomic claims from articles for a personal information-filtering tool.

Output a JSON array. Each element has this shape exactly:

{
  "text": "<canonical paraphrase, one sentence, neutral tense>",
  "original_quote": "<verbatim substring of the article body>",
  "quote_offset_start": <int char offset into the body where original_quote begins>,
  "quote_offset_end":   <int char offset into the body where original_quote ends, exclusive>,
  "kind": "fact" | "opinion" | "data" | "prediction" | "methodology",
  "language": "<bcp47 like en or zh>",
  "entity_hints": [
    { "name": "<surface form>", "kind": "person|company|product|concept|place|other",
      "aliases": ["<other surface form>", ...] }
  ]
}

Rules:
- One claim per element. Do NOT combine independent facts.
- "fact" = verifiable about the world. "data" = a number/statistic. "opinion" = author's view.
  "prediction" = forward-looking statement. "methodology" = how-to / technique.
- "original_quote" MUST be a substring of the body. Offsets MUST be correct.
- Return ONLY the JSON array. No prose, no markdown fence.
```

Create `crates/scout-pipeline/prompts/resolve_entities_system.md`:

```markdown
You normalize entity references in a claim.

Input: a claim and a list of hint entities the extractor proposed.

Output: a JSON array, one element per resolved entity:
{
  "name": "<canonical name>",
  "kind": "person|company|product|concept|place|other",
  "aliases": ["<surface form>", ...],
  "role":  "subject" | "object" | "mention"
}

Rules:
- Pick the most canonical, broadly-known form (e.g. "OpenAI" not "Open AI Inc.").
- Aliases include the surface forms seen in the claim.
- Role: subject = the actor; object = the target; mention = passing reference.
- Return ONLY the JSON array.
```

- [ ] **Step 3: Write the failing test**

Create `crates/scout-pipeline/src/claim_extractor.rs`:

```rust
use crate::prompts;
use async_trait::async_trait;
use scout_core::{
    Article, ChatMessage, ChatRequest, ClaimExtractor, ClaimKind, CoreError, CoreResult,
    EntityHint, LlmClient, RawClaim,
};
use serde::Deserialize;
use std::sync::Arc;

pub struct LlmClaimExtractor {
    llm: Arc<dyn LlmClient>,
    max_tokens: u32,
}

impl LlmClaimExtractor {
    pub fn new(llm: Arc<dyn LlmClient>) -> Self { Self { llm, max_tokens: 4096 } }
}

#[derive(Debug, Deserialize)]
struct WireClaim {
    text: String,
    original_quote: String,
    quote_offset_start: i32,
    quote_offset_end: i32,
    kind: String,
    language: String,
    #[serde(default)]
    entity_hints: Vec<WireHint>,
}

#[derive(Debug, Deserialize)]
struct WireHint {
    name: String,
    kind: String,
    #[serde(default)]
    aliases: Vec<String>,
}

#[async_trait]
impl ClaimExtractor for LlmClaimExtractor {
    async fn extract(&self, article: &Article) -> CoreResult<Vec<RawClaim>> {
        let resp = self.llm.complete(ChatRequest {
            system: Some(prompts::EXTRACT_SYSTEM.into()),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: prompts::extract_user(&article.title, &article.language, &article.content_md),
            }],
            max_tokens: self.max_tokens,
            temperature: 0.0,
        }).await?;
        let wire: Vec<WireClaim> = serde_json::from_str(resp.text.trim())
            .map_err(|e| CoreError::Provider(format!("extractor JSON parse: {e}; payload: {}", resp.text)))?;
        Ok(wire.into_iter().filter_map(|w| {
            let kind = ClaimKind::parse_db(&w.kind)?;
            Some(RawClaim {
                text: w.text,
                original_quote: w.original_quote,
                quote_offset_start: w.quote_offset_start,
                quote_offset_end: w.quote_offset_end,
                kind,
                language: w.language,
                entity_hints: w.entity_hints.into_iter().map(|h| EntityHint {
                    name: h.name, kind: h.kind, aliases: h.aliases,
                }).collect(),
            })
        }).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use scout_llm::mock::MockLlmClient;
    use std::sync::Arc;
    use uuid::Uuid;

    fn article() -> Article {
        Article {
            id: Uuid::new_v4(),
            url: "https://a.test/x".into(),
            title: "A short read".into(),
            author: None,
            source: "test".into(),
            language: "en".into(),
            content_md: "GPT-5 scored 87 on ARC-AGI.".into(),
            content_hash: vec![0; 32],
            ingested_at: Utc::now(),
            processed_at: None,
            status: "pending".into(),
        }
    }

    #[tokio::test]
    async fn parses_well_formed_json_response() {
        let canned = r#"[{
            "text": "GPT-5 scored 87 on ARC-AGI",
            "original_quote": "GPT-5 scored 87 on ARC-AGI.",
            "quote_offset_start": 0,
            "quote_offset_end": 28,
            "kind": "data",
            "language": "en",
            "entity_hints": [
                {"name":"GPT-5","kind":"product","aliases":[]},
                {"name":"ARC-AGI","kind":"concept","aliases":[]}
            ]
        }]"#;
        let mock = Arc::new(MockLlmClient::new().with_rule("Article title", canned));
        let ex = LlmClaimExtractor::new(mock);
        let claims = ex.extract(&article()).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].kind, ClaimKind::Data);
        assert_eq!(claims[0].entity_hints.len(), 2);
    }

    #[tokio::test]
    async fn surfaces_parse_error_with_payload() {
        let mock = Arc::new(MockLlmClient::new().with_rule("Article title", "not json"));
        let ex = LlmClaimExtractor::new(mock);
        let err = ex.extract(&article()).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("extractor JSON parse"));
    }
}
```

- [ ] **Step 4: Update `lib.rs`**

Modify `crates/scout-pipeline/src/lib.rs`:

```rust
//! Pipeline implementations.

pub mod claim_extractor;
pub mod novelty_judge;
pub mod prompts;

pub use claim_extractor::LlmClaimExtractor;
pub use novelty_judge::{ThresholdJudge, Thresholds};
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-pipeline`
Expected: PASS — 7 tests (5 from judge + 2 new).

```bash
git add crates/scout-pipeline
git commit -m "feat(pipeline): LlmClaimExtractor with single-pass structured-output prompt"
```

---

## Task 13: LlmEntityResolver

**Files:**
- Create: `crates/scout-pipeline/src/entity_resolver.rs`
- Modify: `crates/scout-pipeline/src/lib.rs`

- [ ] **Step 1: Write the failing test + implementation in one file**

Create `crates/scout-pipeline/src/entity_resolver.rs`:

```rust
use crate::prompts;
use async_trait::async_trait;
use scout_core::{
    ChatMessage, ChatRequest, ClaimDraft, CoreError, CoreResult, EntityHint, EntityRef,
    EntityResolver, LlmClient,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

/// Trait for the storage hook the resolver needs: upsert entity → return id.
#[async_trait]
pub trait EntityStore: Send + Sync {
    async fn find_by_alias(&self, alias: &str) -> anyhow::Result<Option<Uuid>>;
    async fn upsert(&self, name: &str, kind: &str, aliases: &[String]) -> anyhow::Result<Uuid>;
}

pub struct LlmEntityResolver {
    llm: Arc<dyn LlmClient>,
    store: Arc<dyn EntityStore>,
    max_tokens: u32,
}

impl LlmEntityResolver {
    pub fn new(llm: Arc<dyn LlmClient>, store: Arc<dyn EntityStore>) -> Self {
        Self { llm, store, max_tokens: 1024 }
    }
}

#[derive(Debug, Deserialize)]
struct WireResolved {
    name: String,
    kind: String,
    #[serde(default)]
    aliases: Vec<String>,
    role: String,
}

#[async_trait]
impl EntityResolver for LlmEntityResolver {
    async fn resolve(&self, claim: &ClaimDraft, hints: &[EntityHint]) -> CoreResult<Vec<EntityRef>> {
        let hints_json = serde_json::to_string(hints)
            .map_err(|e| CoreError::Provider(format!("hint serialize: {e}")))?;
        let resp = self.llm.complete(ChatRequest {
            system: Some(prompts::RESOLVE_ENTITIES_SYSTEM.into()),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: prompts::resolve_entities_user(&claim.text, &hints_json),
            }],
            max_tokens: self.max_tokens,
            temperature: 0.0,
        }).await?;
        let wire: Vec<WireResolved> = serde_json::from_str(resp.text.trim())
            .map_err(|e| CoreError::Provider(format!("resolver JSON parse: {e}; payload: {}", resp.text)))?;

        let mut out = Vec::with_capacity(wire.len());
        for w in wire {
            let id = match self.store.find_by_alias(&w.name).await
                .map_err(|e| CoreError::Provider(format!("entity lookup: {e}")))?
            {
                Some(id) => id,
                None => self.store.upsert(&w.name, &w.kind, &w.aliases).await
                    .map_err(|e| CoreError::Provider(format!("entity upsert: {e}")))?,
            };
            out.push(EntityRef { entity_id: id, role: w.role });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scout_core::ClaimKind;
    use scout_llm::mock::MockLlmClient;
    use std::sync::Mutex;

    struct FakeStore { upserts: Mutex<Vec<(String, String)>> }

    #[async_trait]
    impl EntityStore for FakeStore {
        async fn find_by_alias(&self, _: &str) -> anyhow::Result<Option<Uuid>> { Ok(None) }
        async fn upsert(&self, name: &str, kind: &str, _: &[String]) -> anyhow::Result<Uuid> {
            self.upserts.lock().unwrap().push((name.into(), kind.into()));
            Ok(Uuid::new_v4())
        }
    }

    fn draft() -> ClaimDraft {
        ClaimDraft {
            id: Uuid::new_v4(),
            article_id: Uuid::new_v4(),
            text: "OpenAI announced GPT-5".into(),
            kind: ClaimKind::Fact,
            language: "en".into(),
            embedding: None,
        }
    }

    #[tokio::test]
    async fn upserts_all_resolved_entities_and_returns_refs() {
        let canned = r#"[
            {"name":"OpenAI","kind":"company","aliases":["Open AI"],"role":"subject"},
            {"name":"GPT-5","kind":"product","aliases":[],"role":"object"}
        ]"#;
        let llm = Arc::new(MockLlmClient::new().with_rule("Claim:", canned));
        let store = Arc::new(FakeStore { upserts: Mutex::new(Vec::new()) });
        let r = LlmEntityResolver::new(llm, store.clone());
        let refs = r.resolve(&draft(), &[]).await.unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(store.upserts.lock().unwrap().len(), 2);
        assert_eq!(refs[0].role, "subject");
    }
}
```

- [ ] **Step 2: Update `lib.rs`**

```rust
//! Pipeline implementations.

pub mod claim_extractor;
pub mod entity_resolver;
pub mod novelty_judge;
pub mod prompts;

pub use claim_extractor::LlmClaimExtractor;
pub use entity_resolver::{EntityStore, LlmEntityResolver};
pub use novelty_judge::{ThresholdJudge, Thresholds};
```

- [ ] **Step 3: Run + commit**

Run: `cargo test -p scout-pipeline`
Expected: PASS — 8 tests (7 prior + 1).

```bash
git add crates/scout-pipeline
git commit -m "feat(pipeline): LlmEntityResolver with EntityStore hook + alias lookup"
```

---

## Task 14: AnthropicClient

**Files:**
- Create: `crates/scout-llm/src/anthropic.rs`
- Modify: `crates/scout-llm/src/lib.rs`

- [ ] **Step 1: Write the failing wiremock-backed test**

Create `crates/scout-llm/src/anthropic.rs`:

```rust
use async_trait::async_trait;
use reqwest::Client;
use scout_core::{
    ChatRequest, ChatResponse, CoreError, CoreResult, LlmClient,
};
use serde::{Deserialize, Serialize};

const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_BASE: &str = "https://api.anthropic.com";

pub struct AnthropicClient {
    http: Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl AnthropicClient {
    pub fn new(api_key: String, model: String) -> Self {
        Self { http: Client::new(), api_key, base_url: DEFAULT_BASE.into(), model }
    }
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url; self
    }
}

#[derive(Serialize)]
struct WireMessage<'a> { role: &'a str, content: &'a str }

#[derive(Serialize)]
struct WireRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    temperature: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<&'a str>,
    messages: Vec<WireMessage<'a>>,
}

#[derive(Deserialize)]
struct WireContent { #[serde(rename = "type")] _ty: String, text: String }

#[derive(Deserialize)]
struct WireUsage { input_tokens: u32, output_tokens: u32 }

#[derive(Deserialize)]
struct WireResponse { content: Vec<WireContent>, usage: WireUsage }

#[async_trait]
impl LlmClient for AnthropicClient {
    async fn complete(&self, req: ChatRequest) -> CoreResult<ChatResponse> {
        let body = WireRequest {
            model: &self.model,
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            system: req.system.as_deref(),
            messages: req.messages.iter().map(|m| WireMessage {
                role: &m.role, content: &m.content
            }).collect(),
        };
        let resp = self.http
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&body)
            .send().await
            .map_err(|e| CoreError::Provider(format!("anthropic request: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(CoreError::Provider(format!("anthropic {status}: {text}")));
        }
        let wire: WireResponse = resp.json().await
            .map_err(|e| CoreError::Provider(format!("anthropic decode: {e}")))?;
        let text = wire.content.into_iter().map(|c| c.text).collect::<Vec<_>>().join("\n");
        Ok(ChatResponse {
            text,
            input_tokens: wire.usage.input_tokens,
            output_tokens: wire.usage.output_tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scout_core::ChatMessage;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn anthropic_post_with_headers_and_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .and(header("anthropic-version", ANTHROPIC_VERSION))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{"type": "text", "text": "hello world"}],
                "usage": {"input_tokens": 10, "output_tokens": 5}
            })))
            .mount(&server).await;

        let client = AnthropicClient::new("sk-test".into(), "claude-sonnet-4-6".into())
            .with_base_url(server.uri());
        let r = client.complete(ChatRequest {
            system: Some("be terse".into()),
            messages: vec![ChatMessage { role: "user".into(), content: "hi".into() }],
            max_tokens: 32, temperature: 0.0,
        }).await.unwrap();
        assert_eq!(r.text, "hello world");
        assert_eq!(r.input_tokens, 10);
        assert_eq!(r.output_tokens, 5);
    }

    #[tokio::test]
    async fn anthropic_surfaces_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
            .mount(&server).await;

        let client = AnthropicClient::new("sk-test".into(), "claude-sonnet-4-6".into())
            .with_base_url(server.uri());
        let err = client.complete(ChatRequest {
            system: None,
            messages: vec![ChatMessage { role: "user".into(), content: "hi".into() }],
            max_tokens: 1, temperature: 0.0,
        }).await.unwrap_err();
        assert!(format!("{err}").contains("429"));
    }
}
```

- [ ] **Step 2: Update `lib.rs`**

Modify `crates/scout-llm/src/lib.rs`:

```rust
//! LLM and embedder implementations.

pub mod anthropic;
pub mod mock;

pub use anthropic::AnthropicClient;
```

- [ ] **Step 3: Run + commit**

Run: `cargo test -p scout-llm`
Expected: PASS — 5 tests total.

```bash
git add crates/scout-llm
git commit -m "feat(llm): AnthropicClient using /v1/messages with wiremock contract tests"
```

---

## Task 15: OpenAiClient + OpenAiEmbedder

**Files:**
- Create: `crates/scout-llm/src/openai.rs`
- Create: `crates/scout-llm/src/openai_embedder.rs`
- Modify: `crates/scout-llm/src/lib.rs`

- [ ] **Step 1: Write OpenAiClient with failing test**

Create `crates/scout-llm/src/openai.rs`:

```rust
use async_trait::async_trait;
use reqwest::Client;
use scout_core::{ChatRequest, ChatResponse, CoreError, CoreResult, LlmClient};
use serde::{Deserialize, Serialize};

const DEFAULT_BASE: &str = "https://api.openai.com";

pub struct OpenAiClient {
    http: Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl OpenAiClient {
    pub fn new(api_key: String, model: String) -> Self {
        Self { http: Client::new(), api_key, base_url: DEFAULT_BASE.into(), model }
    }
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url; self
    }
}

#[derive(Serialize)]
struct WireMsg<'a> { role: &'a str, content: &'a str }

#[derive(Serialize)]
struct WireReq<'a> {
    model: &'a str,
    max_tokens: u32,
    temperature: f32,
    messages: Vec<WireMsg<'a>>,
}

#[derive(Deserialize)]
struct WireChoice { message: WireRespMsg }
#[derive(Deserialize)]
struct WireRespMsg { content: String }
#[derive(Deserialize)]
struct WireUsage { prompt_tokens: u32, completion_tokens: u32 }
#[derive(Deserialize)]
struct WireResp { choices: Vec<WireChoice>, usage: WireUsage }

#[async_trait]
impl LlmClient for OpenAiClient {
    async fn complete(&self, req: ChatRequest) -> CoreResult<ChatResponse> {
        let mut msgs: Vec<WireMsg> = Vec::new();
        if let Some(s) = req.system.as_deref() {
            msgs.push(WireMsg { role: "system", content: s });
        }
        for m in &req.messages {
            msgs.push(WireMsg { role: &m.role, content: &m.content });
        }
        let body = WireReq {
            model: &self.model,
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            messages: msgs,
        };
        let resp = self.http
            .post(format!("{}/v1/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send().await
            .map_err(|e| CoreError::Provider(format!("openai request: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(CoreError::Provider(format!("openai {status}: {text}")));
        }
        let wire: WireResp = resp.json().await
            .map_err(|e| CoreError::Provider(format!("openai decode: {e}")))?;
        let text = wire.choices.into_iter().next()
            .map(|c| c.message.content).unwrap_or_default();
        Ok(ChatResponse {
            text,
            input_tokens: wire.usage.prompt_tokens,
            output_tokens: wire.usage.completion_tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scout_core::ChatMessage;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn openai_chat_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2}
            })))
            .mount(&server).await;
        let c = OpenAiClient::new("sk".into(), "gpt-4o-mini".into())
            .with_base_url(server.uri());
        let r = c.complete(ChatRequest {
            system: None,
            messages: vec![ChatMessage { role: "user".into(), content: "hi".into() }],
            max_tokens: 4, temperature: 0.0,
        }).await.unwrap();
        assert_eq!(r.text, "ok");
        assert_eq!(r.input_tokens, 4);
    }
}
```

- [ ] **Step 2: Write OpenAiEmbedder with failing test**

Create `crates/scout-llm/src/openai_embedder.rs`:

```rust
use async_trait::async_trait;
use reqwest::Client;
use scout_core::{CoreError, CoreResult, Embedder};
use serde::{Deserialize, Serialize};

const DEFAULT_BASE: &str = "https://api.openai.com";

pub struct OpenAiEmbedder {
    http: Client,
    api_key: String,
    base_url: String,
    model: String,
    dim: usize,
}

impl OpenAiEmbedder {
    pub fn new(api_key: String, model: String, dim: usize) -> Self {
        Self { http: Client::new(), api_key, base_url: DEFAULT_BASE.into(), model, dim }
    }
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url; self
    }
}

#[derive(Serialize)]
struct WireReq<'a> { model: &'a str, input: &'a [&'a str] }

#[derive(Deserialize)]
struct WireDatum { embedding: Vec<f32> }

#[derive(Deserialize)]
struct WireResp { data: Vec<WireDatum> }

#[async_trait]
impl Embedder for OpenAiEmbedder {
    fn dim(&self) -> usize { self.dim }
    async fn embed(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        if texts.is_empty() { return Ok(Vec::new()); }
        let resp = self.http
            .post(format!("{}/v1/embeddings", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&WireReq { model: &self.model, input: texts })
            .send().await
            .map_err(|e| CoreError::Provider(format!("embed request: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CoreError::Provider(format!("embed {status}: {body}")));
        }
        let wire: WireResp = resp.json().await
            .map_err(|e| CoreError::Provider(format!("embed decode: {e}")))?;
        let mut vecs: Vec<Vec<f32>> = wire.data.into_iter().map(|d| d.embedding).collect();
        if vecs.iter().any(|v| v.len() != self.dim) {
            return Err(CoreError::Provider(format!(
                "embed dim mismatch: expected {} got {:?}",
                self.dim, vecs.iter().map(|v| v.len()).collect::<Vec<_>>()
            )));
        }
        // ensure ordering aligned with input (OpenAI returns in order; assert by length)
        if vecs.len() != texts.len() {
            return Err(CoreError::Provider(format!(
                "embed count mismatch: expected {} got {}", texts.len(), vecs.len()
            )));
        }
        Ok(std::mem::take(&mut vecs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn embeds_two_strings() {
        let server = MockServer::start().await;
        let v1 = vec![0.0_f32; 4];
        let v2 = vec![1.0_f32; 4];
        Mock::given(method("POST")).and(path("/v1/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"embedding": v1},
                    {"embedding": v2},
                ]
            })))
            .mount(&server).await;
        let e = OpenAiEmbedder::new("sk".into(), "text-embedding-3-small".into(), 4)
            .with_base_url(server.uri());
        let out = e.embed(&["a","b"]).await.unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1][0], 1.0);
    }
}
```

- [ ] **Step 3: Update `lib.rs`**

```rust
//! LLM and embedder implementations.

pub mod anthropic;
pub mod mock;
pub mod openai;
pub mod openai_embedder;

pub use anthropic::AnthropicClient;
pub use openai::OpenAiClient;
pub use openai_embedder::OpenAiEmbedder;
```

- [ ] **Step 4: Run + commit**

Run: `cargo test -p scout-llm`
Expected: PASS — 7 tests.

```bash
git add crates/scout-llm
git commit -m "feat(llm): OpenAiClient + OpenAiEmbedder with wiremock contract tests"
```

---

## Task 16: Extract worker handler

**Files:**
- Create: `crates/scout-pipeline/src/extract_handler.rs`
- Modify: `crates/scout-pipeline/src/lib.rs`
- Modify: `crates/scout-pipeline/Cargo.toml` (add `scout-storage` to dev-deps for integration test)
- Create: `crates/scout-pipeline/tests/extract_handler_test.rs`
- Create: `crates/scout-pipeline/tests/common/mod.rs`

The extract handler is a small orchestrator: given an `article_id`, fetch the article, run the `ClaimExtractor`, persist the resulting claims, and enqueue a `dedupe` job. The handler returns the list of inserted claim ids so the worker loop can log them.

- [ ] **Step 1: Add dev-deps**

Modify `crates/scout-pipeline/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
scout-llm = { path = "../scout-llm" }
scout-storage = { path = "../scout-storage" }
testcontainers.workspace = true
testcontainers-modules.workspace = true
sha2 = "0.10"
serde_json.workspace = true
```

- [ ] **Step 2: Write a shared `common` for pipeline integration tests**

Create `crates/scout-pipeline/tests/common/mod.rs`:

```rust
use scout_storage::{connect, run_migrations, PgPool};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

pub struct TestDb {
    pub pool: PgPool,
    _container: ContainerAsync<Postgres>,
}

pub async fn fresh_db() -> TestDb {
    let container = Postgres::default()
        .with_name("pgvector/pgvector")
        .with_tag("pg16")
        .start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect(&url, 4).await.expect("connect");
    run_migrations(&pool).await.expect("migrate");
    TestDb { pool, _container: container }
}
```

- [ ] **Step 3: Implement the handler**

Create `crates/scout-pipeline/src/extract_handler.rs`:

```rust
use scout_core::{Article, ClaimExtractor};
use std::sync::Arc;
use uuid::Uuid;

/// Storage hooks the extract handler needs. Two methods only — keep the
/// abstraction small so it's easy to mock in unit tests.
#[async_trait::async_trait]
pub trait ExtractStore: Send + Sync {
    async fn get_article(&self, id: Uuid) -> anyhow::Result<Option<Article>>;
    async fn insert_claim_pending(
        &self,
        article_id: Uuid,
        text: &str,
        original_quote: &str,
        quote_offset_start: i32,
        quote_offset_end: i32,
        kind: scout_core::ClaimKind,
        language: &str,
        entity_hints: &[scout_core::EntityHint],
    ) -> anyhow::Result<Uuid>;
    async fn enqueue_dedupe(&self, article_id: Uuid) -> anyhow::Result<()>;
    async fn set_article_status(&self, id: Uuid, status: &str) -> anyhow::Result<()>;
}

pub struct ExtractHandler {
    extractor: Arc<dyn ClaimExtractor>,
    store: Arc<dyn ExtractStore>,
}

impl ExtractHandler {
    pub fn new(extractor: Arc<dyn ClaimExtractor>, store: Arc<dyn ExtractStore>) -> Self {
        Self { extractor, store }
    }

    pub async fn handle(&self, article_id: Uuid) -> anyhow::Result<Vec<Uuid>> {
        let article = self.store.get_article(article_id).await?
            .ok_or_else(|| anyhow::anyhow!("article {article_id} not found"))?;
        self.store.set_article_status(article_id, "processing").await?;
        let raw_claims = self.extractor.extract(&article).await
            .map_err(|e| anyhow::anyhow!("extractor: {e}"))?;

        let mut ids = Vec::with_capacity(raw_claims.len());
        for rc in raw_claims {
            let id = self.store.insert_claim_pending(
                article_id,
                &rc.text, &rc.original_quote,
                rc.quote_offset_start, rc.quote_offset_end,
                rc.kind, &rc.language, &rc.entity_hints,
            ).await?;
            ids.push(id);
        }
        self.store.enqueue_dedupe(article_id).await?;
        Ok(ids)
    }
}
```

- [ ] **Step 4: Update `lib.rs`**

Modify `crates/scout-pipeline/src/lib.rs`:

```rust
//! Pipeline implementations.

pub mod claim_extractor;
pub mod entity_resolver;
pub mod extract_handler;
pub mod novelty_judge;
pub mod prompts;

pub use claim_extractor::LlmClaimExtractor;
pub use entity_resolver::{EntityStore, LlmEntityResolver};
pub use extract_handler::{ExtractHandler, ExtractStore};
pub use novelty_judge::{ThresholdJudge, Thresholds};
```

- [ ] **Step 5: Write the integration test**

Create `crates/scout-pipeline/tests/extract_handler_test.rs`:

```rust
mod common;

use async_trait::async_trait;
use scout_core::{Article, ClaimKind, EntityHint, NewArticle};
use scout_pipeline::{ExtractHandler, ExtractStore};
use scout_storage::{ArticleRepo, ClaimRepo, NewJob, PgJobQueue};
use sha2::Digest;
use std::sync::Arc;
use uuid::Uuid;

struct StoreAdapter {
    articles: ArticleRepo,
    claims: ClaimRepo,
    jobs: PgJobQueue,
}

#[async_trait]
impl ExtractStore for StoreAdapter {
    async fn get_article(&self, id: Uuid) -> anyhow::Result<Option<Article>> {
        Ok(self.articles.get(id).await?)
    }
    async fn set_article_status(&self, id: Uuid, status: &str) -> anyhow::Result<()> {
        self.articles.set_status(id, status).await?;
        Ok(())
    }
    async fn insert_claim_pending(
        &self, article_id: Uuid, text: &str, original_quote: &str,
        s: i32, e: i32, kind: ClaimKind, language: &str, _hints: &[EntityHint],
    ) -> anyhow::Result<Uuid> {
        Ok(self.claims.insert(scout_storage::NewClaim {
            article_id, text: text.into(), original_quote: original_quote.into(),
            quote_offset_start: s, quote_offset_end: e, kind, language: language.into(),
        }).await?)
    }
    async fn enqueue_dedupe(&self, article_id: Uuid) -> anyhow::Result<()> {
        self.jobs.enqueue(NewJob {
            kind: "dedupe".into(),
            payload: serde_json::json!({"article_id": article_id}),
        }).await?;
        Ok(())
    }
}

#[tokio::test]
async fn handler_extracts_persists_and_enqueues_dedupe() {
    let db = common::fresh_db().await;
    let articles = ArticleRepo::new(db.pool.clone());
    let id = articles.insert(NewArticle {
        url: "https://a.test/h".into(),
        title: "T".into(),
        author: None, source: "test".into(), language: "en".into(),
        raw_html: None, content_md: "body".into(),
        content_hash: sha2::Sha256::digest(b"body").to_vec(),
    }).await.unwrap();

    let canned = r#"[{
        "text": "claim A",
        "original_quote": "body",
        "quote_offset_start": 0,
        "quote_offset_end": 4,
        "kind": "fact",
        "language": "en",
        "entity_hints": []
    }]"#;
    let llm = Arc::new(scout_llm::mock::MockLlmClient::new().with_rule("Article title", canned));
    let extractor = Arc::new(scout_pipeline::LlmClaimExtractor::new(llm));
    let store = Arc::new(StoreAdapter {
        articles: articles.clone(),
        claims: ClaimRepo::new(db.pool.clone()),
        jobs: PgJobQueue::new(db.pool.clone()),
    });
    let handler = ExtractHandler::new(extractor, store);

    let ids = handler.handle(id).await.unwrap();
    assert_eq!(ids.len(), 1);
    let updated = articles.get(id).await.unwrap().unwrap();
    assert_eq!(updated.status, "processing");
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE kind='dedupe'")
        .fetch_one(&db.pool).await.unwrap();
    assert_eq!(count, 1);
}
```

- [ ] **Step 6: Run + commit**

Run: `cargo test -p scout-pipeline --test extract_handler_test`
Expected: PASS — 1 test.

```bash
git add crates/scout-pipeline
git commit -m "feat(pipeline): ExtractHandler orchestrates extractor + repo + dedupe enqueue"
```

---

## Task 17: Dedupe worker handler

**Files:**
- Create: `crates/scout-pipeline/src/dedupe_handler.rs`
- Modify: `crates/scout-pipeline/src/lib.rs`
- Create: `crates/scout-pipeline/tests/dedupe_handler_test.rs`

- [ ] **Step 1: Implement the handler**

Create `crates/scout-pipeline/src/dedupe_handler.rs`:

```rust
use scout_core::{Candidate, ClaimDraft, ClaimKind, Embedder, Novelty, NoveltyJudge};
use scout_core::EntityResolver;
use std::sync::Arc;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait DedupeStore: Send + Sync {
    async fn pending_claims_for_article(&self, article_id: Uuid)
        -> anyhow::Result<Vec<PendingClaim>>;
    async fn set_embedding(&self, claim_id: Uuid, vec: &[f32]) -> anyhow::Result<()>;
    async fn ann_candidates(&self, vec: &[f32], kind: ClaimKind, k: i64)
        -> anyhow::Result<Vec<Candidate>>;
    async fn set_novelty_novel(&self, claim_id: Uuid) -> anyhow::Result<()>;
    async fn set_novelty_duplicate(&self, claim_id: Uuid, dup_of: Uuid) -> anyhow::Result<()>;
    async fn set_novelty_supplement(&self, claim_id: Uuid, supp: Uuid) -> anyhow::Result<()>;
    async fn link_entities(&self, claim_id: Uuid, refs: &[scout_core::EntityRef]) -> anyhow::Result<()>;
    async fn set_article_status(&self, article_id: Uuid, status: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct PendingClaim {
    pub id: Uuid,
    pub text: String,
    pub kind: ClaimKind,
    pub language: String,
}

pub struct DedupeHandler {
    embedder: Arc<dyn Embedder>,
    judge: Arc<dyn NoveltyJudge>,
    resolver: Arc<dyn EntityResolver>,
    store: Arc<dyn DedupeStore>,
    ann_k: i64,
}

impl DedupeHandler {
    pub fn new(
        embedder: Arc<dyn Embedder>,
        judge: Arc<dyn NoveltyJudge>,
        resolver: Arc<dyn EntityResolver>,
        store: Arc<dyn DedupeStore>,
    ) -> Self {
        Self { embedder, judge, resolver, store, ann_k: 20 }
    }

    pub async fn handle(&self, article_id: Uuid) -> anyhow::Result<()> {
        let pending = self.store.pending_claims_for_article(article_id).await?;
        if pending.is_empty() {
            self.store.set_article_status(article_id, "processed").await?;
            return Ok(());
        }
        let texts: Vec<&str> = pending.iter().map(|p| p.text.as_str()).collect();
        let vectors = self.embedder.embed(&texts).await
            .map_err(|e| anyhow::anyhow!("embed: {e}"))?;

        for (claim, vec) in pending.iter().zip(vectors.iter()) {
            self.store.set_embedding(claim.id, vec).await?;

            let candidates = self.store.ann_candidates(vec, claim.kind, self.ann_k).await?;
            // Exclude self in case the row was committed and re-found.
            let candidates: Vec<Candidate> = candidates
                .into_iter().filter(|c| c.claim_id != claim.id).collect();

            let draft = ClaimDraft {
                id: claim.id, article_id, text: claim.text.clone(),
                kind: claim.kind, language: claim.language.clone(),
                embedding: Some(vec.clone()),
            };
            let novelty = self.judge.judge(&draft, &candidates);
            match novelty {
                Novelty::Novel => self.store.set_novelty_novel(claim.id).await?,
                Novelty::Duplicate { dup_of } =>
                    self.store.set_novelty_duplicate(claim.id, dup_of).await?,
                Novelty::Supplement { supplement_of } =>
                    self.store.set_novelty_supplement(claim.id, supplement_of).await?,
            }

            // Entity resolution; hints come from the extractor stage via a separate
            // path in the storage adapter — for MVP we resolve from the claim text alone.
            let refs = self.resolver.resolve(&draft, &[]).await
                .map_err(|e| anyhow::anyhow!("resolve: {e}"))?;
            self.store.link_entities(claim.id, &refs).await?;
        }
        self.store.set_article_status(article_id, "processed").await?;
        Ok(())
    }
}
```

- [ ] **Step 2: Update `lib.rs`**

```rust
//! Pipeline implementations.

pub mod claim_extractor;
pub mod dedupe_handler;
pub mod entity_resolver;
pub mod extract_handler;
pub mod novelty_judge;
pub mod prompts;

pub use claim_extractor::LlmClaimExtractor;
pub use dedupe_handler::{DedupeHandler, DedupeStore, PendingClaim};
pub use entity_resolver::{EntityStore, LlmEntityResolver};
pub use extract_handler::{ExtractHandler, ExtractStore};
pub use novelty_judge::{ThresholdJudge, Thresholds};
```

- [ ] **Step 3: Write the integration test**

Create `crates/scout-pipeline/tests/dedupe_handler_test.rs`:

```rust
mod common;

use async_trait::async_trait;
use scout_core::{Candidate, ClaimKind, EntityRef, NewArticle};
use scout_pipeline::{DedupeHandler, DedupeStore, PendingClaim, ThresholdJudge, Thresholds};
use scout_storage::{ArticleRepo, ClaimEntityRepo, ClaimRepo, NewClaim};
use sha2::Digest;
use std::sync::Arc;
use uuid::Uuid;

struct StoreAdapter {
    articles: ArticleRepo,
    claims: ClaimRepo,
    claim_entities: ClaimEntityRepo,
}

#[async_trait]
impl DedupeStore for StoreAdapter {
    async fn pending_claims_for_article(&self, article_id: Uuid) -> anyhow::Result<Vec<PendingClaim>> {
        Ok(self.claims.pending_for_article(article_id).await?
            .into_iter().map(|p| PendingClaim {
                id: p.id, text: p.text, kind: p.kind, language: p.language,
            }).collect())
    }
    async fn set_embedding(&self, id: Uuid, v: &[f32]) -> anyhow::Result<()> {
        self.claims.set_embedding(id, v).await?; Ok(())
    }
    async fn ann_candidates(&self, v: &[f32], kind: ClaimKind, k: i64) -> anyhow::Result<Vec<Candidate>> {
        Ok(self.claims.ann_search(v, kind, k).await?)
    }
    async fn set_novelty_novel(&self, id: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_novel(id).await?; Ok(())
    }
    async fn set_novelty_duplicate(&self, id: Uuid, d: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_duplicate(id, d).await?; Ok(())
    }
    async fn set_novelty_supplement(&self, id: Uuid, s: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_supplement(id, s).await?; Ok(())
    }
    async fn link_entities(&self, claim_id: Uuid, refs: &[EntityRef]) -> anyhow::Result<()> {
        for r in refs { self.claim_entities.link(claim_id, r.entity_id, &r.role).await?; }
        Ok(())
    }
    async fn set_article_status(&self, id: Uuid, s: &str) -> anyhow::Result<()> {
        self.articles.set_status(id, s).await?; Ok(())
    }
}

struct NoopResolver;
#[async_trait]
impl scout_core::EntityResolver for NoopResolver {
    async fn resolve(&self, _: &scout_core::ClaimDraft, _: &[scout_core::EntityHint])
        -> scout_core::CoreResult<Vec<EntityRef>> { Ok(vec![]) }
}

#[tokio::test]
async fn dedupe_marks_single_claim_novel_and_sets_article_processed() {
    let db = common::fresh_db().await;
    let articles = ArticleRepo::new(db.pool.clone());
    let claims = ClaimRepo::new(db.pool.clone());
    let aid = articles.insert(NewArticle {
        url: "https://a.test/d".into(), title: "T".into(), author: None,
        source: "test".into(), language: "en".into(), raw_html: None,
        content_md: "body".into(),
        content_hash: sha2::Sha256::digest(b"body").to_vec(),
    }).await.unwrap();
    claims.insert(NewClaim {
        article_id: aid, text: "first claim".into(),
        original_quote: "body".into(),
        quote_offset_start: 0, quote_offset_end: 4,
        kind: ClaimKind::Fact, language: "en".into(),
    }).await.unwrap();

    let embedder = Arc::new(scout_llm::mock::MockEmbedder { dim: 1536 });
    let judge = Arc::new(ThresholdJudge::new(Thresholds::defaults()));
    let resolver = Arc::new(NoopResolver);
    let store = Arc::new(StoreAdapter {
        articles: articles.clone(),
        claims: claims.clone(),
        claim_entities: ClaimEntityRepo::new(db.pool.clone()),
    });
    let handler = DedupeHandler::new(embedder, judge, resolver, store);
    handler.handle(aid).await.unwrap();

    let a = articles.get(aid).await.unwrap().unwrap();
    assert_eq!(a.status, "processed");
    let (novelty,): (String,) = sqlx::query_as("SELECT novelty FROM claims WHERE article_id = $1")
        .bind(aid).fetch_one(&db.pool).await.unwrap();
    assert_eq!(novelty, "novel");
}
```

- [ ] **Step 4: Run + commit**

Run: `cargo test -p scout-pipeline --test dedupe_handler_test`
Expected: PASS — 1 test.

```bash
git add crates/scout-pipeline
git commit -m "feat(pipeline): DedupeHandler — embed, ANN, judge, resolve, mark processed"
```

---

## Task 18: Worker main loop

**Files:**
- Create: `crates/scout-pipeline/src/worker.rs`
- Modify: `crates/scout-pipeline/src/lib.rs`
- Create: `crates/scout-pipeline/tests/worker_test.rs`

- [ ] **Step 1: Implement the loop**

Create `crates/scout-pipeline/src/worker.rs`:

```rust
use crate::{DedupeHandler, ExtractHandler};
use scout_storage::PgJobQueue;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct ArticlePayload { article_id: Uuid }

pub struct Worker {
    queue: PgJobQueue,
    extract: Arc<ExtractHandler>,
    dedupe: Arc<DedupeHandler>,
    poll_interval: Duration,
    shutdown: Arc<Notify>,
}

impl Worker {
    pub fn new(
        queue: PgJobQueue,
        extract: Arc<ExtractHandler>,
        dedupe: Arc<DedupeHandler>,
        poll_interval: Duration,
    ) -> Self {
        Self { queue, extract, dedupe, poll_interval, shutdown: Arc::new(Notify::new()) }
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> { self.shutdown.clone() }

    pub async fn run(self) {
        info!("worker loop starting");
        loop {
            if let Some(job) = self.next_job().await {
                let job_id = job.id;
                let result = match job.kind.as_str() {
                    "extract" => self.handle_extract(&job).await,
                    "dedupe"  => self.handle_dedupe(&job).await,
                    other     => Err(anyhow::anyhow!("unknown job kind {other}")),
                };
                match result {
                    Ok(_) => { let _ = self.queue.ack(job_id).await; }
                    Err(e) => {
                        error!(?e, %job_id, "job failed");
                        let _ = self.queue.fail(job_id, &format!("{e}"), true).await;
                    }
                }
            } else {
                tokio::select! {
                    _ = sleep(self.poll_interval) => {},
                    _ = self.shutdown.notified() => {
                        info!("worker shutting down"); return;
                    }
                }
            }
        }
    }

    async fn next_job(&self) -> Option<scout_storage::Job> {
        match self.queue.dequeue(&["extract", "dedupe"]).await {
            Ok(opt) => opt,
            Err(e) => { warn!(?e, "dequeue error"); None }
        }
    }

    async fn handle_extract(&self, job: &scout_storage::Job) -> anyhow::Result<()> {
        let p: ArticlePayload = serde_json::from_value(job.payload.clone())?;
        self.extract.handle(p.article_id).await?;
        Ok(())
    }

    async fn handle_dedupe(&self, job: &scout_storage::Job) -> anyhow::Result<()> {
        let p: ArticlePayload = serde_json::from_value(job.payload.clone())?;
        self.dedupe.handle(p.article_id).await?;
        Ok(())
    }
}
```

- [ ] **Step 2: Update `lib.rs` + add `tracing` to deps**

Modify `crates/scout-pipeline/Cargo.toml` (under `[dependencies]`):

```toml
tokio = { workspace = true, features = ["sync", "time", "rt-multi-thread", "macros"] }
```

Modify `crates/scout-pipeline/src/lib.rs`:

```rust
pub mod claim_extractor;
pub mod dedupe_handler;
pub mod entity_resolver;
pub mod extract_handler;
pub mod novelty_judge;
pub mod prompts;
pub mod worker;

pub use claim_extractor::LlmClaimExtractor;
pub use dedupe_handler::{DedupeHandler, DedupeStore, PendingClaim};
pub use entity_resolver::{EntityStore, LlmEntityResolver};
pub use extract_handler::{ExtractHandler, ExtractStore};
pub use novelty_judge::{ThresholdJudge, Thresholds};
pub use worker::Worker;
```

- [ ] **Step 3: End-to-end pipeline integration test**

Create `crates/scout-pipeline/tests/worker_test.rs`:

```rust
mod common;

use async_trait::async_trait;
use scout_core::{
    Article, Candidate, ClaimKind, EntityHint, EntityRef, NewArticle,
};
use scout_pipeline::{
    DedupeHandler, DedupeStore, ExtractHandler, ExtractStore, LlmClaimExtractor,
    NoveltyJudge, PendingClaim, ThresholdJudge, Thresholds, Worker,
};
use scout_storage::{ArticleRepo, ClaimEntityRepo, ClaimRepo, NewClaim, NewJob, PgJobQueue};
use sha2::Digest;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

struct ExtractAdapter { articles: ArticleRepo, claims: ClaimRepo, jobs: PgJobQueue }
#[async_trait]
impl ExtractStore for ExtractAdapter {
    async fn get_article(&self, id: Uuid) -> anyhow::Result<Option<Article>> {
        Ok(self.articles.get(id).await?)
    }
    async fn set_article_status(&self, id: Uuid, s: &str) -> anyhow::Result<()> {
        self.articles.set_status(id, s).await?; Ok(())
    }
    async fn insert_claim_pending(
        &self, aid: Uuid, t: &str, q: &str, s: i32, e: i32,
        k: ClaimKind, lang: &str, _: &[EntityHint],
    ) -> anyhow::Result<Uuid> {
        Ok(self.claims.insert(NewClaim {
            article_id: aid, text: t.into(), original_quote: q.into(),
            quote_offset_start: s, quote_offset_end: e, kind: k, language: lang.into(),
        }).await?)
    }
    async fn enqueue_dedupe(&self, aid: Uuid) -> anyhow::Result<()> {
        self.jobs.enqueue(NewJob {
            kind: "dedupe".into(),
            payload: serde_json::json!({"article_id": aid}),
        }).await?; Ok(())
    }
}

struct DedupeAdapter {
    articles: ArticleRepo, claims: ClaimRepo, ce: ClaimEntityRepo,
}
#[async_trait]
impl DedupeStore for DedupeAdapter {
    async fn pending_claims_for_article(&self, aid: Uuid) -> anyhow::Result<Vec<PendingClaim>> {
        Ok(self.claims.pending_for_article(aid).await?.into_iter().map(|p| PendingClaim {
            id: p.id, text: p.text, kind: p.kind, language: p.language,
        }).collect())
    }
    async fn set_embedding(&self, id: Uuid, v: &[f32]) -> anyhow::Result<()> {
        self.claims.set_embedding(id, v).await?; Ok(())
    }
    async fn ann_candidates(&self, v: &[f32], k: ClaimKind, n: i64) -> anyhow::Result<Vec<Candidate>> {
        Ok(self.claims.ann_search(v, k, n).await?)
    }
    async fn set_novelty_novel(&self, id: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_novel(id).await?; Ok(())
    }
    async fn set_novelty_duplicate(&self, id: Uuid, d: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_duplicate(id, d).await?; Ok(())
    }
    async fn set_novelty_supplement(&self, id: Uuid, s: Uuid) -> anyhow::Result<()> {
        self.claims.set_novelty_supplement(id, s).await?; Ok(())
    }
    async fn link_entities(&self, cid: Uuid, refs: &[EntityRef]) -> anyhow::Result<()> {
        for r in refs { self.ce.link(cid, r.entity_id, &r.role).await?; } Ok(())
    }
    async fn set_article_status(&self, id: Uuid, s: &str) -> anyhow::Result<()> {
        self.articles.set_status(id, s).await?; Ok(())
    }
}

struct NoopResolver;
#[async_trait]
impl scout_core::EntityResolver for NoopResolver {
    async fn resolve(&self, _: &scout_core::ClaimDraft, _: &[EntityHint])
        -> scout_core::CoreResult<Vec<EntityRef>> { Ok(vec![]) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_processes_extract_then_dedupe_end_to_end() {
    let db = common::fresh_db().await;
    let articles = ArticleRepo::new(db.pool.clone());
    let claims = ClaimRepo::new(db.pool.clone());
    let jobs = PgJobQueue::new(db.pool.clone());

    let aid = articles.insert(NewArticle {
        url: "https://a.test/w".into(), title: "T".into(), author: None,
        source: "test".into(), language: "en".into(), raw_html: None,
        content_md: "body".into(),
        content_hash: sha2::Sha256::digest(b"body").to_vec(),
    }).await.unwrap();
    jobs.enqueue(NewJob {
        kind: "extract".into(),
        payload: serde_json::json!({"article_id": aid}),
    }).await.unwrap();

    let canned = r#"[{
        "text":"c","original_quote":"body","quote_offset_start":0,"quote_offset_end":4,
        "kind":"fact","language":"en","entity_hints":[]
    }]"#;
    let llm = Arc::new(scout_llm::mock::MockLlmClient::new().with_rule("Article title", canned));
    let extractor = Arc::new(LlmClaimExtractor::new(llm));
    let embedder = Arc::new(scout_llm::mock::MockEmbedder { dim: 1536 });
    let judge: Arc<dyn NoveltyJudge> = Arc::new(ThresholdJudge::new(Thresholds::defaults()));
    let resolver = Arc::new(NoopResolver);

    let extract = Arc::new(ExtractHandler::new(extractor, Arc::new(ExtractAdapter {
        articles: articles.clone(), claims: claims.clone(), jobs: jobs.clone(),
    })));
    let dedupe = Arc::new(DedupeHandler::new(embedder, judge, resolver, Arc::new(DedupeAdapter {
        articles: articles.clone(), claims: claims.clone(),
        ce: ClaimEntityRepo::new(db.pool.clone()),
    })));

    let worker = Worker::new(jobs.clone(), extract, dedupe, Duration::from_millis(50));
    let shutdown = worker.shutdown_handle();
    let task = tokio::spawn(worker.run());

    // Poll until article is processed.
    for _ in 0..40 {
        let a = articles.get(aid).await.unwrap().unwrap();
        if a.status == "processed" { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.notify_one();
    task.await.unwrap();

    let a = articles.get(aid).await.unwrap().unwrap();
    assert_eq!(a.status, "processed");
    let (novel_count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM claims WHERE article_id = $1 AND novelty = 'novel'"
    ).bind(aid).fetch_one(&db.pool).await.unwrap();
    assert_eq!(novel_count, 1);
}
```

- [ ] **Step 4: Run + commit**

Run: `cargo test -p scout-pipeline --test worker_test`
Expected: PASS — 1 test.

```bash
git add crates/scout-pipeline
git commit -m "feat(pipeline): Worker main loop with graceful shutdown + end-to-end test"
```

---

## Task 19: scout-web base — Cargo, AppContext, layout template, static assets

**Files:**
- Create: `crates/scout-web/Cargo.toml`
- Create: `crates/scout-web/src/lib.rs`
- Create: `crates/scout-web/src/app.rs`
- Create: `crates/scout-web/src/config.rs`
- Create: `crates/scout-web/src/error.rs`
- Create: `crates/scout-web/src/templates/mod.rs`
- Create: `crates/scout-web/src/templates/layout.rs`
- Create: `crates/scout-web/src/routes/mod.rs`
- Create: `crates/scout-web/src/routes/health.rs`
- Create: `crates/scout-web/static/htmx.min.js`
- Create: `crates/scout-web/static/pico.min.css`
- Create: `crates/scout-web/static/app.css`
- Modify: `Cargo.toml` (workspace members + axum/maud/etc deps)

- [ ] **Step 1: Add workspace deps**

Modify root `Cargo.toml` — append to `workspace.members` `"crates/scout-web",` and add:

```toml
axum = { version = "0.7", features = ["macros"] }
tower = "0.5"
tower-http = { version = "0.5", features = ["trace", "cors"] }
maud = { version = "0.26", features = ["axum"] }
rust-embed = "8"
mime_guess = "2"
cookie = { version = "0.18", features = ["percent-encode", "private"] }
config = { version = "0.14", default-features = false, features = ["toml"] }
clap = { version = "4", features = ["derive", "env"] }
```

- [ ] **Step 2: Write `crates/scout-web/Cargo.toml`**

```toml
[package]
name = "scout-web"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
scout-core   = { path = "../scout-core" }
scout-storage = { path = "../scout-storage" }
scout-llm    = { path = "../scout-llm" }
scout-pipeline = { path = "../scout-pipeline" }
async-trait.workspace = true
anyhow.workspace = true
thiserror.workspace = true
serde.workspace = true
serde_json.workspace = true
sqlx.workspace = true
pgvector.workspace = true
tokio = { workspace = true, features = ["full"] }
tracing.workspace = true
uuid.workspace = true
chrono.workspace = true
axum.workspace = true
tower.workspace = true
tower-http.workspace = true
maud.workspace = true
rust-embed.workspace = true
mime_guess.workspace = true
cookie.workspace = true
config.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
reqwest = { workspace = true, features = ["cookies"] }
testcontainers.workspace = true
testcontainers-modules.workspace = true
sha2 = "0.10"
```

- [ ] **Step 3: Drop placeholder static assets**

For the plan's purpose, commit small placeholder files. Real assets are dropped in at build time.

`crates/scout-web/static/htmx.min.js`:
```javascript
// Replace with the pinned htmx.min.js from https://unpkg.com/htmx.org@2.0.3/dist/htmx.min.js
// Placeholder for compile-time embedding.
```

`crates/scout-web/static/pico.min.css`:
```css
/* Replace with pinned pico.min.css from https://unpkg.com/@picocss/pico@2/css/pico.min.css */
```

`crates/scout-web/static/app.css`:
```css
:root { --max-w: 880px; }
main.container { max-width: var(--max-w); }
.claim { padding: .5rem 0; border-bottom: 1px dashed var(--pico-muted-border-color); }
.claim small { color: var(--pico-muted-color); }
.tag-fact { color: #155724; }
.tag-opinion { color: #856404; }
.tag-data { color: #004085; }
.tag-prediction { color: #6c1f8b; }
.tag-methodology { color: #1b5e8a; }
.badge { display: inline-block; padding: 0 .35em; border-radius: .25rem; font-size: .85em; }
.b-novel { background: #d4edda; color: #155724; }
.b-supplement { background: #fff3cd; color: #856404; }
.b-duplicate { background: #e9ecef; color: #555; }
```

- [ ] **Step 4: Write `config.rs` and `error.rs`**

Create `crates/scout-web/src/config.rs`:

```rust
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig { pub provider: String, pub model: String }

#[derive(Debug, Clone, Deserialize)]
pub struct EmbedderConfig { pub provider: String, pub model: String, pub dim: usize }

#[derive(Debug, Clone, Deserialize)]
pub struct NoveltyConfig {
    pub fact_duplicate_threshold: f32,
    pub fact_supplement_low: f32,
    pub opinion_duplicate_threshold: f32,
    pub opinion_supplement_low: f32,
    pub methodology_duplicate_threshold: f32,
    pub methodology_supplement_low: f32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig { pub poll_interval_ms: u64, pub max_concurrent_jobs: u32 }

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub llm: LlmConfig,
    pub embedder: EmbedderConfig,
    pub novelty: NoveltyConfig,
    pub worker: WorkerConfig,
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}

impl From<&NoveltyConfig> for scout_pipeline::Thresholds {
    fn from(n: &NoveltyConfig) -> Self {
        Self {
            fact_duplicate_threshold: n.fact_duplicate_threshold,
            fact_supplement_low: n.fact_supplement_low,
            opinion_duplicate_threshold: n.opinion_duplicate_threshold,
            opinion_supplement_low: n.opinion_supplement_low,
            methodology_duplicate_threshold: n.methodology_duplicate_threshold,
            methodology_supplement_low: n.methodology_supplement_low,
        }
    }
}
```

Create `crates/scout-web/src/error.rs`:

```rust
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

pub struct AppError(pub anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self { Self(e.into()) }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        tracing::error!(error = ?self.0, "request error");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("internal error: {}", self.0)).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
```

- [ ] **Step 5: Write the layout template**

Create `crates/scout-web/src/templates/mod.rs`:

```rust
pub mod layout;
```

Create `crates/scout-web/src/templates/layout.rs`:

```rust
use maud::{html, Markup, DOCTYPE};

pub fn layout(title: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · Scout" }
                link rel="stylesheet" href="/static/pico.min.css";
                link rel="stylesheet" href="/static/app.css";
                script src="/static/htmx.min.js" {}
            }
            body {
                header.container {
                    nav {
                        ul { li { strong { a href="/" { "Scout" } } } }
                        ul {
                            li { a href="/" { "Inbox" } }
                            li { a href="/settings" { "Settings" } }
                        }
                    }
                }
                main.container { (body) }
            }
        }
    }
}
```

- [ ] **Step 6: Write the app builder + health route**

Create `crates/scout-web/src/app.rs`:

```rust
use crate::config::AppConfig;
use crate::routes;
use axum::routing::get;
use axum::Router;
use rust_embed::RustEmbed;
use scout_core::{Embedder, LlmClient};
use scout_storage::{
    ArticleRepo, ClaimEntityRepo, ClaimRepo, EntityRepo, FeedbackRepo, PgJobQueue, PgPool,
};
use std::sync::Arc;

#[derive(RustEmbed)]
#[folder = "static/"]
struct StaticAssets;

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<AppConfig>,
    pub token: Arc<String>,
    pub cookie_secret: Arc<[u8; 32]>,
    pub pool: PgPool,
    pub articles: ArticleRepo,
    pub claims: ClaimRepo,
    pub entities: EntityRepo,
    pub claim_entities: ClaimEntityRepo,
    pub feedback: FeedbackRepo,
    pub jobs: PgJobQueue,
    pub llm: Arc<dyn LlmClient>,
    pub embedder: Arc<dyn Embedder>,
}

pub fn router(ctx: AppContext) -> Router {
    Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/static/*path", get(serve_static))
        .with_state(ctx)
}

async fn serve_static(axum::extract::Path(path): axum::extract::Path<String>) -> axum::response::Response {
    use axum::response::IntoResponse;
    match StaticAssets::get(&path) {
        Some(asset) => {
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            (
                [(axum::http::header::CONTENT_TYPE, mime.as_ref())],
                asset.data.into_owned(),
            ).into_response()
        }
        None => axum::http::StatusCode::NOT_FOUND.into_response(),
    }
}
```

- [ ] **Step 7: Write the health route + module**

Create `crates/scout-web/src/routes/mod.rs`:

```rust
pub mod health;
```

Create `crates/scout-web/src/routes/health.rs`:

```rust
use axum::http::StatusCode;

pub async fn healthz() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}
```

- [ ] **Step 8: Write `lib.rs`**

Create `crates/scout-web/src/lib.rs`:

```rust
//! Scout web: Axum + Maud SSR + HTMX.

pub mod app;
pub mod config;
pub mod error;
pub mod routes;
pub mod templates;

pub use app::{router, AppContext};
pub use config::AppConfig;
pub use error::{AppError, AppResult};
```

- [ ] **Step 9: Write smoke test**

Create `crates/scout-web/tests/healthz_test.rs`:

```rust
use scout_web::router;

#[tokio::test]
async fn healthz_returns_ok() {
    // Build a router without a full AppContext by routing only healthz directly.
    let router = axum::Router::new().route("/healthz",
        axum::routing::get(scout_web::routes::health::healthz));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });

    let resp = reqwest::get(format!("http://{addr}/healthz")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");

    // The `router` constructor is exercised in later tasks once AppContext is wireable.
    let _ = router;
}
```

- [ ] **Step 10: Run + commit**

Run: `cargo test -p scout-web --test healthz_test`
Expected: PASS — 1 test.

```bash
git add Cargo.toml crates/scout-web
git commit -m "feat(web): scout-web crate base — AppContext, Maud layout, static assets, healthz"
```

---

## Task 20: Bearer-auth middleware + login

**Files:**
- Create: `crates/scout-web/src/auth.rs`
- Create: `crates/scout-web/src/templates/login.rs`
- Create: `crates/scout-web/src/routes/login.rs`
- Modify: `crates/scout-web/src/app.rs` (wire middleware + login routes)
- Modify: `crates/scout-web/src/templates/mod.rs`
- Modify: `crates/scout-web/src/routes/mod.rs`
- Modify: `crates/scout-web/src/lib.rs`
- Create: `crates/scout-web/tests/auth_test.rs`

- [ ] **Step 1: Write the failing auth test**

Create `crates/scout-web/tests/common/mod.rs`:

```rust
use scout_storage::{connect, run_migrations, PgPool};
use scout_web::{router, AppContext, AppConfig};
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio::net::TcpListener;

pub struct TestApp {
    pub url: String,
    pub token: String,
    _container: ContainerAsync<Postgres>,
    _pool: PgPool,
}

pub async fn spawn() -> TestApp {
    let container = Postgres::default()
        .with_name("pgvector/pgvector").with_tag("pg16").start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect(&url, 4).await.unwrap();
    run_migrations(&pool).await.unwrap();

    let cfg = AppConfig {
        llm: scout_web::config::LlmConfig { provider: "anthropic".into(), model: "claude-sonnet-4-6".into() },
        embedder: scout_web::config::EmbedderConfig { provider: "openai".into(), model: "x".into(), dim: 1536 },
        novelty: scout_web::config::NoveltyConfig {
            fact_duplicate_threshold: 0.92, fact_supplement_low: 0.85,
            opinion_duplicate_threshold: 0.96, opinion_supplement_low: 0.88,
            methodology_duplicate_threshold: 0.93, methodology_supplement_low: 0.86,
        },
        worker: scout_web::config::WorkerConfig { poll_interval_ms: 500, max_concurrent_jobs: 4 },
    };
    let token = "test-token-1234567890".to_string();
    let secret = *b"0123456789abcdef0123456789abcdef";
    let ctx = AppContext {
        config: Arc::new(cfg),
        token: Arc::new(token.clone()),
        cookie_secret: Arc::new(secret),
        pool: pool.clone(),
        articles: scout_storage::ArticleRepo::new(pool.clone()),
        claims: scout_storage::ClaimRepo::new(pool.clone()),
        entities: scout_storage::EntityRepo::new(pool.clone()),
        claim_entities: scout_storage::ClaimEntityRepo::new(pool.clone()),
        feedback: scout_storage::FeedbackRepo::new(pool.clone()),
        jobs: scout_storage::PgJobQueue::new(pool.clone()),
        llm: Arc::new(scout_llm::mock::MockLlmClient::new()),
        embedder: Arc::new(scout_llm::mock::MockEmbedder { dim: 1536 }),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(ctx)).await.unwrap();
    });
    TestApp { url: format!("http://{addr}"), token, _container: container, _pool: pool }
}
```

Create `crates/scout-web/tests/auth_test.rs`:

```rust
mod common;

#[tokio::test]
async fn unauthenticated_root_redirects_to_login() {
    let app = common::spawn().await;
    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let resp = client.get(format!("{}/", app.url)).send().await.unwrap();
    assert_eq!(resp.status(), 303);
    let loc = resp.headers().get("location").unwrap().to_str().unwrap();
    assert!(loc.contains("/login"));
}

#[tokio::test]
async fn login_with_bearer_sets_cookie_and_allows_root() {
    let app = common::spawn().await;
    let client = reqwest::Client::builder()
        .cookie_store(true)
        .redirect(reqwest::redirect::Policy::none())
        .build().unwrap();
    let resp = client.post(format!("{}/login", app.url))
        .form(&[("token", app.token.as_str())])
        .send().await.unwrap();
    assert_eq!(resp.status(), 303);

    let resp2 = client.get(format!("{}/", app.url)).send().await.unwrap();
    assert_eq!(resp2.status(), 200);
}

#[tokio::test]
async fn ingest_with_bearer_header_accepted() {
    let app = common::spawn().await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("{}/ingest", app.url))
        .bearer_auth(&app.token)
        .json(&serde_json::json!({
            "url":"https://a.test/x",
            "title":"T",
            "content_html":"<p>hi</p>",
            "lang":"en"
        }))
        .send().await.unwrap();
    assert!(resp.status().is_success() || resp.status() == 202);
}
```

- [ ] **Step 2: Implement auth middleware**

Create `crates/scout-web/src/auth.rs`:

```rust
use crate::app::AppContext;
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use cookie::{Cookie, CookieJar, Key, SameSite};

pub const SESSION_COOKIE: &str = "scout_session";

pub fn cookie_key(secret: &[u8; 32]) -> Key {
    // Use HMAC-only signed cookies, not private (we don't need confidentiality).
    Key::from(secret)
}

pub fn issue_session_cookie(ctx: &AppContext, token: &str) -> Cookie<'static> {
    let mut c = Cookie::new(SESSION_COOKIE, token.to_string());
    c.set_http_only(true);
    c.set_same_site(SameSite::Lax);
    c.set_path("/");
    c.set_secure(false);  // dev; set true behind TLS in deploy
    let mut jar = CookieJar::new();
    let key = cookie_key(&ctx.cookie_secret);
    jar.signed_mut(&key).add(c);
    jar.get(SESSION_COOKIE).cloned().unwrap()
}

fn verified_token_from_cookies(ctx: &AppContext, headers: &HeaderMap) -> Option<String> {
    let cookie_hdr = headers.get(header::COOKIE)?.to_str().ok()?;
    let mut jar = CookieJar::new();
    for c in Cookie::split_parse(cookie_hdr.to_string()) {
        if let Ok(c) = c { jar.add_original(c); }
    }
    let key = cookie_key(&ctx.cookie_secret);
    jar.signed(&key).get(SESSION_COOKIE).map(|c| c.value().to_string())
}

fn bearer_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let h = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    h.strip_prefix("Bearer ").map(|s| s.to_string())
}

pub async fn require_auth(
    State(ctx): State<AppContext>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let headers = req.headers().clone();
    let from_cookie = verified_token_from_cookies(&ctx, &headers);
    let from_bearer = bearer_token_from_headers(&headers);
    let presented = from_cookie.or(from_bearer);
    let ok = matches!(presented, Some(ref t) if t == ctx.token.as_str());

    if !ok {
        // Browser request → redirect; API caller (Bearer) → 401.
        if headers.get(header::ACCEPT).and_then(|v| v.to_str().ok())
            .map(|s| s.contains("text/html")).unwrap_or(false)
        {
            return Redirect::to("/login").into_response();
        }
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(req).await
}
```

- [ ] **Step 3: Login template + route**

Create `crates/scout-web/src/templates/login.rs`:

```rust
use crate::templates::layout::layout;
use maud::{html, Markup};

pub fn login_page(error: Option<&str>) -> Markup {
    layout("Login", html! {
        h1 { "Sign in" }
        @if let Some(e) = error { article.error { (e) } }
        form method="post" action="/login" {
            label { "Bearer token"
                input type="password" name="token" autofocus required;
            }
            button type="submit" { "Sign in" }
        }
    })
}
```

Create `crates/scout-web/src/routes/login.rs`:

```rust
use crate::app::AppContext;
use crate::auth::{issue_session_cookie, SESSION_COOKIE};
use crate::templates::login::login_page;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Form;
use serde::Deserialize;

pub async fn get_login() -> impl IntoResponse { login_page(None) }

#[derive(Deserialize)]
pub struct LoginForm { pub token: String }

pub async fn post_login(
    State(ctx): State<AppContext>,
    Form(form): Form<LoginForm>,
) -> Response {
    if form.token != *ctx.token {
        return (StatusCode::UNAUTHORIZED, login_page(Some("Invalid token"))).into_response();
    }
    let cookie = issue_session_cookie(&ctx, &form.token);
    let mut headers = HeaderMap::new();
    headers.insert(header::SET_COOKIE, cookie.to_string().parse().unwrap());
    (headers, Redirect::to("/")).into_response()
}

pub async fn post_logout() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        format!("{SESSION_COOKIE}=; Path=/; Max-Age=0").parse().unwrap(),
    );
    (headers, Redirect::to("/login")).into_response()
}
```

- [ ] **Step 4: Wire middleware + routes in `app.rs`**

Replace `router(...)` in `crates/scout-web/src/app.rs`:

```rust
pub fn router(ctx: AppContext) -> Router {
    let public = Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/login", get(routes::login::get_login).post(routes::login::post_login))
        .route("/logout", axum::routing::post(routes::login::post_logout))
        .route("/static/*path", get(serve_static));

    let protected = Router::new()
        .route("/", get(|| async { "INBOX placeholder" }))
        .route("/ingest", axum::routing::post(|| async { axum::http::StatusCode::ACCEPTED }))
        .layer(axum::middleware::from_fn_with_state(ctx.clone(), crate::auth::require_auth));

    public.merge(protected).with_state(ctx)
}
```

Update `mod.rs` files:

`crates/scout-web/src/templates/mod.rs`:
```rust
pub mod layout;
pub mod login;
```

`crates/scout-web/src/routes/mod.rs`:
```rust
pub mod health;
pub mod login;
```

`crates/scout-web/src/lib.rs` — add `pub mod auth;`.

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-web --test auth_test`
Expected: PASS — 3 tests.

```bash
git add crates/scout-web
git commit -m "feat(web): signed-cookie session + bearer middleware + /login form"
```

---

## Task 21: Ingest endpoint

**Files:**
- Create: `crates/scout-web/src/routes/ingest.rs`
- Modify: `crates/scout-web/src/routes/mod.rs`
- Modify: `crates/scout-web/src/app.rs`
- Modify: `crates/scout-web/Cargo.toml` (add `sha2` and `html2md` to deps)
- Create: `crates/scout-web/tests/ingest_test.rs`

The endpoint accepts `{url, title, byline?, content_html, lang?}`, converts HTML → markdown server-side, computes `content_hash`, inserts the article (or returns 200 with `status: "exact_duplicate"`), and enqueues an `extract` job.

- [ ] **Step 1: Add deps**

In root `Cargo.toml` `workspace.dependencies`:

```toml
html2md = "0.2"
sha2 = "0.10"
```

In `crates/scout-web/Cargo.toml` `[dependencies]`:

```toml
html2md.workspace = true
sha2.workspace = true
```

- [ ] **Step 2: Implement the handler**

Create `crates/scout-web/src/routes/ingest.rs`:

```rust
use crate::app::AppContext;
use crate::error::AppResult;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use scout_core::NewArticle;
use scout_storage::{NewJob, StorageError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub url: String,
    pub title: String,
    #[serde(default)]
    pub byline: Option<String>,
    pub content_html: String,
    #[serde(default)]
    pub lang: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub article_id: Uuid,
    pub status: &'static str,
}

fn normalize_md(s: &str) -> String {
    // Cheap normalization: collapse whitespace runs so trivial cosmetic differences
    // produce the same hash.
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub async fn post_ingest(
    State(ctx): State<AppContext>,
    Json(req): Json<IngestRequest>,
) -> AppResult<(StatusCode, Json<IngestResponse>)> {
    let content_md = html2md::parse_html(&req.content_html);
    let normalized = normalize_md(&content_md);
    let content_hash = Sha256::digest(normalized.as_bytes()).to_vec();
    let language = req.lang.unwrap_or_else(|| "en".to_string());

    let new_article = NewArticle {
        url: req.url,
        title: req.title,
        author: req.byline,
        source: "browser-extension".into(),
        language,
        raw_html: Some(req.content_html),
        content_md,
        content_hash,
    };

    match ctx.articles.insert(new_article).await {
        Ok(id) => {
            ctx.jobs.enqueue(NewJob {
                kind: "extract".into(),
                payload: serde_json::json!({"article_id": id}),
            }).await?;
            Ok((StatusCode::ACCEPTED, Json(IngestResponse { article_id: id, status: "pending" })))
        }
        Err(StorageError::Duplicate(_)) => {
            // Recover the existing article id by URL.
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM articles WHERE url = $1 LIMIT 1"
            ).bind(&req_url_from(&ctx, &content_hash).await?).fetch_optional(&ctx.pool).await?;
            let id = existing.ok_or_else(|| anyhow::anyhow!("duplicate but row missing"))?;
            Ok((StatusCode::OK, Json(IngestResponse { article_id: id, status: "exact_duplicate" })))
        }
        Err(StorageError::Db(e)) => Err(anyhow::anyhow!(e).into()),
    }
}

// Helper not strictly necessary; using the original request URL is fine.
async fn req_url_from(_ctx: &AppContext, _hash: &[u8]) -> AppResult<String> {
    // Replaced inline: handler keeps the URL in scope. See ingest.rs commit notes.
    Ok(String::new())
}
```

> **Note:** the helper above is a placeholder shape. The real implementation needs to keep `req.url` in scope to recover the duplicate row. Below is the corrected handler — use this version verbatim:

```rust
pub async fn post_ingest(
    State(ctx): State<AppContext>,
    Json(req): Json<IngestRequest>,
) -> AppResult<(StatusCode, Json<IngestResponse>)> {
    let content_md = html2md::parse_html(&req.content_html);
    let normalized = normalize_md(&content_md);
    let content_hash = Sha256::digest(normalized.as_bytes()).to_vec();
    let language = req.lang.unwrap_or_else(|| "en".to_string());
    let url_for_dup = req.url.clone();
    let hash_for_dup = content_hash.clone();

    let new_article = NewArticle {
        url: req.url, title: req.title, author: req.byline,
        source: "browser-extension".into(),
        language, raw_html: Some(req.content_html),
        content_md, content_hash,
    };

    match ctx.articles.insert(new_article).await {
        Ok(id) => {
            ctx.jobs.enqueue(NewJob {
                kind: "extract".into(),
                payload: serde_json::json!({"article_id": id}),
            }).await?;
            Ok((StatusCode::ACCEPTED, Json(IngestResponse { article_id: id, status: "pending" })))
        }
        Err(StorageError::Duplicate(_)) => {
            let existing: Option<Uuid> = sqlx::query_scalar(
                "SELECT id FROM articles WHERE url = $1 OR content_hash = $2 LIMIT 1"
            ).bind(&url_for_dup).bind(&hash_for_dup)
             .fetch_optional(&ctx.pool).await?;
            let id = existing.ok_or_else(|| anyhow::anyhow!("duplicate but row missing"))?;
            Ok((StatusCode::OK, Json(IngestResponse { article_id: id, status: "exact_duplicate" })))
        }
        Err(StorageError::Db(e)) => Err(anyhow::anyhow!(e).into()),
    }
}
```

Delete the first version; commit only the corrected one. (This is a "tidy" beat — the placeholder helper exists only to illustrate the trap; do not commit it.)

- [ ] **Step 3: Wire route in `app.rs`**

Replace the placeholder ingest route in `router`:

```rust
let protected = Router::new()
    .route("/", get(|| async { "INBOX placeholder" }))
    .route("/ingest", axum::routing::post(routes::ingest::post_ingest))
    .layer(axum::middleware::from_fn_with_state(ctx.clone(), crate::auth::require_auth));
```

Update `routes/mod.rs`:

```rust
pub mod health;
pub mod ingest;
pub mod login;
```

- [ ] **Step 4: Write the integration test**

Create `crates/scout-web/tests/ingest_test.rs`:

```rust
mod common;

#[tokio::test]
async fn ingest_creates_article_and_enqueues_extract_job() {
    let app = common::spawn().await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("{}/ingest", app.url))
        .bearer_auth(&app.token)
        .json(&serde_json::json!({
            "url":"https://a.test/ingest-1",
            "title":"Title",
            "content_html":"<p>hello world</p>",
            "lang":"en"
        })).send().await.unwrap();
    assert_eq!(resp.status(), 202);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "pending");
    assert!(body["article_id"].is_string());
}

#[tokio::test]
async fn ingest_second_time_with_same_url_returns_exact_duplicate() {
    let app = common::spawn().await;
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "url":"https://a.test/ingest-dup",
        "title":"T",
        "content_html":"<p>same</p>",
        "lang":"en"
    });
    let _ = client.post(format!("{}/ingest", app.url))
        .bearer_auth(&app.token).json(&payload).send().await.unwrap();
    let r2 = client.post(format!("{}/ingest", app.url))
        .bearer_auth(&app.token).json(&payload).send().await.unwrap();
    assert_eq!(r2.status(), 200);
    let body: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(body["status"], "exact_duplicate");
}
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-web --test ingest_test`
Expected: PASS — 2 tests.

```bash
git add crates/scout-web Cargo.toml
git commit -m "feat(web): POST /ingest — HTML→MD, content-hash dedup, enqueue extract"
```

---

## Task 22: Inbox route

**Files:**
- Create: `crates/scout-web/src/templates/inbox.rs`
- Create: `crates/scout-web/src/routes/inbox.rs`
- Modify: `crates/scout-web/src/templates/mod.rs`
- Modify: `crates/scout-web/src/routes/mod.rs`
- Modify: `crates/scout-web/src/app.rs`
- Create: `crates/scout-web/tests/inbox_test.rs`

Inbox shows the most recent articles, each with a count of `novel` / `supplement` / `duplicate` claims.

- [ ] **Step 1: Write the failing integration test**

Create `crates/scout-web/tests/inbox_test.rs`:

```rust
mod common;

use scout_core::NewArticle;
use sha2::Digest;

#[tokio::test]
async fn inbox_lists_recent_articles_with_counts() {
    let app = common::spawn().await;

    // Seed: insert one article and one claim, mark novel.
    let pool = common::pool_for(&app);
    let articles = scout_storage::ArticleRepo::new(pool.clone());
    let claims = scout_storage::ClaimRepo::new(pool.clone());
    let aid = articles.insert(NewArticle {
        url: "https://a.test/inbox-1".into(),
        title: "Hello inbox".into(),
        author: None, source: "test".into(), language: "en".into(),
        raw_html: None, content_md: "body".into(),
        content_hash: sha2::Sha256::digest(b"body").to_vec(),
    }).await.unwrap();
    let cid = claims.insert(scout_storage::NewClaim {
        article_id: aid, text: "c".into(), original_quote: "body".into(),
        quote_offset_start: 0, quote_offset_end: 4,
        kind: scout_core::ClaimKind::Fact, language: "en".into(),
    }).await.unwrap();
    claims.set_novelty_novel(cid).await.unwrap();
    articles.set_status(aid, "processed").await.unwrap();

    // Auth + fetch.
    let client = reqwest::Client::builder().cookie_store(true)
        .redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let _ = client.post(format!("{}/login", app.url))
        .form(&[("token", app.token.as_str())]).send().await.unwrap();

    let body = client.get(format!("{}/", app.url)).send().await.unwrap()
        .text().await.unwrap();
    assert!(body.contains("Hello inbox"), "title should appear in inbox");
    assert!(body.contains("1 novel") || body.contains("class=\"badge b-novel\""),
            "should show a novel-count badge");
}
```

You'll need a small extension to `common::TestApp` exposing the underlying pool:

In `crates/scout-web/tests/common/mod.rs`, add:

```rust
pub fn pool_for(app: &TestApp) -> scout_storage::PgPool {
    app._pool.clone()
}
```

(Make `_pool` `pub(crate)` or expose via a getter; simplest is to rename `_pool` → `pool` and mark `pub`.)

- [ ] **Step 2: Write the inbox template**

Create `crates/scout-web/src/templates/inbox.rs`:

```rust
use crate::templates::layout::layout;
use chrono::{DateTime, Utc};
use maud::{html, Markup};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct InboxRow {
    pub article_id: Uuid,
    pub title: String,
    pub source: String,
    pub ingested_at: DateTime<Utc>,
    pub status: String,
    pub novel: i64,
    pub supplement: i64,
    pub duplicate: i64,
}

pub fn inbox_page(rows: &[InboxRow]) -> Markup {
    layout("Inbox", html! {
        h1 { "Inbox" }
        @if rows.is_empty() {
            p { "Nothing ingested yet. POST an article to " code { "/ingest" } "." }
        }
        @for r in rows {
            article {
                header {
                    a href=(format!("/articles/{}", r.article_id)) { strong { (r.title) } }
                    br;
                    small { (r.source) " · " (r.ingested_at.format("%Y-%m-%d %H:%M")) " · " (r.status) }
                }
                p {
                    span.badge.b-novel      { (r.novel) " novel" } " "
                    span.badge.b-supplement { (r.supplement) " supplement" } " "
                    span.badge.b-duplicate  { (r.duplicate) " duplicate" }
                }
            }
        }
    })
}
```

- [ ] **Step 3: Write the inbox route**

Create `crates/scout-web/src/routes/inbox.rs`:

```rust
use crate::app::AppContext;
use crate::error::AppResult;
use crate::templates::inbox::{inbox_page, InboxRow};
use axum::extract::State;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub async fn get_inbox(State(ctx): State<AppContext>) -> AppResult<impl IntoResponse> {
    let rows: Vec<(Uuid, String, String, DateTime<Utc>, String, i64, i64, i64)> =
        sqlx::query_as(
            r#"SELECT a.id, a.title, a.source, a.ingested_at, a.status,
                  count(c.id) FILTER (WHERE c.novelty='novel')      AS novel,
                  count(c.id) FILTER (WHERE c.novelty='supplement') AS supplement,
                  count(c.id) FILTER (WHERE c.novelty='duplicate')  AS duplicate
               FROM articles a
               LEFT JOIN claims c ON c.article_id = a.id
               GROUP BY a.id
               ORDER BY a.ingested_at DESC
               LIMIT 50"#,
        ).fetch_all(&ctx.pool).await?;

    let rows: Vec<InboxRow> = rows.into_iter().map(|r| InboxRow {
        article_id: r.0, title: r.1, source: r.2, ingested_at: r.3,
        status: r.4, novel: r.5, supplement: r.6, duplicate: r.7,
    }).collect();
    Ok(inbox_page(&rows))
}
```

- [ ] **Step 4: Wire into router**

In `crates/scout-web/src/app.rs`, replace the `"/"` placeholder:

```rust
let protected = Router::new()
    .route("/", get(routes::inbox::get_inbox))
    .route("/ingest", axum::routing::post(routes::ingest::post_ingest))
    .layer(axum::middleware::from_fn_with_state(ctx.clone(), crate::auth::require_auth));
```

Update mod files:

`crates/scout-web/src/templates/mod.rs`:
```rust
pub mod inbox;
pub mod layout;
pub mod login;
```

`crates/scout-web/src/routes/mod.rs`:
```rust
pub mod health;
pub mod inbox;
pub mod ingest;
pub mod login;
```

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-web --test inbox_test`
Expected: PASS — 1 test.

```bash
git add crates/scout-web
git commit -m "feat(web): GET / inbox with per-article novel/supplement/duplicate counts"
```

---

## Task 23: Article detail route

**Files:**
- Create: `crates/scout-web/src/templates/article.rs`
- Create: `crates/scout-web/src/routes/article.rs`
- Modify: `crates/scout-web/src/templates/mod.rs`
- Modify: `crates/scout-web/src/routes/mod.rs`
- Modify: `crates/scout-web/src/app.rs`
- Create: `crates/scout-web/tests/article_test.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/scout-web/tests/article_test.rs`:

```rust
mod common;

use scout_core::NewArticle;
use sha2::Digest;

#[tokio::test]
async fn article_page_shows_grouped_claims_with_quotes() {
    let app = common::spawn().await;
    let pool = common::pool_for(&app);
    let articles = scout_storage::ArticleRepo::new(pool.clone());
    let claims = scout_storage::ClaimRepo::new(pool.clone());
    let aid = articles.insert(NewArticle {
        url: "https://a.test/article-1".into(),
        title: "Article Title".into(),
        author: Some("Anon".into()), source: "test".into(),
        language: "en".into(), raw_html: None,
        content_md: "GPT-5 scored 87 on ARC-AGI.".into(),
        content_hash: sha2::Sha256::digest(b"x").to_vec(),
    }).await.unwrap();
    let cid = claims.insert(scout_storage::NewClaim {
        article_id: aid, text: "GPT-5 scored 87 on ARC-AGI".into(),
        original_quote: "GPT-5 scored 87 on ARC-AGI.".into(),
        quote_offset_start: 0, quote_offset_end: 28,
        kind: scout_core::ClaimKind::Data, language: "en".into(),
    }).await.unwrap();
    claims.set_novelty_novel(cid).await.unwrap();

    let client = reqwest::Client::builder().cookie_store(true)
        .redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let _ = client.post(format!("{}/login", app.url))
        .form(&[("token", app.token.as_str())]).send().await.unwrap();
    let body = client.get(format!("{}/articles/{}", app.url, aid)).send().await.unwrap()
        .text().await.unwrap();

    assert!(body.contains("Article Title"));
    assert!(body.contains("GPT-5 scored 87 on ARC-AGI"));
    assert!(body.contains("Novel"));  // section heading
}
```

- [ ] **Step 2: Template**

Create `crates/scout-web/src/templates/article.rs`:

```rust
use crate::templates::layout::layout;
use chrono::{DateTime, Utc};
use maud::{html, Markup};
use scout_core::{ClaimKind, NoveltyTag};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ArticleView {
    pub id: Uuid,
    pub url: String,
    pub title: String,
    pub author: Option<String>,
    pub source: String,
    pub ingested_at: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ClaimView {
    pub id: Uuid,
    pub text: String,
    pub original_quote: String,
    pub kind: ClaimKind,
    pub novelty: NoveltyTag,
}

fn tag_class(kind: ClaimKind) -> &'static str {
    match kind {
        ClaimKind::Fact => "tag-fact",
        ClaimKind::Opinion => "tag-opinion",
        ClaimKind::Data => "tag-data",
        ClaimKind::Prediction => "tag-prediction",
        ClaimKind::Methodology => "tag-methodology",
    }
}

pub fn article_page(article: &ArticleView, claims: &[ClaimView]) -> Markup {
    let (novel, supplement, duplicate): (Vec<_>, Vec<_>, Vec<_>) = {
        let mut n = Vec::new(); let mut s = Vec::new(); let mut d = Vec::new();
        for c in claims {
            match c.novelty {
                NoveltyTag::Novel      => n.push(c),
                NoveltyTag::Supplement => s.push(c),
                NoveltyTag::Duplicate  => d.push(c),
                NoveltyTag::Pending    => {}
            }
        }
        (n, s, d)
    };

    layout(&article.title, html! {
        header {
            h1 { (article.title) }
            small {
                (article.source) " · " (article.ingested_at.format("%Y-%m-%d %H:%M"))
                " · " (article.status) " · "
                a href=(article.url) target="_blank" rel="noreferrer" { "source" }
            }
        }

        section {
            h2 { "Novel (" (novel.len()) ")" }
            @for c in &novel { (claim_card(c)) }
        }
        section {
            h2 { "Supplement (" (supplement.len()) ")" }
            @for c in &supplement { (claim_card(c)) }
        }
        section {
            h2 { "Duplicate (" (duplicate.len()) ")" }
            @for c in &duplicate { (claim_card(c)) }
        }
    })
}

fn claim_card(c: &ClaimView) -> Markup {
    html! {
        div.claim {
            p { (c.text) " " small.{ (tag_class(c.kind)) } { "[" (c.kind.as_db_str()) "]" } }
            details { summary { "original quote" } blockquote { (c.original_quote) } }
        }
    }
}
```

- [ ] **Step 3: Route**

Create `crates/scout-web/src/routes/article.rs`:

```rust
use crate::app::AppContext;
use crate::error::AppResult;
use crate::templates::article::{article_page, ArticleView, ClaimView};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use scout_core::{ClaimKind, NoveltyTag};
use uuid::Uuid;

pub async fn get_article(
    State(ctx): State<AppContext>,
    Path(id): Path<Uuid>,
) -> AppResult<axum::response::Response> {
    let a: Option<(Uuid, String, String, Option<String>, String, DateTime<Utc>, String)> =
        sqlx::query_as(
            "SELECT id, url, title, author, source, ingested_at, status
             FROM articles WHERE id = $1"
        ).bind(id).fetch_optional(&ctx.pool).await?;
    let Some((id, url, title, author, source, ingested_at, status)) = a else {
        return Ok((StatusCode::NOT_FOUND, "not found").into_response());
    };

    let rows: Vec<(Uuid, String, String, String, String)> =
        sqlx::query_as(
            "SELECT id, text, original_quote, kind, novelty
             FROM claims WHERE article_id = $1 ORDER BY created_at"
        ).bind(id).fetch_all(&ctx.pool).await?;

    let claims: Vec<ClaimView> = rows.into_iter().filter_map(|(cid, text, quote, kind_s, nov_s)| {
        let kind = ClaimKind::parse_db(&kind_s)?;
        let novelty = match nov_s.as_str() {
            "novel" => NoveltyTag::Novel,
            "supplement" => NoveltyTag::Supplement,
            "duplicate" => NoveltyTag::Duplicate,
            _ => NoveltyTag::Pending,
        };
        Some(ClaimView { id: cid, text, original_quote: quote, kind, novelty })
    }).collect();

    let view = ArticleView { id, url, title, author, source, ingested_at, status };
    Ok(article_page(&view, &claims).into_response())
}
```

- [ ] **Step 4: Wire route + update mod files**

In `crates/scout-web/src/app.rs`, add to `protected`:

```rust
.route("/articles/:id", get(routes::article::get_article))
```

Update `templates/mod.rs` and `routes/mod.rs` to include `article`.

- [ ] **Step 5: Run + commit**

Run: `cargo test -p scout-web --test article_test`
Expected: PASS — 1 test.

```bash
git add crates/scout-web
git commit -m "feat(web): GET /articles/:id detail with claims grouped by novelty"
```

---

## Task 24: scout-cli — single binary with `web` / `worker` / `migrate` / `reembed` modes

**Files:**
- Create: `crates/scout-cli/Cargo.toml`
- Create: `crates/scout-cli/src/main.rs`
- Create: `crates/scout-cli/src/mode_web.rs`
- Create: `crates/scout-cli/src/mode_worker.rs`
- Create: `crates/scout-cli/src/mode_migrate.rs`
- Create: `crates/scout-cli/src/mode_reembed.rs`
- Create: `crates/scout-cli/src/wire.rs`
- Modify: root `Cargo.toml` (add member)

- [ ] **Step 1: Add crate to workspace**

In root `Cargo.toml`, append `"crates/scout-cli",` to `workspace.members`.

- [ ] **Step 2: Write `crates/scout-cli/Cargo.toml`**

```toml
[package]
name = "scout-cli"
version = "0.0.1"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[[bin]]
name = "scout"
path = "src/main.rs"

[dependencies]
scout-core    = { path = "../scout-core" }
scout-storage = { path = "../scout-storage" }
scout-llm     = { path = "../scout-llm" }
scout-pipeline = { path = "../scout-pipeline" }
scout-web     = { path = "../scout-web" }
anyhow.workspace = true
clap.workspace = true
tokio = { workspace = true, features = ["full"] }
tracing.workspace = true
tracing-subscriber.workspace = true
serde.workspace = true
```

- [ ] **Step 3: Write the wiring helper**

Create `crates/scout-cli/src/wire.rs`:

```rust
use anyhow::Context;
use scout_core::{Embedder, LlmClient};
use scout_storage::{
    ArticleRepo, ClaimEntityRepo, ClaimRepo, EntityRepo, FeedbackRepo, PgJobQueue, PgPool,
};
use scout_web::{AppConfig, AppContext};
use std::sync::Arc;

pub struct Env {
    pub database_url: String,
    pub token: String,
    pub bind: String,
    pub cookie_secret: [u8; 32],
    pub anthropic_key: Option<String>,
    pub openai_key: Option<String>,
}

impl Env {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL")?;
        let token        = std::env::var("SCOUT_TOKEN").context("SCOUT_TOKEN")?;
        let bind         = std::env::var("SCOUT_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
        let secret_hex   = std::env::var("SCOUT_COOKIE_SECRET").context("SCOUT_COOKIE_SECRET")?;
        let secret = decode_secret(&secret_hex)?;
        Ok(Self {
            database_url, token, bind, cookie_secret: secret,
            anthropic_key: std::env::var("ANTHROPIC_API_KEY").ok().filter(|s| !s.is_empty()),
            openai_key:    std::env::var("OPENAI_API_KEY").ok().filter(|s| !s.is_empty()),
        })
    }
}

fn decode_secret(hex: &str) -> anyhow::Result<[u8; 32]> {
    let bytes: Vec<u8> = (0..hex.len()).step_by(2)
        .filter_map(|i| u8::from_str_radix(hex.get(i..i+2)?, 16).ok())
        .collect();
    anyhow::ensure!(bytes.len() == 32, "SCOUT_COOKIE_SECRET must decode to 32 bytes (hex)");
    let mut out = [0u8; 32]; out.copy_from_slice(&bytes); Ok(out)
}

pub fn make_llm(cfg: &AppConfig, env: &Env) -> anyhow::Result<Arc<dyn LlmClient>> {
    match cfg.llm.provider.as_str() {
        "anthropic" => {
            let key = env.anthropic_key.clone().context("ANTHROPIC_API_KEY required")?;
            Ok(Arc::new(scout_llm::AnthropicClient::new(key, cfg.llm.model.clone())))
        }
        "openai" => {
            let key = env.openai_key.clone().context("OPENAI_API_KEY required")?;
            Ok(Arc::new(scout_llm::OpenAiClient::new(key, cfg.llm.model.clone())))
        }
        other => anyhow::bail!("unknown llm provider {other}"),
    }
}

pub fn make_embedder(cfg: &AppConfig, env: &Env) -> anyhow::Result<Arc<dyn Embedder>> {
    match cfg.embedder.provider.as_str() {
        "openai" => {
            let key = env.openai_key.clone().context("OPENAI_API_KEY required")?;
            Ok(Arc::new(scout_llm::OpenAiEmbedder::new(
                key, cfg.embedder.model.clone(), cfg.embedder.dim,
            )))
        }
        other => anyhow::bail!("unknown embedder provider {other}"),
    }
}

pub async fn build_context(pool: PgPool, cfg: AppConfig, env: &Env)
    -> anyhow::Result<AppContext>
{
    let llm = make_llm(&cfg, env)?;
    let embedder = make_embedder(&cfg, env)?;
    Ok(AppContext {
        config: Arc::new(cfg),
        token: Arc::new(env.token.clone()),
        cookie_secret: Arc::new(env.cookie_secret),
        pool: pool.clone(),
        articles: ArticleRepo::new(pool.clone()),
        claims: ClaimRepo::new(pool.clone()),
        entities: EntityRepo::new(pool.clone()),
        claim_entities: ClaimEntityRepo::new(pool.clone()),
        feedback: FeedbackRepo::new(pool.clone()),
        jobs: PgJobQueue::new(pool.clone()),
        llm, embedder,
    })
}
```

- [ ] **Step 4: Write each mode**

`crates/scout-cli/src/mode_migrate.rs`:

```rust
use crate::wire::Env;

pub async fn run() -> anyhow::Result<()> {
    let env = Env::from_env()?;
    let pool = scout_storage::connect(&env.database_url, 4).await?;
    scout_storage::run_migrations(&pool).await?;
    println!("migrations applied");
    Ok(())
}
```

`crates/scout-cli/src/mode_web.rs`:

```rust
use crate::wire::{build_context, Env};
use scout_web::{router, AppConfig};

pub async fn run(config_path: String) -> anyhow::Result<()> {
    let env = Env::from_env()?;
    let cfg = AppConfig::load(&config_path)?;
    let pool = scout_storage::connect(&env.database_url, 8).await?;
    let ctx = build_context(pool, cfg, &env).await?;
    let listener = tokio::net::TcpListener::bind(&env.bind).await?;
    tracing::info!(bind = %env.bind, "scout-web listening");
    axum::serve(listener, router(ctx)).await?;
    Ok(())
}
```

`crates/scout-cli/src/mode_worker.rs`:

```rust
use crate::wire::{build_context, Env};
use async_trait::async_trait;
use scout_core::{Article, Candidate, ClaimKind, EntityHint, EntityRef};
use scout_pipeline::{
    DedupeHandler, DedupeStore, EntityStore, ExtractHandler, ExtractStore,
    LlmClaimExtractor, LlmEntityResolver, NoveltyJudge, PendingClaim,
    ThresholdJudge, Thresholds, Worker,
};
use scout_web::AppConfig;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

struct ExtractAdapter { ctx: scout_web::AppContext }
#[async_trait]
impl ExtractStore for ExtractAdapter {
    async fn get_article(&self, id: Uuid) -> anyhow::Result<Option<Article>> {
        Ok(self.ctx.articles.get(id).await?)
    }
    async fn set_article_status(&self, id: Uuid, s: &str) -> anyhow::Result<()> {
        self.ctx.articles.set_status(id, s).await?; Ok(())
    }
    async fn insert_claim_pending(
        &self, aid: Uuid, t: &str, q: &str, s: i32, e: i32,
        k: ClaimKind, lang: &str, _: &[EntityHint],
    ) -> anyhow::Result<Uuid> {
        Ok(self.ctx.claims.insert(scout_storage::NewClaim {
            article_id: aid, text: t.into(), original_quote: q.into(),
            quote_offset_start: s, quote_offset_end: e, kind: k, language: lang.into(),
        }).await?)
    }
    async fn enqueue_dedupe(&self, aid: Uuid) -> anyhow::Result<()> {
        self.ctx.jobs.enqueue(scout_storage::NewJob {
            kind: "dedupe".into(),
            payload: serde_json::json!({"article_id": aid}),
        }).await?; Ok(())
    }
}

struct DedupeAdapter { ctx: scout_web::AppContext }
#[async_trait]
impl DedupeStore for DedupeAdapter {
    async fn pending_claims_for_article(&self, aid: Uuid) -> anyhow::Result<Vec<PendingClaim>> {
        Ok(self.ctx.claims.pending_for_article(aid).await?.into_iter().map(|p| PendingClaim {
            id: p.id, text: p.text, kind: p.kind, language: p.language,
        }).collect())
    }
    async fn set_embedding(&self, id: Uuid, v: &[f32]) -> anyhow::Result<()> {
        self.ctx.claims.set_embedding(id, v).await?; Ok(())
    }
    async fn ann_candidates(&self, v: &[f32], k: ClaimKind, n: i64) -> anyhow::Result<Vec<Candidate>> {
        Ok(self.ctx.claims.ann_search(v, k, n).await?)
    }
    async fn set_novelty_novel(&self, id: Uuid) -> anyhow::Result<()> {
        self.ctx.claims.set_novelty_novel(id).await?; Ok(())
    }
    async fn set_novelty_duplicate(&self, id: Uuid, d: Uuid) -> anyhow::Result<()> {
        self.ctx.claims.set_novelty_duplicate(id, d).await?; Ok(())
    }
    async fn set_novelty_supplement(&self, id: Uuid, s: Uuid) -> anyhow::Result<()> {
        self.ctx.claims.set_novelty_supplement(id, s).await?; Ok(())
    }
    async fn link_entities(&self, cid: Uuid, refs: &[EntityRef]) -> anyhow::Result<()> {
        for r in refs {
            self.ctx.claim_entities.link(cid, r.entity_id, &r.role).await?;
        }
        Ok(())
    }
    async fn set_article_status(&self, id: Uuid, s: &str) -> anyhow::Result<()> {
        self.ctx.articles.set_status(id, s).await?; Ok(())
    }
}

struct EntityStoreAdapter { ctx: scout_web::AppContext }
#[async_trait]
impl EntityStore for EntityStoreAdapter {
    async fn find_by_alias(&self, name: &str) -> anyhow::Result<Option<Uuid>> {
        Ok(self.ctx.entities.find_by_alias(name).await?)
    }
    async fn upsert(&self, name: &str, kind: &str, aliases: &[String]) -> anyhow::Result<Uuid> {
        Ok(self.ctx.entities.upsert(scout_storage::NewEntity {
            canonical_name: name.into(), kind: kind.into(),
            aliases: aliases.to_vec(),
        }).await?)
    }
}

pub async fn run(config_path: String) -> anyhow::Result<()> {
    let env = Env::from_env()?;
    let cfg = AppConfig::load(&config_path)?;
    let pool = scout_storage::connect(&env.database_url, 8).await?;
    let ctx = build_context(pool, cfg.clone(), &env).await?;

    let extractor = Arc::new(LlmClaimExtractor::new(ctx.llm.clone()));
    let resolver = Arc::new(LlmEntityResolver::new(
        ctx.llm.clone(),
        Arc::new(EntityStoreAdapter { ctx: ctx.clone() }),
    ));
    let judge: Arc<dyn NoveltyJudge> = Arc::new(ThresholdJudge::new(Thresholds::from(&cfg.novelty)));
    let extract = Arc::new(ExtractHandler::new(extractor, Arc::new(ExtractAdapter { ctx: ctx.clone() })));
    let dedupe  = Arc::new(DedupeHandler::new(
        ctx.embedder.clone(), judge, resolver,
        Arc::new(DedupeAdapter { ctx: ctx.clone() }),
    ));

    let worker = Worker::new(
        ctx.jobs.clone(), extract, dedupe,
        Duration::from_millis(cfg.worker.poll_interval_ms),
    );
    let shutdown = worker.shutdown_handle();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("ctrl-c received");
            shutdown.notify_one();
        }
    });
    worker.run().await;
    Ok(())
}
```

`crates/scout-cli/src/mode_reembed.rs`:

```rust
use crate::wire::{build_context, Env};
use scout_web::AppConfig;

pub async fn run(config_path: String) -> anyhow::Result<()> {
    let env = Env::from_env()?;
    let cfg = AppConfig::load(&config_path)?;
    let pool = scout_storage::connect(&env.database_url, 4).await?;
    let ctx = build_context(pool, cfg, &env).await?;

    // Stream-through implementation: fetch all claims, re-embed, write back.
    let rows: Vec<(uuid::Uuid, String)> = sqlx::query_as("SELECT id, text FROM claims")
        .fetch_all(&ctx.pool).await?;
    println!("re-embedding {} claims", rows.len());
    for chunk in rows.chunks(64) {
        let texts: Vec<&str> = chunk.iter().map(|(_, t)| t.as_str()).collect();
        let vecs = ctx.embedder.embed(&texts).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        for ((id, _), v) in chunk.iter().zip(vecs.iter()) {
            ctx.claims.set_embedding(*id, v).await?;
        }
    }
    println!("done");
    Ok(())
}
```

- [ ] **Step 5: Write `main.rs`**

Create `crates/scout-cli/src/main.rs`:

```rust
use clap::{Parser, Subcommand};

mod mode_migrate;
mod mode_reembed;
mod mode_web;
mod mode_worker;
mod wire;

#[derive(Parser)]
#[command(name = "scout", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Web    { #[arg(long, default_value = "config.toml")] config: String },
    Worker { #[arg(long, default_value = "config.toml")] config: String },
    Migrate,
    Reembed{ #[arg(long, default_value = "config.toml")] config: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Cli::parse().cmd {
        Cmd::Web { config }     => mode_web::run(config).await,
        Cmd::Worker { config }  => mode_worker::run(config).await,
        Cmd::Migrate            => mode_migrate::run().await,
        Cmd::Reembed { config } => mode_reembed::run(config).await,
    }
}
```

Add `dotenvy = "0.15"` to root `Cargo.toml` `workspace.dependencies` and to `scout-cli/Cargo.toml` `[dependencies]`:

```toml
dotenvy = "0.15"
```

- [ ] **Step 6: Build + commit**

Run: `cargo build -p scout-cli`
Expected: `Finished`.

Run: `cargo run -p scout-cli -- --help`
Expected: usage with `web`/`worker`/`migrate`/`reembed`.

```bash
git add Cargo.toml crates/scout-cli
git commit -m "feat(cli): scout binary with web/worker/migrate/reembed subcommands"
```

---

## Task 25: README + end-to-end smoke test

**Files:**
- Create: `README.md`
- Create: `scripts/smoke.sh`
- Modify: `.github/workflows/ci.yml` (add Postgres service for sqlx check + tests)

The smoke test is a manual recipe that proves the full vertical slice. It assumes a mock LLM is wired (or a real provider with an API key configured). For CI we keep tests using mocks; the smoke is manual.

- [ ] **Step 1: Write `README.md`**

```markdown
# Scout

Personal article information-filtering tool. Ingests articles, ignores what's already been seen, surfaces what's new — and accumulates a knowledge graph as a side effect.

See:
- Design spec: [`docs/superpowers/specs/2026-05-17-scout-design.md`](docs/superpowers/specs/2026-05-17-scout-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-05-17-scout-backend-mvp.md`](docs/superpowers/plans/2026-05-17-scout-backend-mvp.md)

## Quickstart (development)

1. Bring up Postgres:

   ```bash
   docker compose up -d postgres
   ```

2. Create `.env` from `.env.example`, fill in `SCOUT_TOKEN`, `SCOUT_COOKIE_SECRET` (32 random bytes hex), and provider keys.

3. Apply migrations:

   ```bash
   cargo run -p scout-cli -- migrate
   ```

4. Run web + worker in two terminals:

   ```bash
   cargo run -p scout-cli -- web --config config.toml
   cargo run -p scout-cli -- worker --config config.toml
   ```

5. POST an article:

   ```bash
   curl -X POST http://localhost:8080/ingest \
     -H "Authorization: Bearer $SCOUT_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"url":"https://example.com/a","title":"T","content_html":"<p>hi</p>","lang":"en"}'
   ```

6. Open `http://localhost:8080/` in a browser, sign in with the token, see the article and its extracted novel claims.

## Testing

```bash
cargo test --workspace
```

DB-touching tests use `testcontainers`; Docker must be running locally.

## Methodology

This repo follows Kent Beck's TDD + Tidy First. Commits are prefixed:

- `feat:` / `fix:` — behavior change
- `tidy:` — pure structural change, all tests still pass
- `test:` — tests only
- `docs:` — docs only
- `chore:` — build/deps/CI

Structural and behavioral changes never mix in the same commit.
```

- [ ] **Step 2: Write the smoke script**

Create `scripts/smoke.sh`:

```bash
#!/usr/bin/env bash
# Smoke test: ingest two articles, second is dedup, inbox shows both, article page renders.
# Requires the web + worker to be running with mock or real LLM/embedder.
set -euo pipefail

: "${SCOUT_TOKEN:?must set SCOUT_TOKEN}"
BASE="${BASE:-http://localhost:8080}"

curl -fsS -X POST "$BASE/ingest" \
  -H "Authorization: Bearer $SCOUT_TOKEN" -H "Content-Type: application/json" \
  -d '{"url":"https://example.test/smoke-1","title":"Smoke 1","content_html":"<p>first body</p>","lang":"en"}'
echo

curl -fsS -X POST "$BASE/ingest" \
  -H "Authorization: Bearer $SCOUT_TOKEN" -H "Content-Type: application/json" \
  -d '{"url":"https://example.test/smoke-1","title":"Smoke 1","content_html":"<p>first body</p>","lang":"en"}' | tee /tmp/smoke-dup.json
grep -q exact_duplicate /tmp/smoke-dup.json && echo "OK: dedup recognised"

echo "Open $BASE/ and sign in to view results."
```

```bash
chmod +x scripts/smoke.sh
```

- [ ] **Step 3: Upgrade CI to run DB tests**

Replace `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [master]
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: pgvector/pgvector:pg16
        env:
          POSTGRES_DB: scout
          POSTGRES_USER: scout
          POSTGRES_PASSWORD: scout
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U scout -d scout"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      DATABASE_URL: postgres://scout:scout@localhost:5432/scout
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - uses: baptiste0928/cargo-install@v3
        with: { crate: sqlx-cli, args: --no-default-features --features rustls,postgres }
      - run: sqlx migrate run --source migrations
      - run: cargo sqlx prepare --workspace --check -- --tests
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
```

- [ ] **Step 4: Run full test suite + commit**

Run: `cargo test --workspace`
Expected: all tests pass (this requires Docker running).

```bash
git add README.md scripts/smoke.sh .github/workflows/ci.yml
git commit -m "docs: README + smoke script; chore(ci): run sqlx check + full tests against pgvector"
```

---

## Plan self-review notes

Coverage of the spec sections:

- §3 architecture: Tasks 1, 19, 24 (single binary modes + AppContext + docker-compose).
- §4 data model: Task 5 covers all MVP tables; Tasks 6–9 cover repos for each.
- §5 pipeline: Tasks 16 (extract), 17 (dedupe), 18 (worker loop), 21 (ingest).
- §5.3 threshold table: Task 10 (`ThresholdJudge`) with values from spec; Task 19 surfaces them via `AppConfig`.
- §6 trait boundaries: Task 3 defines all traits; Tasks 11–15 implement the MVP impls; Task 12 (Mock) + 13 (Entity) close the resolver story.
- §7 UI surfaces: Tasks 19 (layout/static), 20 (login + auth), 21 (`/ingest`), 22 (`/`), 23 (`/articles/:id`).
  - **Browser extension** is intentionally out of scope for this plan (see [`docs/superpowers/plans/`](.) Plan 2 placeholder).
  - **Feedback UI buttons** are deferred to Plan 5 — the `feedback` table and `FeedbackRepo` exist in this plan but the buttons are not rendered yet.
  - **Digests** are deferred to Plan 3 — tables exist (added in a follow-up migration when Plan 3 runs).
- §8 project layout: Tasks 1, 4, 10, 11, 19, 24 produce the laid-out crates.
- §9 testing: each task ends with running its tests; Task 25 wires sqlx check + full suite in CI.
- §10 methodology: Tasks 6, 7, etc. illustrate the `tidy:` vs `feat:` split where `.sqlx` regeneration is structural.
- §11 tooling: Task 1 sets fmt/clippy; Task 25 adds sqlx check.

Known intentional gaps left for follow-up plans:
- Digest worker + routes (`/digests`) — Plan 3.
- Search, entity list/detail/merge UI — Plan 4.
- Feedback buttons + calibration tooling — Plan 5.
- Browser extension — Plan 2.

Type/name consistency checked: `ClaimKind`, `NoveltyTag`, `RawClaim`, `Candidate`, `ClaimDraft`, `EntityRef`, `EntityHint`, `PendingClaim`, `NewClaim`, `NewArticle`, `NewEntity`, `NewJob`, `Job`, `ArticleRepo`, `ClaimRepo`, `EntityRepo`, `ClaimEntityRepo`, `FeedbackRepo`, `PgJobQueue`, `LlmClaimExtractor`, `LlmEntityResolver`, `ThresholdJudge`, `Thresholds`, `ExtractHandler`/`ExtractStore`, `DedupeHandler`/`DedupeStore`, `EntityStore`, `Worker`, `AppContext`, `AppConfig`, `AppError` — referenced consistently across tasks.

