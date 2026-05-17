# Scout — Design Spec

- **Date:** 2026-05-17
- **Author:** code41 (with Claude)
- **Status:** Draft, awaiting user review

---

## 1. Purpose

**Scout** is a personal article information-filtering tool. It ingests articles the user reads, ignores information that has already been seen in earlier articles, surfaces what is novel — paired with the original passage that contains it — and accumulates a lightweight personal knowledge graph as a side effect.

The product's value sits squarely on one substrate: **the quality of "is this novel?" judgement**. Everything else (UI, ingestion, digests) is around that core.

### 1.1 User goals

- Stop re-reading the same idea ten times across different sources.
- Get a daily/weekly digest of what is genuinely new in the user's reading flow.
- Over time, build a queryable knowledge graph of claims and entities the user has read about.

### 1.2 Non-goals (MVP)

- Multi-user / collaboration / sharing / commenting.
- Rich-text editing or note-taking.
- Mobile native app.
- Visual knowledge-graph rendering (text search is enough; visualization is a later layer).
- Email/push digest delivery (in-app first).
- Detection-evasion scraping or any aggressive crawling.

---

## 2. Core decisions (recap from brainstorming)

| Topic | Decision |
|---|---|
| Form factor | Self-hosted cloud web service + browser extension capture; other ingestion sources pluggable later |
| Ingestion (MVP) | Browser extension only (bypasses anti-scraping); RSS / file upload / URL paste pluggable later |
| Info granularity | Mixed — atomic claims as the dedup unit, entities extracted from claims form a lightweight KG |
| Novelty rule | Per-kind thresholds: facts strict, opinions loose, three-state outcome (`novel` / `duplicate` / `supplement`) |
| Output UX | Inbox (article list with extracted novel claims) + periodic digest (daily/weekly, grouped by topic/entity) |
| Scale target | ~20–50 articles/day, bilingual zh+en, async job queue, multilingual embeddings |
| Stack | Rust full-stack SSR monolith, no fe/be split, single binary, single Postgres |
| LLM strategy | Multi-provider abstraction behind a `LlmClient` trait; MVP defaults to Anthropic Claude (text) + OpenAI `text-embedding-3-small` (vectors) |
| DB | PostgreSQL + `pgvector`, self-hosted (Hetzner / VPS) |
| Auth | Single-user, bearer-token in env; cookie session for Web, `Authorization` header for extension |
| Methodology | Kent Beck TDD + Tidy First (structural and behavioral commits strictly separated) |
| Implementation approach | **Approach C** — single-pass extraction + threshold-based novelty judge, but bounded by traits (`ClaimExtractor`, `NoveltyJudge`, `EntityResolver`, `LlmClient`, `Embedder`) so future swaps to multi-pass or LLM-verifier are local changes |

---

## 3. Architecture overview

### 3.1 Deployment shape

A **single Rust binary** with three modes:

- `scout web` — Axum HTTP server: serves SSR pages, receives extension uploads, internal API
- `scout worker` — consumes Postgres-backed job queue: extraction, novelty judging, entity resolution, digest generation
- `scout cli` — operational commands: `migrate`, `reprocess <article_id>`, `reembed`, `export`, `debug-pipeline`

One `docker-compose.yml` brings up `scout web`, one or more `scout worker`, and Postgres on a VPS.

### 3.2 Clients

- **Browser extension** (Manifest V3, Chrome/Edge): captures article content via Mozilla Readability, POSTs to `/ingest`.
- **Web UI itself** is SSR'd HTML (Maud templates) with HTMX for progressive interactivity — not a SPA, not "frontend/backend separated".

### 3.3 Auth

- Single bearer token stored in env (`SCOUT_TOKEN`).
- Web `/login` exchanges the token for an HTTP-only signed cookie.
- Extension sends `Authorization: Bearer <token>`.
- One Axum middleware validates either route.

---

## 4. Data model

All in one Postgres database; business data, vectors, full-text index, and the job queue co-located.

### 4.1 `articles`

Raw ingested articles.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `url` | TEXT UNIQUE | Canonical URL after redirect resolution |
| `title` | TEXT | |
| `author` | TEXT NULL | |
| `source` | TEXT NOT NULL | `browser-extension` / `rss:<feed>` / `manual` / ... |
| `language` | TEXT NOT NULL | Detected: `zh`, `en`, ... |
| `raw_html` | TEXT NULL | Original captured HTML (debug / re-parse) |
| `content_md` | TEXT NOT NULL | Cleaned markdown body |
| `content_hash` | BYTEA NOT NULL | SHA-256 of normalized `content_md`; used for exact-dup detection |
| `ingested_at` | TIMESTAMPTZ DEFAULT now() | |
| `processed_at` | TIMESTAMPTZ NULL | NULL = pending or failed |
| `status` | TEXT NOT NULL | `pending` / `processing` / `processed` / `failed` / `exact_duplicate` |
| `error` | TEXT NULL | Last failure detail |

Indexes: `(content_hash)` unique partial where status != 'exact_duplicate'; `(status, ingested_at)` for queue/listing.

### 4.2 `claims`

The dedup substrate.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `article_id` | UUID FK → articles ON DELETE CASCADE | |
| `text` | TEXT NOT NULL | LLM-normalized canonical phrasing |
| `original_quote` | TEXT NOT NULL | Verbatim passage from `content_md` |
| `quote_offset_start` | INT NOT NULL | Char offset in `content_md` |
| `quote_offset_end` | INT NOT NULL | |
| `kind` | TEXT NOT NULL | `fact` / `opinion` / `data` / `prediction` / `methodology` |
| `language` | TEXT NOT NULL | |
| `embedding` | vector(1536) | One system-wide embedder; dim locked at deploy |
| `novelty` | TEXT NOT NULL | `pending` / `novel` / `duplicate` / `supplement` |
| `dup_of` | UUID FK → claims NULL | Set when `novelty='duplicate'` |
| `supplement_of` | UUID FK → claims NULL | Set when `novelty='supplement'` |
| `created_at` | TIMESTAMPTZ DEFAULT now() | |

Indexes:
- HNSW `(embedding vector_cosine_ops)` for ANN search
- `(article_id)`
- `(novelty, kind, created_at)` for inbox and digest queries

### 4.3 `entities`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `canonical_name` | TEXT NOT NULL | |
| `kind` | TEXT NOT NULL | `person` / `company` / `product` / `concept` / `place` / `other` |
| `aliases` | TEXT[] | All known surface forms |
| `embedding` | vector(1536) | For alias matching against new mentions |
| `created_at` | TIMESTAMPTZ DEFAULT now() | |

Unique `(canonical_name, kind)`.

### 4.4 `claim_entities`

| Column | Type | Notes |
|---|---|---|
| `claim_id` | UUID FK → claims ON DELETE CASCADE | |
| `entity_id` | UUID FK → entities | |
| `role` | TEXT NOT NULL | `subject` / `object` / `mention` |

PK `(claim_id, entity_id, role)`.

### 4.5 `jobs`

Postgres-backed queue. Consumed via `SELECT ... FOR UPDATE SKIP LOCKED`.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `kind` | TEXT NOT NULL | `extract` / `dedupe` / `digest` |
| `payload` | JSONB NOT NULL | Kind-specific |
| `status` | TEXT NOT NULL | `pending` / `running` / `done` / `failed` |
| `attempts` | INT NOT NULL DEFAULT 0 | |
| `max_attempts` | INT NOT NULL DEFAULT 5 | |
| `scheduled_at` | TIMESTAMPTZ NOT NULL DEFAULT now() | |
| `picked_at` | TIMESTAMPTZ NULL | |
| `finished_at` | TIMESTAMPTZ NULL | |
| `error` | TEXT NULL | Last failure |

Partial index `(scheduled_at) WHERE status='pending'`.

### 4.6 `digests` + `digest_items`

`digests`: `id`, `period` (`daily`/`weekly`), `period_start`, `period_end`, `generated_at`, `summary_md`.

`digest_items`: `digest_id`, `claim_id`, `topic`, `rank`.

### 4.7 `feedback`

Powers the dogfood quality loop and future calibration / verifier few-shots.

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `claim_id` | UUID FK → claims NULL | |
| `entity_id` | UUID FK → entities NULL | |
| `action` | TEXT NOT NULL | `mark_duplicate` / `mark_not_duplicate` / `merge_into` / `merge_entity` / `change_kind` / ... |
| `target_id` | UUID NULL | When the action references another claim/entity (e.g. `merge_into`) |
| `note` | TEXT NULL | Free-text |
| `created_at` | TIMESTAMPTZ DEFAULT now() | |

### 4.8 Embedding policy

One deployment ⇒ one system embedder ⇒ one vector dim. Switching embedders is an offline `scout cli reembed` operation that re-vectorizes all `claims` and `entities`. The multi-provider abstraction applies to *text generation*; embedding choice is a deploy-time decision.

---

## 5. Processing pipeline

### 5.1 Ingest (synchronous HTTP)

```
POST /ingest  (Authorization: Bearer …)
body: { url, title, byline?, content_html, lang? }

1. Server cleans content_html → content_md (server-side cleanup, not just trust extension)
2. Compute content_hash = sha256(normalize(content_md))
3. If articles row exists with same url OR same content_hash → mark exact_duplicate, return 200 { article_id, status: 'exact_duplicate' }
4. Else insert articles(status='pending')
5. Enqueue jobs(kind='extract', payload={article_id})
6. Return 202 { article_id, status: 'pending' }
```

### 5.2 Extract (worker)

```
extract job (article_id):
  article = ArticleRepo.get(article_id)
  raw_claims = ClaimExtractor.extract(article)         // single-pass LLM call
  for rc in raw_claims:
     ClaimRepo.insert(article_id, text=rc.text, kind=rc.kind,
                      original_quote=rc.quote, offsets=…,
                      language=rc.lang, novelty='pending')
  enqueue jobs(kind='dedupe', payload={article_id})
  ArticleRepo.set_status(article_id, 'processing')
```

`ClaimExtractor.extract` returns `RawClaim { text, kind, quote, quote_offset_start, quote_offset_end, language, entity_hints: Vec<EntityHint> }`. The MVP implementation is one LLM call with a structured-output prompt that yields all claims for the article at once. The trait permits a future `MultiPassExtractor` (chunk → claims → classify → entities) without touching callers.

### 5.3 Dedupe (worker)

```
dedupe job (article_id):
  claims = ClaimRepo.pending_for_article(article_id)
  vectors = Embedder.embed(claims.text)               // batched
  for (claim, vec) in zip:
     ClaimRepo.set_embedding(claim.id, vec)
     candidates = ClaimRepo.ann_search(vec, kind=claim.kind, k=20)
     novelty = NoveltyJudge.judge(claim, candidates)
     ClaimRepo.set_novelty(claim.id, novelty)
     entities = EntityResolver.resolve(claim, claim.entity_hints)
     ClaimEntityRepo.link(claim.id, entities)
  ArticleRepo.set_status(article_id, 'processed')
```

`NoveltyJudge` MVP (`ThresholdJudge`) — pure cosine threshold, per-kind:

| Kind | duplicate iff sim ≥ | supplement window |
|---|---|---|
| `fact`, `data` | 0.92 | 0.85 ≤ sim < 0.92 **and** new entity/number token present |
| `opinion`, `prediction` | 0.96 | 0.88 ≤ sim < 0.96 **and** new entity present |
| `methodology` | 0.93 | 0.86 ≤ sim < 0.93 **and** new entity present |

These thresholds are **placeholders**; spec requires calibration after ~200 articles of dogfooding. They live in `config.toml` and are tunable from `/settings`.

`EntityResolver` MVP — LLM call with entity hints + alias-embedding lookup. New entities get inserted; matches reuse existing.

### 5.4 Digest (cron, worker)

```
digest job (period, start, end):
  claims = ClaimRepo.in_period(start, end, novelty in ('novel','supplement'))
  topics = LlmClient.complete(group_by_topic_prompt(claims))   // returns topic labels per claim
  narrative = LlmClient.complete(narrative_prompt(grouped))
  DigestRepo.insert(period, start, end, narrative)
  DigestItemRepo.insert_many(...)
```

Scheduled daily at 06:00 local and weekly Monday 06:00 local.

### 5.5 Failure handling

- Job failure → `attempts++`, exponential backoff (`scheduled_at = now() + 2^attempts minutes`), max 5 attempts then `status='failed'` and an `articles.status='failed'` if relevant.
- LLM/Embedder transient errors (rate limit, 5xx) → retried via job mechanism.
- LLM structured-output parse errors → caught, recorded in `jobs.error`, retried; persistent → `failed` and the article shows up under "needs attention" in `/settings`.
- Partial extraction failure (some claims OK, some malformed) → keep what parsed, log rest.

---

## 6. Trait boundaries (the "Approach C" upgrade hooks)

Defined in `scout-core` (no IO):

```rust
pub trait LlmClient: Send + Sync {
    async fn complete(&self, req: ChatRequest) -> Result<ChatResponse>;
}

pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>>;
}

pub trait ClaimExtractor: Send + Sync {
    async fn extract(&self, article: &Article) -> Result<Vec<RawClaim>>;
}

pub trait NoveltyJudge: Send + Sync {
    fn judge(&self, claim: &ClaimDraft, candidates: &[Candidate]) -> Novelty;
}

pub trait EntityResolver: Send + Sync {
    async fn resolve(&self, claim: &ClaimDraft, hints: &[EntityHint]) -> Result<Vec<EntityRef>>;
}

pub trait JobQueue: Send + Sync {
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;
    async fn dequeue(&self, kinds: &[&str]) -> Result<Option<Job>>;
    async fn ack(&self, id: JobId) -> Result<()>;
    async fn fail(&self, id: JobId, err: &str) -> Result<()>;
}

pub trait ArticleRepo / ClaimRepo / EntityRepo / DigestRepo / FeedbackRepo
    // typed sqlx wrappers, also traits so flows are unit-testable
```

**MVP implementations**:

| Trait | Impl | Notes |
|---|---|---|
| `LlmClient` | `AnthropicClient`, `OpenAiClient` | Chosen via config |
| `Embedder` | `OpenAiEmbedder` | `text-embedding-3-small`, dim 1536 |
| `ClaimExtractor` | `LlmClaimExtractor` | Single-pass structured-output prompt over `LlmClient` |
| `NoveltyJudge` | `ThresholdJudge` | Per-kind cosine thresholds (see §5.3) |
| `EntityResolver` | `LlmEntityResolver` | LLM + alias-embedding lookup |
| `JobQueue` | `PgJobQueue` | `FOR UPDATE SKIP LOCKED` |
| Repos | `Pg*Repo` | sqlx with compile-time-checked queries |

**Future upgrades** (no caller changes required): `GeminiClient`, `OllamaClient`, `VoyageEmbedder`, `MultiPassExtractor`, `LlmVerifierJudge`, `HybridJudge` (threshold pre-filter + LLM verifier on borderline).

Configuration is loaded at startup (`config.toml` + env overrides) and wires traits into a single `AppContext`.

---

## 7. UI surfaces

### 7.1 Browser extension (MVP)

- Manifest V3, Chrome/Edge.
- Content script: Mozilla Readability → `{ title, byline, content_html, url, lang }`.
- Triggers: toolbar button, right-click "Send to Scout", default shortcut `Ctrl+Shift+S`.
- Settings popup: server URL + bearer token, stored in `chrome.storage.sync`.
- Visual feedback: badge `✓` on success, `!` on failure with toast.
- Offline queue: failed requests go to `chrome.storage.local`, drained on next successful handshake.

### 7.2 Web UI

SSR with Maud templates + minimal hand-written CSS on a Pico.css base. HTMX for live interactions (mark duplicate, merge entity, regenerate digest).

| Route | Purpose |
|---|---|
| `GET /login` | Login form (bearer token input) |
| `POST /login` | Bearer token → signed cookie, redirect `/` |
| `GET /` | Inbox: recent articles, each `[title] · source · time · 🟢N novel · 🟡N supplement · ⚪N duplicate` |
| `GET /articles/:id` | Article detail: metadata + source link; below it, claims grouped by novelty with `text`, expandable `original_quote`, entity chips, feedback buttons |
| `GET /digests` | List of past digests |
| `GET /digests/:id` | Digest detail: narrative + grouped claims; each links back to source article |
| `GET /entities` | Entity list by frequency |
| `GET /entities/:id` | Claims mentioning it + co-occurring entities |
| `GET /search?q=...` | Hybrid: full-text + vector similarity over claims & entities |
| `GET /settings` | Threshold tuning, embedder status, `reembed` trigger, failed-article queue |

### 7.3 Feedback loop (in MVP)

Per-claim buttons: `✗ not duplicate`, `✓ confirm duplicate`, `→ merge into …`.
Per-entity action: `merge into …` (entity disambiguation).
Writes to `feedback` table; consumed later for threshold calibration and to seed few-shots when `LlmVerifierJudge` lands.

---

## 8. Project layout

```
scout/
├── Cargo.toml                    # workspace
├── crates/
│   ├── scout-core/               # domain types, traits, no IO
│   ├── scout-llm/                # LlmClient + Embedder implementations
│   ├── scout-pipeline/           # ClaimExtractor / NoveltyJudge / EntityResolver impls
│   ├── scout-storage/            # sqlx repos, PgJobQueue, migrations runner
│   ├── scout-web/                # Axum + Maud + HTMX + asset bundle
│   └── scout-cli/                # binary entry: dispatches web / worker / cli modes
├── extension/                    # browser extension, separate TS project
├── migrations/                   # sqlx migration files
├── docs/
│   ├── superpowers/specs/        # design docs
│   ├── superpowers/plans/        # implementation plans
│   └── adr/                      # architecture decision records
├── docker-compose.yml            # same file used in dev and prod
├── config.example.toml
├── rust-toolchain.toml
└── .github/workflows/ci.yml
```

`scout-core` has no IO and no dependencies on the other crates — easy to unit-test with mock impls. All other crates depend on it.

---

## 9. Testing strategy

| Layer | Tooling | Scope | Speed |
|---|---|---|---|
| Unit | `cargo test`, mocked traits | Internal logic of each impl, prompt assembly, threshold math, offset computation | ms |
| Integration | `testcontainers` Postgres | sqlx repos, `PgJobQueue` SKIP LOCKED behaviour, migrations | s |
| Pipeline | mock LLM/Embedder + real Postgres | End-to-end "ingest one article → assert claims/entities/novelty in DB" | s |
| Contract | `wiremock`-recorded LLM responses | Each `LlmClient` impl conforms to prompt expectations | s |
| Manual e2e | Real LLM + real extension + real Web | Pre-release smoke | minutes |

`MockLlmClient`: maps `(prompt_pattern → response)`, routes by keyword match. Flexible enough to cover both extractor and resolver flows in tests.

---

## 10. Development methodology (TDD + Tidy First)

Every change follows the cycle:

1. Write a failing test that describes the smallest next behavior.
2. `cargo test` — red.
3. Minimum implementation to green.
4. Refactor in the green state.
5. Commit. **Structural changes commit separately from behavioral changes.**

### Commit prefixes

| Prefix | Means |
|---|---|
| `tidy:` | Pure structural — renaming, extracting, moving, formatting, no behavior change. All tests still pass before and after. |
| `feat:` | New behavior. |
| `fix:` | Bug fix (behavior change). |
| `test:` | Tests only. |
| `docs:` | Documentation / spec / ADR. |
| `chore:` | Build, deps, CI config. |

**Hard rule**: a commit is either pure structural or pure behavioral, never both. Reviewing your own diff before commit and splitting if necessary is part of the loop.

---

## 11. Tooling baseline

- **Toolchain pinning**: `rust-toolchain.toml` locks Rust to stable latest.
- **Formatting / lint**: `rustfmt` + `clippy -- -D warnings` enforced in CI.
- **DB migrations**: `sqlx migrate`; `query!` macro for compile-time-checked SQL; CI runs `cargo sqlx prepare --check`.
- **Secrets**: `.env` for local; production env injected by deploy. No secrets in repo.
- **Logging**: `tracing` with JSON output in prod, pretty in dev.
- **Observability (deferred)**: stdout structured logs are MVP; OpenTelemetry export is a later concern.
- **CI**: GitHub Actions — `fmt + clippy + test + sqlx check`. Green required to merge to `master`.
- **Configuration**: `config.toml` (committed example) + env overrides for secrets; loaded once at startup into `AppContext`.

---

## 12. Documentation conventions

- Design specs: `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`
- Implementation plans: `docs/superpowers/plans/YYYY-MM-DD-<topic>-plan.md`
- ADRs for non-trivial architectural decisions: `docs/adr/NNNN-<title>.md` (lightweight: Context / Decision / Consequences)
- README kept minimal and pointing into `docs/`.

---

## 13. Open items to calibrate post-MVP

- Threshold values per kind (§5.3) — calibrate after ~200 articles dogfooded, using `feedback` table.
- Whether to introduce `LlmVerifierJudge` — decide based on the false-positive/false-negative rate observed via feedback.
- Digest grouping strategy — start with LLM-generated topic labels; revisit if labels prove unstable across runs.
- Entity merging policy — MVP is manual via UI; auto-merge above an alias-embedding similarity threshold can come later.

---

## 14. Out of scope (explicitly)

- Any scraping that bypasses paywalls or login walls; extension only captures what the user is currently viewing in their own browser session.
- Hosting other people's data; this is a single-user self-hosted tool.
- Replacing read-later apps; Scout doesn't store articles to re-read, it stores what's *new* in them.
