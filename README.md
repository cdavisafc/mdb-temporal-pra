# MongoDB × Temporal — Partner Reference Architecture

A production-grade reference implementation that shows how **Temporal** and **MongoDB Atlas** work
together to build a durable, change-driven RAG pipeline with a deep-agent chat interface.

> **Developers:** see [docs/RUNBOOK.md](docs/RUNBOOK.md) for prerequisites, API key setup,
> local spin-up, and cloud infra references.

---

## What is Temporal?

[Temporal](https://temporal.io) is a **durable execution platform**. It orchestrates long-running
workflows as code — with automatic retries, checkpointing, and resume-on-failure built in. You
write plain Python functions; Temporal ensures they run to completion even across crashes, deploys,
or network partitions.

In this architecture Temporal owns two critical concerns:

| Concern            | What Temporal guarantees                                                                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ingestion pipeline | A crash mid-embedding resumes from the last completed chunk — never re-embeds what is already done ([durable execution](https://docs.temporal.io/evaluate/major-advantages#fault-oblivious-code)) |
| Agent workflows    | Multi-step agent plans are durable; a failure mid-conversation resumes without losing tool results or memory writes ([workflows as code](https://docs.temporal.io/workflows))                     |

---

## The problem this solves

Customers hand-roll resilient ingestion/embedding pipelines and it hurts
(source: [MongoDB × Temporal proposal](https://docs.google.com/document/d/1pReiGwWCwFj28nWsZ6NiCA9nWrqhcaWgF51s_odUeCs/edit?tab=t.0#heading=h.54b4x1c9rtcf)):

| Customer     | Pain hand-rolled without Temporal                                        |
| ------------ | ------------------------------------------------------------------------ |
| Regilient AI | MD5 change-tracking in production to decide what to re-embed             |
| Glassdoor    | A homegrown "lambda clock" cron to generate embeddings                   |
| Carrier      | A FastAPI pipeline, hand-tuning sequential vs. parallel                  |
| Emerald X    | A 5-hour import that fails on the last step **reruns the entire import** |

This PRA packages the pattern that removes that pain — already in production at DEA Technology,
100ms, Chess.com, and C.R. England.

---

## Partner Solutions Architecture

### High-level design

```mermaid
flowchart LR
    src[("Data sources<br/>S3 / object store")]
    user([User])
    web[["Web"]]
    voyage[["Voyage AI<br/>embeddings + rerank"]]
    openai[["OpenAI<br/>agent model"]]

    subgraph temporal["Temporal — durable compute"]
      direction TB
      ingest["Ingestion workflow<br/>chunk + embed"]
      agent["Research agent<br/>durable loop"]
    end

    atlas[("MongoDB Atlas<br/>vector store + agent state")]

    src -->|"object-created event"| ingest
    ingest -.->|"embed"| voyage
    ingest -->|"write vectors"| atlas

    user -->|"question"| agent
    agent -->|"answer"| user
    agent -->|"vector search"| atlas
    agent -.->|"rerank"| voyage
    agent -.->|"web search"| web
    openai -.->|"reasoning"| agent
```

**How to read it:**

1. **A new object lands in object storage** (AWS S3, or MinIO locally).
2. **The object-created event starts the Temporal `IngestWorkflow` directly** — via an AWS
   Lambda for real S3, or a MinIO webhook locally. Both call the same handler
   (`pipeline/lambda_handler.py` / `POST /ingest-event`).
3. **Temporal** chunks the content, calls **Voyage AI** for embeddings, and upserts into
   **Atlas Search**.
4. A **durable research agent** (OpenAI Agents SDK, running as a Temporal workflow) answers
   questions over the fresh knowledge, using vector search + rerank (and web search) as tools.

> **Design note:** the direct trigger (Lambda / MinIO webhook → `IngestWorkflow`) leverages 
> Temporal's durable execution to provide the "don't lose the
> event once the workflow starts" guarantee. 

### Division of responsibility

| Concern                                                       | Owner                 |
| ------------------------------------------------------------- | --------------------- |
| Orchestration, retries, checkpointing, backfill, resumability | **Temporal**          |
| Operational data, vector index, agent memory & state          | **MongoDB Atlas**     |
| Embeddings & reranking                                        | **MongoDB Voyage AI** |
| Agent reasoning & answers                                     | **OpenAI (Agents SDK)** |

---

## System architecture

```mermaid
flowchart TB
    user([User]) --> ui["React UI (:5173)"]
    minio[("S3 / MinIO")]

    subgraph trig["Trigger · S3 ObjectCreated"]
      lam["AWS Lambda (prod)"]
      hook["MinIO webhook →<br/>/ingest-event (local)"]
    end
    minio --> lam
    minio --> hook

    ui -->|"POST /research + poll"| api["Agent API<br/>FastAPI (:8090)"]

    subgraph worker["Temporal worker · queue 'temporal-pipeline'"]
      iw["IngestWorkflow<br/>fetch → chunk → embed(∥) → index"]
      bw["BackfillWorkflow<br/>re-embed → knowledge_v2"]
      da["DeepResearchAgent<br/>OpenAI Agents SDK loop"]
    end

    lam -->|"start_workflow"| iw
    hook -->|"start_workflow"| iw
    api -->|"start + query progress"| da

    subgraph atlas["MongoDB Atlas"]
      staging[("chunks_staging")]
      know[("knowledge<br/>+ vector index")]
      knowv2[("knowledge_v2")]
      cfg[("temporal_config")]
    end

    voyage[["Voyage AI<br/>voyage-3.5 + rerank-2.5"]]
    openai[["OpenAI<br/>agent model + web search"]]

    iw -.->|"embed"| voyage
    iw -->|"stage"| staging
    iw -->|"index"| know
    bw --> knowv2
    da -.->|"vector_search"| know
    da -.->|"rerank"| voyage
    da -.->|"reason + web"| openai
    da -.->|"active pointer"| cfg
```

### Atlas data model

```text
Database: temporal
├── chunks_staging       ← intermediate chunks during IngestWorkflow
├── knowledge            ← embedded docs + Atlas Vector Search index (active)
├── knowledge_v2         ← BackfillWorkflow writes here on model upgrade (blue/green)
├── temporal_config      ← active collection/index pointer (flipped by cutover)
└── agent_memory         ← reserved for agent memory (not yet written)
```

Retrieval and (future) agent memory live in the **same database** — no second copy, and no sync
lag between what the pipeline writes and what the agent reads.

---

## Ingestion

Ingestion turns objects landing in storage into embedded, searchable knowledge — durably, and
**without a message broker**. An S3 **ObjectCreated** event starts a Temporal `IngestWorkflow`
directly (an AWS Lambda in production, a MinIO webhook locally — both through the shared
`handle_s3_event`). The moment `start_workflow` returns, the change is safe: Temporal runs the
workflow to completion across retries, worker restarts, and infra maintenance.

- **Trigger goes directly to Temporal** Temporal's durable execution provides the "don't lose the 
  event" guarantee; the trigger is a thin adapter (`pipeline/lambda_handler.py` /
  `POST /ingest-event`).
- **Idempotent, update-in-place.** A content-hash check skips re-embedding unchanged objects; an
  edited object re-embeds and upserts in place (see the two-hashes note below).
- **Parallel & scale-out.** Chunks embed in parallel waves; add worker processes on the same task
  queue to scale horizontally.
- **Output.** Embedded chunks land in `knowledge` with an Atlas Vector Search index, ready for the
  agent. Internals: `docs/LLD.md` §5–6.

### Ingest workflow

```mermaid
flowchart TB
    ev(["S3 ObjectCreated event"]) --> trig["Lambda / MinIO webhook<br/>handle_s3_event → start_ingest"]
    trig -->|"start_workflow · TERMINATE_EXISTING<br/>workflow id = ingest-sha1(s3_uri)  (per object key)"| s1

    subgraph wf["IngestWorkflow (Temporal)"]
      direction TB
      s1["Stage 1 · fetch_and_stage_chunks<br/>GET object · doc_content_hash = sha256(bytes) · extract + chunk"]
      hash{"doc_content_hash<br/>already in knowledge?<br/>(content hash, not the URI hash)"}
      s2["Stage 2 · embed_staged_chunk<br/>parallel waves of 10"]
      s3["Stage 3 · index_document<br/>upsert · prune stale · ensure vector index"]
      s1 --> hash
      hash -->|"yes — same bytes"| done1(["done · unchanged"])
      hash -->|"no — new / changed bytes"| s2 --> s3 --> done2(["done · indexed"])
    end

    s1 -.->|"GET"| store[("S3 / MinIO")]
    s1 -->|"stage chunks"| staging[("chunks_staging")]
    s2 -.->|"embed"| voyage[["Voyage voyage-3.5"]]
    s2 -->|"update"| staging
    s3 -->|"upsert"| know[("knowledge + vector index")]
```

> **Two hashes, two jobs.** The **workflow id** hashes the *URI* — `sha1(s3_uri)` — so it's
> stable per object key: a re-upload reuses the id, and `TERMINATE_EXISTING` replaces any
> in-flight run. The **dedupe check** hashes the *content* — `doc_content_hash = sha256(bytes)`
> — so an *edited* file (new bytes) misses the check and is re-embedded, while an *unchanged*
> re-upload matches and short-circuits.

### Sequence — ingestion

```mermaid
sequenceDiagram
    actor Uploader
    participant Store as S3 / MinIO
    participant Trig as Trigger (Lambda / webhook)
    participant WF as IngestWorkflow (Temporal)
    participant Atlas as MongoDB Atlas
    participant Voyage

    Uploader->>Store: upload object (key)
    Store->>Trig: ObjectCreated event
    Trig->>WF: start_workflow (id = ingest-sha1(uri), TERMINATE_EXISTING)
    Trig-->>Store: ack (returns immediately)

    Note over WF: Stage 1 · fetch_and_stage_chunks
    WF->>Store: GET object
    Store-->>WF: bytes
    Note over WF: doc_content_hash = sha256(bytes)
    WF->>Atlas: look up knowledge by doc_id + doc_content_hash
    alt content unchanged (hash matches)
        Atlas-->>WF: match found
        Note over WF: done · unchanged (skip embed + index)
    else new or changed content
        Atlas-->>WF: no match
        WF->>Atlas: insert chunks into chunks_staging
        Note over WF: Stage 2 · embed_staged_chunk (parallel waves of 10)
        loop per chunk
            WF->>Voyage: embed(text)
            Voyage-->>WF: vector
            WF->>Atlas: update chunk (status embedded)
        end
        Note over WF: Stage 3 · index_document
        WF->>Atlas: upsert chunks into knowledge · prune stale · ensure vector index
        Note over WF: done · indexed
    end
```

---

## The agent

The agent answers questions over the ingested knowledge as a **durable Temporal workflow**
(`DeepResearchAgent`), built with the **OpenAI Agents SDK**. Instead of a fixed
retrieve → rerank → answer chain, the model is handed the retrieval pipeline as **tools** and
decides which to call, and how often.

- **Tools.** `vector_search` and `rerank` are Temporal activities over Atlas + Voyage, plus a
  hosted **web search** to supplement the corpus.
- **Durable & auditable.** The reasoning loop *is* a workflow, so every model and tool call is a
  history event — resumable after a crash and fully inspectable in the Temporal UI.
- **Live progress.** Run hooks record human-readable steps; the UI starts the run
  (`POST /research`) and polls a workflow `query` (`GET /research/{id}`) to show the trace as it
  unfolds (step-level, not token streaming).
- **Opt-in.** Loads only when `OPENAI_API_KEY` is set — ingestion runs without it. Full design:
  `docs/agent-retrieval.md`.

### Sequence — research query

```mermaid
sequenceDiagram
    actor User
    participant UI as React UI
    participant API as Agent API
    participant WF as DeepResearchAgent (Temporal)
    participant OAI as OpenAI model
    participant Tools as vector_search / rerank (activities)
    participant Atlas as MongoDB Atlas
    participant Voyage

    User->>UI: ask question
    UI->>API: POST /research {query}
    API->>WF: start_workflow → workflow_id
    API-->>UI: {workflow_id}

    loop agent loop (model decides tools)
        WF->>OAI: model turn (activity, may web-search)
        OAI-->>WF: tool call(s) or final answer
        opt retrieve
            WF->>Tools: vector_search(query)
            Tools->>Voyage: embed query
            Tools->>Atlas: $vectorSearch (active collection)
            Atlas-->>Tools: candidate chunks
            Tools-->>WF: chunks
        end
        opt prioritize
            WF->>Tools: rerank(chunk_ids)
            Tools->>Voyage: rerank-2.5
            Tools-->>WF: top chunks
        end
    end

    loop UI polling (~600 ms)
        UI->>API: GET /research/{id}
        API->>WF: query "progress"
        WF-->>API: steps / answer / done
        API-->>UI: progress
    end
    UI-->>User: cited answer + step trace
```

---

## Quickstart (local demo)

```bash
# 1. Clone and enter the repo
git clone https://github.com/suresharam/mongodb-temporal-sa-pra.git
cd mongodb-temporal-sa-pra

# 2. Copy and fill in credentials
cp .env.example .env
# Edit .env: set MONGODB_URI, VOYAGE_API_KEY, OPENAI_API_KEY

# 3. Install all dependencies (Python + UI)
make setup

# 4. Start everything (MinIO, Temporal, worker, trigger API, agent API + UI)
make start

# 5. Create the Atlas Vector Search index (one-time)
make index

# 6. Seed a sample document to trigger the full pipeline
make seed

# 7. Open the agent UI
open http://localhost:5173

# 8. Tear everything down
make stop
```

`make help` lists all available targets.

| Service           | URL                   | Login                                          |
| ----------------- | --------------------- | ---------------------------------------------- |
| Agent chat UI     | http://localhost:5173 |                                                |
| Temporal Web UI   | http://localhost:8233 |                                                |
| Agent API         | http://localhost:8090 |                                                |
| MinIO console     | http://localhost:9001 | username: `minioadmin`, password: `minioadmin` |
| Trigger API       | http://localhost:8088 | webhook `/ingest-event` (default local trigger) |

---

## Repo layout

```text
mongodb-temporal-sa-pra/
├── README.md
├── Makefile                        ← all dev commands (make help)
├── pyproject.toml                  ← Python deps managed by uv
├── .env.example                    ← copy → .env, fill credentials
├── tests/                          ← pytest (uv run pytest) — handler + parser tests
├── agent/
│   ├── api.py                      ← FastAPI: /research start + poll (:8090)
│   ├── agent_workflow.py           ← DeepResearchAgent (OpenAI Agents SDK loop as workflow)
│   ├── tools.py                    ← agent tools: vector_search, rerank (as activities)
│   └── ui/                         ← React/Vite chat UI (:5173)
├── pipeline/
│   ├── worker.py                   ← Temporal worker process
│   ├── trigger.py                  ← shared handle_s3_event → start IngestWorkflow
│   ├── trigger_api.py              ← webhook /ingest-event + manual /ingest-trigger
│   ├── lambda_handler.py           ← AWS Lambda entrypoint for real S3 (same handler)
│   ├── workflows/
│   │   ├── ingest_workflow.py      ← IngestWorkflow: fetch → chunk → embed → index
│   │   └── backfill_workflow.py    ← BackfillWorkflow: re-embed → knowledge_v2
│   ├── activities/
│   │   ├── ingest.py               ← fetch + stage + embed + index activities
│   │   └── backfill.py             ← re-embed activity
│   ├── extractors/                 ← md / pdf / csv / text extractors
│   ├── config_store.py             ← active collection/index pointer
│   └── search_index.py             ← idempotent Atlas Vector Search management
└── infra/
    ├── docker-compose.yml          ← MinIO (S3 events → webhook /ingest-event)
    └── atlas_indexes.json          ← Vector Search index definitions
```

---

## Developer guide

| Document                               | Description                                                                                       |
| -------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **[docs/RUNBOOK.md](docs/RUNBOOK.md)** | Prerequisites, API key setup, local spin-up, cloud infra references                               |
| **[docs/LLD.md](docs/LLD.md)**         | Low-level design — data contracts, workflow internals, scaling to multiple sources and data types |
| **[docs/agent-retrieval.md](docs/agent-retrieval.md)** | The deep agent — retrieval, rerank, synthesis, and how it ties to the vector store |
| **[docs/decisions/](docs/decisions/)** | Architecture Decision Records — e.g. ADR 0001 (direct-from-S3 triggering)                          |
