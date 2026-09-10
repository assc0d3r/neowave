# NEOWAVE Intelligence Terminal — Production Reference Implementation

A persistent web/archive layer for `$NEOWAVE V3.2.1 FULLSTACK`. It is deliberately **not a second structural engine**. Canonical wave count, scenarios, structural activation/invalidation, finalized levels and target state must enter the terminal through a locked-analysis record produced by the canonical NEOWAVE engine.

## What is implemented

- Liquid-glass cinematic landing experience + dense AMOLED/neon research terminal.
- 10 selectable themes; theme is presentation-only and never changes semantic decision colors.
- Server-side SQLite persistence with WAL mode, foreign keys and immutable lock-file copies.
- Permanent Stock Library with search, sorting, favourites and instant stock-workspace retrieval.
- Locked-analysis V2.2 ingestion with structural/data-integrity gates.
- Optional strict JSON Schema validation (`jsonschema`).
- Primary/alternate scenario preservation; rule ledger; finalized levels; next-required evidence.
- Completed-bar manifest display and no synthesized chart data when the lock does not contain a series.
- Per-stock analysis chronology and automatic delta vs the prior latest lock.
- Immutable source JSON viewer, SHA-256 source hash, audit trail, forecast record registry.
- Market radar over saved latest states, compare view, performance/accountability registry.
- Optional external canonical-engine job bridge via `NEOWAVE_ENGINE_COMMAND`.
- Multi-file upload handoff for OHLC CSV/TSV, chart PDFs/images, pivot JSON and existing locked-analysis JSON.
- Render free-plan deployment blueprint in `render.yaml`.
- Optional bearer-token protection for all write endpoints.
- Production-oriented CSP/security headers and bounded JSON request size.
- Docker and direct Python deployment paths.

## Quick start

```bash
cd NEOWAVE_Intelligence_Terminal_Production
python server.py
```

Open `http://127.0.0.1:8080`.

The clean production database begins empty. Import `examples/DEMO_LOCKED_ANALYSIS_V2_2.json` only if you want a clearly labelled non-market demo record.

## Import a canonical lock

Use **Analyze → Import Locked Analysis V2.2** in the UI, or:

```bash
python scripts/import_lock.py examples/DEMO_LOCKED_ANALYSIS_V2_2.json
```

## Configure direct `$NEOWAVE` execution

The UI will never fake an analysis if no engine bridge exists. Configure a command that accepts these placeholders and emits `LOCKED_ANALYSIS.json` (or a filename matching `*LOCKED*ANALYSIS*.json`) into `{output_dir}`:

```bash
export NEOWAVE_ENGINE_COMMAND='python /opt/neowave/canonical_bridge.py --exchange {exchange} --symbol {symbol} --output-dir {output_dir}'
```

For upload-driven processing the command can also receive `{input_dir}` and `{input_manifest}`:

```bash
export NEOWAVE_ENGINE_COMMAND='python /opt/neowave/canonical_bridge.py --exchange {exchange} --symbol {symbol} --input-dir {input_dir} --input-manifest {input_manifest} --output-dir {output_dir}'
```

The bundled `NEOWAVE_V3_2_1_FULLSTACK_RUNTIME.py` is included as a contract/runtime reference, but its own header explicitly preserves the structural firewall and does not invent canonical wave count. Your production bridge must therefore connect the complete canonical structural engine before emitting a final locked-analysis record.

## Deploy on Render

This package includes `render.yaml` for Render's free Docker web-service flow. See `docs/RENDER_AUTOMATION.md`.

## Write authentication

```bash
export NEOWAVE_ADMIN_TOKEN='replace-with-a-long-random-secret'
python server.py
```

Enter the same token under **Settings → Admin token**. In internet-facing production, terminate HTTPS and user authentication at a hardened reverse proxy/SSO layer; the terminal token is a write gate, not a full IAM system.

## Data directory

By default:

- SQLite DB: `data/neowave.db`
- Immutable lock copies: `data/locks/`
- Engine job artifacts: `data/jobs/`

Override with `NEOWAVE_DATA_DIR` or `NEOWAVE_DB_PATH`.

## Structural firewall

The server rejects canonical ingestion when core integrity requirements fail, including missing V2.2 fields, missing scenario/rule ledgers, developing-bar exclusion failure, participation evidence attempting structural mutation, or upstream validation failure. The UI renders locked state only; it does not calculate wave count or structural levels.

## Important schema note

The supplied Locked Analysis Schema V2.2 contains the historical constant `$NEOWAVE_AUTONOMOUS_INPUT_V10` for `data_acquisition.engine`, while the current V3.2.1 master contract identifies the V11.5.1 MARE exchange-native lineage. The terminal records this as an explicit compatibility warning instead of silently rewriting either source. Resolve that upstream contract/schema version drift before a final institutional deployment.

See `docs/ARCHITECTURE.md`, `docs/API.md`, `docs/DEPLOYMENT.md`, and `docs/SECURITY.md`.
