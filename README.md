# sgregister-collector

Daily collector for **SGregister** (Sentral godkjenning) — the public
register of Norwegian construction firms holding central approval
under SAK10. Published by Direktoratet for byggkvalitet (DiBK).

Data source: `https://sgregister.dibk.no`
Catalog entry: `data.norge.no/data-services/c443ab25-ae23-3b67-b6b3-3782c08f4d46`

## Architecture

Pattern B pipeline. Two repos:

- **sgregister-collector** (this repo) — fetches raw data from the
  API, writes immutable `raw/{date}/` artefacts to GCS. No parsing,
  no state, no CDC.
- **sgregister-parser** — airgapped. Reads `raw/`, produces
  `state/pool.parquet`, `state/snapshots.parquet`, and
  `cdc/changelog/{date}.parquet`.

## Source endpoints

| Endpoint | Purpose |
|---|---|
| `GET /search` | HTML, unpaginated. Full universe of currently approved orgnrs (~10.5K rows as of April 2026, ~3.9 MB). |
| `GET /api/enterprises/{orgnr}` | JSON detail record. 404 if orgnr not currently approved. |

No authentication. Rate limit empirically ~12 req/s across up to 50
concurrent workers.

## Run modes

Only `daily` is implemented.

`RUN_MODE=daily` — fetch universe + detail for every approved
orgnr. Full run at 15 workers takes ~15 minutes.

## Output

```
gs://sondre_brreg_data/sgregister/raw/{YYYY-MM-DD}/
    universe.txt                 sorted orgnrs, one per line
    enterprises.jsonl.gz         one record per line, full envelope
    meta.json                    run metadata (size, counts, run_id)
```

## Tripwire

The collector aborts if the universe drops below `UNIVERSE_MIN`
(default 10,000) or if the extracted orgnr count disagrees with the
server-reported `treff` count by more than 5. This guards against a
silent upstream switch to true pagination.

## Environment

| Var | Default |
|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` |
| `GCS_PREFIX` | `sgregister` |
| `RUN_MODE` | `daily` |
| `SCRAPE_DELAY` | `0.05` |
| `MAX_WORKERS` | `15` |
| `UNIVERSE_MIN` | `10000` |
| `STATE_DIR` | `/tmp/sgregister-collector` |

## Schedule

Proposed: daily 07:40 Europe/Oslo (Cloud Scheduler in europe-west1).
Fits between enheter-parser (07:15) and integration-layer (08:00).

## LUAS

`orgnr` — one row per legal entity. `document_id = orgnr`. See
`sgregister-parser` for the change semantics.
