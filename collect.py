"""Collector entrypoint: SGregister API → raw/ on GCS.

Downloads the full universe (``/search`` HTML) and every approved
enterprise's detail record (``/api/enterprises/{orgnr}``). Writes
raw immutable artefacts to GCS. No parsing, no state management,
no CDC. The parser (separate repo) reads these files downstream.

Output path: ``gs://{GCS_BUCKET}/{GCS_PREFIX}/raw/{YYYY-MM-DD}/``

Three files are written per run:

* ``universe.txt`` — one orgnr per line, sorted ascending.
* ``enterprises.jsonl.gz`` — one JSON object per line, full envelope
  as returned by ``/api/enterprises/{orgnr}``. Order matches
  ``universe.txt``.
* ``meta.json`` — run metadata: ``treff_count`` (server-reported
  universe size), ``fetched_count`` (detail records successfully
  retrieved), ``missing_count`` (404s), ``last_modified`` (search
  page Last-Modified header), ``run_id``, ``started_at``,
  ``finished_at``, ``client_requests``.

Modes
-----
``daily``
    Fetch universe + all details. Produces the three files above.

Environment variables
---------------------
GCS_BUCKET : str
    Target GCS bucket. Default ``sondre_brreg_data``.
    Empty string for local-only mode.
GCS_PREFIX : str
    GCS path prefix. Default ``sgregister``.
RUN_MODE : str
    ``daily``. Default ``daily``.
SCRAPE_DELAY : float
    Seconds between detail-record requests per worker. Default ``0.05``.
MAX_WORKERS : int
    Concurrent detail-record workers. Default ``15``.
UNIVERSE_MIN : int
    Abort the run if universe size drops below this threshold
    (tripwire against silent pagination change upstream). Default
    ``10000``.
STATE_DIR : str
    Local working directory. Default ``/tmp/sgregister-collector``.
"""

import os
import sys
import json
import gzip
import uuid
import tempfile
from datetime import date, datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from client import SGClient

GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "sgregister")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
SCRAPE_DELAY = float(os.environ.get("SCRAPE_DELAY", "0.05"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "15"))
UNIVERSE_MIN = int(os.environ.get("UNIVERSE_MIN", "10000"))
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/sgregister-collector")


def gcs_bucket():
    """Return a google-cloud-storage Bucket handle, or ``None``.

    Returns ``None`` when ``GCS_BUCKET`` is empty (local-only mode).
    """
    if not GCS_BUCKET:
        return None
    from google.cloud import storage
    return storage.Client().bucket(GCS_BUCKET)


def upload(local_path, gcs_path, bucket):
    """Upload a file from the local filesystem to GCS.

    Parameters
    ----------
    local_path : str
        Source file on the local filesystem.
    gcs_path : str
        Destination object name within the bucket.
    bucket : google.cloud.storage.Bucket or None
        Target bucket, or ``None`` to skip upload (local-only mode).
    """
    if bucket is None:
        return
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    size = os.path.getsize(local_path)
    print(f"  Uploaded gs://{GCS_BUCKET}/{gcs_path} ({size:,} bytes)", flush=True)


def write_universe(orgnrs, out_dir):
    """Write sorted orgnrs to ``universe.txt`` in ``out_dir``.

    Parameters
    ----------
    orgnrs : list of str
        Orgnrs to write. Written sorted ascending, one per line.
    out_dir : str
        Directory to write into. Created if missing.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "universe.txt")
    with open(path, "w", encoding="utf-8") as f:
        for o in sorted(orgnrs):
            f.write(o + "\n")
    return path


def write_enterprises_jsonl(records, out_dir):
    """Gzip-write detail records to ``enterprises.jsonl.gz``.

    Parameters
    ----------
    records : list of dict
        Detail records as returned by ``/api/enterprises/{orgnr}``.
        Records with value ``None`` are skipped.
    out_dir : str
        Directory to write into.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "enterprises.jsonl.gz")
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for rec in records:
            if rec is None:
                continue
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


def write_meta(meta, out_dir):
    """Write run metadata to ``meta.json`` in ``out_dir``."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "meta.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, sort_keys=True)
    return path


def collect_details(client, orgnrs, max_workers=MAX_WORKERS):
    """Fetch detail records for every orgnr concurrently.

    Parameters
    ----------
    client : SGClient
        Client instance to reuse across workers.
    orgnrs : list of str
        Universe of orgnrs to fetch.
    max_workers : int
        Thread-pool size. Default ``MAX_WORKERS`` (15).

    Returns
    -------
    tuple
        ``(records, missing)`` where ``records`` is a list of
        successful JSON envelopes (same order as ``orgnrs``, with
        missing entries replaced by ``None``) and ``missing`` is a
        list of orgnrs that returned HTTP 404.
    """
    records = [None] * len(orgnrs)
    missing = []
    done = 0
    total = len(orgnrs)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_to_idx = {ex.submit(client.fetch_enterprise, o): i for i, o in enumerate(orgnrs)}
        for fut in as_completed(fut_to_idx):
            i = fut_to_idx[fut]
            rec = fut.result()
            if rec is None:
                missing.append(orgnrs[i])
            else:
                records[i] = rec
            done += 1
            if done % 500 == 0 or done == total:
                print(f"  fetched {done}/{total}  missing={len(missing)}", flush=True)
    return records, missing


def run_daily():
    """Execute one daily collection run.

    Fetches the universe, fetches every detail record, writes three
    artefacts to ``raw/{today}/``, and uploads to GCS.
    """
    today = date.today().isoformat()
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{started_at}] sgregister-collector run_id={run_id} date={today}", flush=True)

    client = SGClient(delay=SCRAPE_DELAY)
    out_dir = os.path.join(STATE_DIR, "raw", today)

    print("Fetching universe from /search ...", flush=True)
    universe = client.fetch_universe()
    n_universe = len(universe["orgnrs"])
    print(f"  universe={n_universe} treff_count={universe['treff_count']} bytes={universe['body_bytes']:,}", flush=True)

    if n_universe < UNIVERSE_MIN:
        raise RuntimeError(
            f"Universe size {n_universe} below tripwire UNIVERSE_MIN={UNIVERSE_MIN}. "
            f"Suspect upstream pagination change. Aborting."
        )
    if universe["treff_count"] != -1 and abs(universe["treff_count"] - n_universe) > 5:
        raise RuntimeError(
            f"Universe count mismatch: treff_count={universe['treff_count']} "
            f"vs extracted orgnrs={n_universe}. Aborting."
        )

    print(f"Writing universe.txt ...", flush=True)
    univ_path = write_universe(universe["orgnrs"], out_dir)

    print(f"Fetching {n_universe} detail records at {MAX_WORKERS} workers ...", flush=True)
    records, missing = collect_details(client, universe["orgnrs"])

    print(f"Writing enterprises.jsonl.gz ({sum(1 for r in records if r is not None)} records) ...", flush=True)
    jsonl_path = write_enterprises_jsonl(records, out_dir)

    finished_at = datetime.now(timezone.utc).isoformat()
    meta = {
        "run_id": run_id,
        "run_mode": RUN_MODE,
        "collection_date": today,
        "started_at": started_at,
        "finished_at": finished_at,
        "universe_size": n_universe,
        "treff_count": universe["treff_count"],
        "search_last_modified": universe["last_modified"],
        "search_body_bytes": universe["body_bytes"],
        "fetched_count": sum(1 for r in records if r is not None),
        "missing_count": len(missing),
        "missing_orgnrs": sorted(missing),
        "client_requests": client.request_count,
        "scrape_delay": SCRAPE_DELAY,
        "max_workers": MAX_WORKERS,
    }
    meta_path = write_meta(meta, out_dir)

    bucket = gcs_bucket()
    upload(univ_path, f"{GCS_PREFIX}/raw/{today}/universe.txt", bucket)
    upload(jsonl_path, f"{GCS_PREFIX}/raw/{today}/enterprises.jsonl.gz", bucket)
    upload(meta_path, f"{GCS_PREFIX}/raw/{today}/meta.json", bucket)

    print(f"[{finished_at}] done. universe={n_universe} fetched={meta['fetched_count']} missing={meta['missing_count']}", flush=True)


def main():
    """Dispatch on ``RUN_MODE``."""
    if RUN_MODE == "daily":
        run_daily()
    else:
        print(f"Unknown RUN_MODE: {RUN_MODE}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
