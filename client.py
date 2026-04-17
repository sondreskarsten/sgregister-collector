"""HTTP client for the SGregister (Sentral godkjenning) public API.

Published by Direktoratet for byggkvalitet (DiBK). Base URL
``https://sgregister.dibk.no``. No authentication required.

Two endpoints are used:

* ``GET /search`` — HTML page listing every currently approved
  enterprise. Unpaginated: the server renders the full universe in
  one response (~3.9 MB, ~10.5K rows as of April 2026). Response
  body contains ``Turbo.visit('/enterprises/{orgnr}')`` anchors for
  every approved orgnr. Query params ``query``, ``country[]``,
  ``county[]``, ``municipality[]``, ``subject_area``,
  ``development_class`` are honoured server-side; this client does
  not use them because the pipeline needs the full universe.

* ``GET /api/enterprises/{orgnr}`` — JSON detail record. Returns
  HTTP 404 for any orgnr not currently in the approval universe
  (including previously approved firms whose approval has lapsed).

Rate limits
-----------
Empirically the server accepts ~12 requests per second across up to
50 concurrent connections without degradation. This client defaults
to 15 workers with a 0.05 s per-request delay.

Data model
----------
Each detail record is a single-object JSON envelope ``{"dibk-sgdata":
{...}}``. Children:

* ``enterprise`` — identity, contact, business and postal address.
* ``status`` — ``approved`` (bool), ``approval_period_to`` (ISO
  date), ``approval_certificate`` (URL to PDF).
* ``additional_terms`` — three booleans covering insurance and
  apprenticeship registration.
* ``valid_approval_areas[]`` — list of approval area records keyed by
  ``function`` × ``subject_area`` × ``pbl`` × ``grade``.
"""

import re
import time
import requests


BASE = "https://sgregister.dibk.no"

_ORGNR_RE = re.compile(r"enterprises/(\d{9})")


class SGClient:
    """HTTP client for the SGregister API.

    Maintains a persistent ``requests.Session`` for connection reuse.
    Implements simple delay between successful calls plus progressive
    backoff on HTTP 429.

    Parameters
    ----------
    delay : float
        Seconds to sleep between successful requests. Default ``0.05``.
    timeout : float
        Per-request timeout in seconds. Default ``30``.
    """

    def __init__(self, delay=0.05, timeout=30):
        self.delay = delay
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        self._session.headers["User-Agent"] = "sgregister-collector/1.0 (+sondreskarsten)"
        self._request_count = 0

    def fetch_universe(self):
        """Fetch the full HTML search page and extract the orgnr universe.

        Returns
        -------
        dict
            ``{"orgnrs": [str], "treff_count": int, "last_modified":
            str, "body_bytes": int}`` — ``orgnrs`` is sorted and
            de-duplicated. ``treff_count`` is the server-reported
            result count parsed from the page header.
        """
        headers = {"Accept": "text/html"}
        resp = self._session.get(f"{BASE}/search", headers=headers, timeout=self.timeout)
        self._request_count += 1
        resp.raise_for_status()
        body = resp.text
        orgnrs = sorted(set(_ORGNR_RE.findall(body)))
        treff = re.search(r"Søkeresultater \((\d+) treff\)", body)
        treff_count = int(treff.group(1)) if treff else -1
        last_modified = resp.headers.get("Last-Modified", "")
        return {
            "orgnrs": orgnrs,
            "treff_count": treff_count,
            "last_modified": last_modified,
            "body_bytes": len(body.encode("utf-8")),
        }

    def fetch_enterprise(self, orgnr):
        """Fetch a single enterprise's full SG record.

        Parameters
        ----------
        orgnr : str
            9-digit Norwegian organisasjonsnummer.

        Returns
        -------
        dict or None
            The parsed JSON envelope ``{"dibk-sgdata": {...}}`` on
            HTTP 200. Returns ``None`` on HTTP 404 (orgnr not in the
            approval universe). Raises for all other status codes.
        """
        url = f"{BASE}/api/enterprises/{orgnr}"
        for attempt in range(5):
            resp = self._session.get(url, timeout=self.timeout)
            self._request_count += 1
            if resp.status_code == 200:
                if self.delay > 0:
                    time.sleep(self.delay)
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 502, 503, 504):
                backoff = min(60, 2 ** attempt)
                time.sleep(backoff)
                continue
            resp.raise_for_status()
        resp.raise_for_status()

    @property
    def request_count(self):
        """Number of HTTP requests issued by this client since init."""
        return self._request_count
