"""HTTP client for the transactions API. Standard library only.

Behaviour here is driven by what the live endpoint actually does, established
by probing it before any of this was written. Three findings changed the
design, and each would have caused a real defect if assumed rather than
checked:

1.  Filtered requests return **HTTP 206 Partial Content**, not 200. A client
    treating "not 200" as failure would retry every successful page until the
    attempt cap, then fail a run that had worked.

2.  `amount` is serialised as a JSON **string** ("1455.8"). Handled in the
    validation layer; noted here because it is a property of the wire format.

3.  An invalid parameter such as `limit=abc` returns **200 with the parameter
    silently ignored**, yielding the whole table instead of an error. The
    server cannot be relied on to reject bad input, so page parameters are
    validated locally before the request is made.

Pagination is ordered by the `id` column rather than `transaction_date`.
Offset pagination over a non-unique sort key is not stable between requests:
rows sharing a sort value can be returned twice or skipped entirely as the
offset advances. `id` is a unique sequence, so ordering by it makes paging
deterministic.
"""

from __future__ import annotations

import json
import logging
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Status codes that represent success for this API. 206 is returned whenever
# a Range or count preference is in play, which is most filtered requests.
SUCCESS_STATUSES = frozenset({200, 206})

# Transient conditions worth retrying. 429 is rate limiting; 5xx are server
# faults. Other 4xx are configuration or contract errors that will never
# succeed on retry, so retrying them only delays a clear failure.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504, 507, 509})


class ApiError(RuntimeError):
    """Non-retryable API failure, or retries exhausted."""


@dataclass
class RequestStats:
    """Per-run counters, surfaced as run metrics rather than log-only."""

    requests_made: int = 0
    retries: int = 0
    retry_reasons: dict[str, int] = field(default_factory=dict)
    total_wait_seconds: float = 0.0

    def record_retry(self, reason: str, wait: float) -> None:
        self.retries += 1
        self.retry_reasons[reason] = self.retry_reasons.get(reason, 0) + 1
        self.total_wait_seconds += wait


class TransactionsApiClient:
    """Thin client over the transactions endpoint.

    Owns HTTP concerns only - authentication, pagination, retry, timeouts.
    It knows nothing about what a transaction is, which is what would let it
    be reused against a second PostgREST source without modification.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        auth_token: str,
        *,
        page_size: int = 100,
        max_retries: int = 5,
        backoff_base_seconds: float = 0.5,
        max_backoff_seconds: float = 30.0,
        timeout_seconds: float = 30.0,
        max_total_retry_seconds: float = 300.0,
    ) -> None:
        if page_size < 1:
            raise ValueError(f"page_size must be >= 1, got {page_size}")
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {max_retries}")

        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._auth_token = auth_token
        self.page_size = page_size
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.timeout_seconds = timeout_seconds
        self.max_total_retry_seconds = max_total_retry_seconds
        self.stats = RequestStats()
        self._ssl_context = ssl.create_default_context()

    # ---- low-level request with retry -----------------------------------

    def _build_request(self, path: str, params: dict[str, Any] | None,
                       prefer: str | None) -> urllib.request.Request:
        url = f"{self.base_url}{path}"
        if params:
            # safe='.*' keeps PostgREST operator syntax readable in logs,
            # e.g. transaction_date=gte.2024-01-01T00:00:00Z
            url += "?" + urllib.parse.urlencode(params, safe=".*:")
        req = urllib.request.Request(url, method="GET")
        req.add_header("apikey", self._api_key)
        req.add_header("Authorization", f"Bearer {self._auth_token}")
        req.add_header("Accept", "application/json")
        if prefer:
            req.add_header("Prefer", prefer)
        return req

    def _sleep_for(self, attempt: int, retry_after: str | None) -> float:
        """Exponential backoff with full jitter.

        Jitter is what makes retry safe under concurrency: without it,
        parallel workers retry in lockstep and periodically overwhelm a source
        that is already struggling.

        A Retry-After header is honoured when present. Guessing when the
        server has already said is both wasteful and impolite.
        """
        if retry_after:
            try:
                return min(float(retry_after), self.max_backoff_seconds)
            except (TypeError, ValueError):
                pass  # header may be an HTTP date; fall through to backoff
        ceiling = min(
            self.backoff_base_seconds * (2**attempt), self.max_backoff_seconds
        )
        return random.uniform(0, ceiling)

    def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        prefer: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, str]]:
        """Issue a GET with retry. Returns (records, response headers)."""
        deadline = time.monotonic() + self.max_total_retry_seconds
        last_error: str | None = None

        for attempt in range(self.max_retries + 1):
            req = self._build_request(path, params, prefer)
            self.stats.requests_made += 1
            try:
                with urllib.request.urlopen(
                    req, timeout=self.timeout_seconds, context=self._ssl_context
                ) as resp:
                    status = resp.status
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    body = resp.read().decode("utf-8", errors="replace")

                if status in SUCCESS_STATUSES:
                    try:
                        payload = json.loads(body)
                    except json.JSONDecodeError as exc:
                        raise ApiError(
                            f"response was not valid JSON: {exc}"
                        ) from exc
                    if not isinstance(payload, list):
                        raise ApiError(
                            f"expected a JSON array, got {type(payload).__name__}"
                        )
                    return payload, headers

                last_error = f"unexpected status {status}"

            except urllib.error.HTTPError as exc:
                status = exc.code
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                if status not in RETRYABLE_STATUSES:
                    # 401, 403, 404, 400 and friends will not succeed on
                    # retry. Fail immediately with the cause visible.
                    detail = exc.read().decode("utf-8", errors="replace")[:400]
                    raise ApiError(
                        f"HTTP {status} is not retryable: {detail}"
                    ) from exc
                last_error = f"HTTP {status}"
                if attempt < self.max_retries and time.monotonic() < deadline:
                    wait = self._sleep_for(attempt, retry_after)
                    self.stats.record_retry(last_error, wait)
                    logger.warning(
                        "retrying after %s (attempt %d/%d, waiting %.2fs)",
                        last_error, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise ApiError(
                    f"{last_error} after {attempt + 1} attempts"
                ) from exc

            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                # Timeouts and connection resets are transient by nature.
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.max_retries and time.monotonic() < deadline:
                    wait = self._sleep_for(attempt, None)
                    self.stats.record_retry(type(exc).__name__, wait)
                    logger.warning(
                        "retrying after %s (attempt %d/%d, waiting %.2fs)",
                        last_error, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise ApiError(
                    f"{last_error} after {attempt + 1} attempts"
                ) from exc

            # Non-exception, non-success status: retry if allowed.
            if attempt < self.max_retries and time.monotonic() < deadline:
                wait = self._sleep_for(attempt, None)
                self.stats.record_retry(last_error or "unknown", wait)
                time.sleep(wait)
                continue
            raise ApiError(f"{last_error} after {attempt + 1} attempts")

        raise ApiError(f"exhausted retries: {last_error}")

    # ---- public API ------------------------------------------------------

    def count(self, since: str | None = None) -> int | None:
        """Exact row count from the Content-Range header, or None."""
        params: dict[str, Any] = {"limit": 1, "select": "id"}
        if since:
            params["transaction_date"] = f"gte.{since}"
        _, headers = self._request("/transactions", params, prefer="count=exact")
        content_range = headers.get("content-range", "")
        if "/" in content_range:
            total = content_range.rsplit("/", 1)[-1]
            if total.isdigit():
                return int(total)
        return None

    def iter_transactions(
        self, since: str | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield every record, paging until the source is exhausted.

        `since` applies the API-native date filter, which is the mechanism
        behind incremental ingestion. Note this is `gte`, not `gt`: with `gt`
        any record sharing the exact watermark timestamp would be skipped
        permanently. The re-read overlap is harmless because bronze loads are
        upserts keyed on transaction_id.

        The loop stops on an empty page or a short page. Both conditions are
        needed: a short page means the source is exhausted, and an empty page
        covers the boundary case where the total is an exact multiple of the
        page size.
        """
        offset = 0
        seen_ids: set[Any] = set()

        while True:
            params: dict[str, Any] = {
                "limit": self.page_size,
                "offset": offset,
                # Deterministic paging requires a unique sort key.
                "order": "id.asc",
            }
            if since:
                params["transaction_date"] = f"gte.{since}"

            page, _ = self._request("/transactions", params)
            logger.info(
                "fetched page offset=%d size=%d records=%d",
                offset, self.page_size, len(page),
            )

            if not page:
                return

            for record in page:
                # Defensive: offset paging over a mutating source can repeat
                # a row. Detecting it here keeps the count honest rather than
                # relying on the upsert to quietly absorb it.
                rid = record.get("id", record.get("transaction_id"))
                if rid in seen_ids:
                    logger.warning("duplicate row id %r returned by paging", rid)
                    continue
                seen_ids.add(rid)
                yield record

            if len(page) < self.page_size:
                return
            offset += self.page_size
