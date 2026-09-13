"""Runtime configuration, sourced entirely from the environment.

No credential is ever hardcoded. Values come from real environment variables
or from a local env file that is excluded from version control.

The module fails fast: if required settings are absent, the process exits with
a clear message before any network call is attempted. Discovering a missing
variable after a partial run wastes time and leaves ambiguous state behind.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Candidate env filenames, in precedence order. Several are accepted because
# Windows Explorer makes leading-dot filenames awkward and silently appends
# .txt to files saved from Notepad.
ENV_FILENAMES = (
    ".env",
    "assignment.env",
    ".env.local",
    "assignment.env.txt",
    ".env.txt",
)

REPO_ROOT = Path(__file__).resolve().parent.parent


class ConfigError(RuntimeError):
    """Raised when configuration is missing or malformed."""


def _load_env_file(search_root: Path | None = None) -> str | None:
    """Populate os.environ from the first env file found. Returns its name.

    Real environment variables win over file contents, which is what allows CI
    to inject settings without a file present. Read as utf-8-sig, and with a
    residual BOM strip, because PowerShell writes a byte-order mark that would
    otherwise corrupt the first key name into an unreadable string.
    """
    root = search_root or REPO_ROOT
    for name in ENV_FILENAMES:
        path = root / name
        if not path.is_file():
            continue
        for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip().lstrip("\ufeff").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip().lstrip("\ufeff").strip()
            value = value.strip().strip("'\"")
            if key:
                os.environ.setdefault(key, value)
        return name
    return None


def _env_str(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.environ.get(name, "" if default is None else default).strip()
    if required and not value:
        raise ConfigError(
            f"Required environment variable {name} is not set.\n"
            f"Copy assignment.env.example to assignment.env and fill it in, "
            f"or export {name} directly."
        )
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings."""

    api_base_url: str
    api_key: str = field(repr=False)
    auth_token: str = field(repr=False)

    page_size: int = 100
    max_retries: int = 5
    backoff_base_seconds: float = 0.5
    request_timeout_seconds: float = 30.0

    lookback_hours: int = 72
    initial_watermark: str = "1970-01-01T00:00:00Z"

    env_file_used: str | None = None

    # ---- Derived paths. Kept here so no module invents its own layout. ----

    @property
    def repo_root(self) -> Path:
        return REPO_ROOT

    @property
    def warehouse_path(self) -> Path:
        # SQLite, not DuckDB. The pipeline uses the standard library only, and
        # sqlite3 ships with Python while duckdb does not. Named accurately so
        # nobody opens it with the wrong client. dbt reads CSV exports of
        # these tables rather than the file itself; see scripts/export_for_dbt.py.
        return REPO_ROOT / "warehouse" / "transactions.sqlite"

    @property
    def state_dir(self) -> Path:
        return REPO_ROOT / "state"

    @property
    def outputs_dir(self) -> Path:
        return REPO_ROOT / "outputs"

    def redacted(self) -> dict[str, object]:
        """Settings safe to log. Credentials are never included."""
        return {
            "api_base_url": self.api_base_url,
            "api_key": f"set ({len(self.api_key)} chars)" if self.api_key else "MISSING",
            "auth_token_same_as_key": self.auth_token == self.api_key,
            "page_size": self.page_size,
            "max_retries": self.max_retries,
            "backoff_base_seconds": self.backoff_base_seconds,
            "request_timeout_seconds": self.request_timeout_seconds,
            "lookback_hours": self.lookback_hours,
            "initial_watermark": self.initial_watermark,
            "env_file_used": self.env_file_used,
        }


def load_settings(
    *, search_root: Path | None = None, require_api: bool = True
) -> Settings:
    """Load and validate settings.

    `require_api` is False for runs that read the offline CSV fixture. A
    reviewer without API credentials must still be able to execute the
    pipeline end to end, so the offline path cannot depend on values it never
    uses. Credentials are then validated at the point the API client is
    constructed, which is the only place they are needed.
    """
    env_file = _load_env_file(search_root)

    base_url = _env_str("ASSESSMENT_API_BASE_URL", required=require_api).rstrip("/")
    if base_url and not base_url.startswith(("http://", "https://")):
        raise ConfigError(
            f"ASSESSMENT_API_BASE_URL must include the scheme, got {base_url!r}"
        )

    api_key = _env_str("ASSESSMENT_API_KEY", required=require_api)

    # The assessment states the bearer token may equal the API key. Defaulting
    # to the key keeps configuration to two values instead of three, while
    # still allowing them to diverge.
    auth_token = _env_str("ASSESSMENT_AUTH_TOKEN") or api_key

    page_size = _env_int("ASSESSMENT_PAGE_SIZE", 100)
    if page_size < 1:
        raise ConfigError(f"ASSESSMENT_PAGE_SIZE must be >= 1, got {page_size}")

    lookback_hours = _env_int("ASSESSMENT_LOOKBACK_HOURS", 72)
    if lookback_hours < 0:
        raise ConfigError(
            f"ASSESSMENT_LOOKBACK_HOURS must be >= 0, got {lookback_hours}"
        )

    return Settings(
        api_base_url=base_url,
        api_key=api_key,
        auth_token=auth_token,
        page_size=page_size,
        max_retries=_env_int("ASSESSMENT_MAX_RETRIES", 5),
        backoff_base_seconds=_env_float("ASSESSMENT_BACKOFF_BASE_SECONDS", 0.5),
        request_timeout_seconds=_env_float("ASSESSMENT_REQUEST_TIMEOUT_SECONDS", 30.0),
        lookback_hours=lookback_hours,
        initial_watermark=_env_str(
            "ASSESSMENT_INITIAL_WATERMARK", "1970-01-01T00:00:00Z"
        ),
        env_file_used=env_file,
    )
