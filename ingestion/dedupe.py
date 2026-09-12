"""Duplicate detection on the natural key.

The specification defines a duplicate as a record describing the same
real-world transaction under a different transaction_id, identical across all
other fields. Detection is therefore a hash over every field except
transaction_id.

Flag, do not drop
-----------------
The assessment permits either and asks for the choice to be justified. This
implementation flags duplicates in bronze and resolves them downstream.

Dropping at ingestion is irreversible. Two questions become permanently
unanswerable: how often does the source emit duplicates, and is that rate
changing? The first is what you need to raise the issue with the source
owner; the second is an early indicator of an upstream defect such as a retry
or replay bug.

A duplicate is also a different kind of thing from an invalid record. An
invalid record violates the contract; a duplicate conforms to it and is
merely redundant. Routing duplicates to quarantine would conflate two
conditions with different owners and different remediations.

Survivorship must be deterministic
----------------------------------
The surviving record is the one with the lowest transaction_id. The specific
rule matters less than the fact that it is stable across runs: an arbitrary
survivor would make downstream aggregates non-reproducible, which would
quietly falsify the idempotency claim.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from collections.abc import Sequence

from ingestion.models import NATURAL_KEY_FIELDS, Transaction

logger = logging.getLogger(__name__)


def natural_key_hash(txn: Transaction) -> str:
    """Stable hash over every field except transaction_id.

    Values are normalised before hashing. Without normalisation, trailing
    whitespace or an amount written as "238.2" versus "238.20" would produce
    different hashes for records that are in fact the same transaction, and
    the duplicate would go undetected.

    Truncated to 32 hex characters: ample to avoid collision at any realistic
    volume, and short enough to stay readable in query output.
    """
    parts = []
    for field in NATURAL_KEY_FIELDS:
        value = getattr(txn, field)
        if field == "amount":
            # Normalise the decimal representation so 238.2 == 238.20
            value = format(value.normalize(), "f")
        else:
            value = str(value).strip()
        parts.append(f"{field}={value}")
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def flag_duplicates(
    transactions: Sequence[Transaction],
) -> tuple[dict[str, str], dict[str, str | None], int]:
    """Identify duplicate groups.

    Returns:
        hashes:       transaction_id -> natural key hash
        duplicate_of: transaction_id -> surviving transaction_id, or None
                      when this record is itself the survivor
        count:        number of redundant records (group size minus one)
    """
    hashes: dict[str, str] = {}
    groups: dict[str, list[str]] = defaultdict(list)

    for txn in transactions:
        h = natural_key_hash(txn)
        hashes[txn.transaction_id] = h
        groups[h].append(txn.transaction_id)

    duplicate_of: dict[str, str | None] = {}
    redundant = 0

    for h, ids in groups.items():
        if len(ids) == 1:
            duplicate_of[ids[0]] = None
            continue
        # Deterministic survivor: lowest transaction_id, stable across runs.
        survivor = min(ids)
        redundant += len(ids) - 1
        logger.warning(
            "duplicate group on natural key %s: %s (surviving: %s)",
            h, sorted(ids), survivor,
        )
        for tid in ids:
            duplicate_of[tid] = None if tid == survivor else survivor

    return hashes, duplicate_of, redundant
