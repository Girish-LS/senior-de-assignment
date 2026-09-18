"""Print the bronze row count. Used by the capture script to assert
idempotency around the incremental rerun."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_to_databricks import connect, databricks_config  # noqa: E402

cfg = databricks_config()
conn = connect(cfg)
try:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {cfg['catalog']}.{cfg['schema']}.bronze_transactions"
    )
    print(cur.fetchone()[0])
finally:
    conn.close()
