"""Test package.

Ingestion logs duplicate groups and unparseable watermarks at WARNING, which
is correct in production and noise in a test run. Several tests deliberately
exercise those paths, so the logger is quietened here rather than having each
test suppress it individually.
"""

import logging

logging.getLogger("ingestion").setLevel(logging.ERROR)
