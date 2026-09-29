"""
Makes ingestion/batch/ and ml/ importable by their plain module names (e.g.
`import load_cricsheet`) from anywhere under tests/, without turning either
directory into a package -- they're meant to be run as standalone scripts
(`python load_cricsheet.py`, `python feature_engineering.py`), so no
__init__.py / package layout is introduced just for testing.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
for path in [REPO_ROOT / "ingestion" / "batch", REPO_ROOT / "ml"]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
