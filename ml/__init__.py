import sys
from pathlib import Path

# Make the Lambda code in src/ (shared feature engineering) importable from the repo root.
_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
