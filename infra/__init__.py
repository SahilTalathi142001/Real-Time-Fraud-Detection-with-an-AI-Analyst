import sys
from pathlib import Path

# Stacks share schema definitions with the Lambda code in src/.
_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
