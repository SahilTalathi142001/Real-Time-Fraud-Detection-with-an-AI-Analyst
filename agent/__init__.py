import sys
from pathlib import Path

# The agent's runtime code lives in src/ next to the Lambda code (it reuses src/common).
SRC = str(Path(__file__).resolve().parents[1] / "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
