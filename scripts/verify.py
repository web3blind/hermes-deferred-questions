"""Run all portable regressions serially through the canonical Hermes runner."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
raise SystemExit(subprocess.run([str(root / "scripts/run_tests.sh"), *sys.argv[1:]], cwd=root).returncode)
