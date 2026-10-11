"""Import the real directory bridge without shadowing an existing module path."""
import os
import shutil
import subprocess
import sys

from test_deferred_questions import CORE, PROJECT


def test_directory_bridge_preserves_import_precedence(tmp_path):
    target = tmp_path / 'plugin'
    target.mkdir()
    shutil.copy2(PROJECT / '__init__.py', target / '__init__.py')
    shutil.copytree(PROJECT / 'hermes_deferred_questions', target / 'hermes_deferred_questions')
    first = tmp_path / 'existing'
    first.mkdir()
    (first / 'dq_precedence_probe.py').write_text("origin = 'existing'\n")
    (target / 'dq_precedence_probe.py').write_text("origin = 'plugin'\n")
    env = dict(os.environ, HOME=str(tmp_path), HERMES_HOME=str(tmp_path / 'home'), PYTHONPATH=str(CORE))
    code = '''import importlib.util
import sys
from pathlib import Path
root, existing = sys.argv[1:]
sys.path.append(existing)
before = list(sys.path)
spec = importlib.util.spec_from_file_location('isolated_bridge', Path(root) / '__init__.py')
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)
import dq_precedence_probe
assert dq_precedence_probe.origin == 'existing'
assert sys.path == before + [root]
assert callable(bridge.register)
spec.loader.exec_module(bridge)
assert sys.path == before + [root]
print('bridge import precedence and repeated-load idempotency passed')
'''
    result = subprocess.run([sys.executable, '-c', code, str(target), str(first)],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'idempotency passed' in result.stdout
