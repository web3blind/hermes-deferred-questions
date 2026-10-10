"""Build actual wheel/sdist and import the declared entrypoint without installation."""
import os
import shutil
import subprocess
import sys
import zipfile

from test_deferred_questions import PROJECT, CORE


def test_build_wheel_sdist_and_load_entrypoint(tmp_path):
    root = tmp_path / 'source'
    root.mkdir()
    for name in ('pyproject.toml', 'README.md', 'LICENSE'):
        shutil.copy2(PROJECT / name, root / name)
    shutil.copytree(PROJECT / 'hermes_deferred_questions', root / 'hermes_deferred_questions')
    out = tmp_path / 'dist'
    out.mkdir()
    built = subprocess.run([sys.executable, '-c',
        'from setuptools.build_meta import build_wheel, build_sdist; '
        f'build_wheel({str(out)!r}); build_sdist({str(out)!r})'],
        cwd=root, capture_output=True, text=True, timeout=60)
    assert built.returncode == 0, built.stdout + built.stderr
    wheel, = out.glob('*.whl')
    with zipfile.ZipFile(wheel) as archive:
        assert 'hermes_deferred_questions/lifecycle.py' in archive.namelist()
    env = dict(os.environ, HERMES_HOME=str(tmp_path / 'home'), HOME=str(tmp_path),
               PYTHONPATH=os.pathsep.join((str(wheel), str(CORE))))
    code = '''import importlib.metadata
from hermes_cli.plugins import PluginManager
from pathlib import Path
import os
home = Path(os.environ['HERMES_HOME']); home.mkdir()
home.joinpath('config.yaml').write_text('plugins:\\n  enabled: [deferred-questions]\\n')
ep, = [e for e in importlib.metadata.entry_points(group='hermes_agent.plugins') if e.name == 'deferred-questions']
module = ep.load()
manager = PluginManager(scope_key=str(home))
manager.discover_and_load()
assert manager._plugins['deferred-questions'].enabled
manager.discover_and_load(force=True)
assert ep.load() is module
manager.unload()
print('wheel entrypoint imported and real manager force identity preserved')
'''
    loaded = subprocess.run([sys.executable, '-c', code], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=60)
    assert loaded.returncode == 0, loaded.stdout + loaded.stderr
    assert 'identity preserved' in loaded.stdout
