"""Portable path selectors survive core runner's credential-free env -i."""
import argparse
import os
import sys
from pathlib import Path

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--dq-core-root')
parser.add_argument('--dq-vk-plugin-root')
selectors, _ = parser.parse_known_args()
for key, value in [('HERMES_CORE_ROOT', selectors.dq_core_root),
                   ('HERMES_VK_PLUGIN_ROOT', selectors.dq_vk_plugin_root)]:
    if value:
        os.environ[key] = value
if not os.environ.get('HERMES_CORE_ROOT'):
    raise RuntimeError('Run scripts/run_tests.sh with HERMES_CORE_ROOT set')

def pytest_addoption(parser):
    parser.addoption('--dq-core-root')
    parser.addoption('--dq-vk-plugin-root')

sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import installed  # noqa: F401, E402
from test_entrypoint_lifecycle import ep  # noqa: F401, E402
