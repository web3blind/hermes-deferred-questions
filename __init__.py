"""Hermes directory-plugin bridge to the conventional, reload-stable package.

The Git installer installs dependencies, not this project's distribution. Make
this reviewed checkout importable using ordinary Python package discovery. Core
force reload evicts hermes_plugins.*, not hermes_deferred_questions.*; retaining
the package is essential to drain/recover native bindings safely.
"""
from pathlib import Path
import sys

_package_root = str(Path(__file__).resolve().parent)
if _package_root not in sys.path:
    sys.path.insert(0, _package_root)
from hermes_deferred_questions import register, configure_parser, runtime, store, lifecycle
