# Contributing

Use Python 3.11+ and a Hermes source checkout with the plugin lifecycle, platform
handler, pre-dispatch authorization and canonical routing APIs described in README.
Install the core's development and selected transport dependencies into an isolated
Python environment. Do not alter a running gateway's environment to test changes.

Set `HERMES_CORE_ROOT` to that checkout and `TMPDIR` to a writable scratch directory.
For the full VK callback integration, set `HERMES_VK_PLUGIN_ROOT` to a reviewed
hermes-vk-platform 0.2.6+ checkout. Run `scripts/run_tests.sh` (or
`python3 scripts/verify.py`); it uses the core's canonical per-file runner serially,
a unique basetemp and scratch HOME/HERMES_HOME. Do not run pytest directly as the
final verification. Core's canonical runner selects the checkout's development
venv before `HERMES_PYTHON`; ensure the intended interpreter actually appears in
its output. Missing optional public VK checkout is an explicit skip, not evidence
of passing integration.

Preserve fail-closed recipient/route checks, first-answer wins, idempotency,
uncertain-effect holds, and owned-worker drain on unload/reload. Add behavioral
regressions rather than source-text assertions. Do not commit test output,
databases, secrets, local paths or private profile fixtures. Never use live
credentials or send messages in the test suite. No CI workflow is shipped until
its exact setup has been exercised.
