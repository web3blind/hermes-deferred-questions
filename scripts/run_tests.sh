#!/usr/bin/env bash
set -euo pipefail
PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${HERMES_CORE_ROOT:?Set HERMES_CORE_ROOT to a Hermes source checkout}"
CORE="$(cd "$HERMES_CORE_ROOT" && pwd)"
SCRATCH="${TMPDIR:-${TMP:-${TEMP:-}}}"
if [[ -z "$SCRATCH" || ! -d "$SCRATCH" ]]; then
  printf '%s\n' 'Set TMPDIR to an existing scratch directory.' >&2
  exit 2
fi
RUN="$(mktemp -d "$SCRATCH/deferred-questions-tests.XXXXXXXX")"
mkdir -p "$RUN/home" "$RUN/hermes"
export HOME="$RUN/home" HERMES_HOME="$RUN/hermes"
# Core's env -i intentionally drops plugin-specific variables. Pass only these
# nonsecret path selectors as explicit pytest options; conftest restores them.
OPTIONS=("--dq-core-root=$CORE")
if [[ -n "${HERMES_VK_PLUGIN_ROOT:-}" ]]; then
  OPTIONS+=("--dq-vk-plugin-root=$HERMES_VK_PLUGIN_ROOT")
fi
cd "$CORE"
exec "$CORE/scripts/run_tests.sh" -j 1 --file-timeout 180 "$PROJECT/tests" -- \
  --import-mode=importlib "--rootdir=$PROJECT" "-c=$PROJECT/pytest.ini" \
  "--basetemp=$RUN/basetemp" --tb=short "${OPTIONS[@]}" "$@"
