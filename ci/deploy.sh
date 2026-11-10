#!/usr/bin/env bash
# build -> test -> publish, retried when publish loses a race (exit 75).
#
#   ci/deploy.sh [--no-reuse]                (extra args go to build.py)
#
# A retry starts from the *new* deploy tip, so manifests carried over for
# unchanged projects are always the ones currently deployed, and the diff
# that decides what to rebuild is taken against the newly deployed source.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT:-build/deploy}

for attempt in 1 2 3; do
  echo "== deploy attempt $attempt"
  git fetch --quiet origin "+refs/heads/deploy:refs/remotes/origin/deploy" 2>/dev/null || true
  python ci/build.py --out "$OUT" --previous origin/deploy "$@"
  if [[ -z "${SKIP_TESTS:-}" ]]; then DEPLOY_DIR=$OUT pytest -q; fi
  rc=0; bash ci/publish.sh "$OUT" || rc=$?
  [[ $rc -eq 75 ]] || exit $rc
done
echo "::error::deploy kept moving under us after 3 attempts"; exit 1
