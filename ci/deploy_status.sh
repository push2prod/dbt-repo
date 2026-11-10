#!/usr/bin/env bash
# Is `deploy` an up-to-date build of master's tip?
#
#   ci/deploy_status.sh          exit 0 = in sync, 1 = drift (deploy needed)
#
# "In sync" means nothing deployable changed between deploy's Source-Commit
# and master's tip. Commits that only touch README/tests/CI config don't need
# a new deploy commit, so Source-Commit may legitimately lag the tip.
set -euo pipefail
git fetch --quiet origin master deploy
TIP=$(git rev-parse origin/master)
SRC=$(git log -1 --format='%(trailers:key=Source-Commit,valueonly)' origin/deploy | head -1 | tr -d '[:space:]')
[[ -n "$SRC" ]] || { echo "drift: deploy has no Source-Commit"; exit 1; }
# Inputs that end up in the deploy tree or change how it is built.
DEPLOYABLE=(dags packages projects requirements/dbt.txt ci/parse.env ci/build.py)
CHANGED=$(git diff --name-only "$SRC" "$TIP" -- "${DEPLOYABLE[@]}")
if [[ -z "$CHANGED" ]]; then
  echo "in sync: deploy built from ${SRC:0:12}, master at ${TIP:0:12}, no deployable change in between"
else
  echo "drift: master ${TIP:0:12} has deployable changes not in deploy (built from ${SRC:0:12}):"
  echo "$CHANGED" | sed 's/^/  /'
  exit 1
fi
