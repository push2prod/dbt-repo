#!/usr/bin/env bash
# Extract the current `deploy` tree (if the branch exists) into <dir>, as the
# generator's --previous: unchanged projects are copied from it byte for byte.
#
#   infra/scripts/fetch_previous.sh <dir>
set -euo pipefail

dir=$1
mkdir -p "$dir"
if git fetch -q --depth 1 origin refs/heads/deploy 2>/dev/null; then
  git archive FETCH_HEAD | tar -x -C "$dir"
  echo "previous: deploy@$(git rev-parse --short FETCH_HEAD) -> $dir"
else
  echo "previous: no deploy branch yet; every project is built"
fi
