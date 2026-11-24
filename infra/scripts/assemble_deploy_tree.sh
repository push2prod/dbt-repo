#!/usr/bin/env bash
# Assemble exactly what `deploy` will hold: <commit>'s tree with <overlay> copied
# on top. The CI checks run against this directory.
#
#   bash infra/scripts/assemble_deploy_tree.sh <commit> <overlay-dir> <out-dir>
set -euo pipefail

commit=$1
overlay=$2
out=$3
rm -rf "$out"
mkdir -p "$out"
git archive "$commit" | tar -x -C "$out"
cp -a "$overlay/." "$out/"
