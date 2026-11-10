#!/usr/bin/env bash
# Minimal stand-in for the git-sync sidecar (same on-disk contract):
#   <root>/.worktrees/<sha>/   one checkout per revision (fresh files, fresh mtimes)
#   <root>/repo -> .worktrees/<sha>   swapped atomically with rename(2)
# Old worktrees are removed after STALE_WORKTREE_TIMEOUT seconds (git-sync v4
# --stale-worktree-timeout; its default 0 means "right after the swap").
#
#   strain/gitsync.sh <remote> <branch> <root> [period-seconds]
set -euo pipefail
REMOTE=$1 BRANCH=$2 ROOT=$3 PERIOD=${4:-10}
STALE=${STALE_WORKTREE_TIMEOUT:-0}
mkdir -p "$ROOT/.worktrees"
[[ -d "$ROOT/.mirror" ]] || git clone -q --bare "$REMOTE" "$ROOT/.mirror"
declare -A retired=()
while true; do
  git -C "$ROOT/.mirror" fetch -q origin "+refs/heads/$BRANCH:refs/heads/$BRANCH" 2>/dev/null || true
  want=$(git -C "$ROOT/.mirror" rev-parse -q --verify "refs/heads/$BRANCH" || true)
  have=$(basename "$(readlink "$ROOT/repo" 2>/dev/null || echo none)")
  if [[ -n "$want" && "$want" != "$have" ]]; then
    wt="$ROOT/.worktrees/$want"
    rm -rf "$wt"; mkdir -p "$wt"
    # -m: fresh mtimes, as a real git-sync checkout has (Airflow re-parses by mtime)
    git -C "$ROOT/.mirror" archive "$want" | tar -x -m -C "$wt"
    ln -sfn ".worktrees/$want" "$ROOT/repo.tmp" && mv -T "$ROOT/repo.tmp" "$ROOT/repo"
    echo "$(date -u +%FT%TZ) synced ${want:0:12}"
    [[ "$have" != none ]] && retired[$have]=$(date +%s)
  fi
  now=$(date +%s)
  for sha in "${!retired[@]}"; do
    if (( now - retired[$sha] >= STALE )); then
      rm -rf "$ROOT/.worktrees/$sha"; unset "retired[$sha]"
      echo "$(date -u +%FT%TZ) removed old worktree ${sha:0:12}"
    fi
  done
  sleep "$PERIOD"
done
