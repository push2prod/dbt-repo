#!/usr/bin/env bash
# End-to-end check of build + publish under each merge strategy, against a
# throwaway local "origin". No GitHub, no warehouse.
#
#   bash ci/e2e_merge_strategies.sh      (needs dbt from requirements/dbt.txt on PATH)
#
# Scenarios:
#   0. first deploy                           -> every project built
#   1. squash-merge a finance change          -> only finance rebuilt
#   2. rebase-merge 2 commits in one push:    -> all 3 rebuilt. HEAD~1 only shows
#      (a) shared macro, (b) marketing model     marketing; content hashing still
#                                                sees the shared-package change
#   3. merge-commit a README-only PR          -> nothing published
#   4. re-run of an old workflow (stale SHA)  -> refused, deploy unchanged
#   5. force-push rewriting master            -> deploys new tip
set -euo pipefail

SRC=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
export GIT_AUTHOR_NAME=e2e GIT_AUTHOR_EMAIL=e2e@example.invalid GIT_COMMITTER_NAME=e2e GIT_COMMITTER_EMAIL=e2e@example.invalid

git init -q --bare -b master "$WORK/origin.git"
git clone -q "$WORK/origin.git" "$WORK/repo" 2>/dev/null
cd "$WORK/repo"
git -C "$SRC" ls-files -z --cached --others --exclude-standard | (cd "$SRC" && xargs -0 tar cf -) | tar xf -
git add -A && git commit -qm "initial" && git push -q origin master

deploy() {  # build + publish current HEAD; print what happened
  python ci/build.py --out "$WORK/build" --previous origin/deploy | grep summary
  bash ci/publish.sh "$WORK/build" "${1:-HEAD}" deploy 2>&1 | grep -vE '^(To |   [0-9a-f]+\.\.|remote:| \* \[new)' || true
}
deploy_head_source() { git log -1 --format='%(trailers:key=Source-Commit,valueonly)' origin/deploy | tr -d '[:space:]'; }
expect() { if [[ "$1" == "$2" ]]; then echo "  ok: $3"; else echo "  FAIL: $3 (got '$1', want '$2')"; exit 1; fi; }

echo "== 0. first deploy"
deploy
FIRST=$(git rev-parse HEAD)

echo "== 1. squash merge: PR with 3 commits touching finance -> 1 commit on master"
git checkout -qb pr1
for i in 1 2 3; do echo "-- wip $i" >> projects/finance/models/marts/fct_revenue.sql; git commit -qam "wip $i"; done
git checkout -q master && git merge -q --squash pr1 && git commit -qm "finance: tweak revenue (#1)" && git push -q origin master
deploy
expect "$(deploy_head_source)" "$(git rev-parse HEAD)" "deploy points at squash commit"

echo "== 2. rebase merge: 2 commits, one push (shared macro + marketing)"
git checkout -qb pr2
sed -i 's/precision=2/precision=4/' packages/shared_macros/macros/cents_to_dollars.sql && git commit -qam "shared: more precision"
echo "-- note" >> projects/marketing/models/marts/campaign_performance.sql && git commit -qam "marketing: note"
git checkout -q master && git rebase -q pr2 && git push -q origin master
echo "  HEAD~1..HEAD touches: $(git diff --name-only HEAD~1 HEAD | tr '\n' ' ')(diff-based detection would rebuild marketing only)"
deploy
expect "$(git show origin/deploy:projects/core/dbt_packages/shared_macros/macros/cents_to_dollars.sql | grep -c 'precision=4')" "1" \
  "core shipped with the new shared macro although nothing under projects/core changed"

echo "== 3. merge commit: README-only PR"
git checkout -qb pr3 && echo "docs" >> README.md && git add README.md && git commit -qm "docs"
git checkout -q master && git merge -q --no-ff pr3 -m "Merge pull request #3" && git push -q origin master
BEFORE=$(git rev-parse origin/deploy)
deploy
git fetch -q origin deploy
expect "$(git rev-parse origin/deploy)" "$BEFORE" "no new deploy commit for a no-op change"

echo "== 4. stale re-run of the first workflow"
BEFORE=$(git rev-parse origin/deploy)
deploy "$FIRST"
git fetch -q origin deploy
expect "$(git rev-parse origin/deploy)" "$BEFORE" "old SHA did not overwrite newer deploy"

echo "== 5. force-push rewrites master (old deploy source no longer an ancestor)"
git reset -q --hard "$FIRST" && echo "-- hotfix" >> projects/core/models/marts/fct_orders.sql && git commit -qam "hotfix on rewritten history"
git push -q -f origin master
deploy
expect "$(deploy_head_source)" "$(git rev-parse HEAD)" "deploy follows rewritten master"

echo "== deploy branch history (linear, one commit per deployed state)"
git fetch -q origin deploy && git log --format='  %h %s' origin/deploy
