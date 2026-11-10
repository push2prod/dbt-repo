#!/usr/bin/env bash
# End-to-end check of build + publish against a throwaway local "origin".
# No GitHub, no warehouse. Needs dbt from requirements/dbt.txt on PATH.
#
#   bash ci/e2e_merge_strategies.sh
#
#  0. first deploy                                  -> all projects built
#  1. squash merge (3 PR commits -> 1)              -> only finance re-parsed
#  2. rebase merge, 2 commits in one push           -> HEAD~1 shows only marketing, but the
#     (shared macro, then marketing)                   earlier shared-macro commit rebuilds all 3
#  3. merge commit, README only                     -> nothing published, status "in sync"
#  4. deploy skipped/failed for one push, then the  -> both finance and core rebuilt
#     next push lands                                  (diff vs *deployed* source, not HEAD~1)
#  5. stalled run: built at X, master moved to Y    -> late publish of X is skipped
#     and Y was deployed
#  6. two runs for the same tip race                -> loser is rejected (CAS), retries, no-op
#  7. new project added / project deleted           -> built / removed
#  8. force-push rewrites master                    -> deploys new tip
set -euo pipefail

SRC=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
export GIT_AUTHOR_NAME=e2e GIT_AUTHOR_EMAIL=e2e@example.invalid GIT_COMMITTER_NAME=e2e GIT_COMMITTER_EMAIL=e2e@example.invalid
export SKIP_TESTS=1  # pytest is covered separately; keeps this fast

git init -q --bare -b master "$WORK/origin.git"
git clone -q "$WORK/origin.git" "$WORK/repo" 2>/dev/null
cd "$WORK/repo"
git -C "$SRC" ls-files -z --cached --others --exclude-standard | (cd "$SRC" && xargs -0 tar cf -) | tar xf -
git add -A && git commit -qm "initial" && git push -q origin master 2>/dev/null

show() { grep -E '^\[build\] (rebuilding|[a-z_]+: (built|reused))|^(published|deploy tree|::)' | sed 's/^/    /' || true; }
deploy() { OUT="$WORK/build" bash ci/deploy.sh 2>&1 | show; }
build_only() { python ci/build.py --out "$1" --previous origin/deploy 2>&1 | show; }
publish_only() { local rc=0; bash ci/publish.sh "$1" > "$WORK/pub.log" 2>&1 || rc=$?; show < "$WORK/pub.log"; return $rc; }
merge_pr() { git push -q origin master 2>/dev/null; }
fetch() { git fetch -q origin "+refs/heads/deploy:refs/remotes/origin/deploy" 2>/dev/null || true; }
deploy_tip() { fetch; git rev-parse origin/deploy; }
src_of_deploy() { fetch; git log -1 --format='%(trailers:key=Source-Commit,valueonly)' origin/deploy | tr -d '[:space:]'; }
expect() { if [[ "$1" == "$2" ]]; then echo "  ok: $3"; else echo "  FAIL: $3 (got '$1', want '$2')"; exit 1; fi; }
built_at() { fetch; git show "origin/deploy:projects/$1/target/build-info.json" | python3 -c "import json,sys; print(json.load(sys.stdin)['built_at_commit'])"; }

echo "== 0. first deploy"
deploy
expect "$(src_of_deploy)" "$(git rev-parse HEAD)" "deploy built from master tip"

echo "== 1. squash merge"
git checkout -qb pr1
for i in 1 2 3; do echo "-- wip $i" >> projects/finance/models/marts/fct_revenue.sql; git commit -qam "wip $i"; done
git checkout -q master && git merge -q --squash pr1 >/dev/null && git commit -qm "finance: tweak revenue (#1)" && merge_pr
CORE_BEFORE=$(built_at core)
deploy
expect "$(built_at finance)" "$(git rev-parse HEAD)" "finance manifest re-generated"
expect "$(built_at core)" "$CORE_BEFORE" "core manifest untouched (same bytes)"

echo "== 2. rebase merge: 2 commits, one push"
git checkout -qb pr2
sed -i 's/precision=2/precision=4/' packages/shared_macros/macros/cents_to_dollars.sql && git commit -qam "shared: more precision"
echo "-- note" >> projects/marketing/models/marts/campaign_performance.sql && git commit -qam "marketing: note"
git checkout -q master && git rebase -q pr2 && merge_pr
echo "  (HEAD~1..HEAD only touches: $(git diff --name-only HEAD~1 HEAD))"
deploy
expect "$(built_at core)" "$(git rev-parse HEAD)" "core rebuilt because its shared package changed"

echo "== 3. merge commit, README only"
git checkout -qb pr3 && echo "docs" >> README.md && git commit -qam "docs"
git checkout -q master && git merge -q --no-ff pr3 -m "Merge pull request #3" && merge_pr
BEFORE=$(deploy_tip)
deploy
expect "$(deploy_tip)" "$BEFORE" "no deploy commit for a no-op merge"
bash ci/deploy_status.sh | sed 's/^/  /'

echo "== 4. one push never deployed (failed/skipped run), then next push"
echo "-- a" >> projects/finance/models/marts/fct_invoices.sql && git commit -qam "finance change (deploy failed)" && merge_pr
echo "-- b" >> projects/core/models/marts/fct_orders.sql && git commit -qam "core change" && merge_pr
echo "  (HEAD~1..HEAD only touches: $(git diff --name-only HEAD~1 HEAD))"
if bash ci/deploy_status.sh >/dev/null; then echo "  FAIL: status should report drift"; exit 1; else echo "  ok: deploy_status reports drift"; fi
deploy
expect "$(built_at finance)" "$(git rev-parse HEAD)" "finance change from the undeployed push is picked up"

echo "== 5. stalled run publishes late"
build_only "$WORK/stalled"                        # run for X builds, then hangs
echo "-- c" >> projects/marketing/models/staging/stg_sessions.sql && git commit -qam "marketing change Y" && merge_pr
deploy                                            # run for Y deploys
AFTER_Y=$(deploy_tip)
publish_only "$WORK/stalled"                      # X wakes up and tries to publish
expect "$(deploy_tip)" "$AFTER_Y" "stale build did not overwrite the newer deploy"

echo "== 6. two runs for the same tip race (push + reconcile)"
echo "-- d" >> projects/finance/models/staging/stg_payments.sql && git commit -qam "finance change Z" && merge_pr
fetch; build_only "$WORK/runA"; build_only "$WORK/runB"   # both read the same deploy tip
publish_only "$WORK/runB"
rc=0; publish_only "$WORK/runA" || rc=$?
expect "$rc" "75" "loser of the race is rejected, not merged or forced"
deploy                                                     # its retry, rebuilt on the new tip
expect "$(src_of_deploy)" "$(git rev-parse HEAD)" "deploy is still a build of the tip"

echo "== 7. add a project, then delete one"
cp -r projects/core projects/sales && sed -i 's/\bcore\b/sales/g' projects/sales/dbt_project.yml projects/sales/profiles.yml
git add -A && git commit -qm "add sales" && merge_pr && deploy
fetch; expect "$(git cat-file -t origin/deploy:projects/sales/target/manifest.json)" "blob" "new project built"
git rm -rq projects/sales && git commit -qm "remove sales" && merge_pr && deploy
fetch; expect "$(git cat-file -t origin/deploy:projects/sales 2>/dev/null || echo gone)" "gone" "deleted project removed from deploy"

echo "== 8. force-push rewrites master"
git reset -q --hard HEAD~3 && echo "-- hotfix" >> projects/core/models/marts/dim_customers.sql && git commit -qam "hotfix on rewritten history"
git push -q -f origin master 2>/dev/null
deploy
expect "$(src_of_deploy)" "$(git rev-parse HEAD)" "deploy follows rewritten master"
bash ci/deploy_status.sh | sed 's/^/  /'

echo "== deploy history (linear, fast-forward only)"
git log --format='  %h %s' origin/deploy
