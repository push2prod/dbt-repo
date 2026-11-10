#!/usr/bin/env bash
# Commit a built deploy tree onto the `deploy` branch, safely under races.
#
#   ci/publish.sh <build-dir>
#
# Reads <build-dir>.meta.json (written by ci/build.py): the source commit that
# was built and the deploy commit the build was based on.
#
# Exit codes: 0 = published, or deliberately skipped
#             75 = lost a race (deploy moved); rebuild on the new tip and retry
#
# How master and deploy stay consistent
# -------------------------------------
# * `deploy` is NOT master + a commit. It is its own linear history of
#   snapshots: each commit holds the full deploy tree, its parent is the
#   previous deploy, and it carries a `Source-Commit:` trailer pointing at the
#   master commit it was built from. Nothing is ever merged or rebased, so
#   there are no content conflicts, whatever the merge strategy on master.
# * Invariant: deploy only ever moves forward to a build of the *current
#   master tip*.
#     1. Freshness: if the source is no longer origin/master's tip, skip.
#        A newer run (queued behind us) or the hourly reconcile deploys the tip.
#        This is what stops a stalled or re-run old job from publishing an
#        old state.
#     2. Compare-and-swap: the new commit's parent is the deploy commit the
#        build *read*, and the push is a fast-forward with a lease on that commit. If anything
#        moved deploy in the meantime, the push is rejected (not merged, not
#        overwritten) and we exit 75. ci/deploy.sh then rebuilds against the
#        new deploy (so carried-over manifests are the current ones) and retries.
#     Never blind-force-push this branch: the rejection *is* the lock.
# * Identical tree -> no commit (no git-sync churn, no Airflow reparse storm).
#   deploy's Source-Commit can then lag master by commits that change nothing
#   deployable. ci/deploy_status.sh reports "in sync" in that case.
set -euo pipefail

BUILD_DIR=$(cd "$1" && pwd)
META="$BUILD_DIR.meta.json"
BRANCH=${DEPLOY_BRANCH:-deploy}
SOURCE_BRANCH=${SOURCE_BRANCH:-master}
REMOTE=${REMOTE:-origin}

read_meta() { python3 -c "import json,sys; v=json.load(open(sys.argv[1]))[sys.argv[2]]; print(v or '')" "$META" "$1"; }
SOURCE_SHA=$(read_meta source_commit)
BASE=$(read_meta base_deploy)

git fetch --quiet "$REMOTE" \
  "+refs/heads/$SOURCE_BRANCH:refs/remotes/$REMOTE/$SOURCE_BRANCH" \
  "+refs/heads/$BRANCH:refs/remotes/$REMOTE/$BRANCH" 2>/dev/null \
  || git fetch --quiet "$REMOTE" "+refs/heads/$SOURCE_BRANCH:refs/remotes/$REMOTE/$SOURCE_BRANCH"
TIP=$(git rev-parse "refs/remotes/$REMOTE/$SOURCE_BRANCH")
CURRENT=$(git rev-parse -q --verify "refs/remotes/$REMOTE/$BRANCH^{commit}" || true)

# 1. Freshness: only a build of the current master tip may be published.
if [[ "$SOURCE_SHA" != "$TIP" ]]; then
  echo "::notice::built ${SOURCE_SHA:0:12} but $SOURCE_BRANCH is now at ${TIP:0:12}; skipping (the newer commit gets its own deploy)"
  exit 0
fi

# 2. The base we built on must still be the deploy tip, otherwise retry.
if [[ "$CURRENT" != "$BASE" ]]; then
  echo "::notice::deploy moved (${BASE:0:12} -> ${CURRENT:0:12}) since the build read it; rebuild needed"
  exit 75
fi

INDEX=$(mktemp)
trap 'rm -f "$INDEX"' EXIT
rm -f "$INDEX"
# add -f: projects ship target/ and dbt_packages/, which their .gitignore excludes.
# Never stage Python bytecode: importing the DAGs (tests, Airflow) writes
# __pycache__/*.pyc into the tree, and their embedded timestamps would make
# every run look like a change.
GIT_INDEX_FILE=$INDEX git --work-tree="$BUILD_DIR" add -A -f -- . ':(exclude,glob)**/__pycache__/**' ':(exclude,glob)**/*.pyc'
TREE=$(GIT_INDEX_FILE=$INDEX git write-tree)

if [[ -n "$BASE" && "$(git rev-parse "$BASE^{tree}")" == "$TREE" ]]; then
  echo "deploy tree unchanged; nothing to publish (deploy already equals a build of ${SOURCE_SHA:0:12})"
  exit 0
fi

MSG=$(printf 'deploy: %s [skip ci]\n\nSource-Commit: %s\nWorkflow-Run: %s\n' \
  "$(git log -1 --format=%s "$SOURCE_SHA")" "$SOURCE_SHA" \
  "${GITHUB_SERVER_URL:-}/${GITHUB_REPOSITORY:-local}/actions/runs/${GITHUB_RUN_ID:-0}")
export GIT_AUTHOR_NAME=${GIT_AUTHOR_NAME:-deploy-bot} GIT_AUTHOR_EMAIL=${GIT_AUTHOR_EMAIL:-deploy-bot@users.noreply.github.com}
export GIT_COMMITTER_NAME=$GIT_AUTHOR_NAME GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL
COMMIT=$(git commit-tree "$TREE" ${BASE:+-p "$BASE"} -m "$MSG")

# Compare-and-swap on the deploy tip. COMMIT's parent is BASE, so this is
# always a fast-forward; the lease only adds "and the remote must still be
# BASE" (or "must not exist yet" when BASE is empty), checked by the server.
# Branch protection with "no force pushes" still allows it.
if git push --quiet --force-with-lease="refs/heads/$BRANCH:$BASE" "$REMOTE" "$COMMIT:refs/heads/$BRANCH"; then
  echo "published ${COMMIT:0:12} from ${SOURCE_SHA:0:12}"
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && echo "Published \`$COMMIT\` to \`$BRANCH\` from \`$SOURCE_SHA\`" >> "$GITHUB_STEP_SUMMARY"
  exit 0
fi
echo "::notice::push rejected: deploy moved concurrently; rebuild needed"
exit 75
