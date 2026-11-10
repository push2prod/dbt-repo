#!/usr/bin/env bash
# Commit a built deploy tree onto the `deploy` branch and push it.
#
#   ci/publish.sh <build-dir> <source-sha> [deploy-branch]
#
# * The deploy tree is committed with plumbing (write-tree/commit-tree) into a
#   throwaway index, so master's working tree and .gitignore files are never
#   involved. `add -f` is needed because projects ship target/ and
#   dbt_packages/, which their .gitignore excludes.
# * The deploy branch is linear: one commit per *deployed state*, never a
#   merge, never force-pushed. Rolling back = reverting a deploy commit (or
#   pointing git-sync at an older deploy SHA).
# * No-op: if the tree equals the current deploy tree (for example a
#   README-only change), nothing is pushed and git-sync sees nothing new.
# * Stale guard: if the current deploy came from a commit that already
#   contains <source-sha> (a re-run of an old workflow), refuse to go back.
# * The commit message carries [skip ci] and Source-Commit / Source-Tree
#   trailers. Source-Tree is the master *tree* hash, which is the same whether
#   the PR was squashed, rebased or merged, so it is the stable thing to look up.
set -euo pipefail

BUILD_DIR=$(cd "$1" && pwd)
SOURCE_SHA=$(git rev-parse "$2^{commit}")
BRANCH=${3:-deploy}
REMOTE=${REMOTE:-origin}

git fetch --quiet "$REMOTE" "+refs/heads/$BRANCH:refs/remotes/$REMOTE/$BRANCH" 2>/dev/null || true
PARENT=$(git rev-parse -q --verify "refs/remotes/$REMOTE/$BRANCH^{commit}" || true)

if [[ -n "$PARENT" ]]; then
  PREV_SOURCE=$(git log -1 --format='%(trailers:key=Source-Commit,valueonly)' "$PARENT" | head -1 | tr -d '[:space:]')
  if [[ -n "$PREV_SOURCE" && "$PREV_SOURCE" != "$SOURCE_SHA" ]] \
     && git cat-file -e "$PREV_SOURCE^{commit}" 2>/dev/null \
     && git merge-base --is-ancestor "$SOURCE_SHA" "$PREV_SOURCE"; then
    echo "::warning::deploy already at $PREV_SOURCE, which contains $SOURCE_SHA; not going back"
    exit 0
  fi
fi

INDEX=$(mktemp)
trap 'rm -f "$INDEX"' EXIT
rm -f "$INDEX"
export GIT_INDEX_FILE=$INDEX
git --work-tree="$BUILD_DIR" add -A -f .
TREE=$(git write-tree)
unset GIT_INDEX_FILE

if [[ -n "$PARENT" && "$(git rev-parse "$PARENT^{tree}")" == "$TREE" ]]; then
  echo "deploy tree unchanged ($TREE); nothing to publish"
  exit 0
fi

SUBJECT=$(git log -1 --format=%s "$SOURCE_SHA")
MSG=$(printf 'deploy: %s [skip ci]\n\nSource-Commit: %s\nSource-Tree: %s\nWorkflow-Run: %s\n' \
  "$SUBJECT" "$SOURCE_SHA" "$(git rev-parse "$SOURCE_SHA^{tree}")" \
  "${GITHUB_SERVER_URL:-}/${GITHUB_REPOSITORY:-local}/actions/runs/${GITHUB_RUN_ID:-0}")

export GIT_AUTHOR_NAME=${GIT_AUTHOR_NAME:-deploy-bot} GIT_AUTHOR_EMAIL=${GIT_AUTHOR_EMAIL:-deploy-bot@users.noreply.github.com}
export GIT_COMMITTER_NAME=$GIT_AUTHOR_NAME GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL
COMMIT=$(git commit-tree "$TREE" ${PARENT:+-p "$PARENT"} -m "$MSG")

# Plain push, no force: if someone else moved the branch in the meantime this
# fails, and the next run builds on top of the new tip.
git push "$REMOTE" "$COMMIT:refs/heads/$BRANCH"
echo "published $COMMIT (tree $TREE) from $SOURCE_SHA"
[[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && echo "Published \`$COMMIT\` to \`$BRANCH\` from \`$SOURCE_SHA\`" >> "$GITHUB_STEP_SUMMARY"
exit 0
