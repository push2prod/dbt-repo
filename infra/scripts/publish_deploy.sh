#!/usr/bin/env bash
# Publish the generated overlay on the `deploy` branch that git-sync tracks.
#
#   bash infra/scripts/publish_deploy.sh <source-sha> <source-branch> <overlay-dir>
#
# `deploy` is rewritten on every deploy to: <source-branch>'s history, ending at
# <source-sha>, plus ONE commit adding exactly the overlay's files in place
# (<project>/target/manifest.json, dbt_packages/, utils/dbt_factory/projects.json).
# So `git diff <source-sha> deploy` lists only generated files.
#
# Needs `origin` to be pushable. Commit identity: DEPLOY_GIT_NAME / DEPLOY_GIT_EMAIL.
set -euo pipefail

sha=$(git rev-parse --verify "$1^{commit}")
branch=$2
overlay=$(cd "$3" && pwd)
repo=$(git rev-parse --show-toplevel)

# An older pipeline must never rewrite `deploy` back to an older commit.
if [ "$(git ls-remote origin "refs/heads/$branch" | cut -f1)" != "$sha" ]; then
  echo "$branch moved on during this pipeline; the newer pipeline deploys"
  exit 0
fi

# Current tip of `deploy` (empty if the branch doesn't exist yet): the push lease.
lease=$(git ls-remote origin refs/heads/deploy | cut -f1)

work=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$work"' EXIT
git worktree add -q --detach "$work" "$sha"
cp -a "$overlay/." "$work/"
cd "$work"
# Add exactly the overlay's files; -f because target/ and dbt_packages/ are gitignored.
(cd "$overlay" && find . -type f -print0) | git add -f --pathspec-from-file=- --pathspec-file-nul
git -c user.name="${DEPLOY_GIT_NAME:-deploy-bot}" -c user.email="${DEPLOY_GIT_EMAIL:-deploy-bot@localhost}" \
  commit -q --allow-empty -m "deploy: generated files for ${sha:0:12} [skip ci]" -m "Source-Commit: $sha"

# Skip when `deploy` already is this commit + these files (re-run, retried job).
# (Parent read from the raw commit, so a --depth 1 fetch is enough.)
if [ -n "$lease" ] && git fetch -q --depth 1 origin refs/heads/deploy \
  && [ "$(git rev-parse FETCH_HEAD)" = "$lease" ] \
  && [ "$(git cat-file commit "$lease" | sed -n 's/^parent //p')" = "$sha" ] \
  && [ "$(git rev-parse "$lease^{tree}")" = "$(git rev-parse "HEAD^{tree}")" ]; then
  echo "deploy is already $sha + the same generated files; nothing to push"
  exit 0
fi

# Rewritten every time, so force; the lease fails the job if `deploy` moved meanwhile.
git push -q --force-with-lease="refs/heads/deploy:$lease" origin "HEAD:refs/heads/deploy"
echo "deploy -> $(git rev-parse --short HEAD) (= $branch@${sha:0:12} + generated files)"
