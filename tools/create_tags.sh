#!/usr/bin/env bash
# Creates the milestone tags at the squash-merge commits and publishes them as GitHub pre-releases.
#
# Why this script exists: the automation session that merged the milestone PRs could push branches and merge
# PRs but was not allowed to create tags or releases, so the tags are created here, once, with your credentials.
#
# Requires: a clone with remote "origin", push rights, and an authenticated `gh` CLI.   Usage: tools/create_tags.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

declare -A SHA=(
  [v0.1.0]=9ef9b87fe98cf78f51c05c74abadd1d14954daf4
  [v0.2.0]=8a5800545f522d34ac2237c74e8512435c61a6df
  [v0.3.0]=2095e61114f1e932a33f5ada35b92e98d38971c1
  [v0.4.0]=952a465cc904cdc1e89de4315099634bb87f62f9
  [v0.5.0]=333cb6b2c48c373bce1827d0a4e9348180ac0012
  [v0.6.0]=47be6dfcc5ce2dbde49e62a97613eca39a08618d
  [v0.7.0]=1e13707c69a773dec5256a742440e50f85571678
  [v0.8.0]=b2e660a7363a3f69c87df2d7bb32ca80ccb73ae1
  [v0.9.0]=cdb15a0fd71ec28b8853fa1fbd27ddaab32382d0
)

git fetch -q origin main --tags
for tag in $(printf '%s\n' "${!SHA[@]}" | sort -V); do
  sha=${SHA[$tag]}
  if ! git merge-base --is-ancestor "$sha" origin/main; then
    echo "ERROR: $sha ($tag) is not on origin/main" >&2; exit 1
  fi
  title=$(sed -n '1s/^<!-- title: \(.*\) -->$/\1/p' "docs/release_notes/$tag.md")
  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "tag $tag already exists - skipping"
  else
    git tag -a "$tag" "$sha" -m "$title"
    git push origin "refs/tags/$tag"
  fi
  if gh release view "$tag" >/dev/null 2>&1; then
    echo "release $tag already exists - skipping"
  else
    tail -n +2 "docs/release_notes/$tag.md" > "/tmp/notes-$tag.md"
    kind=--prerelease
    [[ $tag == v1.* ]] && kind=--latest   # v1.x = finished project, not a milestone pre-release
    gh release create "$tag" --verify-tag $kind --title "$tag - $title" --notes-file "/tmp/notes-$tag.md"
  fi
done
echo "Done."
