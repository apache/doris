#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cp "${ROOT}/04-release-complete.sh" "$tmp/"
cat > "$tmp/release.env" <<'EOF'
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
VERSION="9.9.9"
RC="rc01"
TAG="${VERSION}-${RC}"
PKG_BASE="apache-doris-${TAG}-src"
RELEASE_PKG_BASE="apache-doris-${VERSION}-src"
WORK_DIR="${ROOT}/${TAG}"
REPO_DIR="${ROOT}"
GIT_REMOTE="apache-test"
GITHUB_REPO="apache/doris-test"
DEV_SVN_BASE="https://dist.example.test/dev/doris"
DEV_SVN_DIR="${DEV_SVN_BASE}/${TAG}"
RELEASE_SVN_BASE="https://dist.example.test/release/doris"
RELEASE_SERIES="${VERSION%.*}"
RELEASE_BIN_DOWNLOAD_BASE="https://binaries.example.test"
BIN_FILES=(
  "/nonexistent/apache-doris-9.9.9-bin-x64.tar.gz"
  "/nonexistent/apache-doris-9.9.9-bin-arm64.tar.gz"
)
DOWNLOAD_PAGE_URL="https://doris.example.test/download/"
ANNOUNCE_RELEASE_NOTES_URL="https://doris.example.test/release-notes"
RELEASE_NOTES_URL=""
DEV_LIST="dev@example.test"
SIGNER_NAME="Release Manager"
EOF

# The release SVN publish is already done, so the run goes straight to the tag
# and GitHub release steps.
cat > "$tmp/svn" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
url="${@: -1}"
[[ "$1" == "info" ]] || { echo "unexpected svn command: $*" >&2; exit 1; }
case "$url" in
  https://dist.example.test/release/doris/9.9|https://dist.example.test/release/doris/9.9/9.9.9|https://dist.example.test/release/doris/9.9/9.9.9/*) ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$tmp/svn"

cat > "$tmp/svnmucc" <<'EOF'
#!/usr/bin/env bash
echo "svnmucc must not run when the release is already published" >&2
exit 1
EOF
chmod +x "$tmp/svnmucc"

cat > "$tmp/gpg" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$tmp/gpg"

# FAKE_TAG_PUSHED decides whether the RC-free tag is already on the remote,
# FAKE_TAG_COMMIT lets a case point it at a different commit.
cat > "$tmp/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
rc_commit="3333333333333333333333333333333333333333"
tag_commit="${FAKE_TAG_COMMIT:-$rc_commit}"
while [[ "${1:-}" == "-C" ]]; do shift 2; done
case "${1:-}" in
  rev-parse)
    # Real git echoes the argument back and exits 1 for a missing ref unless
    # -q --verify is passed. Reproduce that, so a caller that forgets the
    # flags captures the argument instead of an empty string and the case
    # below fails loudly.
    quiet=0
    for a in "$@"; do [[ "$a" == "--verify" ]] && quiet=1; done
    ref="${@: -1}"
    case "$ref" in
      "9.9.9-rc01^{commit}") printf '%s\n' "$rc_commit" ;;
      "9.9.9^{commit}")
        if [[ "${FAKE_TAG_LOCAL:-0}" -eq 1 ]]; then
          printf '%s\n' "$tag_commit"
        else
          [[ "$quiet" -eq 1 ]] || printf '%s\n' "$ref"
          exit 1
        fi
        ;;
      *) [[ "$quiet" -eq 1 ]] || printf '%s\n' "$ref"; exit 1 ;;
    esac
    ;;
  ls-remote)
    [[ "${FAKE_TAG_PUSHED:-0}" -eq 1 ]] || exit 0
    printf '%s\trefs/tags/9.9.9\n%s\trefs/tags/9.9.9^{}\n' "$tag_commit" "$tag_commit"
    ;;
  tag|push) printf '%s\n' "$*" >> "$FAKE_GIT_LOG" ;;
  *) echo "unexpected git command: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/git"

# FAKE_RELEASE_EXISTS decides whether the GitHub release is already published.
cat > "$tmp/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_GH_LOG"
case "$1 ${2:-}" in
  "auth status") exit 0 ;;
  "release view")
    [[ "${FAKE_RELEASE_EXISTS:-0}" -eq 1 ]] || exit 1
    if [[ "$*" == *"--json url"* ]]; then
      printf 'https://github.test/apache/doris-test/releases/tag/9.9.9\n'
    fi
    ;;
  "api repos/apache/doris-test/releases/latest") printf '9.9.9-old\n' ;;
  "release create")
    shift 2
    printf '%s\n' "$*" > "$FAKE_GH_CREATE"
    while (($#)); do
      if [[ "$1" == "--notes-file" ]]; then cp "$2" "$FAKE_GH_BODY"; fi
      shift
    done
    ;;
  *) echo "unexpected gh invocation: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/gh"

export PATH="$tmp:$PATH"
export FAKE_GIT_LOG="$tmp/git.log" FAKE_GH_LOG="$tmp/gh.log"
export FAKE_GH_CREATE="$tmp/gh-create.log" FAKE_GH_BODY="$tmp/gh-body.md"

run() { : > "$FAKE_GIT_LOG"; : > "$FAKE_GH_LOG"; rm -f "$FAKE_GH_CREATE" "$FAKE_GH_BODY"; }

# --- fresh run: tag is created and pushed, release is published not-Latest ---
run
out="$(printf 'y\ny\ny\nn\ny\n' | bash "$tmp/04-release-complete.sh" 2>&1)"

grep -qF "tag -a 9.9.9 -m 9.9.9 release 333333333" "$FAKE_GIT_LOG" || {
  echo "the RC-free tag must be created from the RC commit" >&2; cat "$FAKE_GIT_LOG" >&2; exit 1; }
grep -qF "push apache-test refs/tags/9.9.9" "$FAKE_GIT_LOG" || {
  echo "the RC-free tag must be pushed" >&2; cat "$FAKE_GIT_LOG" >&2; exit 1; }
[[ -f "$FAKE_GH_CREATE" ]] || { echo "the GitHub release must be created" >&2; exit 1; }
grep -qF -- "--latest=false" "$FAKE_GH_CREATE" || {
  echo "declining the Latest question must publish the release as not-Latest" >&2
  cat "$FAKE_GH_CREATE" >&2; exit 1; }
grep -qF -- "--title Apache Doris 9.9.9 Release" "$FAKE_GH_CREATE" || {
  echo "unexpected release title" >&2; cat "$FAKE_GH_CREATE" >&2; exit 1; }
grep -qF "The release currently marked Latest on apache/doris-test is 9.9.9-old." <<<"$out" || {
  echo "the Latest question must say which release is Latest today" >&2
  printf '%s\n' "$out" >&2; exit 1; }

# body follows the shape used by every previous Doris release
grep -qxF '[Change Log](https://doris.example.test/release-notes)' "$FAKE_GH_BODY" || {
  echo "the body must open with the change log link" >&2; cat "$FAKE_GH_BODY" >&2; exit 1; }
grep -qxF -- '- Official Downloads: https://doris.example.test/download' "$FAKE_GH_BODY" || {
  echo "the body must link the official download page" >&2; cat "$FAKE_GH_BODY" >&2; exit 1; }
grep -qF '[apache-doris-9.9.9-src.tar.gz](https://dist.example.test/release/doris/9.9/9.9.9/apache-doris-9.9.9-src.tar.gz)' "$FAKE_GH_BODY" || {
  echo "the body must link the release SVN source tarball" >&2; cat "$FAKE_GH_BODY" >&2; exit 1; }
for arch in x64 arm64; do
  grep -qxF -- "- Binary(${arch}):" "$FAKE_GH_BODY" || {
    echo "the body must carry a Binary(${arch}) section" >&2; cat "$FAKE_GH_BODY" >&2; exit 1; }
  grep -qF "(https://binaries.example.test/apache-doris-9.9.9-bin-${arch}.tar.gz)" "$FAKE_GH_BODY" || {
    echo "the ${arch} binary must use the release download base" >&2; cat "$FAKE_GH_BODY" >&2; exit 1; }
done

# --- accepting the Latest question ------------------------------------------
run
printf 'y\ny\ny\ny\ny\n' | bash "$tmp/04-release-complete.sh" >/dev/null 2>&1
grep -qF -- "--latest=false" "$FAKE_GH_CREATE" && {
  echo "accepting the Latest question must publish the release as Latest" >&2
  cat "$FAKE_GH_CREATE" >&2; exit 1; }
grep -qF -- "--latest" "$FAKE_GH_CREATE" || {
  echo "accepting the Latest question must pass --latest" >&2
  cat "$FAKE_GH_CREATE" >&2; exit 1; }

# --- re-run: tag already pushed and release already published ----------------
run
out="$(printf 'y\ny\n' | FAKE_TAG_LOCAL=1 FAKE_TAG_PUSHED=1 FAKE_RELEASE_EXISTS=1 \
  bash "$tmp/04-release-complete.sh" 2>&1)"

if [[ -s "$FAKE_GIT_LOG" ]]; then
  echo "a re-run must not create or push the tag again" >&2; cat "$FAKE_GIT_LOG" >&2; exit 1
fi
[[ -f "$FAKE_GH_CREATE" ]] && { echo "a re-run must not create the release again" >&2; exit 1; }
grep -qF "release tag 9.9.9 already on apache-test" <<<"$out" || {
  echo "a re-run must report the tag as already pushed" >&2; printf '%s\n' "$out" >&2; exit 1; }
grep -qF "GitHub release already published" <<<"$out" || {
  echo "a re-run must report the release as already published" >&2; printf '%s\n' "$out" >&2; exit 1; }
[[ -f "$tmp/9.9.9-rc01/announce-email.txt" ]] || {
  echo "a re-run must still write the announce email draft" >&2; exit 1; }

# --- an existing tag on a different commit must stop the run ----------------
run
if printf 'y\ny\ny\ny\n' | FAKE_TAG_LOCAL=1 FAKE_TAG_PUSHED=1 \
    FAKE_TAG_COMMIT=4444444444444444444444444444444444444444 \
    bash "$tmp/04-release-complete.sh" >/dev/null 2>&1; then
  echo "a 9.9.9 tag pointing at another commit must not be accepted" >&2
  exit 1
fi

# --- --skip-github-release pushes the tag and leaves the release alone -------
run
printf 'y\ny\ny\n' | bash "$tmp/04-release-complete.sh" --skip-github-release >/dev/null 2>&1
grep -qF "push apache-test refs/tags/9.9.9" "$FAKE_GIT_LOG" || {
  echo "--skip-github-release must still push the tag" >&2; cat "$FAKE_GIT_LOG" >&2; exit 1; }
[[ -f "$FAKE_GH_CREATE" ]] && { echo "--skip-github-release must not create the release" >&2; exit 1; }

exit 0
