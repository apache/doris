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
DEV_SVN_BASE="https://dist.example.test/dev/doris"
DEV_SVN_DIR="${DEV_SVN_BASE}/${TAG}"
RELEASE_SVN_BASE="https://dist.example.test/release/doris"
RELEASE_SERIES="${VERSION%.*}"
DOWNLOAD_PAGE_URL="https://doris.example.test/download/"
ANNOUNCE_RELEASE_NOTES_URL="https://doris.example.test/release-notes"
RELEASE_NOTES_URL=""
DEV_LIST="dev@example.test"
SIGNER_NAME="Release Manager"
REPO_DIR="${ROOT}"
GIT_REMOTE="apache-test"
BIN_FILES=()
EOF

# This case is about the SVN state. The 9.9.9 tag is already pushed and gh
# reports itself unauthenticated, so both of those steps report and return.
cat > "$tmp/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
commit="2222222222222222222222222222222222222222"
while [[ "${1:-}" == "-C" ]]; do shift 2; done
case "${1:-}" in
  rev-parse) printf '%s\n' "$commit" ;;
  ls-remote) printf '%s\trefs/tags/9.9.9\n%s\trefs/tags/9.9.9^{}\n' "$commit" "$commit" ;;
  *) echo "unexpected git command: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/git"

cat > "$tmp/gh" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
chmod +x "$tmp/gh"

# The release is already published. FAKE_DEV_DIR_PRESENT decides whether the
# stale RC folder is still sitting in the dev SVN.
cat > "$tmp/svn" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cmd="$1"
shift
url="${@: -1}"
case "$cmd" in
  info)
    case "$url" in
      https://dist.example.test/dev/doris/*)
        [[ "${FAKE_DEV_DIR_PRESENT:-0}" -eq 1 ]] || exit 1
        ;;
      https://dist.example.test/release/doris/9.9|https://dist.example.test/release/doris/9.9/9.9.9|https://dist.example.test/release/doris/9.9/9.9.9/*)
        ;;
      *)
        exit 1
        ;;
    esac
    ;;
  *)
    echo "unexpected svn command: $cmd $url" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$tmp/svn"

cat > "$tmp/svnmucc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_SVNMUCC_LOG"
EOF
chmod +x "$tmp/svnmucc"

cat > "$tmp/gpg" <<'EOF'
#!/usr/bin/env bash
echo "gpg must not run when nothing is published" >&2
exit 1
EOF
chmod +x "$tmp/gpg"

export PATH="$tmp:$PATH"
export FAKE_SVNMUCC_LOG="$tmp/svnmucc.log"

# --- re-run after a completed release, dev RC folder already gone -----------
: > "$FAKE_SVNMUCC_LOG"
out="$(printf 'y\ny\n' | bash "$tmp/04-release-complete.sh" 2>&1)"

if [[ -s "$FAKE_SVNMUCC_LOG" ]]; then
  echo "a re-run over a completed release must not touch SVN" >&2
  cat "$FAKE_SVNMUCC_LOG" >&2
  exit 1
fi
if ! grep -qF "release artifacts already published" <<<"$out"; then
  echo "a re-run must report that the artifacts are already published" >&2
  printf '%s\n' "$out" >&2
  exit 1
fi
[[ -f "$tmp/9.9.9-rc01/announce-email.txt" ]] || {
  echo "a re-run must still write the announce email draft" >&2
  exit 1
}
if [[ "$(head -1 "$tmp/9.9.9-rc01/announce-email.txt")" != "Subject: [ANNOUNCE] Apache Doris 9.9.9 release" ]]; then
  echo "announce-email.txt must start with the subject line" >&2
  head -1 "$tmp/9.9.9-rc01/announce-email.txt" >&2
  exit 1
fi
if ! grep -qxF "Subject: [ANNOUNCE] Apache Doris 9.9.9 release" "$tmp/9.9.9-rc01/announce-email.eml"; then
  echo "announce-email.eml must carry the subject header" >&2
  exit 1
fi

# --- re-run after a completed release, stale dev RC folder left behind ------
: > "$FAKE_SVNMUCC_LOG"
rm -rf "$tmp/9.9.9-rc01"
out="$(printf 'y\ny\ny\n' | FAKE_DEV_DIR_PRESENT=1 bash "$tmp/04-release-complete.sh" 2>&1)"

if ! grep -qF "rm https://dist.example.test/dev/doris/9.9.9-rc01" "$FAKE_SVNMUCC_LOG"; then
  echo "a stale dev RC folder must be removed on a re-run" >&2
  cat "$FAKE_SVNMUCC_LOG" >&2
  exit 1
fi
if grep -qE '(mkdir|mv|put) ' "$FAKE_SVNMUCC_LOG"; then
  echo "a re-run must only remove the stale RC folder, not republish" >&2
  cat "$FAKE_SVNMUCC_LOG" >&2
  exit 1
fi
[[ -f "$tmp/9.9.9-rc01/announce-email.txt" ]] || {
  echo "a re-run must still write the announce email draft" >&2
  exit 1
}

# --- declining a step stops the run without changing anything ---------------
: > "$FAKE_SVNMUCC_LOG"
rm -rf "$tmp/9.9.9-rc01"
out="$(printf 'y\nn\n' | FAKE_DEV_DIR_PRESENT=1 bash "$tmp/04-release-complete.sh" 2>&1)"

if [[ -s "$FAKE_SVNMUCC_LOG" ]]; then
  echo "declining a step must not run any SVN operation" >&2
  cat "$FAKE_SVNMUCC_LOG" >&2
  exit 1
fi
if ! grep -qF "stopped before step" <<<"$out"; then
  echo "declining a step must say which step was stopped" >&2
  printf '%s\n' "$out" >&2
  exit 1
fi
if [[ -e "$tmp/9.9.9-rc01/announce-email.txt" ]]; then
  echo "declining a step must not continue to the later steps" >&2
  exit 1
fi
