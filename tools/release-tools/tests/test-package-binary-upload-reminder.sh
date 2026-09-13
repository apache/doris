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

cp "${ROOT}/02-package-sign-upload.sh" "$tmp/"
mkdir -p "$tmp/repo" "$tmp/bins"
printf 'binary bytes\n' > "$tmp/bins/apache-doris-9.9.9-bin-x64.tar.gz"
printf 'binary bytes\n' > "$tmp/bins/apache-doris-9.9.9-bin-arm64.tar.gz"

cat > "$tmp/release.env" <<EOF
ROOT="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_DIR="$tmp/repo"
VERSION="9.9.9"
RC="rc01"
TAG="\${VERSION}-\${RC}"
GIT_REMOTE="apache-test"
PKG_BASE="apache-doris-\${TAG}-src"
ARCHIVE_PREFIX="\${PKG_BASE}/"
WORK_DIR="\${ROOT}/\${TAG}"
BIN_FILES=(
  "$tmp/bins/apache-doris-9.9.9-bin-x64.tar.gz"
  "$tmp/bins/apache-doris-9.9.9-bin-arm64.tar.gz"
)
BIN_DOWNLOAD_BASE="https://binaries.example.test"
SIGNING_KEY="DEADBEEF"
DEV_SVN_BASE="https://dist.example.test/dev/doris"
DEV_SVN_DIR="\${DEV_SVN_BASE}/\${TAG}"
EOF

cat > "$tmp/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
commit="1111111111111111111111111111111111111111"
case "$1" in
  rev-parse) printf '%s\n' "$commit" ;;
  ls-remote) printf '%s\trefs/tags/9.9.9-rc01\n' "$commit" ;;
  archive)   printf 'fake source tree\n' ;;
  *) echo "unexpected git command: $1" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/git"

cat > "$tmp/gpg" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  --verify) exit 0 ;;
  -u)
    out=""
    prev=""
    for a in "$@"; do
      [[ "$prev" == "--output" ]] && out="$a"
      prev="$a"
    done
    [[ -n "$out" ]] || { echo "no --output in: $*" >&2; exit 1; }
    printf 'fake signature\n' > "$out"
    ;;
  *) echo "unexpected gpg invocation: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/gpg"

# Answering "n" stops the script before the SVN commit. The binaries are already
# signed at that point, so the reminder still has to reach the RM.
out="$(printf 'n\n' | PATH="$tmp:$PATH" "$tmp/02-package-sign-upload.sh" 2>&1)"

for name in apache-doris-9.9.9-bin-x64.tar.gz apache-doris-9.9.9-bin-arm64.tar.gz; do
  [[ -f "$tmp/bins/$name.asc" ]] || { echo "missing signature: $name.asc" >&2; exit 1; }
  if ! grep -qF "$name.asc" <<<"$out"; then
    echo "02-package-sign-upload.sh did not remind the RM to upload $name.asc" >&2
    printf '%s\n' "$out" >&2
    exit 1
  fi
done

if ! grep -qF "https://binaries.example.test/" <<<"$out"; then
  echo "the upload reminder must name the binary download base" >&2
  printf '%s\n' "$out" >&2
  exit 1
fi

if ! grep -qiF "NOT uploaded by this script" <<<"$out"; then
  echo "the upload reminder must state that the script does not upload signatures" >&2
  printf '%s\n' "$out" >&2
  exit 1
fi
