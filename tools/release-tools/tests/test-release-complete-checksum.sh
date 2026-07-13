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
EOF

cat > "$tmp/svn" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cmd="$1"
shift
case "$cmd" in
  info)
    url="${@: -1}"
    case "$url" in
      https://dist.example.test/release/doris/9.9|https://dist.example.test/release/doris/9.9/9.9.9)
        exit 1
        ;;
    esac
    ;;
  cat)
    url="${@: -1}"
    printf '%s\n' "$url" >> "$FAKE_SVN_CAT_LOG"
    case "$url" in
      *.tar.gz)
        printf 'release artifact bytes\n'
        ;;
      *.sha512)
        digest="$(printf 'release artifact bytes\n' | sha512sum | awk '{print $1}')"
        printf '%s  apache-doris-9.9.9-rc01-src.tar.gz\n' "$digest"
        ;;
      *.asc)
        printf 'fake detached signature\n'
        ;;
      *)
        echo "unexpected svn cat url: $url" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "unexpected svn command: $cmd" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$tmp/svn"

cat > "$tmp/gpg" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" > "$FAKE_GPG_LOG"
[[ "$1" == "--verify" ]]
[[ "$2" == "apache-doris-9.9.9-rc01-src.tar.gz.asc" ]]
[[ "$3" == "apache-doris-9.9.9-rc01-src.tar.gz" ]]
[[ -f "$2" ]]
[[ -f "$3" ]]
EOF
chmod +x "$tmp/gpg"

cat > "$tmp/svnmucc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" > "$FAKE_SVNMUCC_LOG"
series_dir_created=0
while (($#)); do
  case "$1" in
    mkdir)
      if [[ "$2" == "https://dist.example.test/release/doris/9.9" ]]; then
        series_dir_created=1
      elif [[ "$2" == "https://dist.example.test/release/doris/9.9/9.9.9" && "$series_dir_created" -eq 0 ]]; then
        echo "release series directory must be created before release leaf directory" >&2
        exit 1
      fi
      shift 2
      ;;
    mv)
      shift 3
      ;;
    put)
      checksum_file="$2"
      cp "$checksum_file" "$FAKE_FINAL_SHA512"
      shift 3
      ;;
    rm)
      shift 2
      ;;
    -m|-u|-p)
      shift 2
      ;;
    --non-interactive|--no-auth-cache)
      shift
      ;;
    *)
      shift
      ;;
  esac
done
EOF
chmod +x "$tmp/svnmucc"

export PATH="$tmp:$PATH"
export FAKE_SVN_CAT_LOG="$tmp/svn-cat.log"
export FAKE_GPG_LOG="$tmp/gpg.log"
export FAKE_SVNMUCC_LOG="$tmp/svnmucc.log"
export FAKE_FINAL_SHA512="$tmp/final.sha512"

printf 'y\n' | bash "$tmp/04-release-complete.sh" >/dev/null

if grep -q 'mv https://dist.example.test/dev/doris/9.9.9-rc01/apache-doris-9.9.9-rc01-src.tar.gz.sha512' "$FAKE_SVNMUCC_LOG"; then
  echo "release completion must not move the RC checksum sidecar unchanged" >&2
  exit 1
fi

grep -q 'apache-doris-9.9.9-rc01-src.tar.gz$' "$FAKE_SVN_CAT_LOG"
grep -q 'apache-doris-9.9.9-rc01-src.tar.gz.sha512$' "$FAKE_SVN_CAT_LOG"
grep -q 'apache-doris-9.9.9-rc01-src.tar.gz.asc$' "$FAKE_SVN_CAT_LOG"
grep -q -- '--verify apache-doris-9.9.9-rc01-src.tar.gz.asc apache-doris-9.9.9-rc01-src.tar.gz' "$FAKE_GPG_LOG"
grep -q 'mkdir https://dist.example.test/release/doris/9.9' "$FAKE_SVNMUCC_LOG"
grep -q 'https://dist.example.test/release/doris/9.9/9.9.9/apache-doris-9.9.9-src.tar.gz' "$FAKE_SVNMUCC_LOG"
grep -q 'put ' "$FAKE_SVNMUCC_LOG"
grep -q 'apache-doris-9.9.9-src.tar.gz$' "$FAKE_FINAL_SHA512"
if grep -q 'apache-doris-9.9.9-rc01-src.tar.gz' "$FAKE_FINAL_SHA512"; then
  echo "final checksum sidecar still references the RC tarball name" >&2
  exit 1
fi
