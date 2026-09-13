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

cp "${ROOT}/03-vote-mail.sh" "$tmp/"
cat > "$tmp/release.env" <<'EOF'
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
VERSION="9.9.9"
RC="rc01"
TAG="${VERSION}-${RC}"
WORK_DIR="${ROOT}/${TAG}"
BIN_FILES=("/nonexistent/apache-doris-${VERSION}-bin-x64.tar.gz")
BIN_DOWNLOAD_BASE="https://binaries.example.test"
APACHE_ID="rm"
APACHE_EMAIL="rm@apache.org"
SIGNER_NAME="Release Manager"
SIGNING_KEY="DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
DEV_SVN_BASE="https://dist.example.test/dev/doris"
DEV_SVN_DIR="${DEV_SVN_BASE}/${TAG}"
KEYS_URL="https://downloads.example.test/doris/KEYS"
DEV_LIST="dev@example.test"
RELEASE_NOTES_URL="https://github.example.test/issues/1"
VERIFY_GUIDE_URL="https://doris.example.test/release-verify"
EOF

cat > "$tmp/gpg" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == "--list-keys" ]]
printf 'fpr:::::::::DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF:\n'
EOF
chmod +x "$tmp/gpg"

PATH="$tmp:$PATH" "$tmp/03-vote-mail.sh" >/dev/null

body="$tmp/9.9.9-rc01/vote-email.txt"
eml="$tmp/9.9.9-rc01/vote-email.eml"
subject="[VOTE] Release for Apache Doris 9.9.9-rc01"

for f in "$body" "$eml"; do
  [[ -f "$f" ]] || { echo "03-vote-mail.sh did not write $f" >&2; exit 1; }
done

# The subject must reach the RM from the draft itself, not only from the console.
if [[ "$(head -1 "$body")" != "Subject: ${subject}" ]]; then
  echo "vote-email.txt must start with the subject line: Subject: ${subject}" >&2
  head -1 "$body" >&2
  exit 1
fi

if ! grep -qxF "Subject: ${subject}" "$eml"; then
  echo "vote-email.eml is missing the subject header: Subject: ${subject}" >&2
  exit 1
fi

# The RM's own binding vote closes the body, ahead of the signature.
for f in "$body" "$eml"; do
  if ! grep -qxF "Here is my +1(binding)" "$f"; then
    echo "$f is missing the binding vote line" >&2
    exit 1
  fi
  vote_line="$(grep -nxF "Here is my +1(binding)" "$f" | head -1 | cut -d: -f1)"
  regards_line="$(grep -nxF "Best Regards," "$f" | head -1 | cut -d: -f1)"
  if [[ -z "$regards_line" || "$vote_line" -ge "$regards_line" ]]; then
    echo "$f must place the binding vote before the signature" >&2
    exit 1
  fi
done
