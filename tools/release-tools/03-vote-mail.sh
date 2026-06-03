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

# Step 03 - generate the [VOTE] email draft for dev@doris.apache.org.
# Draft only: it writes a .txt and .eml; you send it from your @apache.org mail.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release.env
source "${HERE}/release.env"

ok()  { echo "[ OK ] $*"; }
die() { echo "[FAIL] $*" >&2; exit 1; }

# release notes link (prompt if not preset)
rn="${RELEASE_NOTES_URL}"
if [[ -z "$rn" ]]; then read -r -p "Release Notes URL (the issue link): " rn; fi
[[ -n "$rn" ]] || die "release notes url required"

# signer fingerprint
if [[ -n "${SIGNING_KEY}" ]]; then
  SIGNER="${SIGNING_KEY}"
else
  SIGNER="$(gpg --list-secret-keys --with-colons 2>/dev/null | awk -F: '$1=="sec"{w=1} $1=="fpr"&&w{print $10; exit}')"
fi
[[ -n "$SIGNER" ]] || die "no signing key found; run ./01-check-env.sh first"
FPR="$(gpg --list-keys --with-colons "$SIGNER" | awk -F: '/^fpr:/{print $10; exit}')"

mkdir -p "$WORK_DIR"
subject="[VOTE] Release Apache Doris ${TAG}"
body_file="$WORK_DIR/vote-email.txt"
eml_file="$WORK_DIR/vote-email.eml"

read -r -d '' BODY <<EOF || true
Hi all,

Please review and vote on Apache Doris ${TAG} release.

The release candidate has been tagged in GitHub as ${TAG}, available here:
https://github.com/apache/doris/releases/tag/${TAG}

Release Notes are here:
${rn}

Thanks to everyone who has contributed to this release.

The artifacts (source, signature and checksum) corresponding to this release
candidate can be found here:
${DEV_SVN_DIR}/

This has been signed with PGP key ${FPR}, corresponding to ${APACHE_EMAIL}.
KEYS file is available here:
${KEYS_URL}
It is also listed here:
https://people.apache.org/keys/committer/${APACHE_ID}.asc

To verify and build, you can refer to following link:
${VERIFY_GUIDE_URL}

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

Best Regards,
Mingyu Chen (${APACHE_ID})
EOF

printf '%s\n' "$BODY" > "$body_file"
{
  echo "To: ${DEV_LIST}"
  echo "Subject: ${subject}"
  echo "Content-Type: text/plain; charset=UTF-8"
  echo
  printf '%s\n' "$BODY"
} > "$eml_file"

ok "subject: ${subject}"
ok "body:    ${body_file}"
ok "eml:     ${eml_file}  (open in your apache.org mail client)"
echo "----------------------------------------------------------------"
cat "$body_file"
echo "----------------------------------------------------------------"
echo "Review, then SEND MANUALLY from your @apache.org address to ${DEV_LIST}."
echo "(Not auto-sent by design - it's a public ASF list.)"
