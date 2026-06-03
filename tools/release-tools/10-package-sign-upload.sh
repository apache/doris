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

# Step 10 - package the source tarball, sign it, and upload to the Apache dev SVN.
# The SVN commit is outward-facing: it pauses twice for confirmation before commit.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release.env
source "${HERE}/release.env"

ok()   { echo "[ OK ] $*"; }
warn() { echo "[WARN] $*"; }
die()  { echo "[FAIL] $*" >&2; exit 1; }
confirm() { local a; read -r -p "$1 [y/N] " a; [[ "$a" == y || "$a" == Y ]]; }
export GPG_TTY="$(tty || true)"

svn_auth=(--non-interactive --no-auth-cache)
[[ -n "${ASF_USERNAME:-}" ]] && svn_auth+=(--username "$ASF_USERNAME")
[[ -n "${ASF_PASSWORD:-}" ]] && svn_auth+=(--password "$ASF_PASSWORD")

# resolve signer
if [[ -n "${SIGNING_KEY}" ]]; then
  SIGNER="${SIGNING_KEY}"
else
  SIGNER="$(gpg --list-secret-keys --with-colons 2>/dev/null | awk -F: '$1=="sec"{w=1} $1=="fpr"&&w{print $10; exit}')"
fi
[[ -n "$SIGNER" ]] || die "no signing key found; run ./00-check-env.sh first"
ok "signer: $SIGNER"

# 1. verify the tag matches the apache remote (don't release a local-only tag)
cd "$REPO_DIR"
git rev-parse "$TAG" >/dev/null 2>&1 || die "local tag $TAG not found in $REPO_DIR"
local_tag="$(git rev-parse "$TAG")"
remote_tag="$(git ls-remote --tags "$GIT_REMOTE" "refs/tags/$TAG" | awk -v t="refs/tags/$TAG" '$2==t{print $1}')"
[[ -n "$remote_tag" ]] || die "tag $TAG not found on remote $GIT_REMOTE"
[[ "$local_tag" == "$remote_tag" ]] || die "tag mismatch: local=$local_tag $GIT_REMOTE=$remote_tag"
ok "tag $TAG matches $GIT_REMOTE"

# 2. package via git archive from the tag
mkdir -p "$WORK_DIR"
art="$WORK_DIR/${PKG_BASE}.tar.gz"
echo "archiving $TAG -> $art"
git archive --format=tar --prefix="$ARCHIVE_PREFIX" "$TAG" | gzip > "$art"
ok "source tarball: $(du -h "$art" | cut -f1)  $art"

# 3. sign + checksum (inside WORK_DIR so the files reference bare basenames)
cd "$WORK_DIR"
f="${PKG_BASE}.tar.gz"
rm -f "$f.asc" "$f.sha512"
gpg -u "$SIGNER" --armor --output "$f.asc" --detach-sign "$f"
gpg --verify "$f.asc" "$f"
ok "signature ok: $f.asc"
sha512sum "$f" > "$f.sha512"
sha512sum --check "$f.sha512"
ok "sha512 ok: $f.sha512"
echo; ls -l "$f" "$f.asc" "$f.sha512"; echo

# 4. upload to dev SVN (two confirmations; nothing public happens before them)
echo "Target dev SVN folder: ${DEV_SVN_DIR}/"
confirm "Checkout + add these 3 files for the above SVN URL?" || { warn "stopping before SVN."; exit 0; }
wc="$WORK_DIR/dev-svn"
rm -rf "$wc"
svn checkout --depth empty "${svn_auth[@]}" "$DEV_SVN_BASE" "$wc"
mkdir -p "$wc/$TAG"
cp "$f" "$f.asc" "$f.sha512" "$wc/$TAG/"
svn add "$wc/$TAG"
echo "--- svn status ---"; svn status "$wc"; echo
echo "Will commit the above to: ${DEV_SVN_DIR}/"
confirm "FINAL confirm - svn commit now?" || { warn "left staged (uncommitted) at $wc"; exit 0; }
svn commit "${svn_auth[@]}" -m "Add ${TAG}" "$wc"
ok "committed. Vote artifacts now at: ${DEV_SVN_DIR}/"
echo "Next: ./20-vote-mail.sh"
