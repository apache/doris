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

# Step 04 - publish the passed RC source artifact to the Apache release SVN
# and generate the [ANNOUNCE] email draft.
#
# The release SVN commit is public and requires PMC permission. This script
# pauses for confirmation before changing SVN, and it never sends email.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release.env
source "${HERE}/release.env"

ok()   { echo "[ OK ] $*"; }
warn() { echo "[WARN] $*"; }
die()  { echo "[FAIL] $*" >&2; exit 1; }
confirm() { local a; read -r -p "$1 [y/N] " a; [[ "$a" == y || "$a" == Y ]]; }

mail_only=0
usage() {
  cat <<EOF
Usage: $0 [--mail-only]

Publishes Apache Doris ${TAG} source artifacts from dev SVN to release SVN as
Apache Doris ${VERSION}, then writes the [ANNOUNCE] email draft.

Options:
  --mail-only   Only write announce-email.txt and announce-email.eml.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mail-only) mail_only=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

svn_auth=(--non-interactive --no-auth-cache)
[[ -n "${ASF_USERNAME:-}" ]] && svn_auth+=(--username "$ASF_USERNAME")
[[ -n "${ASF_PASSWORD:-}" ]] && svn_auth+=(--password "$ASF_PASSWORD")
svnmucc_auth=(--non-interactive --no-auth-cache)
[[ -n "${ASF_USERNAME:-}" ]] && svnmucc_auth+=(-u "$ASF_USERNAME")
[[ -n "${ASF_PASSWORD:-}" ]] && svnmucc_auth+=(-p "$ASF_PASSWORD")

RELEASE_PKG_BASE="${RELEASE_PKG_BASE:-apache-doris-${VERSION}-src}"
RELEASE_SERIES="${RELEASE_SERIES:-${VERSION%.*}}"
RELEASE_SVN_DIR="${RELEASE_SVN_DIR:-${RELEASE_SVN_BASE}/${RELEASE_SERIES}/${VERSION}}"
DOWNLOAD_PAGE_URL="${DOWNLOAD_PAGE_URL:-https://doris.apache.org/download/}"
ANNOUNCE_RELEASE_NOTES_URL="${ANNOUNCE_RELEASE_NOTES_URL:-}"

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "missing tool: $1"
}

write_announce_email() {
  local rn subject body_file eml_file

  rn="${ANNOUNCE_RELEASE_NOTES_URL:-${RELEASE_NOTES_URL:-}}"
  if [[ -z "$rn" ]]; then
    read -r -p "Release Notes URL for announce email: " rn
  fi
  [[ -n "$rn" ]] || die "release notes url required"

  mkdir -p "$WORK_DIR"
  subject="[ANNOUNCE] Apache Doris ${VERSION} release"
  body_file="$WORK_DIR/announce-email.txt"
  eml_file="$WORK_DIR/announce-email.eml"

  read -r -d '' BODY <<EOF || true
Hi All,

We are pleased to announce the release of Apache Doris ${VERSION}.

Apache Doris is a real-time analytics and hybrid search database for AI agents.

The release is available at:
${DOWNLOAD_PAGE_URL}

The source artifacts are available at:
${RELEASE_SVN_DIR}/

Thanks to everyone who has contributed to this release, and the release note can be found here:
${rn}

Best Regards,

On behalf of the Doris team,
${SIGNER_NAME}
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
}

publish_to_release_svn() {
  local src_tar src_asc src_sha512 dst_tar dst_asc dst_sha512 checksum_dir src_tar_file final_sha512_file

  require_tool svn
  require_tool svnmucc
  require_tool gpg
  require_tool sha512sum

  src_tar="${DEV_SVN_DIR}/${PKG_BASE}.tar.gz"
  src_asc="${src_tar}.asc"
  src_sha512="${src_tar}.sha512"
  dst_tar="${RELEASE_PKG_BASE}.tar.gz"
  dst_asc="${dst_tar}.asc"
  dst_sha512="${dst_tar}.sha512"

  echo "== Apache Doris ${TAG} - complete release =="
  echo "Source dev SVN folder:     ${DEV_SVN_DIR}/"
  echo "Target release SVN folder: ${RELEASE_SVN_DIR}/"
  echo
  echo "Source files:"
  echo "  ${PKG_BASE}.tar.gz"
  echo "  ${PKG_BASE}.tar.gz.asc"
  echo "  ${PKG_BASE}.tar.gz.sha512"
  echo "Target files:"
  echo "  ${dst_tar}"
  echo "  ${dst_asc}"
  echo "  ${dst_sha512}"
  echo
  warn "Only PMC members can write to the release SVN directory."

  svn info "${svn_auth[@]}" "$src_tar" >/dev/null || die "source artifact not found: $src_tar"
  svn info "${svn_auth[@]}" "$src_asc" >/dev/null || die "source signature not found: $src_asc"
  svn info "${svn_auth[@]}" "$src_sha512" >/dev/null || die "source checksum not found: $src_sha512"
  if svn info "${svn_auth[@]}" "$RELEASE_SVN_DIR" >/dev/null 2>&1; then
    die "release SVN folder already exists: $RELEASE_SVN_DIR (use --mail-only to regenerate the email)"
  fi

  checksum_dir="$(mktemp -d)"
  src_tar_file="${PKG_BASE}.tar.gz"
  final_sha512_file="${checksum_dir}/${dst_sha512}"
  svn cat "${svn_auth[@]}" "$src_tar" > "${checksum_dir}/${src_tar_file}"
  svn cat "${svn_auth[@]}" "$src_sha512" > "${checksum_dir}/${src_tar_file}.sha512"
  svn cat "${svn_auth[@]}" "$src_asc" > "${checksum_dir}/${src_tar_file}.asc"
  (
    cd "$checksum_dir"
    sha512sum --check "${src_tar_file}.sha512"
    gpg --verify "${src_tar_file}.asc" "$src_tar_file"
    cp "$src_tar_file" "$dst_tar"
    sha512sum "$dst_tar" > "$dst_sha512"
    sha512sum --check "$dst_sha512"
  )
  ok "source RC checksum and signature verified: ${src_tar_file}"
  ok "final sha512 ok: ${dst_sha512}"

  echo "--- svnmucc operations ---"
  echo "mkdir ${RELEASE_SVN_DIR}"
  echo "mv    ${src_tar}"
  echo "  ->  ${RELEASE_SVN_DIR}/${dst_tar}"
  echo "mv    ${src_asc}"
  echo "  ->  ${RELEASE_SVN_DIR}/${dst_asc}"
  echo "put   ${final_sha512_file}"
  echo "  ->  ${RELEASE_SVN_DIR}/${dst_sha512}"
  echo "rm    ${DEV_SVN_DIR}"
  echo
  echo "Will commit the URL operations above in one SVN revision."
  confirm "FINAL confirm - publish release SVN and remove dev RC now?" || { warn "stopping before SVN commit."; rm -rf "$checksum_dir"; exit 0; }

  if ! svnmucc "${svnmucc_auth[@]}" -m "Release Doris ${VERSION}" \
    mkdir "$RELEASE_SVN_DIR" \
    mv "$src_tar" "${RELEASE_SVN_DIR}/${dst_tar}" \
    mv "$src_asc" "${RELEASE_SVN_DIR}/${dst_asc}" \
    put "$final_sha512_file" "${RELEASE_SVN_DIR}/${dst_sha512}" \
    rm "$DEV_SVN_DIR"; then
    rm -rf "$checksum_dir"
    die "svnmucc release publish failed"
  fi
  rm -rf "$checksum_dir"
  ok "committed release artifacts: ${RELEASE_SVN_DIR}/"
  ok "removed dev RC folder: ${DEV_SVN_DIR}/"
}

if [[ "$mail_only" -eq 0 ]]; then
  publish_to_release_svn
else
  ok "mail-only mode: skipping SVN publish"
fi

write_announce_email
