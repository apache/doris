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

# Step 04 - publish the passed RC source artifact to the Apache release SVN,
# tag the release on GitHub, and generate the [ANNOUNCE] email draft.
#
# The release SVN commit is public and requires PMC permission. Every step asks
# for confirmation before it runs, and this script never sends email.
#
# The script is idempotent: it reads the current dev SVN, release SVN, tag and
# GitHub release state first, skips whatever is already done, and can be re-run
# safely after a successful publish or after stopping at any confirmation
# prompt.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release.env
source "${HERE}/release.env"

ok()   { echo "[ OK ] $*"; }
warn() { echo "[WARN] $*"; }
die()  { echo "[FAIL] $*" >&2; exit 1; }
confirm() { local a; read -r -p "$1 [y/N] " a; [[ "$a" == y || "$a" == Y ]]; }

# Keep the scratch directory in a global: an EXIT trap fires after the local
# scope of the function that created it is gone, so a local would expand empty
# and leak the directory whenever `set -e` aborts mid-run.
CHECKSUM_DIR=""
cleanup_checksum_dir() {
  [[ -n "$CHECKSUM_DIR" ]] || return 0
  rm -rf "$CHECKSUM_DIR"
  CHECKSUM_DIR=""
}
trap cleanup_checksum_dir EXIT

mail_only=0
skip_github_release=0
usage() {
  cat <<EOF
Usage: $0 [--mail-only] [--skip-github-release]

Publishes Apache Doris ${TAG} source artifacts from dev SVN to release SVN as
Apache Doris ${VERSION}, pushes the RC-free ${VERSION} tag, publishes the GitHub
release, then writes the [ANNOUNCE] email draft.

Every step asks for confirmation first, and answering anything but y stops the
run without changing more state. Re-running is safe: an already published
release, an existing tag and an existing GitHub release are detected and left
alone.

Options:
  --mail-only             Only write announce-email.txt and announce-email.eml.
  --skip-github-release   Push the tag but leave the GitHub release alone.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mail-only) mail_only=1; shift ;;
    --skip-github-release) skip_github_release=1; shift ;;
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
RELEASE_SVN_PARENT_DIR="${RELEASE_SVN_PARENT_DIR:-${RELEASE_SVN_DIR%/*}}"
DOWNLOAD_PAGE_URL="${DOWNLOAD_PAGE_URL:-https://doris.apache.org/download/}"
ANNOUNCE_RELEASE_NOTES_URL="${ANNOUNCE_RELEASE_NOTES_URL:-}"
# The GitHub release body has always linked the convenience binaries to their
# public mirror, which is not the same host the vote email advertises.
RELEASE_BIN_DOWNLOAD_BASE="${RELEASE_BIN_DOWNLOAD_BASE:-https://download.velodb.io}"
# The RC-free tag that carries the GitHub release, e.g. 4.0.8-rc02 -> 4.0.8.
RELEASE_TAG="${RELEASE_TAG:-${VERSION}}"
GITHUB_RELEASE_TITLE="${GITHUB_RELEASE_TITLE:-Apache Doris ${VERSION} Release}"
REPO_DIR="${REPO_DIR:-}"
GIT_REMOTE="${GIT_REMOTE:-}"

SRC_TAR="${DEV_SVN_DIR}/${PKG_BASE}.tar.gz"
SRC_ASC="${SRC_TAR}.asc"
SRC_SHA512="${SRC_TAR}.sha512"
DST_TAR="${RELEASE_PKG_BASE}.tar.gz"
DST_ASC="${DST_TAR}.asc"
DST_SHA512="${DST_TAR}.sha512"

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "missing tool: $1"
}

# Every step announces what it is about to do and waits for confirmation, so a
# run can be stopped at any boundary without leaving half-finished state.
step_no=0
step() {
  local title="$1" line
  shift
  step_no=$((step_no + 1))
  echo
  echo "== step ${step_no}: ${title} =="
  for line in "$@"; do
    echo "     ${line}"
  done
  confirm "Proceed with step ${step_no}?" || {
    warn "stopped before step ${step_no}: ${title}"
    warn "nothing further was changed; re-run this script to continue."
    exit 0
  }
}

svn_url_exists() { svn info "${svn_auth[@]}" "$1" >/dev/null 2>&1; }

# --- discovered state ------------------------------------------------------
DEV_DIR_EXISTS=0
SRC_PRESENT=0             # of SRC_TAR, SRC_ASC, SRC_SHA512
RELEASE_DIR_EXISTS=0
RELEASE_PARENT_EXISTS=0
DST_PRESENT=0             # of DST_TAR, DST_ASC, DST_SHA512
DST_MISSING=()

discover_svn_state() {
  local url

  DEV_DIR_EXISTS=0
  SRC_PRESENT=0
  RELEASE_DIR_EXISTS=0
  RELEASE_PARENT_EXISTS=0
  DST_PRESENT=0
  DST_MISSING=()

  svn_url_exists "$DEV_SVN_DIR" && DEV_DIR_EXISTS=1
  for url in "$SRC_TAR" "$SRC_ASC" "$SRC_SHA512"; do
    svn_url_exists "$url" && SRC_PRESENT=$((SRC_PRESENT + 1))
  done

  svn_url_exists "$RELEASE_SVN_PARENT_DIR" && RELEASE_PARENT_EXISTS=1
  svn_url_exists "$RELEASE_SVN_DIR" && RELEASE_DIR_EXISTS=1
  if [[ "$RELEASE_DIR_EXISTS" -eq 1 ]]; then
    for url in "$DST_TAR" "$DST_ASC" "$DST_SHA512"; do
      if svn_url_exists "${RELEASE_SVN_DIR}/${url}"; then
        DST_PRESENT=$((DST_PRESENT + 1))
      else
        DST_MISSING+=("$url")
      fi
    done
  else
    DST_MISSING=("$DST_TAR" "$DST_ASC" "$DST_SHA512")
  fi

  echo
  echo "--- current state ---"
  echo "dev SVN     ${DEV_SVN_DIR}/"
  echo "            folder: $([[ "$DEV_DIR_EXISTS" -eq 1 ]] && echo present || echo absent), RC artifacts: ${SRC_PRESENT}/3"
  echo "release SVN ${RELEASE_SVN_DIR}/"
  echo "            folder: $([[ "$RELEASE_DIR_EXISTS" -eq 1 ]] && echo present || echo absent), release artifacts: ${DST_PRESENT}/3"
}

verify_and_build_checksum() {
  local src_tar_file

  CHECKSUM_DIR="$(mktemp -d)"
  src_tar_file="${PKG_BASE}.tar.gz"
  FINAL_SHA512_FILE="${CHECKSUM_DIR}/${DST_SHA512}"

  svn cat "${svn_auth[@]}" "$SRC_TAR" > "${CHECKSUM_DIR}/${src_tar_file}"
  svn cat "${svn_auth[@]}" "$SRC_SHA512" > "${CHECKSUM_DIR}/${src_tar_file}.sha512"
  svn cat "${svn_auth[@]}" "$SRC_ASC" > "${CHECKSUM_DIR}/${src_tar_file}.asc"
  (
    cd "$CHECKSUM_DIR"
    sha512sum --check "${src_tar_file}.sha512"
    gpg --verify "${src_tar_file}.asc" "$src_tar_file"
    cp "$src_tar_file" "$DST_TAR"
    sha512sum "$DST_TAR" > "$DST_SHA512"
    sha512sum --check "$DST_SHA512"
  )
  ok "source RC checksum and signature verified: ${src_tar_file}"
  ok "final sha512 ok: ${DST_SHA512}"
}

publish_to_release_svn() {
  local -a svnmucc_ops op_lines

  svnmucc_ops=()
  op_lines=()
  if [[ "$RELEASE_PARENT_EXISTS" -eq 0 ]]; then
    op_lines+=("mkdir ${RELEASE_SVN_PARENT_DIR}")
    svnmucc_ops+=(mkdir "$RELEASE_SVN_PARENT_DIR")
  fi
  if [[ "$RELEASE_DIR_EXISTS" -eq 0 ]]; then
    op_lines+=("mkdir ${RELEASE_SVN_DIR}")
    svnmucc_ops+=(mkdir "$RELEASE_SVN_DIR")
  fi
  op_lines+=("mv    ${SRC_TAR}")
  op_lines+=("  ->  ${RELEASE_SVN_DIR}/${DST_TAR}")
  svnmucc_ops+=(mv "$SRC_TAR" "${RELEASE_SVN_DIR}/${DST_TAR}")
  op_lines+=("mv    ${SRC_ASC}")
  op_lines+=("  ->  ${RELEASE_SVN_DIR}/${DST_ASC}")
  svnmucc_ops+=(mv "$SRC_ASC" "${RELEASE_SVN_DIR}/${DST_ASC}")
  op_lines+=("put   ${FINAL_SHA512_FILE}")
  op_lines+=("  ->  ${RELEASE_SVN_DIR}/${DST_SHA512}")
  svnmucc_ops+=(put "$FINAL_SHA512_FILE" "${RELEASE_SVN_DIR}/${DST_SHA512}")
  op_lines+=("rm    ${DEV_SVN_DIR}")
  svnmucc_ops+=(rm "$DEV_SVN_DIR")

  step "Publish to the release SVN and remove the dev RC folder" \
    "This is public and requires PMC permission." \
    "All of it lands in one SVN revision:" \
    "${op_lines[@]}"

  if ! svnmucc "${svnmucc_auth[@]}" -m "Release Doris ${VERSION}" "${svnmucc_ops[@]}"; then
    die "svnmucc release publish failed"
  fi
  cleanup_checksum_dir
  ok "committed release artifacts: ${RELEASE_SVN_DIR}/"
  ok "removed dev RC folder: ${DEV_SVN_DIR}/"
}

remove_dev_rc_folder() {
  step "Remove the leftover dev RC folder" \
    "The release artifacts are already published, so the RC folder is stale." \
    "rm    ${DEV_SVN_DIR}"

  if ! svnmucc "${svnmucc_auth[@]}" -m "Remove ${TAG} after releasing Doris ${VERSION}" \
      rm "$DEV_SVN_DIR"; then
    die "svnmucc dev RC removal failed"
  fi
  ok "removed dev RC folder: ${DEV_SVN_DIR}/"
}

git_repo() { git -C "$REPO_DIR" "$@"; }

# apache/doris out of any remote URL form, including one that carries a token.
github_repo() {
  if [[ -n "${GITHUB_REPO:-}" ]]; then
    printf '%s\n' "$GITHUB_REPO"
    return 0
  fi
  git_repo remote get-url "$GIT_REMOTE" 2>/dev/null |
    sed -E 's#^.*github\.com[:/]##; s#\.git$##; s#/$##'
}

# Peeled commit of a tag on the remote, empty when the tag is not there.
remote_tag_commit() {
  local tag="$1" out
  out="$(git_repo ls-remote --tags "$GIT_REMOTE" "refs/tags/${tag}" "refs/tags/${tag}^{}" 2>/dev/null || true)"
  awk -v t="refs/tags/${tag}^{}" '$2==t{print $1; found=1} END{if(!found) exit 1}' <<<"$out" 2>/dev/null ||
    awk -v t="refs/tags/${tag}" '$2==t{print $1}' <<<"$out"
}

handle_release_tag() {
  local rc_commit local_commit remote_commit

  if [[ "$TAG" != *rc* ]]; then
    ok "tag ${TAG} carries no rc suffix; no separate release tag is needed"
    return 0
  fi

  [[ -n "$REPO_DIR" && -n "$GIT_REMOTE" ]] ||
    die "REPO_DIR and GIT_REMOTE must be set in release.env to push the ${RELEASE_TAG} tag"

  # -q --verify matters here: plain rev-parse echoes the argument back and
  # exits non-zero for a tag that does not exist, which would read as a commit.
  rc_commit="$(git_repo rev-parse -q --verify "${TAG}^{commit}" 2>/dev/null || true)"
  [[ -n "$rc_commit" ]] || die "RC tag ${TAG} not found in ${REPO_DIR}"

  local_commit="$(git_repo rev-parse -q --verify "${RELEASE_TAG}^{commit}" 2>/dev/null || true)"
  remote_commit="$(remote_tag_commit "$RELEASE_TAG")"

  if [[ -n "$local_commit" && "$local_commit" != "$rc_commit" ]]; then
    die "local tag ${RELEASE_TAG} points at ${local_commit}, but ${TAG} is ${rc_commit} - resolve by hand"
  fi
  if [[ -n "$remote_commit" && "$remote_commit" != "$rc_commit" ]]; then
    die "${GIT_REMOTE} tag ${RELEASE_TAG} points at ${remote_commit}, but ${TAG} is ${rc_commit} - resolve by hand"
  fi
  if [[ -n "$local_commit" && -n "$remote_commit" ]]; then
    ok "release tag ${RELEASE_TAG} already on ${GIT_REMOTE} at ${rc_commit:0:11}"
    return 0
  fi

  step "Tag the release as ${RELEASE_TAG}" \
    "${TAG} is an RC tag, so the release needs the RC-free tag as well." \
    "commit: ${rc_commit}" \
    "$([[ -n "$local_commit" ]] && echo "local tag ${RELEASE_TAG}: already created" || echo "git tag -a ${RELEASE_TAG} -m '${RELEASE_TAG} release' ${rc_commit}")" \
    "git push ${GIT_REMOTE} refs/tags/${RELEASE_TAG}   <- public"

  if [[ -z "$local_commit" ]]; then
    git_repo tag -a "$RELEASE_TAG" -m "${RELEASE_TAG} release" "$rc_commit"
    ok "created local tag ${RELEASE_TAG}"
  fi
  git_repo push "$GIT_REMOTE" "refs/tags/${RELEASE_TAG}"
  ok "pushed ${RELEASE_TAG} to ${GIT_REMOTE}"
}

github_release_body() {
  local rn base name arch f
  rn="${ANNOUNCE_RELEASE_NOTES_URL:-${RELEASE_NOTES_URL:-}}"

  printf '[Change Log](%s)\n\n' "$rn"
  printf -- '- Official Downloads: %s\n\n' "${DOWNLOAD_PAGE_URL%/}"
  printf -- '- Source:\n'
  printf '    - [%s.tar.gz](%s/%s.tar.gz) ([asc](%s/%s.tar.gz.asc))([sha512](%s/%s.tar.gz.sha512))\n' \
    "$RELEASE_PKG_BASE" \
    "$RELEASE_SVN_DIR" "$RELEASE_PKG_BASE" \
    "$RELEASE_SVN_DIR" "$RELEASE_PKG_BASE" \
    "$RELEASE_SVN_DIR" "$RELEASE_PKG_BASE"

  [[ "${#BIN_FILES[@]}" -gt 0 ]] || return 0
  base="${RELEASE_BIN_DOWNLOAD_BASE%/}"
  for f in "${BIN_FILES[@]}"; do
    name="$(basename "$f")"
    arch="${name#apache-doris-${VERSION}-bin-}"
    arch="${arch%.tar.gz}"
    printf '\n'
    printf -- '- Binary(%s):\n' "$arch"
    printf '    - [%s](%s/%s) ([asc](%s/%s.asc))([sha512](%s/%s.sha512))\n' \
      "$name" "$base" "$name" "$base" "$name" "$base" "$name"
  done
}

publish_github_release() {
  local repo body_file current_latest latest_flag latest_label

  if [[ "$skip_github_release" -eq 1 ]]; then
    ok "--skip-github-release: leaving the GitHub release alone"
    return 0
  fi
  if ! command -v gh >/dev/null 2>&1; then
    warn "gh is not installed; skipping the GitHub release for ${RELEASE_TAG}"
    return 0
  fi
  if ! gh auth status >/dev/null 2>&1; then
    warn "gh is not authenticated; skipping the GitHub release for ${RELEASE_TAG}"
    return 0
  fi

  repo="$(github_repo)"
  [[ -n "$repo" ]] || die "cannot determine the GitHub repository; set GITHUB_REPO in release.env"

  if gh release view "$RELEASE_TAG" --repo "$repo" >/dev/null 2>&1; then
    ok "GitHub release already published: $(gh release view "$RELEASE_TAG" --repo "$repo" --json url --jq .url)"
    return 0
  fi

  mkdir -p "$WORK_DIR"
  body_file="$WORK_DIR/github-release.md"
  github_release_body > "$body_file"

  current_latest="$(gh api "repos/${repo}/releases/latest" --jq .tag_name 2>/dev/null || true)"

  step "Publish the GitHub release for ${RELEASE_TAG}" \
    "repo:  ${repo}" \
    "title: ${GITHUB_RELEASE_TITLE}" \
    "body:  ${body_file}" \
    "This is public. The body is printed below; edit the file and re-run to change it." \
    "$(sed 's/^/       /' "$body_file")"

  echo
  if [[ -n "$current_latest" ]]; then
    echo "The release currently marked Latest on ${repo} is ${current_latest}."
  fi
  if confirm "Mark ${RELEASE_TAG} as the Latest release?"; then
    latest_flag="--latest"
    latest_label="Latest"
  else
    latest_flag="--latest=false"
    latest_label="not Latest"
  fi

  gh release create "$RELEASE_TAG" \
    --repo "$repo" \
    --title "$GITHUB_RELEASE_TITLE" \
    --notes-file "$body_file" \
    --verify-tag \
    "$latest_flag"
  ok "published GitHub release ${RELEASE_TAG} (${latest_label})"
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

  # The .txt draft carries the subject line too, so the whole mail can be
  # copied from one file. The .eml keeps it as a real header below.
  {
    echo "Subject: ${subject}"
    echo
    printf '%s\n' "$BODY"
  } > "$body_file"
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

if [[ "$mail_only" -eq 0 ]]; then
  require_tool svn
  require_tool svnmucc
  require_tool gpg
  require_tool sha512sum
  require_tool git

  echo "== Apache Doris ${TAG} - complete release =="
  echo "Source dev SVN folder:     ${DEV_SVN_DIR}/"
  echo "Target release SVN folder: ${RELEASE_SVN_DIR}/"
  warn "Only PMC members can write to the release SVN directory."

  step "Inspect the dev and release SVN state" \
    "Read-only: svn info on the dev RC folder and the release folder." \
    "Nothing is changed by this step; it decides what the later steps do."
  discover_svn_state

  if [[ "$DST_PRESENT" -eq 3 ]]; then
    ok "release artifacts already published: ${RELEASE_SVN_DIR}/"
    ok "nothing to publish; this run only cleans up and drafts the email"
    if [[ "$DEV_DIR_EXISTS" -eq 1 ]]; then
      remove_dev_rc_folder
    else
      ok "dev RC folder already removed: ${DEV_SVN_DIR}/"
    fi
  elif [[ "$DST_PRESENT" -eq 0 ]]; then
    [[ "$SRC_PRESENT" -eq 3 ]] || die "cannot publish: ${SRC_PRESENT}/3 RC artifacts in ${DEV_SVN_DIR}/ and 0/3 published in ${RELEASE_SVN_DIR}/"
    if [[ "$RELEASE_DIR_EXISTS" -eq 1 ]]; then
      warn "release folder exists but is empty; it will be reused instead of created"
    fi

    step "Verify the RC artifacts and build the final checksum" \
      "Downloads the RC tarball, signature and checksum to a temp directory," \
      "checks the sha512 and the detached signature that the voters approved," \
      "then writes ${DST_SHA512} for the RC-free tarball name." \
      "Local only: no SVN state is changed by this step."
    verify_and_build_checksum

    publish_to_release_svn
  else
    die "release folder is half-published (${DST_PRESENT}/3): missing ${DST_MISSING[*]} in ${RELEASE_SVN_DIR}/ - fix it by hand, this script only publishes a complete set"
  fi

  handle_release_tag
  publish_github_release
else
  ok "mail-only mode: skipping SVN publish"
fi

step "Write the [ANNOUNCE] email draft" \
  "Writes announce-email.txt and announce-email.eml under ${WORK_DIR}." \
  "Local only: the mail is never sent by this script."
write_announce_email
