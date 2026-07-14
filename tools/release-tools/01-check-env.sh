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

# Step 01 - prepare / check the signing environment.
# Read-mostly. The only state-changing paths (import key / generate key /
# publish KEYS / edit gpg.conf) are opt-in and prompt before acting.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release.env
source "${HERE}/release.env"

ok()   { echo "[ OK ] $*"; }
warn() { echo "[WARN] $*"; }
err()  { echo "[FAIL] $*"; }
die()  { err "$*"; exit 1; }
confirm() { local a; read -r -p "$1 [y/N] " a; [[ "$a" == y || "$a" == Y ]]; }

# does $1 (fingerprint) appear in a KEYS stream piped on stdin?
key_in_keys() {
  local fpr="$1" kr ret=1; kr="$(mktemp -d)"
  if gpg --homedir "$kr" --import >/dev/null 2>&1 && gpg --homedir "$kr" --list-keys "$fpr" >/dev/null 2>&1; then ret=0; fi
  rm -rf "$kr"; return "$ret"
}

# svn auth args (only added if exported)
svn_auth=(--non-interactive --no-auth-cache)
[[ -n "${ASF_USERNAME:-}" ]] && svn_auth+=(--username "$ASF_USERNAME")
[[ -n "${ASF_PASSWORD:-}" ]] && svn_auth+=(--password "$ASF_PASSWORD")

problems=0
echo "== Apache Doris ${TAG} - signing environment check =="

# 1. required tools
for t in git gpg svn svnmucc sha512sum curl gzip; do
  if command -v "$t" >/dev/null 2>&1; then ok "tool: $t"; else err "missing tool: $t"; problems=$((problems+1)); fi
done

# 2. GPG_TTY (needed so the passphrase prompt can appear)
export GPG_TTY="$(tty || true)"
ok "GPG_TTY=${GPG_TTY:-<none>}"

# 3. gpg.conf SHA512 preference (Apache recommendation)
gpgconf_file="${GNUPGHOME:-$HOME/.gnupg}/gpg.conf"
if grep -q "cert-digest-algo SHA512" "$gpgconf_file" 2>/dev/null; then
  ok "gpg.conf has SHA512 digest preferences"
else
  warn "gpg.conf missing SHA512 preferences ($gpgconf_file)"
  if confirm "Append recommended SHA512 lines to gpg.conf?"; then
    {
      echo "personal-digest-preferences SHA512"
      echo "cert-digest-algo SHA512"
      echo "default-preference-list SHA512 SHA384 SHA256 SHA224 AES256 AES192 AES CAST5 ZLIB BZIP2 ZIP Uncompressed"
    } >> "$gpgconf_file"
    ok "updated $gpgconf_file"
  fi
fi

# 4. resolve a usable secret key (one primary fpr per secret key)
list_secret_fprs() {
  gpg --list-secret-keys --with-colons 2>/dev/null \
    | awk -F: '$1=="sec"{w=1} $1=="fpr"&&w{print $10; w=0}'
}
resolve_secret_key() {
  if [[ -n "${SIGNING_KEY}" ]]; then
    gpg --list-secret-keys "${SIGNING_KEY}" >/dev/null 2>&1 && { echo "${SIGNING_KEY}"; return 0; }
    return 1
  fi
  local fprs n; fprs="$(list_secret_fprs)"; n="$(printf '%s\n' "$fprs" | grep -c . || true)"
  if   [[ "$n" -eq 1 ]]; then printf '%s\n' "$fprs"; return 0
  elif [[ "$n" -gt 1 ]]; then return 2
  fi
  return 1
}

import_key_from_file() {
  local p; read -r -p "Path to exported secret key file (.asc/.gpg): " p
  [[ -f "$p" ]] || die "no such file: $p"
  gpg --import "$p"
}

gen_key() {
  local rn em
  read -r -p "Real name (>=5 chars, e.g. morningman): " rn
  read -r -p "Apache email (xxx@apache.org): " em
  echo "Generating RSA-4096 sign key, no expiry. You'll be asked for a passphrase - REMEMBER IT."
  gpg --quick-generate-key "${rn} (CODE SIGNING KEY) <${em}>" rsa4096 sign never
}

# append the new public key to dev + release KEYS and commit (so voters can verify)
# append the key to the dev + release KEYS files and commit (append-only).
# idempotent: skips a repo whose KEYS already contains the key, so re-running
# after a partial failure won't duplicate the entry.
# caller is responsible for asking the user first.
publish_key_to_keys() {
  local fpr="$1" spec name base wc
  mkdir -p "${WORK_DIR}"
  for spec in "dev=${DEV_SVN_BASE}" "release=${RELEASE_SVN_BASE}"; do
    name="${spec%%=*}"; base="${spec#*=}"
    wc="${WORK_DIR}/keys-${name}-$$"
    rm -rf "$wc"                       # avoid stale working-copy collisions
    echo ">> ${base}/KEYS"
    svn checkout --depth files "${svn_auth[@]}" "$base" "$wc"
    if key_in_keys "$fpr" < "$wc/KEYS"; then
      ok "key already in ${name} KEYS - skip"
      continue
    fi
    # append only - never rewrite existing KEYS content
    { echo; gpg --list-sigs "$fpr"; gpg --armor --export "$fpr"; } >> "$wc/KEYS"
    echo "--- appended to ${name}/KEYS (tail) ---"; tail -n 18 "$wc/KEYS"
    svn commit "${svn_auth[@]}" -m "Add KEY for ${APACHE_ID}" "$wc"
    ok "committed KEYS to ${base}"
  done
  warn "MANUAL: paste this fingerprint into https://id.apache.org (OpenPGP field):"
  gpg --fingerprint "$fpr" | sed -n '2p'
}

SIGNER=""
if SIGNER="$(resolve_secret_key)"; then
  ok "signing key resolved: ${SIGNER}"
else
  rc=$?
  if [[ "$rc" -eq 2 ]]; then
    err "multiple secret keys found - set SIGNING_KEY in release.env to choose one:"
    gpg --list-secret-keys --keyid-format=long
    problems=$((problems+1))
  else
    warn "no usable secret key in this environment. Options:"
    echo "  1) import an existing exported secret key from a file"
    echo "  2) generate a NEW key here and publish it to the Doris KEYS files"
    echo "  3) skip (you will sign on another machine)"
    read -r -p "choose [1/2/3]: " choice
    case "$choice" in
      1) import_key_from_file; SIGNER="$(resolve_secret_key || true)";;
      2) gen_key; SIGNER="$(resolve_secret_key || true)"
         if [[ -n "$SIGNER" ]] && confirm "Publish this new key to dev+release KEYS now?"; then
           publish_key_to_keys "$SIGNER"
         fi;;
      3) warn "skipping local key; sign on another machine.";;
      *) warn "no choice made.";;
    esac
    if [[ -n "$SIGNER" ]]; then ok "signing key resolved: ${SIGNER}"; else warn "still no signing key"; problems=$((problems+1)); fi
  fi
fi

# 5. is the signing key published in the live KEYS? + 6. test signature
if [[ -n "$SIGNER" ]]; then
  fpr="$(gpg --list-keys --with-colons "$SIGNER" | awk -F: '/^fpr:/{print $10; exit}')"
  ok "fingerprint: ${fpr}"

  if curl -fsSL "$KEYS_URL" 2>/dev/null | key_in_keys "$fpr"; then
    ok "key is present in published KEYS"
  else
    warn "key NOT found in published KEYS ($KEYS_URL) - voters can't verify until it's published"
    if confirm "Append this key to the Doris dev+release KEYS and commit now?"; then
      publish_key_to_keys "$fpr"
      if svn cat "${svn_auth[@]}" "${RELEASE_SVN_BASE}/KEYS" 2>/dev/null | key_in_keys "$fpr"; then
        ok "key now in release SVN KEYS (downloads.apache.org mirror syncs in a few min)"
      else
        err "key still not in release SVN KEYS after commit - check output above"
        problems=$((problems+1))
      fi
    else
      warn "skipped KEYS publish; key MUST be in published KEYS before the vote"
      problems=$((problems+1))
    fi
  fi

  tf="$(mktemp)"; echo "doris ${TAG} signing test" > "$tf"
  if gpg -u "$SIGNER" --armor --detach-sign -o "$tf.asc" "$tf" >/dev/null 2>&1 \
     && gpg --verify "$tf.asc" "$tf" >/dev/null 2>&1; then
    ok "test sign + verify succeeded (passphrase/agent working)"
  else
    err "test signing failed - check passphrase / GPG_TTY / pinentry"; problems=$((problems+1))
  fi
  rm -f "$tf" "$tf.asc"
fi

# 7. ASF SVN credentials
if [[ -n "${ASF_USERNAME:-}" && -n "${ASF_PASSWORD:-}" ]]; then
  ok "ASF_USERNAME/ASF_PASSWORD present in env"
else
  warn "ASF_USERNAME/ASF_PASSWORD not both set - needed for SVN upload (step 02) and KEYS publish"
fi

echo
if [[ "$problems" -eq 0 ]]; then
  ok "environment looks READY for ${TAG}"
else
  err "${problems} problem(s) above - resolve before packaging"; exit 1
fi
