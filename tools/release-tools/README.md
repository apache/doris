<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris release helper scripts

Helper scripts for an Apache Doris Release Manager (RM) to cut and complete a **source** release:
package and sign the source tarball, upload it to the Apache dev SVN, draft the `[VOTE]` email,
publish the passed RC to the Apache release SVN, and draft the `[ANNOUNCE]` email.

## Prerequisites
Have these ready before you run anything:
- The release **tag is already created and pushed** to `apache/doris` — these scripts only verify it
  (branch prep, patch merges and tag creation are out of scope).
- A local clone of `apache/doris` with that tag fetched.
- These tools on `PATH`: `git`, `gpg`, `svn`, `svnmucc`, `sha512sum`, `curl`, `gzip`.
- A GPG signing key — or let step 01 import an existing one, or generate and publish a new one.
- ASF credentials exported in your shell (used for the SVN upload and the KEYS publish):
  ```bash
  export ASF_USERNAME=<your-apache-id>
  export ASF_PASSWORD='<your-apache-ldap-password>'
  ```
- `release.env` filled in for this release — see [Configuration](#configuration) for the fields.

## Steps
Run from this directory, in order:
1. **Check the signing environment** — `./01-check-env.sh`. Re-run until it ends with
   `environment looks READY`.
2. **Package, sign & upload** — `./02-package-sign-upload.sh`. Builds and signs the source tarball
   and uploads it to the dev SVN. It pauses twice for confirmation; nothing public happens before
   the final confirm.
3. **Draft the vote email** — `./03-vote-mail.sh`. Prompts for the Release Notes URL and prints the
   draft; review it and send it yourself from your `@apache.org` address.
4. **Complete the release** — after the vote result has passed, `./04-release-complete.sh` copies the
   source artifacts from dev SVN to release SVN with the `rc` suffix removed from file names, removes
   the RC folder from dev SVN, and drafts the `[ANNOUNCE]` email. This step requires PMC permission
   for the release SVN commit.

---

Everything below is reference detail.

## Files
- `release.env` — all configuration (version, paths, signing key, SVN URLs, email). **Edit this first.**
- `01-check-env.sh` — check / prepare the GPG signing environment and ASF credentials.
- `02-package-sign-upload.sh` — `git archive` the tag, GPG-sign, sha512, sign any prebuilt binaries
  locally, then upload the source artifacts to the dev SVN.
- `03-vote-mail.sh` — generate the `[VOTE]` email draft.
- `04-release-complete.sh` — publish the passed RC source artifacts to the release SVN and generate
  the `[ANNOUNCE]` email draft.

## Configuration
The scripts are reusable across releases — they hold no version; edit `release.env` each time.
Set at least:
- `VERSION` / `RC` — e.g. `4.0.6` and `rc02`; `TAG` is derived as `${VERSION}-${RC}`.
- `GIT_REMOTE` — the git remote pointing at `github.com/apache/doris`.
- `APACHE_ID` / `APACHE_EMAIL` / `SIGNER_NAME` — your committer id, `@apache.org` email, and the
  display name used to sign the vote email.
- `SIGNING_KEY` — fingerprint of the key to sign with (leave empty to auto-detect a single local secret key).
- `BIN_FILES` — optional absolute paths to prebuilt binary tarballs to sign locally (see below). Leave
  the list empty (the default) to skip binary signing and run the source-only flow. When set, step 03
  advertises each binary in the vote email under `BIN_DOWNLOAD_BASE`; when empty that section is omitted.

`REPO_DIR` defaults to the enclosing checkout (`${ROOT}/../../`) since these scripts live inside
`apache/doris`; override it only if you run them against a different clone.

The SVN URLs, dev mailing list, download page and verify-guide link rarely change; the defaults
target the official Doris dist repos. The **vote SVN artifacts are source-only**:
`apache-doris-<tag>-src.tar.gz` + `.asc` + `.sha512`. The **release SVN artifacts** are published
under `release/doris/<major.minor>/<version>/` and drop the RC suffix from the file names:
`apache-doris-<version>-src.tar.gz` + `.asc` + `.sha512`. All script output (source tarball,
signatures, SVN checkouts, email drafts) goes to `WORK_DIR` (`<this-dir>/<tag>`, i.e.
`tools/release-tools/<tag>`, by default).

## What each step does
- **01** verifies the required tools, your `GPG_TTY` / `gpg.conf`, resolves (or helps you create) a
  signing key, checks it is present in the live published `KEYS`, runs a test sign + verify, and
  confirms your ASF credentials. It is read-mostly: every state-changing action (edit `gpg.conf`,
  import / generate a key, publish `KEYS`) prompts before acting.
- **02** checks the local tag matches `GIT_REMOTE`, builds the source tarball from the tag with
  `git archive`, signs it and writes the `.sha512`. If `BIN_FILES` is non-empty it then GPG-signs and
  sha512s each prebuilt binary tarball, writing the `.asc` / `.sha512` sidecars **next to** each binary
  (these are NOT uploaded — you upload the binaries and their sidecars yourself). Finally it uploads
  the three **source** files to `dev/doris/<tag>/` on the Apache dist SVN. **Eyeball the target URL**
  at the confirm prompt before committing — nothing public happens until the final confirm.
- **03** writes `vote-email.txt` and `vote-email.eml` into `WORK_DIR` and prints the draft. Review it
  and send it yourself from your `@apache.org` address.
- **04** checks the passed RC artifacts in dev SVN, then uses `svnmucc` URL operations to create
  `release/doris/<major.minor>/<version>/`, move the tarball, detached signature, and checksum there
  with RC-free file names, and remove `dev/doris/<tag>/` in one SVN revision. It then writes
  `announce-email.txt` and `announce-email.eml`; use `./04-release-complete.sh --mail-only` to
  regenerate the email without touching SVN.

## Not automated (on purpose)
- Sending the vote email — it goes to a public ASF list, so you review and send it manually.
- Uploading binary packages — step 02 can *sign* the tarballs listed in `BIN_FILES`, but binaries are
  not part of the ASF source vote, so you upload them (with their `.asc`/`.sha512`) manually.
- Tallying the vote result and sending the `[RESULT]` email before step 04.
- Publishing Maven staging repositories, updating the website / GitHub release notes, and cleaning
  up old release versions beyond the current RC folder.
