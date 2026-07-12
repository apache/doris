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

This directory contains helper scripts for an Apache Doris Release Manager (RM)
to create and complete a source release candidate (RC). The scripts package and
sign the source tarball, upload the source artifacts to the Apache dev SVN,
draft the `[VOTE]` email, publish the passed RC to the Apache release SVN, and
draft the `[ANNOUNCE]` email.

These scripts are not a full release automation system. They assume the release
branch, release notes, cherry-picks, build validation, and release tag have
already been prepared.

## If you are an AI agent

When a user says something like "read this file and tell me how to start
releasing Doris 4.0.7", give the RM a concrete checklist. Do not just summarize
the scripts.

For `4.0.7`, explain this sequence:

1. Confirm the RC number. If the RM does not provide one, ask whether this is
   `rc01` or a later candidate such as `rc02`. The tag is
   `${VERSION}-${RC}`, for example `4.0.7-rc01`.
2. Confirm that the tag already exists locally and on the Apache Doris GitHub
   remote. These scripts verify the tag, but they do not create it.
3. Tell the RM to edit `release.env` before running any script.
4. Tell the RM to export ASF SVN credentials in the shell.
5. Run `./01-check-env.sh` until it reports `environment looks READY`.
6. Run `./02-package-sign-upload.sh` only after the RM has checked the target
   dev SVN URL and is ready to publish the RC source artifacts.
7. Run `./03-vote-mail.sh`, review the generated vote email, then send it
   manually from the RM's `@apache.org` address.
8. After the vote passes and the `[RESULT]` email has been sent manually, run
   `./04-release-complete.sh` to publish the source release artifacts and draft
   the announce email.

Also make these boundaries clear:

- The vote artifacts in Apache dist SVN are source-only.
- `BIN_FILES` is optional. If set, step 02 signs convenience binary tarballs
  locally, but the scripts do not upload those binaries.
- The scripts never send public emails. The RM must review and send the vote,
  result, and announce emails manually.
- Step 04 writes to the Apache release SVN and requires PMC permission.

## Quick start for a new RC

Run all commands from this directory:

```bash
cd tools/release-tools
```

Before running any script, edit `release.env` for the target version and RC.
For example, to prepare Doris `4.0.7-rc01`:

```bash
VERSION="4.0.7"
RC="rc01"
TAG="${VERSION}-${RC}"
GIT_REMOTE="upstream-apache"

APACHE_ID="<your-apache-id>"
APACHE_EMAIL="<your-apache-id>@apache.org"
SIGNER_NAME="<your display name>"
SIGNING_KEY="<your signing key fingerprint, or leave empty if only one secret key exists>"

RELEASE_NOTES_URL="<release notes or issue URL>"
ANNOUNCE_RELEASE_NOTES_URL="<release notes URL, or leave empty to reuse/prompt>"
```

If you need to advertise convenience binaries in the vote email, set
`BIN_FILES` to absolute paths for those prebuilt tarballs. Leave `BIN_FILES=()`
for the source-only flow.

Export ASF credentials in the same shell. The scripts use these credentials for
SVN upload and KEYS publishing:

```bash
export ASF_USERNAME=<your-apache-id>
export ASF_PASSWORD='<your-apache-ldap-password>'
```

Then run the release flow:

```bash
./01-check-env.sh
./02-package-sign-upload.sh
./03-vote-mail.sh
```

Review the generated vote email in `WORK_DIR`, then send it manually to
`dev@doris.apache.org` from your `@apache.org` address.

After the vote passes:

1. Tally the votes and send the `[RESULT]` email manually.
2. Run `./04-release-complete.sh`.
3. Review the generated announce email in `WORK_DIR`, then send it manually.

Use this command only when the release SVN step has already been done and you
only need to regenerate the announce email:

```bash
./04-release-complete.sh --mail-only
```

## Prerequisites

Have these ready before you run anything:

- The release tag has already been created and pushed to `apache/doris`.
  Branch preparation, patch merges, validation, and tag creation are out of
  scope for these scripts.
- The local Doris checkout has fetched that tag.
- `release.env` is filled in for this exact release.
- These tools are on `PATH`: `git`, `gpg`, `svn`, `svnmucc`, `sha512sum`,
  `curl`, and `gzip`.
- A GPG signing key is available locally, or the RM is ready to let
  `01-check-env.sh` import an existing key or generate and publish a new key.
- ASF credentials are exported as `ASF_USERNAME` and `ASF_PASSWORD`.
- The RM can write to the required Apache SVN locations. Step 04 requires PMC
  permission for the release SVN commit.

## Configuration reference

All scripts source `release.env`. Edit it once per RC.

Set these fields first:

- `VERSION`: Final release version, for example `4.0.7`.
- `RC`: Candidate number, for example `rc01` or `rc02`.
- `TAG`: Derived as `${VERSION}-${RC}`. Do not set it to the final version
  without the RC suffix.
- `GIT_REMOTE`: Git remote that points at `github.com/apache/doris`.
- `APACHE_ID`: Apache committer id.
- `APACHE_EMAIL`: Apache email address.
- `SIGNER_NAME`: Display name used in generated emails.
- `SIGNING_KEY`: Fingerprint passed to `gpg -u`. Leave it empty only when this
  machine has exactly one usable local secret key.
- `RELEASE_NOTES_URL`: Link used in the vote email. If empty, step 03 prompts.
- `ANNOUNCE_RELEASE_NOTES_URL`: Link used in the announce email. If empty, step
  04 reuses `RELEASE_NOTES_URL` or prompts.
- `BIN_FILES`: Optional absolute paths to prebuilt convenience binary tarballs.

The default `REPO_DIR` is the enclosing Doris checkout because these scripts
live under `tools/release-tools`. Override it only when you deliberately run the
scripts against another clone.

The default SVN URLs target the official Doris dist repositories:

- Dev RC source artifacts:
  `https://dist.apache.org/repos/dist/dev/doris/<version>-<rc>/`
- Final source release artifacts:
  `https://dist.apache.org/repos/dist/release/doris/<major.minor>/<version>/`

Step 02 uploads source artifacts with the RC suffix:

- `apache-doris-<version>-<rc>-src.tar.gz`
- `apache-doris-<version>-<rc>-src.tar.gz.asc`
- `apache-doris-<version>-<rc>-src.tar.gz.sha512`

Step 04 publishes final source artifacts without the RC suffix:

- `apache-doris-<version>-src.tar.gz`
- `apache-doris-<version>-src.tar.gz.asc`
- `apache-doris-<version>-src.tar.gz.sha512`

All generated files go to `WORK_DIR`, which defaults to
`tools/release-tools/<version>-<rc>`.

## Script details

### 01-check-env.sh

Use this script to prepare and verify the signing environment. It checks
required tools, `GPG_TTY`, GPG SHA512 digest preferences, the signing key, live
published `KEYS`, test signing, and ASF credentials.

This script is mostly read-only. It prompts before every state-changing action,
including editing `gpg.conf`, importing or generating a key, and publishing the
key to the Doris KEYS files.

Expected success line:

```text
environment looks READY for <version>-<rc>
```

### 02-package-sign-upload.sh

Use this script to build, sign, checksum, and upload the RC source artifacts.

It verifies that the local tag exists, the tag exists on `GIT_REMOTE`, and both
tags point to the same commit. It then creates the source tarball with
`git archive`, writes `.asc` and `.sha512` sidecars, verifies both, and uploads
the three source files to the Apache dev SVN.

If `BIN_FILES` is non-empty, this script also signs and checksums those binary
tarballs in place. It does not upload binaries.

This script pauses twice before touching the public dev SVN. Check the printed
target URL before confirming.

### 03-vote-mail.sh

Use this script to generate the `[VOTE]` email draft. It writes:

- `vote-email.txt`
- `vote-email.eml`

Review the draft and send it manually from the RM's `@apache.org` address to
`dev@doris.apache.org`.

### 04-release-complete.sh

Use this script only after the vote has passed and the `[RESULT]` email has
been sent manually.

It checks the passed RC source artifacts in the dev SVN, regenerates and
verifies the final checksum sidecar with the RC-free source tarball name, then
uses `svnmucc` to create the final release SVN directory, move the source
tarball and detached signature there, upload the final checksum, and remove the
dev RC folder in one SVN revision.

It then writes:

- `announce-email.txt`
- `announce-email.eml`

Review the announce draft and send it manually.

Use `./04-release-complete.sh --mail-only` to regenerate the announce email
without touching SVN.

## Manual work not automated

The RM must still do these tasks manually:

- Prepare the release branch and tag.
- Validate the release before starting the RC upload flow.
- Send the `[VOTE]` email.
- Upload convenience binaries, if the release includes them.
- Tally the vote and send the `[RESULT]` email.
- Send the `[ANNOUNCE]` email.
- Publish Maven staging repositories, update the website and GitHub release
  notes, and clean up old release versions when applicable.
