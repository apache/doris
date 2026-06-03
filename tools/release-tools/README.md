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

Helper scripts for an Apache Doris Release Manager to cut a **source** release candidate, in three steps:
**check the signing environment → package + sign + upload the source tarball → draft the `[VOTE]` email.**

The scripts are reusable across releases — they hold no version. Edit `release.env` each time.
Branch prep, issue cleanup, patch merges and tag creation are out of scope: the tag must already
exist and be pushed to `apache/doris` before you start.

## Files
- `release.env` — all configuration (version, paths, signing key, SVN URLs, email). **Edit this first.**
- `00-check-env.sh` — check / prepare the GPG signing environment and ASF credentials.
- `10-package-sign-upload.sh` — `git archive` the tag, GPG-sign, sha512, upload to the dev SVN.
- `20-vote-mail.sh` — generate the `[VOTE]` email draft.

## Prerequisites
- Tools on `PATH`: `git`, `gpg`, `svn`, `sha512sum`, `curl`, `gzip`.
- A local clone of `apache/doris` with the release tag fetched.
- A GPG signing key (step 00 can import an existing one, or generate and publish a new one).
- ASF credentials exported in your shell (used for the SVN upload and the KEYS publish):
  ```bash
  export ASF_USERNAME=<your-apache-id>
  export ASF_PASSWORD='<your-apache-ldap-password>'
  ```

## Configure
Open `release.env` and set at least:
- `REPO_DIR` — path to your local `apache/doris` git checkout.
- `VERSION` / `RC` — e.g. `4.0.6` and `rc02`; `TAG` is derived as `${VERSION}-${RC}`.
- `GIT_REMOTE` — the git remote pointing at `github.com/apache/doris`.
- `APACHE_ID` / `APACHE_EMAIL` — your committer id and `@apache.org` email.
- `SIGNING_KEY` — fingerprint of the key to sign with (leave empty to auto-detect a single local secret key).

The SVN URLs, dev mailing list and verify-guide link rarely change; the defaults target the official
Doris dist repos. Artifacts are **source-only**: `apache-doris-<tag>-src.tar.gz` + `.asc` + `.sha512`.
All output (tarball, signatures, SVN checkouts, email draft) goes to `WORK_DIR`
(`~/doris-release/<tag>` by default), outside the git repo.

## Run order
```bash
./00-check-env.sh             # must end with "environment looks READY"
./10-package-sign-upload.sh   # pauses twice for confirmation before the public SVN commit
./20-vote-mail.sh             # prompts for the Release Notes URL, then prints the draft
```

### What each step does
- **00** verifies the required tools, your `GPG_TTY` / `gpg.conf`, resolves (or helps you create) a
  signing key, checks it is present in the live published `KEYS`, runs a test sign + verify, and
  confirms your ASF credentials. It is read-mostly: every state-changing action (edit `gpg.conf`,
  import / generate a key, publish `KEYS`) prompts before acting.
- **10** checks the local tag matches `GIT_REMOTE`, builds the source tarball from the tag with
  `git archive`, signs it and writes the `.sha512`, then uploads the three files to
  `dev/doris/<tag>/` on the Apache dist SVN. **Eyeball the target URL** at the confirm prompt before
  committing — nothing public happens until the final confirm.
- **20** writes `vote-email.txt` and `vote-email.eml` into `WORK_DIR` and prints the draft. Review it
  and send it yourself from your `@apache.org` address.

## Not automated (on purpose)
- Sending the vote email — it goes to a public ASF list, so you review and send it manually.
- Binary packages — not part of the ASF source vote.
- Post-vote steps — tallying the result, the result email, and moving the artifacts from `dev/` to
  `release/` once the vote passes.
