---
{
"title": "Release Doris Shade",
"language": "en"
}
---

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

# Release Doris Shade

Its code base is independent of the main Doris code base, located at:

- https://github.com/apache/doris-shade

## Preparing for release

First, please refer to the [release preparation](./release-prepare.md) document to prepare for the release.

## Releasing to Maven

Let's take the release of Doris Shade v1.0.0 as an example.

### 1. Prepare the branch

Create a branch in the code base: 1.0.0-release, and checkout to this branch.

### 2. Release to Maven staging

Execute the following command to start generating release tags:

```bash
mvn release:clean
mvn release:prepare -DpushChanges=false
```

Among them, `-DpushChanges=false` means that during the execution process, the newly generated branches and tags will not be pushed to the code base.

After executing the `release:prepare` command, the following three pieces of information will be requested:

1. Version information of Doris Shade: We can use the default, you can directly press Enter or enter the version you want. The version format is `{shade.version}`, such as `1.0.0`.
2. Release tag of Doris Shade: The release process will generate a tag locally. We can use the default tag name, such as `1.0.0`.
3. The version number of the next version of Doris Shade: this version number is only used when generating local branches, and has no practical significance. We can fill in one according to the rules. For example, the current version to be released is: `1.0.0`, then fill in `1.0.1-SNAPSHOT` for the next version number.

`mvn release:prepare` may ask for a GPG passphrase. If there is `gpg: no valid OpenPGP data found` error, you can execute `export GPG_TTY=$(tty)` and try again.

After `mvn release:prepare` is executed successfully, a tag and a branch will be generated locally. And the current branch will add two commits. The first commit corresponds to the newly generated tag, and the second is the next version of the branch. It can be viewed through `git log`.

After the local tag is confirmed to be correct, the tag needs to be pushed to the code base:

`git push upstream --tags`

Where upstream points to the `apache/doris-shade` codebase.

Finally, execute perform:

```
mvn release:perform
```

After successful execution, you can find the newly released version in [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories):

![](/images/staging-repositories.png)

**Note that the `.asc` signature file needs to be included. **

If the operation is wrong. It is necessary to delete the local tag, the tag in the code base, and the two newly generated commits locally. And drop staging. Then perform the above steps again.

After checking, click the `close` button in the figure to complete the staging release.

### 3. Prepare svn

Check out the svn repository:

```
svn co https://dist.apache.org/repos/dist/dev/doris/
```

Package tag source code, and generate signature file and sha256 verification file. Here we take `1.0.0` as an example. Other tag operations are the same

```
git archive --format=tar 1.14_2.12-1.0.0 --prefix=apache-doris-shade-1.0.0-src/ | gzip > apache-doris-shade-1.0.0-src.tar.gz
gpg -u xxx@apache.org --armor --output apache-doris-shade-1.0.0-src.tar.gz.asc --detach-sign apache-doris-shade-1.0.0-src.tar. gz
sha512sum apache-doris-shade-1.14_2.12-1.0.0-src.tar.gz > apache-doris-shade-1.0.0-src.tar.gz.sha512

Mac:
shasum -a 512 apache-doris-shade-1.0.0-src.tar.gz > apache-doris-shade-1.0.0-src.tar.gz.sha512
```

You end up with three files:

```
apache-doris-shade-1.0.0-src.tar.gz
apache-doris-shade-1.0.0-src.tar.gz.asc
apache-doris-shade-1.0.0-src.tar.gz.sha512
```

Move these three files to the svn directory:

```
doris/doris-shade/1.0.0/
```

The final svn directory structure is similar to:

```
├── 1.2.3-rc01
│ ├── apache-doris-1.2.3-src.tar.gz
│ ├── apache-doris-1.2.3-src.tar.gz.asc
│ ├── apache-doris-1.2.3-src.tar.gz.sha512
...
├── KEYS
├── doris-shade
│ └── 1.0.0
│ ├── apache-doris-shade-1.0.0-src.tar.gz
│ ├── apache-doris-shade-1.0.0-src.tar.gz.asc
│ └── apache-doris-shade-1.0.0-src.tar.gz.sha512
```

Among them, 1.2.3-rc01 is the directory of Doris main code, and `doris-shade/1.0.0` is the content of this release.

Note, for the preparation of the KEYS file, please refer to the introduction in [Release Preparation](./release-prepare.md).

### 4. Voting

Initiate a vote in the dev@doris mail group, the template is as follows:

```
Hi all,

This is a call for the vote to release Apache Doris-Shade 1.0.0

The git tag for the release:
https://github.com/apache/doris-shade/releases/tag/doris-shade-1.0.0

Release Notes are here:
https://github.com/apache/doris-shade/blob/doris-shade-1.0.0/CHANGE-LOG.txt

Thanks to everyone who has contributed to this release.

The release candidates:
https://dist.apache.org/repos/dist/dev/doris/doris-shade/

KEYS file is available here:
https://downloads.apache.org/doris/KEYS

To verify and build, you can refer to the following link:
https://doris.apache.org/community/release-and-verify/release-verify

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...
```

## Completing the release

Please refer to the [Complete Release](./release-complete.md) document to complete the entire release process.

## APPENDIX: Releasing TO SNAPSHOT

Snapshot is not an Apache Release version, it is only used for preview before release. After being discussed and approved by the PMC, the Snapshot version can be released

Change to the doris shade directory

```
mvn deploy
```

Afterwards you can see the snapshot version here:

```
https://repository.apache.org/content/repositories/snapshots/org/apache/doris/doris-shade/
```