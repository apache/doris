---
{
"title": "Release Doris Connectors",
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

# Releases Doris Connectors

Doris Connectors currently contains:

* Flink Doris Connector
* Spark Doris Connector

The code base is separate from the main Doris code base and is located at:

- https://github.com/apache/incubator-doris-flink-connector
- https://github.com/apache/incubator-doris-spark-connector

## Preparing for release

First, see the [release preparation](./release-prepare.md) documentation to prepare for the release.

## Releasing to Maven

Let's take the example of releasing Flink Connector v1.0.0.

### 1. Prepare the branch

Create a branch in the codebase: branch-1.0, and checkout to that branch.

### 2. release to Maven staging

Since Flink Connector releases different releases for different Flink versions (e.g. 1.11, 1.12, 1.13), we need to handle each version separately.

Let's take Flink version 1.14.3 and scala version 2.12 as an example.

First, replace flink.version and scala.version in pom.xml with

```
cd flink-doris-connector/
sed -i 's/\${env.flink.version}/1.14.3/g' pom.xml
sed -i 's/\${env.flink.minor.version}/1.14/g' pom.xml
sed -i 's/\${env.scala.version}/2.12/g' pom.xml

Mac:

sed -i '' 's/\${env.flink.version}/1.14.3/g' pom.xml
sed -i '' 's/\${env.flink.minor.version}/1.14/g' pom.xml
sed -i '' 's/\${env.scala.version}/2.12/g' pom.xml
```

After replacing, commit the local changes to.

```
git add . -u
git commit -m "prepare for 1.14_2.12-1.0.0"
```

Execute the following command to start generating the release tag.

```bash
cd flink-doris-connector/
mvn release:clean
mvn release:prepare -DpushChanges=false
```

where `-DpushChanges=false` means that the newly generated branches and tags are not pushed to the codebase during execution.

After executing the `release:prepare` command, the following three pieces of information will be requested.

1. the version of the Doris Flink Connector: which we can do by default, either by entering a carriage return or by typing in the version you want. The version format is `{connector.version}`, e.g. `1.0.0`.
2. The release tag of Doris Flink Connector: the release process will generate a tag locally, we can use the default tag name, such as `1.14_2.12-1.0.0`.
3. The version number of the next version of Doris Flink Connector: This version number is only used for generating local branches and has no real meaning. For example, if the current release is `1.0.0`, then the next version number should be `1.0.1-SNAPSHOT`.

`mvn release:prepare` may ask for GPG passphrase, if you get `gpg: no valid OpenPGP data found` error, you can try after executing `export GPG_TTY=$(tty)`.

If `mvn release:prepare` succeeds, a tag and a branch will be created locally, and two new commits will be added to the current branch, the first one corresponding to the newly created tag and the second one to the branch of the next release, which can be viewed via `git log`.

Once the local tag is verified, you need to push the tag to the repository.

`git push upstream --tags`

where upstream points to the `apache/incubator-doris-flink-connector` repository.

Finally, execute perform:

```
mvn release:perform
```

After successful execution, the version just released can be found in [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories)

![](/images/staging-repositories.png)

**Note that the `.asc` signature file needs to be included.**

If there is an error. You need to delete the local tag, the tag in the codebase, and the two newly generated local commits. And drop the staging. Then re-execute the above steps.

After checking, click the `close` button in the figure to finish staging release.

### 3. Prepare svn

Check out the svn repository.

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
```

Package the tag source code and generate the signature file and sha256 checksum file. Here we take `1.14_2.12-1.0.0` as an example.

```
git archive --format=tar 1.14_2.12-1.0.0 --prefix=apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src/ | gzip > apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz
gpg -u xxx@apache.org --armor --output apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.asc  --detach-sign apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz
sha512sum apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz > apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.sha512

Mac:
shasum -a 512 apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz > apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.sha512
```

The end result is three files:

```
apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz
apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.asc
apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.sha512
```

Move these three files to the svn directory:

```
doris/flink-connector/1.0.0/
```

The final svn directory structure will look like this:

```
|____0.15
| |____0.15.0-rc04
| | |____apache-doris-0.15.0-incubating-src.tar.gz.sha512
| | |____apache-doris-0.15.0-incubating-src.tar.gz.asc
| | |____apache-doris-0.15.0-incubating-src.tar.gz
|____KEYS
|____flink-connector
| |____1.0.0
| | |____apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz
| | |____apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.sha512
| | |____apache-doris-flink-connector-1.14_2.12-1.0.0-incubating-src.tar.gz.asc
```

Where 0.15 is the directory of Doris main code, and under `flink-connector/1.0.0` is the content of this release.

Note that the preparation of the KEYS file can be found in [release preparation](. /release-prepare.md).

### 4. Polling

Initiate a poll in the dev@doris mailgroup, with the following template.

```
Hi All,

This is a call for vote to release Flink Connectors v1.0.0 for Apache Doris(Incubating).

- apache-doris-flink-connector-1.14_2.12-1.0.0-incubating

The release node:
xxxxx

The release candidates:
https://dist.apache.org/repos/dist/dev/incubator/doris/flink-connector/1.0.0/

Maven 2 staging repository:
https://repository.apache.org/content/repositories/orgapachedoris-1002/org/apache/doris/doris-flink-connector/

Git tag for the release:
https://github.com/apache/incubator-doris-flink-connector/tree/1.14_2.12-1.0.0

Keys to verify the Release Candidate:
https://downloads.apache.org/incubator/doris/KEYS

Look at here for how to verify this release candidate:
http://doris.incubator.apache.org/community/release-and-verify/release-verify.html

The vote will be open for at least 72 hours or until necessary number of votes are reached.

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason
```

After the dev mail group is approved, send an email to the general@incubator mail group for IPMC voting.

```
Hi All,

This is a call for vote to release Flink Connectors v1.0.0 for Apache Doris(Incubating).

- apache-doris-flink-connector-1.14_2.12-1.0.0-incubating

The release node:
xxxxx

The release candidates:
https://dist.apache.org/repos/dist/dev/incubator/doris/flink-connector/1.0.0/

Maven 2 staging repository:
https://repository.apache.org/content/repositories/orgapachedoris-1002/org/apache/doris/doris-flink-connector/

Git tag for the release:
https://github.com/apache/incubator-doris-flink-connector/tree/1.14_2.12-1.0.0

Keys to verify the Release Candidate:
https://downloads.apache.org/incubator/doris/KEYS

Look at here for how to verify this release candidate:
http://doris.incubator.apache.org/community/release-and-verify/release-verify.html

Vote thread at dev@doris: [1]

The vote will be open for at least 72 hours or until necessary number of votes are reached.

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

[1] vote thread in dev@doris
```

## Completing the release

Please refer to the [Release Completion](./release-complete.md) document to complete the release process.

## Appendix: Releasing to SNAPSHOT

Snapshot is not an Apache Release version and is only used for pre-release previews. Snapshot versions can be released after discussion and approval by the PMC

Switch to the flink connector directory, we will use flink version 1.13.5, scalar 2.12 as an example

```
cd flink-doris-connector
mvn deploy -Dflink.version=1.13.5 -Dscala.version=2.12
```

After that you can see the snapshot version here.

```
https://repository.apache.org/content/repositories/snapshots/org/apache/doris/doris-flink-connector/
```
