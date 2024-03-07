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

- https://github.com/apache/doris-flink-connector
- https://github.com/apache/doris-spark-connector

## Preparing for release

First, see the [release preparation](./release-prepare.md) documentation to prepare for the release.

## Releasing to Maven

Let's take the release of Spark Connector 1.2.0 as an example.

### 1. Prepare branch and tag

Create a branch in the code base: release-1.2.0, and checkout to this branch.

Modify the version revision in the pom to the version number to be released, which is 1.2.0. And submit this modification, for example:
```
git commit -a -m "Commit for release 1.2.0"
```

Create tag and push
```
git tag 1.2.0
git push origin 1.2.0
```

### 2. Release to Maven staging

Because Spark Connector releases different releases for different Spark versions (such as 2.3, 3.1, 3.2), we need to process each version separately at compile time.

Let's take Spark version 2.3 and scala version 2.11 as examples:
```
mvn clean install \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3
```
>Note: For related parameters, please refer to the compilation command in the build.sh script, and revision is the version number to be released this time.

```
mvn deploy \
-Papache-release \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3 
```

After successful execution, you can find the newly released version in [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories):

![](/images/staging-repositories.png)

**Note that the `.asc` signature file needs to be included. **

If the operation is wrong, you need to drop the staging. Then perform the above steps again.

After checking, click the `close` button in the figure to complete the staging release.

### 3. Prepare svn

Check out the svn repository:

```
svn co https://dist.apache.org/repos/dist/dev/doris/
```

Package tag source code, and generate signature file and sha256 verification file. Here we take `1.14_2.12-1.0.0` as an example. Other tag operations are the same

```
git archive --format=tar release-1.2.0 --prefix=apache-doris-spark-connector-1.2.0-src/ | gzip > apache-doris-spark-connector-1.2.0-src.tar.gz

gpg -u xxx@apache.org --armor --output apache-doris-spark-connector-1.2.0-src.tar.gz.asc --detach-sign apache-doris-spark-connector-1.2.0- src.tar.gz
sha512sum apache-doris-spark-connector-1.2.0-src.tar.gz > apache-doris-spark-connector-1.2.0-src.tar.gz.sha512

Mac:
shasum -a 512 apache-doris-spark-connector-1.2.0-src.tar.gz > apache-doris-spark-connector-1.2.0-src.tar.gz.sha512
```

You end up with three files:

```
apache-doris-spark-connector-1.2.0-src.tar.gz
apache-doris-spark-connector-1.2.0-src.tar.gz.asc
apache-doris-spark-connector-1.2.0-src.tar.gz.sha512
```

Move these three files to the svn directory:

```
doris/spark-connector/1.2.0/
```

The final svn directory structure is similar to:

```
|____0.15
| |____0.15.0-rc04
| | |____apache-doris-0.15.0-incubating-src.tar.gz.sha512
| | |____apache-doris-0.15.0-incubating-src.tar.gz.asc
| | |____apache-doris-0.15.0-incubating-src.tar.gz
|____KEYS
|____spark-connector
| |____1.2.0
| | |____apache-doris-spark-connector-1.2.0-src.tar.gz
| | |____apache-doris-spark-connector-1.2.0-src.tar.gz.asc
| | |____apache-doris-spark-connector-1.2.0-src.tar.gz.sha512
```

Among them, 0.15 is the directory of Doris main code, and `spark-connector/1.2.0` is the content of this release.

Note, for the preparation of the KEYS file, please refer to the introduction in [Release Preparation](./release-prepare.md).

### 4. Voting

Initiate a vote in the dev@doris mail group, the template is as follows:

```
Hi all,

This is a call for the vote to release Apache Doris Spark Connector 1.2.0

The git tag for the release:
https://github.com/apache/doris-spark-connector/releases/tag/1.2.0

Release Notes are here:
https://github.com/apache/doris-spark-connector/issues/109

Thanks to everyone who has contributed to this release.

The release candidates:
https://dist.apache.org/repos/dist/dev/doris/spark-connector/1.2.0/

Maven 2 staging repository:
https://repository.apache.org/content/repositories/orgapachedoris-1031


KEYS file is available here:
https://downloads.apache.org/doris/KEYS

To verify and build, you can refer to the following link:
https://doris.apache.org/community/release-and-verify/release-verify

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...
```

## Finish publishing

Please refer to the [Complete Release](./release-complete.md) document to complete the entire release process.

## APPENDIX: Releasing TO SNAPSHOT

Snapshot is not an Apache Release version, it is only used for preview before release. After being discussed and approved by the PMC, the Snapshot version can be released

Switch to the spark connector directory, we take spark version 2.3, scala 2.11 as an example


```
cd spark-doris-connector
mvn deploy \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3 \
```

Afterwards you can see the snapshot version here:

```
https://repository.apache.org/content/repositories/snapshots/org/apache/doris/doris-spark-connector/
```