---
{
    "title": "Verify Apache Release",
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

# Verify Apache Release

To verify the release, following checklist can used to reference:

1. [ ] Download links are valid.
2. [ ] Checksums and PGP signatures are valid.
3. [ ] DISCLAIMER or DISCLAIMER-WIP is included.
4. [ ] Source code artifacts have correct names matching the current release.
5. [ ] LICENSE and NOTICE files are correct for the repository.
6. [ ] All files have license headers if necessary.
7. [ ] No compiled archives bundled in source archive.
8. [ ] Building is OK.

## 1. Download source package, signature file, hash file and KEYS

Download all artifacts, take a.b.c-incubating as an example:

``` shell
wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.sha512

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.asc

wget https://www.apache.org/dist/incubator/doris/KEYS
```

## 2. Verify signature and hash

GnuPG is recommended, which can install by yum install gnupg or apt-get install gnupg.

Here we use Doris main code release as an example. Other releases are similar.

``` shell
gpg --import KEYS
gpg --verify apache-doris-a.b.c-incubating-src.tar.gz.asc apache-doris-a.b.c-incubating-src.tar.gz
sha512sum --check apache-doris-a.b.c-incubating-src.tar.gz.sha512
```

## 3. Verify license header

Here we use [apache/skywalking-eyes](https://github.com/apache/skywalking-eyes) for source license header validation.

Go to the source directory and execute the following command (requires a Docker environment).

```
docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
```

The output is similar to the following:

```
INFO GITHUB_TOKEN is not set, license-eye won't comment on the pull request
INFO Loading configuration from file: .licenserc.yaml
INFO Totally checked 5611 files, valid: 3926, invalid: 0, ignored: 1685, fixed: 0
```

where an invalid of 0 means the check passed.

> Some non-Apache License header files are documented in `.licenserc.yaml`.

## 4. Verify building

* For Doris main code compilation, see [compilation documentation](../installing/compilation.html)
* Flink Doris Connector compilation, see [compilation documentation](../extending-doris/flink-doris-connector.md)
* Spark Doris Connector compilation, see [compilation documentation](../extending-doris/spark-doris-connector.md)
