---
{
    "title": "Verify the Apache release version",
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

# Validate Apache Releases

This validation step can be used for validation during release polling and also for validation of released versions.

The following steps can be followed to verify.

1. [ ] The download link is legal.
2. [ ] The PGP signature are valid.
3. [ ] The source code matches the current release version.
4. [ ] The LICENSE and NOTICE files are correct.
5. [ ] All files carry the necessary protocol header.
6. [ ] The compiled content is not included in the source package.
7. [ ] The compilation can be executed smoothly.

Here we use the verification of the Doris Core version as an example. Note that other components have their corresponding names changed.

## 1. download the source package, signature file, checksum file and KEYS

Download all relevant files, using a.b.c-incubating as an example:

``` shell
wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.sha512

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.asc

wget https://downloads.apache.org/incubator/doris/KEYS
```

> In case of poll verification, you need to get the relevant files from the svn address provided in the email.

## 2. Check signature and checksum value

It is recommended to use GunPG, which can be installed by the following command.

``` shell
CentOS: yum install gnupg
Ubuntu: apt-get install gnupg
```

``` shell
gpg --import KEYS
gpg --verify apache-doris-a.b.c-incubating-src.tar.gz.asc apache-doris-a.b.c-incubating-src.tar.gz
sha512sum --check apache-doris-a.b.c-incubating-src.tar.gz.sha512
```
> Note: If gpg --import reports **no valid user IDs**, it may be that the gpg version does not match. You can upgrade the version to 2.2.x or above

## 3. Verify the source protocol header

Here we use [skywalking-eyes](https://github.com/apache/skywalking-eyes) for protocol validation.

Go to the root of the source code and execute:

```
sudo docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
```

The results of the run are as follows.

```
INFO GITHUB_TOKEN is not set, license-eye won't comment on the pull request
INFO Loading configuration from file: .licenserc.yaml
INFO Totally checked 5611 files, valid: 3926, invalid: 0, ignored: 1685, fixed: 0
```

If invalid is 0, then the validation passes.

## 4. Verify compilation

Please see the compilation documentation of each component to verify the compilation.

* For Doris Core, see [compilation documentation](/docs/install/source-install/compilation)
* Flink Doris Connector, see [compilation documentation](/docs/ecosystem/flink-doris-connector)
* Spark Doris Connector, see [compilation documentation](/docs/ecosystem/spark-doris-connector)

## 5. Vote
See the [ASF voting process]((https://www.apache.org/foundation/voting.html)) page for general information about voting.

After the verification is completed, the following template can be used to send voting emails to the dev@doris:

```
+1 (binding) or +1 (non-binding)

I checked:

[x] The download link is legal.
[x] The PGP signature is valid.
[x] The source code matches the current release version.
[x] The LICENSE and NOTICE files are correct.
[x] All files carry the necessary protocol header.
[x] The compiled content is not included in the source package.
[x] The compilation can be executed smoothly.

Other comments...
```

For a release vote to pass, a minimum of three positive binding votes and more positive binding votes than negative binding votes must be cast. Releases may not be vetoed. Votes cast by PMC members are binding, however, non-binding votes are greatly encouraged and a sign of a healthy project.

Votes on whether a package is ready to release use majority approval -- at least three PMC members must vote affirmatively for release, and there must be more positive than negative votes. Releases may not be vetoed. For the sake of easy auditing, PMC member votes are typically marked "binding." However, the Release Manager needs to verify the validity of the votes. This can be done by checking whether the email addresses on the PMC roster match the email addresses of the voters.

Generally the community will cancel the release vote if anyone identifies serious problems, but in most cases the ultimate decision lies with the individual serving as release manager. The specifics of the process may vary from project to project, but the 'minimum quorum of three +1 votes' rule is universal.

**Note that there is no implicit +1 from the release manager, or from anyone in any ASF vote. Only explicit votes are valid.** The release manager is encouraged to vote on releases, like any reviewer would do.
