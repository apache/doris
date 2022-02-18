---
{
    "title": "Release Doris Core",
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

# Release Doris Core

Doris Core refers to the content published in https://github.com/apache/incubator-doris.

## Preparing for release

First, see the [release preparation](./release-prepare.md) documentation for release preparation.

### Preparing a branch

You need to create a new branch before releasing. For example.

```
$ git checkout -b branch-0.9
```

This branch should be more fully tested to make features available, bugs converged, and important bugs fixed.
This process requires waiting for the community to see if there are any necessary patches that need to be merged in for this release, and if so, cherry-picking it to the release branch.

### Clean up issues

Go through all the issues that belong to this release, close the ones that are done, and if they can't be done, defer them to a later release.

### Merge necessary patches

If someone in the community says there are important bugs that need to be merged in, then the Release Manager needs to evaluate and merge the important patches into the release branch.

## Validation branch

### Stability testing

Pass the batched branch to the QA students for stability testing. If during the testing process, there are issues that need to be fixed, then if during the testing process, there are issues that need to be fixed, then after they are fixed, the PRs that fix the issues need to be merged into the branch of the pending release.

Only after the whole branch is stable, can you prepare to release the version.

### Compile verification

Please refer to the compilation documentation for compilation to ensure that the source code is compiled correctly.

### Prepare Release Nodes

## Community Posting Voting Process

### Tagging

Once the above branch is more stable, you can tag this branch.
Remember to modify the `build_version` variable in `gensrc/script/gen_build_version.sh` when creating the tag. For example `build_version="0.10.0-release"`

Example:

```
$ git checkout branch-0.9
$ git tag -a 0.9.0-rc01 -m "0.9.0 release candidate 01"
$ git push origin 0.9.0-rc01
Counting objects: 1, done.
Writing objects: 100% (1/1), 165 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@github.com:apache/incubator-doris.git
 * [new tag]         0.9.0-rc01 -> 0.9.0-rc01

$ git tag
```

### Package, sign and upload

For the following steps, you also need to log in to the user account directly through a terminal such as SecureCRT, not through `su - user` or `ssh`, otherwise the password input box will not be displayed and an error will be reported.

```
$ git checkout 0.9.0-rc01

$ git archive --format=tar 0.9.0-rc01 --prefix=apache-doris-0.9.0-incubating-src/ | gzip > apache-doris-0.9.0-incubating-src.tar.gz

$ gpg -u xxx@apache.org --armor --output apache-doris-0.9.0-incubating-src.tar.gz.asc --detach-sign apache-doris-0.9.0-incubating-src.tar.gz

$ gpg --verify apache-doris-0.9.0-incubating-src.tar.gz.asc apache-doris-0.9.0-incubating-src.tar.gz

$ sha512sum apache-doris-0.9.0-incubating-src.tar.gz > apache-doris-0.9.0-incubating-src.tar.gz.sha512

$ sha512sum --check apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

Then upload the packaged content to the svn repository by first downloading the svn library at:

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
```

Organize all the previously obtained files into the following svn path:

```
./doris/
|-- 0.11.0-rc1
|   |-- apache-doris-0.11.0-incubating-src.tar.gz
|   |-- apache-doris-0.11.0-incubating-src.tar.gz.asc
|   `-- apache-doris-0.11.0-incubating-src.tar.gz.sha512
`-- KEYS
```

Upload these file:

```
svn add 0.11.0-rc1
svn commit -m "Add 0.11.0-rc1"
```

### Email the community at dev@doris.apache.org to vote

[VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

The release candidate has been tagged in GitHub as 0.9.0-rc01, available
here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

Release Notes are here:
https://github.com/apache/incubator-doris/issues/1891

Thanks to everyone who has contributed to this release.

The artifacts (source, signature and checksum) corresponding to this release
candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc1/

This has been signed with PGP key 33DBF2E0, corresponding to
lide@apache.org.
KEYS file is available here:
https://downloads.apache.org/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

To verify and build, you can refer to following wiki:
https://github.com/apache/incubator-doris/wiki/How-to-verify-Apache-Release
https://wiki.apache.org/incubator/IncubatorReleaseChecklist

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

Best Regards,
xxx

----
DISCLAIMER: 
Apache Doris (incubating) is an effort undergoing incubation at The
Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted
projects until a further review indicates that the
infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF
projects.

While incubation status is not necessarily a reflection
of the completeness or stability of the code, it does indicate
that the project has yet to be fully endorsed by the ASF.
```

### After the vote is approved, send the Result email

[Result][VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Thanks to everyone, and this vote is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 Zhao Chun
+1 xxx
+1 Li Chaoyong
+1 Mingyu Chen

Best Regards,
xxx

```

### Email general@incubator.apache.org to vote

**If not an incubator program, please skip**

[VOTE] Release Apache Doris 0.9.0-incubating-rc01

```
Hi all,

Please review and vote on Apache Doris 0.9.0-incubating-rc01 release.

Apache Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.

The Apache Doris community has voted on and approved this release:
https://lists.apache.org/thread.html/d70f7c8a8ae448bf6680a15914646005c6483564464cfa15f4ddc2fc@%3Cdev.doris.apache.org%3E

The vote result email thread:
https://lists.apache.org/thread.html/64d229f0ba15d66adc83306bc8d7b7ccd5910ecb7e842718ce6a61da@%3Cdev.doris.apache.org%3E

The release candidate has been tagged in GitHub as 0.9.0-rc01, available here:
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01

There is no CHANGE LOG file because this is the first release of Apache Doris.
Thanks to everyone who has contributed to this release, and there is a simple release notes can be found here:
https://github.com/apache/incubator-doris/issues/406

The artifacts (source, signature and checksum) corresponding to this release candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/

This has been signed with PGP key 33DBF2E0, corresponding to lide@apache.org.
KEYS file is available here:
https://downloads.apache.org/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

To verify and build, you can refer to following instruction:

Firstly, you must be install and start docker service, and then you could build Doris as following steps:

Step1: Pull the docker image with Doris building environment
$ docker pull apache/incubator-doris:build-env-1.3.1
You can check it by listing images, its size is about 3.28GB.

Step2: Run the Docker image
You can run image directly:
$ docker run -it apache/incubator-doris:build-env-1.3.1

Step3: Download Doris source
Now you should in docker environment, and you can download Doris source package.
(If you have downloaded source and it is not in image, you can map its path to image in Step2.)
$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/0.9/0.9.0-rc01/apache-doris-0.9.0.rc01-incubating-src.tar.gz

Step4: Build Doris
Now you can decompress and enter Doris source path and build Doris.
$ tar zxvf apache-doris-0.9.0.rc01-incubating-src.tar.gz
$ cd apache-doris-0.9.0.rc01-incubating-src
$ sh build.sh

Best Regards,
xxx

----
DISCLAIMER: 
Apache Doris (incubating) is an effort undergoing incubation at The
Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted
projects until a further review indicates that the
infrastructure, communications, and decision making process have
stabilized in a manner consistent with other successful ASF
projects.

While incubation status is not necessarily a reflection
of the completeness or stability of the code, it does indicate
that the project has yet to be fully endorsed by the ASF.
```

The thread link for the email can be found here.

`https://lists.apache.org/list.html?dev@doris.apache.org`

### Send Result email to general@incubator.apache.org

**If not an incubator project, please skip**

[RESULT][VOTE] Release Apache Doris 0.9.0-incubating-rc01


```
Hi,

Thanks to everyone, and the vote for releasing Apache Doris 0.9.0-incubating-rc01 is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 Willem Jiang
+1 Justin Mclean
+1 ShaoFeng Shi
+1 Makoto Yui

The vote thread:
https://lists.apache.org/thread.html/da05fdd8d84e35de527f27200b5690d7811a1e97d419d1ea66562130@%3Cgeneral.incubator.apache.org%3E

Best Regards,
xxx
```

## Completing the release

Please refer to the [Release Completion](./release-complete.md) document to complete the release process.