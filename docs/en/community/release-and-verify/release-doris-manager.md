---
{
"title": "Release Doris Manager",
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

# Publish Doris Manager

Its codebase is separate from the main Doris codebase at:

- https://github.com/apache/incubator-doris-manager

## ready to publish

First, refer to the [release preparation](./release-prepare.md) documentation for release preparation.

## prepare branch

Before publishing, you need to create a new branch. E.g:

````text
$ git checkout -b branch-1.0.0
````

This branch needs to be fully tested, so that functions are available, bugs are closed, and important bugs are fixed. This process requires waiting for the community to see if the necessary patch needs to be merged in this release, and if so, cherry-pick it to the release branch.

## clean up issues

Go through all issues that belong to this version, close the ones that have been completed, and postpone them to a later version if they cannot be completed

## Merge necessary patches

During the release waiting process, there may be more important patches merged. If someone in the community says that there are important bugs that need to be merged, then the Release Manager needs to evaluate and merge important patches into the release branch

## Verify branch

### Stability Test

Give the prepared branch to QA students for stability testing. If there is a problem that needs to be fixed during the testing process, if there is a problem that needs to be fixed during the testing process, after the repair is completed, the PR that fixes the problem needs to be merged into the branch of the to-be-released version.

After the entire branch is stable, the release version can be prepared.

### Compile and verify

Please refer to the compilation documentation for compilation to ensure that the source code is compiled correctly.

## Community release voting process

### tag

When the above branch is relatively stable, you can tag this branch.

E.g:

````text
$ git checkout branch-1.0.0
$ git tag -a 1.0.0-rc01 -m "doris manager 1.0.0 release candidate 01"
$ git push origin 1.0.0-rc01
Counting objects: 1, done.
Writing objects: 100% (1/1), 165 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@github.com:apache/incubator-doris-manager.git
 * [new tag] 1.0.0-rc01 -> 1.0.0-rc01

$ git tag
````

### Package, sign and upload

In the following steps, you also need to log in to the user account directly through a terminal such as SecureCRT. You cannot switch it through su - user or ssh, otherwise the password input box will not be displayed and an error will be reported.

````
git archive --format=tar 1.0.0-rc01 --prefix=apache-doris-incubating-manager-src-1.0.0-rc01/ | gzip > apache-doris-incubating-manager-src-1.0.0-rc01 .tar.gz

gpg -u xxx@apache.org --armor --output apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.asc --detach-sign apache-doris-incubating-manager-src- 1.0.0-rc01.tar.gz

gpg --verify apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.asc apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz

sha512sum apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz > apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.sha512

sha512sum --check apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.sha512
````

Then upload the packaged content to the svn repository, first download the svn library:

````
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
````

Organize all the files obtained before into the following svn paths

````
./doris/
├── doris-manager
│ └── 1.0.0
│ ├── apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz
│ ├── apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.asc
│ └── apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz.sha512
````

upload these files

````text
svn add 1.0.0-rc01
svn commit -m "Add doris manager 1.0.0-rc01"
````

### Email the community dev@doris.apache.org to vote

[VOTE] Release Apache Doris Manager 1.0.0-incubating-rc01

````
Hi All,

This is a call for vote to release Doris Manager v1.0.0 for Apache Doris(Incubating).

- apache-doris-incubating-manager-src-1.0.0-rc01

The release node:



The release candidates:
https://dist.apache.org/repos/dist/dev/incubator/doris/doris-manager/1.0.0/

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


Brs,
xxxx
------------------
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

````

### After voting, send Result email

[Result][VOTE] Release Apache Doris Manager 1.0.0-incubating-rc01

````text
Thanks to everyone, and this vote is now closed.

It has passed with 4 +1 (binding) votes and no 0 or -1 votes.

Binding:
+1 jiafeng Zhang
+1 xxx
+1 EmmyMiao87
+1 Mingyu Chen

Best Regards,
xxx
````

After passing the dev mailing group, send an email to the general@incubator mailing group for IPMC voting.

````text
Hi all,

Please review and vote on Apache Doris Manager 1.0.0-incubating-rc01 release.

Doris manager is a platform for automatic installation, deployment and management of Doris groups

The Apache Doris community has voted on and approved this release:
https://lists.apache.org/thread.html/d70f7c8a8ae448bf6680a15914646005c6483564464cfa15f4ddc2fc@%3Cdev.doris.apache.org%3E

The vote result email thread:
https://lists.apache.org/thread.html/64d229f0ba15d66adc83306bc8d7b7ccd5910ecb7e842718ce6a61da@%3Cdev.doris.apache.org%3E

The release candidate has been tagged in GitHub as 1.0.0-rc01, available here:
https://github.com/apache/incubator-doris-manager/releases/tag/1.0.0-rc01

There is no CHANGE LOG file because this is the first release of Apache Doris.
Thanks to everyone who has contributed to this release, and there is a simple release notes can be found here:
https://github.com/apache/incubator-doris/issues/406

The artifacts (source, signature and checksum) corresponding to this release candidate can be found here:
https://dist.apache.org/repos/dist/dev/incubator/doris/doris-manager/1.0.0/

This has been signed with PGP key 33DBF2E0, corresponding to lide@apache.org.
KEYS file is available here:
https://downloads.apache.org/incubator/doris/KEYS
It is also listed here:
https://people.apache.org/keys/committer/lide.asc

The vote will be open for at least 72 hours.
[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because ...

To verify and build, you can refer to the following instruction:

Firstly, you must be install and start docker service, and then you could build Doris as following steps:

$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/doris-manager/1.0.0/apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz

Step4: Build Doris
Now you can decompress and enter Doris source path and build Doris.
$ tar zxvf apache-doris-incubating-manager-src-1.0.0-rc01.tar.gz
$ cd apache-doris-incubating-manager-src-1.0.0-rc01
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
````

The thread link for the mail can be found here:

````
https://lists.apache.org/list.html?dev@doris.apache.org
````

### Send Result email to general@incubator.apache.org

[RESULT][VOTE] Release Apache Doris Manager 1.0.0-incubating-rc01

````text
Hi,

Thanks to everyone, and the vote for releasing Apache Doris Manager 1.0.0-incubating-rc01 is now closed.

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
````

## Finish publishing

Please refer to the [Completing the Release](https://doris.apache.org/en-US/community/release-and-verify/release-complete.html) documentation to complete all release processes.
