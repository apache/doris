---
{
    "title": "发布 Doris 主代码",
    "language": "zh-CN"
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

# 发布 Doris Core

Doris Core 指发布 https://github.com/apache/incubator-doris 中的内容。

## 准备发布

首先，请参阅 [发版准备](./release-prepare.md) 文档进行发版准备。

### 准备分支

发布前需要先新建一个分支。例如：

```
$ git checkout -b branch-0.9
```

这个分支要进行比较充分的测试，使得功能可用，bug收敛，重要bug都得到修复。
这个过程需要等待社区，看看是否有必要的patch需要在这个版本合入，如果有，需要把它 cherry-pick 到发布分支。

### 清理issue

将属于这个版本的所有 issue 都过一遍，关闭已经完成的，如果没法完成的，推迟到更晚的版本。

### 合并必要的Patch

在发布等待过程中，可能会有比较重要的Patch合入，如果社区有人说要有重要的Bug需要合入，那么 Release Manager 需要评估并将重要的Patch合入到发布分支中。

## 验证分支

### 稳定性测试

将打好的分支交给 QA 同学进行稳定性测试。如果在测试过程中，出现需要修复的问题，则如果在测试过程中，出现需要修复的问题，待修复好后，需要将修复问题的 PR 合入到待发版本的分支中。

待整个分支稳定后，才能准备发版本。

### 编译验证

请参阅编译文档进行编译，以确保源码编译正确性。

### 准备 Release Notes

## 社区发布投票流程

### 打 tag

当上述分支已经比较稳定后，就可以在此分支上打 tag。
记得在创建 tag 时，修改 `gensrc/script/gen_build_version.sh` 中的 `build_version` 变量。如 `build_version="0.10.0-release"`

例如：

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

### 打包、签名上传

如下步骤，也需要通过 SecureCRT 等终端直接登录用户账户，不能通过 su - user 或者 ssh 转，否则密码输入 box 会显示不出来而报错。

```
$ git checkout 0.9.0-rc01

$ git archive --format=tar 0.9.0-rc01 --prefix=apache-doris-0.9.0-incubating-src/ | gzip > apache-doris-0.9.0-incubating-src.tar.gz

$ gpg -u xxx@apache.org --armor --output apache-doris-0.9.0-incubating-src.tar.gz.asc --detach-sign apache-doris-0.9.0-incubating-src.tar.gz

$ gpg --verify apache-doris-0.9.0-incubating-src.tar.gz.asc apache-doris-0.9.0-incubating-src.tar.gz

$ sha512sum apache-doris-0.9.0-incubating-src.tar.gz > apache-doris-0.9.0-incubating-src.tar.gz.sha512

$ sha512sum --check apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

然后将打包的内容上传到svn仓库中，首先下载 svn 库：

```
svn co https://dist.apache.org/repos/dist/dev/incubator/doris/
```

将之前得到的全部文件组织成以下svn路径

```
./doris/
|-- 0.11.0-rc1
|   |-- apache-doris-0.11.0-incubating-src.tar.gz
|   |-- apache-doris-0.11.0-incubating-src.tar.gz.asc
|   `-- apache-doris-0.11.0-incubating-src.tar.gz.sha512
`-- KEYS
```

上传这些文件

```
svn add 0.11.0-rc1
svn commit -m "Add 0.11.0-rc1"
```

### 发邮件到社区 dev@doris.apache.org 进行投票

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

To verify and build, you can refer to following link:
http://doris.incubator.apache.org/community/release-and-verify/release-verify.html

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

### 投票通过后，发 Result 邮件

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

### 发邮件到 general@incubator.apache.org 进行投票

**如非孵化器项目，请跳过**

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

邮件的 thread 连接可以在这里找到：

`https://lists.apache.org/list.html?dev@doris.apache.org`

### 发 Result 邮件到 general@incubator.apache.org

**如非孵化器项目，请跳过**

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

## 完成发布

请参阅 [完成发布](./release-complete.md) 文档完成所有发布流程。