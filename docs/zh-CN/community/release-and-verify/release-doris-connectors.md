---
{
"title": "发布 Doris Connectors",
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

# 发布 Doris Connectors

Doris Connectors 目前包含：

* Doris Flink Connector
* Doris Spark Connector。

其代码库独立于 Doris 主代码库，分别位于：

- https://github.com/apache/doris-flink-connector
- https://github.com/apache/doris-spark-connector

## 准备发布

首先，请参阅 [发版准备](./release-prepare.md) 文档进行发版准备。

## 发布到 Maven

我们以发布 Spark Connector 1.2.0 为例。

### 1. 准备分支和tag

在代码库中创建分支：release-1.2.0，并 checkout 到该分支。

修改pom中的版本revision为要发布的版本号，即1.2.0。并且提交本次修改，例如：
```
git commit -a -m "Commit for release 1.2.0"
```

创建tag并推送
```
git tag 1.2.0
git push origin 1.2.0
```

### 2. 发布到 Maven staging

因为 Spark Connector 针对不同 Spark 版本（如 2.3, 3.1, 3.2）发布不同的 Release, 因此我们需要针对每一个版本单独在编译时进行处理。

下面我们以 Spark 版本 2.3，scala 版本 2.11 为例说明：
```
mvn clean install \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3
```
>注意：相关参数可以参考build.sh脚本中的编译命令，revision为本次要发布的版本号。

```
mvn deploy \
-Papache-release \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3
```

执行成功后，在 [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories) 里面可以找到刚刚发布的版本：

![](/images/staging-repositories.png)

**注意需要包含 `.asc` 签名文件。**

如果操作有误，需要将 staging drop 掉。然后重新执行上述步骤。

检查完毕后，点击图中的 `close` 按钮完成 staging 发布。

### 3. 准备 svn

检出 svn 仓库：

```
svn co https://dist.apache.org/repos/dist/dev/doris/
```

打包 tag 源码，并生成签名文件和sha256校验文件。这里我们以 `1.14_2.12-1.0.0` 为例。其他 tag 操作相同

```
git archive --format=tar release-1.2.0 --prefix=apache-doris-spark-connector-1.2.0-src/ | gzip > apache-doris-spark-connector-1.2.0-src.tar.gz

gpg -u xxx@apache.org --armor --output apache-doris-spark-connector-1.2.0-src.tar.gz.asc  --detach-sign apache-doris-spark-connector-1.2.0-src.tar.gz
sha512sum apache-doris-spark-connector-1.2.0-src.tar.gz > apache-doris-spark-connector-1.2.0-src.tar.gz.sha512

Mac:
shasum -a 512 apache-doris-spark-connector-1.2.0-src.tar.gz > apache-doris-spark-connector-1.2.0-src.tar.gz.sha512
```

最终得到三个文件：

```
apache-doris-spark-connector-1.2.0-src.tar.gz
apache-doris-spark-connector-1.2.0-src.tar.gz.asc
apache-doris-spark-connector-1.2.0-src.tar.gz.sha512
```

将这三个文件移动到 svn 目录下：

```
doris/spark-connector/1.2.0/
```

最终 svn 目录结构类似：

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

其中 0.15 是 Doris 主代码的目录，而 `spark-connector/1.2.0` 下就是本次发布的内容了。

注意，KEYS 文件的准备，可参阅 [发版准备](./release-prepare.md) 中的介绍。

### 4. 投票

在 dev@doris 邮件组发起投票，模板如下：

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

To verify and build, you can refer to following link:
https://doris.apache.org/community/release-and-verify/release-verify

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release this package because …
```

## 完成发布

请参阅 [完成发布](./release-complete.md) 文档完成所有发布流程。

## 附录：发布到 SNAPSHOT

Snapshot 并非 Apache Release 版本，仅用于发版前的预览。在经过 PMC 讨论通过后，可以发布 Snapshot 版本

切换到 spark connector 目录， 我们以 spark 版本 2.3，scala 2.11 为例


```
cd spark-doris-connector
mvn deploy \
-Dspark.version=2.3.0 \
-Dscala.version=2.11 \
-Dspark.major.version=2.3 \
```

之后你可以在这里看到 snapshot 版本：

```
https://repository.apache.org/content/repositories/snapshots/org/apache/doris/doris-spark-connector/
```
