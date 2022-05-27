---
{
    "title": "完成发布",
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

# 完成发布

本文档中的步骤，是在完成 dev@doris 或 general@incubator 邮件组中的发版投票并通过后，进行的后续步骤。

## 上传 package 到 release

当正式发布投票成功后，先发[Result]邮件，然后就准备 release package。
将之前在dev下发布的对应文件夹下的源码包、签名文件和hash文件拷贝到另一个目录 0.9.0-incubating，注意文件名字中不要rcxx (可以rename，但不要重新计算签名，hash可以重新计算，结果不会变)

```
From:
https://dist.apache.org/repos/dist/dev/incubator/doris/

To:
https://dist.apache.org/repos/dist/release/incubator/doris/
```

第一次发布的话 KEYS 文件也需要拷贝过来。然后add到svn release 下。

```
add 成功后就可以在下面网址上看到你发布的文件
https://dist.apache.org/repos/dist/release/incubator/doris/0.xx.0-incubating/

稍等一段时间后，能在 apache 官网看到：
http://www.apache.org/dist/incubator/doris/0.9.0-incubating/
```

## 在 Doris 官网和 github 发布链接

我们以 Doris Core 为例。其他组件注意替换对应的名称。

### 创建下载链接

下载链接：

```
http://www.apache.org/dyn/closer.cgi?filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz&action=download

wget --trust-server-names "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz"
```

原始位置:

```
https://www.apache.org/dist/incubator/doris/0.9.0-incubating/

http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz
```

源码包：

```
http://www.apache.org/dyn/closer.cgi/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz

ASC:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.asc

sha512:
http://archive.apache.org/dist/incubator/doris/0.9.0-incubating/apache-doris-0.9.0-incubating-src.tar.gz.sha512
```

KEYS:
```
http://archive.apache.org/dist/incubator/doris/KEYS
```

refer to: <http://www.apache.org/dev/release-download-pages#closer>

### Maven

在 [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories) 中找到对应的 Staging Repo, 点击 `Release` 进行正式发布。

### 准备 release note

需要修改如下两个地方：

1、Github 的 release 页面

```
https://github.com/apache/incubator-doris/releases/tag/0.9.0-rc01
```

2、Doris 官网下载页面

下载页面是一个 markdown 文件，地址如下。
```
docs/zh-CN/downloads/downloads.md
docs/en/downloads/downloads.md
```

1. 需要将上一次发布版本的下载包地址改为 apache 的归档地址（见后）。
2. 增加新版本的下载信息。

### svn 上清理旧版本的包

1. svn 上删除旧版本的包

由于 svn 只需要保存最新版本的包，所以当有新版本发布的时候，旧版本的包就应该从 svn 上清理。

```
https://dist.apache.org/repos/dist/release/incubator/doris/
https://dist.apache.org/repos/dist/dev/incubator/doris/
```
保持这两个地址中，只有最新版本的包即可。

2. 将 Doris 官网的下载页面中，旧版本包的下载地址改为归档页面的地址 

```
下载页面: http://doris.apache.org/downloads.html
归档页面: http://archive.apache.org/dist/incubator/doris
```

Apache 会有同步机制去将历史的发布版本进行一个归档，具体操作见：[how to archive](https://www.apache.org/legal/release-policy.html#how-to-archive)
所以即使旧的包从 svn 上清除，还是可以在归档页面中找到。

## Announce 邮件

Title:

```
[ANNOUNCE] Apache Doris (incubating) 0.9.0 Release
```

发送邮件组：

```
dev@doris.apache.org
```

孵化器项目，还需发送到：

```
general@incubator.apache.org
```

邮件正文：

```
Hi All,

We are pleased to announce the release of Apache Doris 0.9.0-incubating.

Apache Doris (incubating) is an MPP-based interactive SQL data warehousing for reporting and analysis.

The release is available at:
http://doris.apache.org/master/zh-CN/downloads/downloads.html

Thanks to everyone who has contributed to this release, and the release note can be found here:
https://github.com/apache/incubator-doris/releases

Best Regards,

On behalf of the Doris team,
xxx

---
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



