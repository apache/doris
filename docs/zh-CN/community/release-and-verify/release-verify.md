---
{
    "title": "验证 Apache 发布版本",
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

# 验证 Apache 发布版本

该验证步骤可用于发版投票时的验证，也可以用于对已发布版本的验证。

可以按照以下步骤进行验证：

1. [ ] 下载链接合法。
2. [ ] 校验值和 PGP 签名合法。
3. [ ] 代码和当前发布版本相匹配。
4. [ ] LICENSE 和 NOTICE 文件正确。
5. [ ] 所有文件都携带必要的协议说明。
6. [ ] 在源码包中不包含已经编译好的内容。
7. [ ] 编译能够顺利执行。

这里我们以 Doris Core 版本的验证为例。其他组件注意修改对应名称。

## 1. 下载源码包、签名文件、校验值文件和 KEYS

下载所有相关文件, 以 a.b.c-incubating 为示例:

``` shell
wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.sha512

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.asc

wget https://downloads.apache.org/incubator/doris/KEYS
```

> 如果是投票验证，则需从邮件中提供的 svn 地址获取相关文件。

## 2. 检查签名和校验值

推荐使用 GunPG，可以通过以下命令安装：

``` shell
CentOS: yum install gnupg
Ubuntu: apt-get install gnupg
```

这里以 Doris 主代码 release 为例。其他 release 类似。

``` shell
gpg --import KEYS
gpg --verify apache-doris-a.b.c-incubating-src.tar.gz.asc apache-doris-a.b.c-incubating-src.tar.gz
sha512sum --check apache-doris-a.b.c-incubating-src.tar.gz.sha512
```
> 注意： gpg --import 如果报错 **no valid user IDs**, 此时可能是gpg版本不匹配，可升级版本至2.2.x或以上


## 3. 验证源码协议头

这里我们使用 [skywalking-eyes](https://github.com/apache/skywalking-eyes) 进行协议验证。

进入源码根目录并执行：

```
sudo docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
```

运行结果如下：

```
INFO GITHUB_TOKEN is not set, license-eye won't comment on the pull request
INFO Loading configuraftion from file: .licenserc.yaml
INFO Totally checked 5611 files, valid: 3926, invalid: 0, ignored: 1685, fixed: 0
```

如果 invalid 为 0，则表示验证通过。

## 4. 验证编译

请参阅各组件的编译文档验证编译。

* Doris 主代码编译，请参阅 [编译文档](/docs/install/source-install/compilation)
* Flink Doris Connector 编译，请参阅 [编译文档](/docs/ecosystem/flink-doris-connector)
* Spark Doris Connector 编译，请参阅 [编译文档](/docs/ecosystem/spark-doris-connector)
