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

可以按照以下步骤对发布版本进行验证：

1. [ ] 下载链接是否合法。
2. [ ] 校验值和 PGP 签名是否合法。
3. [ ] 是否包含 DISCLAIMER-WIP。
4. [ ] 代码是否和当前发布版本相匹配。
5. [ ] LICENSE 和 NOTICE 文件是否正确。
6. [ ] 所有文件都携带必要的协议说明。
7. [ ] 在源码包中不包含已经编译好的内容。
8. [ ] 编译是否能够顺利执行。

## 1. 下载源码包、签名文件、校验值文件和 KEYS

下载所有相关文件, 以 a.b.c-incubating 为示例:

``` shell
wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.sha512

wget https://www.apache.org/dist/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.asc

wget https://www.apache.org/dist/incubator/doris/KEYS
```

## 2. 检查签名和校验值

推荐使用 GunPG，可以通过以下命令安装：

``` shell
CentOS: yum install gnupg
Ubuntu: apt-get install gnupg
```

``` shell
gpg --import KEYS
gpg --verify apache-doris-a.b.c-incubating-src.tar.gz.asc apache-doris-a.b.c-incubating-src.tar.gz
sha512sum --check apache-doris-a.b.c-incubating-src.tar.gz.sha512
```

## 3. 验证源码协议头

推荐使用 Apache RAT 验证源码协议，可以从以下链接下载：

``` shell
wget http://mirrors.tuna.tsinghua.edu.cn/apache/creadur/apache-rat-0.13/apache-rat-0.13-bin.tar.gz
tar zxvf apache -rat -0.13 -bin.tar.gz
```

假设源码目录名称为 apache-doris-a.b.c-incubating-src，可以使用以下命令进行验证。
这个命令会产生一个文件，其中列举了所有非 ASF 协议的文件。

``` shell
/usr/java/jdk/bin/java  -jar apache-rat-0.13/apache-rat-0.13.jar -a -d apache-doris-a.b.c-incubating-src -E apache-doris-a.b.c-incubating-src/.rat-excludes 
```

## 4. 验证编译

详细的编译步骤，请参阅 [编译文档](../installing/compilation.html)
