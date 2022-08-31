---
{
    "title": "Doris Docker 快速搭建开发环境",
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

# Doris Docker 快速搭建开发环境

## 相关详细文档导航

- [使用 Docker 开发镜像编译](../../docs/install/source-install/compilation.md)
- [部署](../../docs/install/install-deploy.md)
- [VSCode Be 开发调试](./be-vscode-dev.md)

## 环境准备

- 安装 Docker
- VSCode
    - Remote 插件

## 运行镜像

```bash
$ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apache/incubator-doris:build-env-ldb-toolchain-latest
```

此处按需注意 [挂载的问题](../../docs/install/source-install/compilation.md)

> 见链接中：建议以挂载本地 Doris 源码目录的方式运行镜像 .....
由于如果是使用 windows 开发，挂载会存在跨文件系统访问的问题，请自行斟酌设置

创建目录并下载 doris

```bash
su <your user>
mkdir code && cd code
git clone https://github.com/apache/doris.git
```

## 编译

```bash
sh build.sh
```

## 运行

手动创建 `meta_dir` 元数据存放位置, 默认值为 `${DORIS_HOME}/doris-meta`

```bash
mkdir meta_dir
```

启动FE

```bash
cd output/fe
sh bin/start_fe.sh --daemon
```

启动BE

```bash
cd output/be
sh bin/start_be.sh --daemon
```

mysql-client 连接

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```
