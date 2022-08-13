---
{
  "title": "FE 开发环境搭建 - Visual Studio Code (VSCode)", 
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

# 使用 VSCode 搭建 FE 开发环境

有些开发者是在 开发机/WSL/docker 上搭建 FE 开发环境，但是这样的开发环境不能支持本地开发，有些习惯于使用 VSCode 的开发者可以配置远程开发调试

## 环境准备

* JDK11+ (Java 插件需要 JDK11+) (笔者是在 `home` 目录下建立了一个 lib 目录，分别安装了 [JDK11](https://github.com/adoptium/temurin11-binaries/releases/) 和 JDK8 ，分别用于插件和编译)
* VSCode
  + Extension Pack for Java 插件
  + Remote 插件

## 下载代码编译

1. 从 https://github.com/apache/doris.git 下载源码到本地

2. 使用 VSCode 打开代码 `/fe` 目录

## 配置 VSCode

在 `.vscode` 目录下创建 `settings.json` 文件, 分别配置

* `"java.configuration.runtimes"`
* `"java.jdt.ls.java.home"` -- 必须另外配置，指向 JDK11+ 的目录，用于配置 vscode-java 插件
* `"maven.executable.path"` -- 指向 maven 的目录，用于配置 maven-language-server 插件

example:

```json
{
    "java.configuration.runtimes": [
        {
            "name": "JavaSE-1.8",
            "path": "/!!!path!!!/jdk-1.8.0_191"
        },
        {
            "name": "JavaSE-11",
            "path": "/!!!path!!!/jdk-11.0.14.1+1",
            "default": true
        },
    ],
    "java.jdt.ls.java.home": "/!!!path!!!/jdk-11.0.14.1+1",
    "maven.executable.path": "/!!!path!!!/maven/bin/mvn"
}
```

## 编译

其他文章已经介绍的比较清楚了：
* [使用 LDB toolchain 编译](/docs/install/source-install/compilation-with-ldb-toolchain)
* ......

为了进行调试，需要在 fe 启动时，加上调试的参数，比如 `-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005` 。

具体是在 `incubator-doris/output/fe/bin/start_fe.sh` 里 `$JAVA $final_java_opt` 后面加上上面的参数。
