---
{
    "title": "在 MacOS 平台上编译",
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

# 在 MacOS 平台上编译

本文介绍如何在 macOS 平台上编译源码。

## 环境要求

1. macOS 12 (Monterey) 及以上（_**Intel和Apple Silicon均支持**_）
2. [Homebrew](https://brew.sh/)

## 编译步骤

1. 使用[Homebrew](https://brew.sh/)安装依赖
    ```shell
    brew install automake autoconf libtool pkg-config texinfo coreutils gnu-getopt \
        python@3 cmake ninja ccache bison byacc gettext wget pcre maven llvm@16 openjdk@11 npm
    ```

:::tip
使用 brew 安装的 jdk 版本为 11，因为在 macOS上，arm64 版本的 brew 默认没有 8 版本的 jdk
:::

2. 编译源码
    ```shell
    bash build.sh
    ```

## 第三方库

1. [Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation)页面有所有第三方库的源码，可以直接下载[doris-thirdparty-source.tgz](https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-source.tgz)获得。

2. 可以在[Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation)页面直接下载预编译好的第三方库，省去编译第三方库的过程，参考下面的命令。
    ```shell
    cd thirdparty
    rm -rf installed

    # Intel 芯片
    curl -L https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-x86_64.tar.xz \
        -o - | tar -Jxf -

    # Apple Silicon 芯片
    curl -L https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-arm64.tar.xz \
        -o - | tar -Jxf -

    # 保证protoc和thrift能够正常运行
    cd installed/bin

    ./protoc --version
    ./thrift --version
    ```
3. 运行`protoc`和`thrift`的时候可能会遇到**无法打开，因为无法验证开发者**的问题，可以到前往`安全性与隐私`。点按`通用`面板中的`仍要打开`按钮，以确认打算打开该二进制。参考[https://support.apple.com/zh-cn/HT202491](https://support.apple.com/zh-cn/HT202491)。

## 启动

1. 通过命令设置好`file descriptors`（_**注意：关闭当前终端会话后需要重新设置**_）。
    ```shell
    ulimit -n 65536
    ```
    也可以将该配置写到到启动脚本中，以便下次打开终端会话时不需要再次设置。
    ```shell
    # bash
    echo 'ulimit -n 65536' >>~/.bashrc
    
    # zsh
    echo 'ulimit -n 65536' >>~/.zshrc
    ```
    执行以下命令，查看设置是否生效。
    ```shell
    $ ulimit -n
    65536
    ```

2. 启动BE
    ```shell
    cd output/be/bin
    ./start_be.sh --daemon
    ```

3. 启动FE
    ```shell
    cd output/fe/bin
    ./start_fe.sh --daemon
    ```

## 常见问题

1. 启动BE失败，日志显示错误`fail to open StorageEngine, res=file descriptors limit is too small`

   参考前面提到的设置`file descriptors`。

2. Java版本

   使用 brew 安装的 jdk 版本为 11，因为在 macOS上，arm64 版本的 brew 默认没有 8 版本的 jdk，也可以自行下载 jdk 的安装包进行安装
