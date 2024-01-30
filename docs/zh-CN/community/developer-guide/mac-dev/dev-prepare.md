---
{
    'title': 'Doris Mac 开发调试准备', 
    'language': 'zh-CN'
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

## 安装环境依赖

```shell
brew install automake autoconf libtool pkg-config texinfo coreutils gnu-getopt \
python@3 cmake ninja ccache bison byacc gettext wget pcre maven llvm@16 openjdk@11 npm
```

*使用 brew 安装的 jdk 版本为 11，因为在 macOS上，arm64 版本的 brew 默认没有 8 版本的 jdk*

**依赖说明：**
1. Java、Maven 等可以单独下载，方便管理
    - Mac 推荐 [Zulu JDK8](https://www.azul.com/downloads/?version=java-8-lts&os=macos&package=jdk#zulu)
    - Maven 从 [Maven 官网下载](https://maven.apache.org/download.cgi)即可
    - 自行下载的 Java 与 Maven 需要配置环境变量
2. 其他依赖的环境变量 (示例为 Apple Silicon 芯片 Mac)
    - llvm: `export PATH="/opt/homebrew/opt/llvm/bin:$PATH"`
    - bison: `export PATH = "/opt/homebrew/opt/bison/bin:$PATH`
    - texinfo: `export PATH = "/opt/homebrew/opt/texinfo/bin:$PATH`
    - python: `ln -s -f /opt/homebrew/bin/python3 /opt/homebrew/bin/python`
   
## 安装 thrift

**注意：** 仅在只调试FE的情况下需要安装 thrift，同时调试 BE 和 FE 时，BE 的三方库包含 thrift

```shell
MacOS: 
    1. 下载：`brew install thrift@0.16.0`
    2. 建立软链接： 
        `mkdir -p ./thirdparty/installed/bin`
        # Apple Silicon 芯片 macOS
        `ln -s /opt/homebrew/Cellar/thrift@0.16.0/0.16.0/bin/thrift ./thirdparty/installed/bin/thrift`
        # Intel 芯片 macOS
        `ln -s /usr/local/Cellar/thrift@0.16.0/0.16.0/bin/thrift ./thirdparty/installed/bin/thrift`

注：macOS 执行 `brew install thrift@0.16.0` 可能会报找不到版本的错误，解决方法如下，在终端执行：
    1. `brew tap-new $USER/local-tap`
    2. `brew extract --version='0.16.0' thrift $USER/local-tap`
    3. `brew install thrift@0.16.0`
参考链接: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
```

## 拉取自己的代码

1. 拉取代码

    ```shell
    cd ~
    mkdir DorisDev
    cd DorisDev
    git clone https://github.com/GitHubID/doris.git
    ```

2. 设置环境变量
 
    ```shell
    export DORIS_HOME=~/DorisDev/doris
    export PATH=$DORIS_HOME/bin:$PATH
    ```

## 下载 Doris 编译依赖

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

## 修改系统最大文件句柄数

```shell
# bash
echo 'ulimit -n 65536' >>~/.bashrc
    
# zsh
echo 'ulimit -n 65536' >>~/.zshrc
```

## 编译 Doris

```shell
cd $DORIS_HOME
sh build.sh
```

## 编译过程中可能会遇到高版本的 Node.js 导致的错误

opensslErrorStack: ['error:03000086:digital envelope routines::initialization error']
library: 'digital envelope routines'
reason: 'unsupported'
code: 'ERR_OSSL_EVP_UNSUPPORTED'
以下命令解决问题。参考[https://stackoverflow.com/questions/74726224/opensslerrorstack-error03000086digital-envelope-routinesinitialization-e](https://stackoverflow.com/questions/74726224/opensslerrorstack-error03000086digital-envelope-routinesinitialization-e)

```shell
#指示Node.js使用旧版的OpenSSL提供程序
export NODE_OPTIONS=--openssl-legacy-provider
```


## 配置 Debug 环境

```shell
# 将编译好的包cp出来
    
cp -r output ../doris-run
    
# 配置FE/BE的conf
1、IP、目录
2、BE 额外配置 min_file_descriptor_number = 10000
```

## 开始用 IDE 进行 Debug

[CLion Mac 调试 BE](./be-clion-dev.md)

[IntelliJ IDEA Mac 调试 FE](./fe-idea-dev.md)
