---
{
    "title": "Compilation on MacOS",
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

# Compilation on MacOS

This topic is about how to compile Doris from source with macOS (both x86_64 and arm64).

## Environment Requirements

1. macOS 12 (Monterey) or newer（_**both Intel chip and Apple Silicon chips are supported**_）
2. [Homebrew](https://brew.sh/)

## Steps

1. Use [Homebrew](https://brew.sh/) to install dependencies.
    ```shell
    brew install automake autoconf libtool pkg-config texinfo coreutils gnu-getopt \
        python@3 cmake ninja ccache bison byacc gettext wget pcre maven llvm@16 openjdk@11 npm
    ```

:::tip
The version of jdk installed using brew is 11, because on macOS, the arm64 version of brew does not have version 8 of jdk by default
:::

2. Compile from source.
    ```shell
    bash build.sh
    ```

## Third-Party Libraries

1. The [Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation) page contains the source code of all third-party libraries. You can download [doris-thirdparty-source.tgz](https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-source.tgz) to obtain them.

2. You can download the _**precompiled**_ third party library from the [Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation) page. You may refer to the following commands:
    ```shell
    cd thirdparty
    rm -rf installed

    # Intel chips
    curl -L https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-x86_64.tar.xz \
        -o - | tar -Jxf -

    # Apple Silicon chips
    curl -L https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-arm64.tar.xz \
        -o - | tar -Jxf -

    # Make sure that protoc and thrift can run successfully.
    cd installed/bin

    ./protoc --version
    ./thrift --version
    ```
3. When running `protoc` or `thrift`, you may meet an error which says **the app can not be opened because the developer cannot be verified**. Go to `Security & Privacy`. Click the `Open Anyway` button in the `General` pane to confirm your intent to open the app. See [https://support.apple.com/en-us/HT202491](https://support.apple.com/en-us/HT202491).

## Start-up

1. Set `file descriptors` （_**NOTICE: If you have closed the current session, you need to set this variable again**_）。
    ```shell
    ulimit -n 65536
    ```
    You can also write this configuration into the initialization files so you don't need to set the variables again when opening a new terminal session.
    ```shell
    # bash
    echo 'ulimit -n 65536' >>~/.bashrc
    
    # zsh
    echo 'ulimit -n 65536' >>~/.zshrc
    ```
    Check if the configuration works by executing the following command.
    ```shell
    $ ulimit -n
    65536
    ```

2. Start BE up
    ```shell
    cd output/be/bin
    ./start_be.sh --daemon
    ```

3. Start FE up
    ```shell
    cd output/fe/bin
    ./start_fe.sh --daemon
    ```

## FAQ

1. Fail to start BE up. The log shows: `fail to open StorageEngine, res=file descriptors limit is too small`

   To fix this, please refer to the "Start-up" section above and reset  `file descriptors`.

2. Java Version

   The version of jdk installed with brew is 11, because on macOS, the arm64 version of brew does not have version 8 of jdk by default, and you can also download the jdk installation package for installation