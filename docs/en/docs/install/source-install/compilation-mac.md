---
{
    "title": "Compilation on macOS",
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

# Compilation on macOS

This post introduces how to compile from source on macOS (both x86_64 and arm64).

## Prerequisites

1. macOS 12 (Monterey) or newer（_**both Intel chip and Apple Silicon chip are supported**_）
2. Apple Clang 13 or newer（the latest version is recommended）
3. [Homebrew](https://brew.sh/)

## Steps

1. Use [Homebrew](https://brew.sh/) to install tools
    ```shell
    brew install automake autoconf libtool pkg-config texinfo coreutils gnu-getopt \
        python cmake ninja ccache bison byacc gettext wget pcre maven openjdk@11 npm
    ```

2. Compile from source
    ```shell
    bash build.sh
    ```

## The Third Party Libraries

1. [Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation) page contains all sources of the third party libraries, users can download [doris-thirdparty-source.tgz](https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-source.tgz) to achieve the sources.

2. If the macOS is powered by _**Intel**_ chip，users can download the _**prebuilt**_ third party libraries [doris-thirdparty-prebuilt-darwin-x86_64.tar.xz](https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-x86_64.tar.xz) from [Apache Doris Third Party Prebuilt](https://github.com/apache/doris-thirdparty/releases/tag/automation) page and omit the building process for the third party libraries. Please refer to the following commands.
    ```shell
    cd thirdparty
    curl -L https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-darwin-x86_64.tar.xz \
        -o doris-thirdparty-prebuilt-darwin-x86_64.tar.xz
    tar -xvf doris-thirdparty-prebuilt-darwin-x86_64.tar.xz
    ```

## Start-up

1. Set `file descriptors` up（_**NOTICE: This way is only available in current terminal session. Users who close the current session and open a new terminal session should set the variable again**_）。
    ```shell
    ulimit -n 65536
    ```
    Users can also write the configuration to initialization files and don't need to set the variable again when opening a new terminal session.
    ```shell
    # bash
    echo 'ulimit -n 65536' >>~/.bashrc
    
    # zsh
    echo 'ulimit -n 65536' >>~/.zshrc
    ```
    Check the configuration by executing the following command.
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

### Fail to start BE up，errors in log: `fail to open StorageEngine, res=file descriptors limit is too small`
Please refer to the foregoing contents about `file descriptors`.

### Java version
Java 11 is recommended.
