---
{
    'title': 'Dev & Debug prepare on Mac', 
    'language': 'en'
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

## Install environment dependency

```shell
brew install automake autoconf libtool pkg-config texinfo coreutils gnu-getopt \
python@3 cmake ninja ccache bison byacc gettext wget pcre maven llvm@16 openjdk@11 npm
```

*The version of jdk installed using brew is 11, because on macOS, the arm64 version of brew does not have version 8 of jdk by default*

**Dependency description:**
1. Java, Maven, etc. can be downloaded separately for easy management
    - Mac recommend [Zulu JDK8](https://www.azul.com/downloads/?version=java-8-lts&os=macos&package=jdk#zulu)
    - Maven Download from [Maven website](https://maven.apache.org/download.cgi) is ok
            - Self-downloaded Java and Maven need to configure environment variables
2. Other dependent environment variables (example for Apple Silicon Macs)
    - llvm: `export PATH="/opt/homebrew/opt/llvm/bin:$PATH"`
    - bison: `export PATH = "/opt/homebrew/opt/bison/bin:$PATH`
    - texinfo: `export PATH = "/opt/homebrew/opt/texinfo/bin:$PATH`
    - python: `ln -s -f /opt/homebrew/bin/python3 /opt/homebrew/bin/python`
   
## Install thrift

**Note：** Thrift needs to be installed only when debugging FE only. When debugging BE and FE at the same time, the three-party library of BE contains thrift

```shell
MacOS: 
    1. Download：`brew install thrift@0.16.0`
    2. Create a soft link： 
        `mkdir -p ./thirdparty/installed/bin`
        # Apple Silicon CPU macOS
        `ln -s /opt/homebrew/Cellar/thrift@0.16.0/0.16.0/bin/thrift ./thirdparty/installed/bin/thrift`
        # Intel CPU macOS
        `ln -s /usr/local/Cellar/thrift@0.16.0/0.16.0/bin/thrift ./thirdparty/installed/bin/thrift`
    
Note：macOS implement `brew install thrift@0.16.0` it may report an error that the version cannot be found. The solution is as follows, execute in the terminal:
    1. `brew tap-new $USER/local-tap`
    2. `brew extract --version='0.16.0' thrift $USER/local-tap`
    3. `brew install thrift@0.16.0`
reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
```

## pull your own code

1. pull code

    ```shell
    cd ~
    mkdir DorisDev
    cd DorisDev
    git clone https://github.com/GitHubID/doris.git
    ```

2. set environment variables
 
    ```shell
    export DORIS_HOME=~/DorisDev/doris
    export PATH=$DORIS_HOME/bin:$PATH
    ```

## Download Doris compilation dependencies

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

## Set `file descriptors`

```shell
# bash
echo 'ulimit -n 65536' >>~/.bashrc
    
# zsh
echo 'ulimit -n 65536' >>~/.zshrc
```

## compile Doris

```shell
cd $DORIS_HOME
sh build.sh
```
## Compilation Error with Higher Version of Node.js

During the compilation process, errors may occur due to a higher version of Node.js.

- opensslErrorStack: [ 'error:03000086:digital envelope routines::initialization error' ]
  - library: 'digital envelope routines'
  - reason: 'unsupported'
  - code: 'ERR_OSSL_EVP_UNSUPPORTED'

For more information and a possible solution, you can refer to this [Stack Overflow post](https://stackoverflow.com/questions/74726224/opensslerrorstack-error03000086digital-envelope-routinesinitialization-e).


```shell
## Instruct Node.js to use an older version of the OpenSSL provider.
export NODE_OPTIONS=--openssl-legacy-provider
```


## Configure Debug environment

```shell
# cp out the compiled package
    
cp -r output ../doris-run
    
# Configure FE/BE's conf
1、IP、directory
2、BE additional configuration min_file_descriptor_number = 10000
```

## Start Debugging with IDE

[CLion Debug BE On Mac](./be-clion-dev.md)

[IntelliJ IDEA Debug FE On Mac](./fe-idea-dev.md)
