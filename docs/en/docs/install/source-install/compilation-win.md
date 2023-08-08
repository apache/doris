---
{
    "title": "Compilation on Windows",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Compilation on Windows 

This topic is about how to compile Doris from source with Windows.

## Environment Requirements

1. Windows 11 or Windows 10, Version 1903, Build 18362 or newer
2. Normal-functioning WSL2

## Steps

1. Install the Oracle Linux 7.9 distribution from the Microsoft Store

   > You can also install other distros you want via Docker images or Github installs

2. Open CMD and specify the identity to run WSL2

    ```shell
    wsl -d OracleLinux_7_9 -u root
    ```

3. Install dependencies

    ```shell
    # Install required system packages
    sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 zip util-linux wget git python2
      
    # Install autoconf-2.69
    wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
        tar zxf autoconf-2.69.tar.gz && \
        cd autoconf-2.69 && \
        ./configure && \
        make && \
        make install
      
    # install bison-3.0.4
    wget http://ftp.gnu.org/gnu/bison/bison-3.0.4.tar.gz && \
        tar xzf bison-3.0.4.tar.gz && \
        cd bison-3.0.4 && \
        ./configure && \
        make && \
        make install
    ```

4. Install LDB_TOOLCHAIN and other major compilation environments

    - [Java8](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u131-linux-x64.tar.gz)
    - [Apache Maven 3.6.3](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz)
    - [Node v12.13.0](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz)
    - [LDB_TOOLCHAIN](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.18/ldb_toolchain_gen.sh)

5. Configure environment variables

6. Pull Doris source code

    ```
    git clone http://github.com/apache/doris.git
    ```

7. Compile

    ```
    cd doris
    sh build.sh
    ```
## Note

The default data storage drive letter of WSL2 distribution is the C drive. If neccessary, you can change that to prevent the system drive letter from getting full.
