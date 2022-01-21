---
{
    "title": "Compiling with LDB toolchain",
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

# Compiling with LDB toolchain

This document describes how to compile Doris using the LDB toolchain, which will later replace the Docker development image as the default source code compilation method for Doris.

> Thanks to [Amos Bird](https://github.com/amosbird) for this contribution.

## Prepare the environment

This works for most Linux distributions (CentOS, Ubuntu, etc.).

1. Download `ldb_toolchain_gen.sh`

    The latest `ldb_toolchain_gen.sh` can be downloaded from [here](https://github.com/amosbird/ldb_toolchain_gen/releases). This script is used to generate the ldb toolchain.
    
    > For more information, you can visit [https://github.com/amosbird/ldb_toolchain_gen](https://github.com/amosbird/ldb_toolchain_gen)

2. Execute the following command to generate the ldb toolchain

    ```
    sh ldb_toolchain_gen.sh /path/to/ldb_toolchain/
    ```
    
    where `/path/to/ldb_toolchain/` is the directory where the toolchain is installed.
    
    After successful execution, the following directory structure will be created under `/path/to/ldb_toolchain/`.
    
    ```
    ├── bin
    ├── include
    ├── lib
    ├── share
    ├── test
    └── usr
    ```
    
3. Download and install other compiled components

    1. [Java8](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u131-linux-x64.tar.gz)
    2. [Apache Maven 3.8.4](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/apache-maven-3.6.3-bin.tar.gz)
    3. [Node v12.13.0](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v12.13.0-linux-x64.tar.gz)

	python is also needed.

4. Download Doris source code

    ```
    git clone https://github.com/apache/incubator-doris.git
    ```
    
    After downloading, go to the Doris source directory, create the `custom_env.sh`, file, and set the PATH environment variable, e.g.
    
    ```
    export JAVA_HOME=/path/to/jave/
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=/path/to/maven/bin:$PATH
    export PATH=/path/to/node/bin:$PATH
    export PATH=/path/to/ldb-toolchain/bin:$PATH
    ```

## Compiling Doris

Go to the Doris source code directory and execute.

```
sh build.sh
```

This script will compile the third-party libraries first and then the Doris components (FE, BE) later. The compiled output is in the `output/` directory.
