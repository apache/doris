---
{
"title": "Compilation With Arm",
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

# Apache Doris ARM architecture compiled

This document describes how to compile Doris on the ARM64 platform.

Note that this document is for guidance only. Compiling in different environments may give other errors.

## KylinOS

### Software and hardware environment

1. KylinOS Version：

   ```
   $> cat /etc/.kyinfo
   name=Kylin-Server
   milestone=10-SP1-Release-Build04-20200711
   arch=arm64
   beta=False
   time=2020-07-11 17:16:54
   dist_id=Kylin-Server-10-SP1-Release-Build04-20200711-arm64-2020-07-11 17:16:54
   ```

2. CPU Model:

   ```
   $> cat /proc/cpuinfo
   model name  : Phytium,FT-2000+/64
   ```

### Compile with ldb-toolchain

This method works for Doris versions after [commit 7f3564](https://github.com/apache/doris/commit/7f3564cca62de49c9f2ea67fcf735921dbebb4d1).

Download [ldb\_toolchain\_gen.aarch64.sh](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh)

For the subsequent compilation method, please refer to [Compile with LDB toolchain](./compilation-with-ldb-toolchain.md)

Note that both jdk and nodejs need to download the corresponding aarch64 version:

1. [Java8-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz)
2. [Node v12.13.0-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz)

## CentOS & Ubuntu

### Hardware Environment

1. System version: CentOS 8.4, Ubuntu 20.04
2. System Architecture: ARM X64
3. CPU: 4C
4. Memory: 16 GB
5. Hard disk: 40GB (SSD), 100GB (SSD)

### Software Environment

#### Software environment comparison table

| component name                                               | component version                         |
| ------------------------------------------------------------ | ----------------------------------------- |
| Git                                                          | 2.0+                                      |
| JDK                                                          | 1.8.0                                     |
| Maven                                                        | 3.6.3                                     |
| NodeJS                                                       | 16.3.0                                    |
| LDB-Toolchain                                                | 0.9.1                                     |
| 常备环境：<br />byacc<br />patch<br />automake<br />libtool<br />make<br />which<br />file<br />ncurses-devel<br />gettext-devel<br />unzip<br />bzip2<br />zip<br />util-linux<br />wget<br />git<br />python2 | yum or apt can be installed automatically |
| autoconf                                                     | 2.69                                      |
| bison                                                        | 3.0.4                                     |

#### Software environment installation command

##### CentOS 8.4

- Create software download and installation package root directory and software installation root directory

  ```shell
  # Create the root directory of the software download and installation package
  mkdir /opt/tools
  # Create software installation root directory
  mkdir /opt/software
  ````

- Git

  ```shell
  # Save the trouble of compiling and install directly with yum
  yum install -y git
  ````

- JDK8

  ```shell
  # Two ways, the first is to save additional download and configuration, directly use yum to install, install the devel package to get some tools, such as the jps command
  yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
  
  # The second is to download the installation package of the arm64 architecture, decompress and configure the environment variables and use
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  mv jdk1.8.0_291 /opt/software/jdk8
  ````

- Maven

  ```shell
  cd /opt/tools
  # After the wget tool is downloaded, directly decompress the configuration environment variable to use
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  mv apache-maven-3.6.3 /opt/software/maven
  ````

- NodeJS

  ```shell
  cd /opt/tools
  # Download the installation package for arm64 architecture
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ````

- LDB-Toolchain

  ```shell
  cd /opt/tools
  # Download LDB-Toolchain ARM version
  wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh && \
  sh ldb_toolchain_gen.aarch64.sh /opt/software/ldb_toolchain/
  ````

- Configure environment variables

  ```shell
  # Configure environment variables
  vim /etc/profile.d/doris.sh
  export JAVA_HOME=/opt/software/jdk8
  export MAVEN_HOME=/opt/software/maven
  export NODE_JS_HOME=/opt/software/nodejs
  export LDB_HOME=/opt/software/ldb_toolchain
  export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$NODE_JS_HOME/bin:$LDB_HOME/bin:$PATH
  
  # save and exit and refresh environment variables
  source /etc/profile.d/doris.sh
  
  # test for success
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
  > gcc-11
  ````

- Install other extra environments and components

  ```shell
  # install required system packages
  sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 bison zip util-linux wget git python2
  
  # install autoconf-2.69
  cd /opt/tools
  wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
      tar zxf autoconf-2.69.tar.gz && \
      mv autoconf-2.69 /opt/software/autoconf && \
      cd /opt/software/autoconf && \
      ./configure && \
      make && \
      make install
  ````

##### Ubuntu 20.04

- Update apt-get repository

  ```shell
  apt-get update
  ````

- Check the shell command set

  Ubuntu's shell installs dash instead of bash by default. It needs to be switched to bash to execute. Run the following command to view the details of sh and confirm which program corresponds to the shell:

  ```shell
  ls -al /bin/sh
  ````

  The shell can be switched back to bash by:

  ```shell
  sudo dpkg-reconfigure dash
  ````

  Then select no or no and confirm

  Doing so will reconfigure dash from being the default shell tool

- Create software download and installation package root directory and software installation root directory

  ```shell
  # Create the root directory of the software download and installation package
  mkdir /opt/tools
  # Create software installation root directory
  mkdir /opt/software
  ````

- Git

  ```shell
  # Save the trouble of compiling and install directly with apt-get
  apt-get -y install git
  ````

- JDK8

  ```shell
  # Download the installation package of arm64 architecture, decompress and configure environment variables and use
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  mv jdk1.8.0_291 /opt/software/jdk8
  ````

- Maven

  ```shell
  cd /opt/tools
  # After the wget tool is downloaded, directly decompress the configuration environment variable to use
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  mv apache-maven-3.6.3 /opt/software/maven
  ````

- NodeJS

  ```shell
  cd /opt/tools
  # Download the installation package for arm64 architecture
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ````

- LDB-Toolchain

  ```shell
  cd /opt/tools
  # Download LDB-Toolchain ARM version
  wget https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh && \
  sh ldb_toolchain_gen.aarch64.sh /opt/software/ldb_toolchain/
  ````

- Configure environment variables

  ```shell
  # Configure environment variables
  vim /etc/profile.d/doris.sh
  export JAVA_HOME=/opt/software/jdk8
  export MAVEN_HOME=/opt/software/maven
  export NODE_JS_HOME=/opt/software/nodejs
  export LDB_HOME=/opt/software/ldb_toolchain
  export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$NODE_JS_HOME/bin:$LDB_HOME/bin:$PATH
  
  # save and exit and refresh environment variables
  source /etc/profile.d/doris.sh
  
  # test for success
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
     > gcc-11
     ````

- Install other extra environments and components

  ```shell
  # install required system packages
  sudo apt install -y build-essential cmake flex automake bison binutils-dev libiberty-dev zip libncurses5-dev curl ninja-build
  sudo apt-get install -y make
  sudo apt-get install -y unzip
  sudo apt-get install -y python2
  sudo apt-get install -y byacc
  sudo apt-get install -y automake
  sudo apt-get install -y libtool
  sudo apt-get install -y bzip2
  sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
  sudo apt update
  sudo apt install gcc-11 g++-11
  sudo apt-get -y install autoconf autopoint
  
  # install autoconf-2.69
  cd /opt/tools
  wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
      tar zxf autoconf-2.69.tar.gz && \
      mv autoconf-2.69 /opt/software/autoconf && \
      cd /opt/software/autoconf && \
      ./configure && \
      make && \
      make install
  ````

#### Download the source code

```shell
cd /opt
git clone https://github.com/apache/doris.git
```

#### Install and deploy

##### Check if AVX2 instruction set is supported

If there is data returned, it means support, if no data is returned, it means not supported

```shell
cat /proc/cpuinfo | grep avx2
````

##### Execute compilation

```shell
# For machines that support AVX2 instruction set, you can compile them directly
sh build.sh
# For machines that do not support the AVX2 instruction set, use the following command to compile
USE_AVX2=OFF sh build.sh
````

### common problem

1. Compile the third-party library libhdfs3.a , the folder cannot be found

    - Problem Description

      During the compilation and installation process, the following error occurred

      > not found lib/libhdfs3.a file or directory

    - problem causes

      There is a problem with the dependency download of the third-party library

    - solution

      - Use a third-party download repository

        ```shell
        export REPOSITORY_URL=https://doris-thirdparty-repo.bj.bcebos.com/thirdparty
        sh /opt/doris/thirdparty/build-thirdparty.sh
        ````

        REPOSITORY_URL contains all third-party library source packages and their historical versions.

2. python command not found

   - Problem Description

     - An exception is thrown when executing build.sh

       > /opt/doris/env.sh: line 46: python: command not found
       >
       > Python 2.7.18

   - problem causes

     After searching, it is found that the system uses `python2.7`, `python3.6`, `python2`, `python3` by default to execute python commands. Doris installation requires python 2.7+ version, so only need Just add a command named `python` to connect, both version 2 and version 3 can be used

   - solution

     Establish a soft link to the `python` command in `\usr\bin`

     ```shell
     # View python installation directory
     whereis python
     # Establish soft connection
     sudo ln -s /usr/bin/python2.7 /usr/bin/python
     ````

3. There is no output directory after compilation

   - Problem Description

     - After the execution of build.sh ends, the output folder is not found in the directory.

   - problem causes

     Failed to compile, need to recompile

   - solution

     ```shell
     sh build.sh --clean
     ````

4. spark-dpp compilation failed

   - Problem Description

     - After compiling build.sh, compiling to Spark-DPP fails with an error

       > Failed to execute goal on project spark-dpp

   - problem causes

     The last error message is due to the download failure (and because it failed to connect to the repo.maven.apache.org central repository)

     > Could not transfer artifact org.apache.spark:spark-sql_2.12:jar:2.4.6 from/to central (https://repo.maven.apache.org/maven2): Transfer failed for https://repo .maven.apache.org/maven2/org/apache/spark/spark-sql_2.12/2.4.6/spark-sql_2.12-2.4.6.jar: Unknown host repo.maven.apache.org

     rebuild

   - solution

     - rebuild

5. The remaining space is insufficient, and the compilation fails

   - Problem Description

     - Failed to build CXX object during compilation, indicating insufficient free space

       > fatal error: error writing to /tmp/ccKn4nPK.s: No space left on device
       > 1112 | } // namespace doris::vectorized
       > compilation terminated.

   - problem causes

     Insufficient free space on the device

   - solution

     Expand the remaining space of the device, such as deleting unnecessary files, etc.

6. Failed to start FE, transaction -20 problem

   - Problem Description

     When starting FE, a transaction error 20 is reported, and the status is UNKNOWN

     > [BDBEnvironment.setup():198] error to open replicated environment. will exit.
     > com.sleepycat.je.rep.ReplicaWriteException: (JE 18.3.12) Problem closing transaction 20. The current state is:UNKNOWN. The node transitioned to this state at:Fri Apr 22 12:48:08 CST 2022

   - problem causes

     Insufficient hard disk space, need more space

   - solution

     Free up hard disk space or mount a new hard disk

7. BDB environment setting is abnormal, disk search error

   - Problem Description

     An exception is reported when starting FE after migrating the drive letter where FE is located

     > 2022-04-22 16:21:44,092 ERROR (MASTER 172.28.7.231_9010_1650606822109(-1)|1) [BDBJEJournal.open():306] catch an exception when setup bdb environment. will exit.
     > com.sleepycat.je.DiskLimitException: (JE 18.3.12) Disk usage is not within je.maxDisk or je.freeDisk limits and write operations are prohibited: maxDiskLimit=0 freeDiskLimit=5,368,709,120 adjustedMaxDiskLimit=0 maxDiskOverage=0 freeDiskShortage=1,536,552,960 diskFreeSpace =3,832,156,160 availableLogSize=-1,536,552,960 totalLogSize=4,665 activeLogSize=4,665 reservedLogSize=0 protectedLogSize=0 protectedLogSizeMap={}

   - problem causes

     The location of the FE is migrated, the hard disk information stored in the metadata cannot be matched, or the hard disk is damaged or not mounted

   - solution

     - Check if the hard disk is normal, initialized and mounted correctly
     - Fix FE metadata
     - If it is a test machine, you can delete the metadata directory and restart

8. Could not find pkg.m4 file in pkg.config

   - Problem Description

     - A file not found error occurred during compilation, and the error is as follows

       > Couldn't find pkg.m4 from pkg-config. Install the appropriate package for your distribution or set ACLOCAL_PATH to the directory containing pkg.m4.

     - By looking up the above log, it is found that there is a problem with the compilation of the third-party library `libxml2`

   - problem causes

     `libxml2` tripartite library compilation error, pkg.m4 file not found

     ***guess:***

     1. An exception occurs when the Ubuntu system loads environment variables, resulting in the index under the ldb directory not being successfully loaded
     2. The retrieval of environment variables during libxml2 compilation fails, resulting in the compilation process not retrieving the ldb/aclocal directory

   - solution

     Copy the `pkg.m4` file in the ldb/aclocal directory to the libxml2/m4 directory, and recompile the third-party library

     ```shell
     cp /opt/software/ldb_toolchain/share/aclocal/pkg.m4 /opt/incubator-doris/thirdparty/src/libxml2-v2.9.10/m4
     sh /opt/incubator-doris/thirdparty/build-thirdparty.sh
     ````

9. Failed to execute test CURL_HAS_TLS_PROXY

   - Problem Description

     - An error is reported during the compilation process of the three-party package, the error is as follows

       > -- Performing Test CURL_HAS_TLS_PROXY - Failed
       > CMake Error at cmake/dependencies.cmake:15 (get_property):
       > INTERFACE_LIBRARY targets may only have whitelisted properties. The
       > property "LINK_LIBRARIES_ALL" is not allowed.

     - After viewing the log, it is found that the internal is due to curl `No such file or directory`

       > fatal error: curl/curl.h: No such file or directory
       > 2 | #include <curl/curl.h>
       > compilation terminated.
       > ninja: build stopped: subcommand failed.

   - problem causes

     There is an error in the compilation environment. After checking the gcc version, it is found that it is the 9.3.0 version that comes with the system, so it is not compiled with ldb, and the ldb environment variable needs to be set

   - solution

     Configure ldb environment variables

     ```shell
     # Configure environment variables
     vim /etc/profile.d/ldb.sh
     export LDB_HOME=/opt/software/ldb_toolchain
     export PATH=$LDB_HOME/bin:$PATH
     # save and exit and refresh environment variables
     source /etc/profile.d/ldb.sh
     # test
     gcc --version
     > gcc-11
     ````

10. Other abnormal problems

   - Problem Description
  
     If there is an error message of the following components, it will be solved with this solution
  
     - bison related
       1. When installing bison-3.0.4, I get fseterr.c error
     - flex related
       1. flex command not found
     - cmake related
       1. cmake command not found
       2. cmake cannot find the dependent library
       3. cmake cannot find CMAKE_ROOT
       4. Compiler set not found in cmake environment variable CXX
     - boost related
       1. Boost.Build build engine failed
     - mysql related
       1. Could not find mysql client dependency a file
     - gcc related
       1. GCC version requires 11+
  
   - problem causes
  
     Not compiled with Ldb-Toolschain
  
   - solution
  
     - Check if the Ldb-Toolschain environment variable is configured
     - Check if gcc version is `gcc-11`
     - Delete the ldb directory after the `ldb-toolschain.sh` script is executed, re-execute and configure the environment variables, and verify the gcc version


