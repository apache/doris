---
{
"title": "Compilation with Arm",
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

# Compile with ARM

This topic is about how to compile Doris on the ARM64 platform.

Note that this is for reference only. Other errors may occur when compiling in different environments.

## KylinOS

### Software and Hardware Environment

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

### Compile with ldb-Toolchain

This method works for Doris versions after [commit 7f3564](https://github.com/apache/doris/commit/7f3564cca62de49c9f2ea67fcf735921dbebb4d1).

Download [ldb\_toolchain\_gen.aarch64.sh](https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh)

For detailed instructions, please refer to [Compile with ldb-toolchain](./compilation-with-ldb-toolchain.md)

Note that you need to download the corresponding aarch64 versions of jdk and nodejs:

1. [Java8-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz)
2. [Node v16.3.0-aarch64](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz)

## CentOS & Ubuntu

### Hardware Environment

1. System Version: CentOS 8.4, Ubuntu 20.04
2. System Architecture: ARM X64
3. CPU: 4C
4. Memory: 16 GB
5. Hard Disk: 40GB (SSD), 100GB (SSD)

### Software Environment

#### Software Environment List

| Component Name                                               | Component Version              |
| ------------------------------------------------------------ | ------------------------------ |
| Git                                                          | 2.0+                           |
| JDK                                                          | 1.8.0                          |
| Maven                                                        | 3.6.3                          |
| NodeJS                                                       | 16.3.0                         |
| LDB-Toolchain                                                | 0.9.1                          |
| Commonly Used Components<br />byacc<br />patch<br />automake<br />libtool<br />make<br />which<br />file<br />ncurses-devel<br />gettext-devel<br />unzip<br />bzip2<br />zip<br />util-linux<br />wget<br />git<br />python2 | yum install or apt-get install |
| autoconf                                                     | 2.69                           |
| bison                                                        | 3.0.4                          |

#### Software Environment Installation Command

##### CentOS 8.4

- Create root directories

  ```shell
  # Create root directory for software download and installation packages
  mkdir /opt/tools
  # Create root directory for software installation
  mkdir /opt/software
  ````

- Git

  ```shell
  # yum install (save the trouble of compilation)
  yum install -y git
  ````

- JDK8 (2 methods)

  ```shell
  # 1. yum install, which can avoid additional download and configuration. Installing the devel package is to get tools such as the jps command.
  yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel
  
  # 2. Download the installation package of the arm64 architecture, decompress it, and configure the environment variables.
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  mv jdk1.8.0_291 /opt/software/jdk8
  ````

- Maven

  ```shell
  cd /opt/tools
  # Download the wget tool, decompress it, and configure the environment variables.
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  mv apache-maven-3.6.3 /opt/software/maven
  ````

- NodeJS

  ```shell
  cd /opt/tools
  # Download the installation package of the arm64 architecture
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ````

- ldb-toolchain

  ```shell
  cd /opt/tools
  # Download ldb-toolchain ARM version
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
  
  # Save, exit, and refresh environment variables
  source /etc/profile.d/doris.sh
  
  # Test
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
  > gcc-11
  ````

- Install other environments and components

  ```shell
  # Install required system packages
  sudo yum install -y byacc patch automake libtool make which file ncurses-devel gettext-devel unzip bzip2 bison zip util-linux wget git python2
  
  # Install autoconf-2.69
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

  The Ubuntu shell installs dash instead of bash by default. It needs to be switched to bash for proper execution. Run the following command to view the details of sh and confirm which program corresponds to the shell:

  ```shell
  ls -al /bin/sh
  ````

  The shell can be switched back to bash by:

  ```shell
  sudo dpkg-reconfigure dash
  ````

  Then select no to confirm.

  After these steps, dash will no longer be the default shell tool.

- Create root directories

  ```shell
  # Create root directory for software download and installation packages
  mkdir /opt/tools
  # Create root directory for software installation
  mkdir /opt/software
  ````

- Git

  ```shell
  # apt-get install, which can save the trouble of compilation
  apt-get -y install git
  ````

- JDK8

  ```shell
  # Download the installation package of the ARM64 architecture, decompress it, and configure environment variables.
  cd /opt/tools
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/jdk-8u291-linux-aarch64.tar.gz && \
  tar -zxvf jdk-8u291-linux-aarch64.tar.gz && \
  mv jdk1.8.0_291 /opt/software/jdk8
  ````

- Maven

  ```shell
  cd /opt/tools
  # Download the wget tool, decompress it, and configure the environment variables.
  wget https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
  tar -zxvf apache-maven-3.6.3-bin.tar.gz && \
  mv apache-maven-3.6.3 /opt/software/maven
  ````

- NodeJS

  ```shell
  cd /opt/tools
  # Download the installation package of ARM64 architecture.
  wget https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz && \
  tar -xvf node-v16.3.0-linux-arm64.tar.xz && \
  mv node-v16.3.0-linux-arm64 /opt/software/nodejs
  ````

- ldb-toolchain

  ```shell
  cd /opt/tools
  # Download ldb-toolchain ARM version
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
  
  # Save, exit, and refresh environment variables
  source /etc/profile.d/doris.sh
  
  # Test
  java -version
  > java version "1.8.0_291"
  mvn -version
  > Apache Maven 3.6.3
  node --version
  > v16.3.0
  gcc --version
     > gcc-11
     ````

- Install other environments and components

  ```shell
  # Install required system packages
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
  
  # Install autoconf-2.69
  cd /opt/tools
  wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz && \
      tar zxf autoconf-2.69.tar.gz && \
      mv autoconf-2.69 /opt/software/autoconf && \
      cd /opt/software/autoconf && \
      ./configure && \
      make && \
      make install
  ````

#### Download Source Code

```shell
cd /opt
git clone https://github.com/apache/doris.git
```

#### Install and Deploy

##### Check if the AVX2 instruction set is supported

If there is data returned, that means AVX2 is supported; otherwise, AVX2 is not supported.

```shell
cat /proc/cpuinfo | grep avx2
````

##### Execute compilation

```shell
# For machines that support the AVX2 instruction set, compile directly
sh build.sh
# For machines that do not support the AVX2 instruction set, use the following command to compile
USE_AVX2=OFF sh build.sh
````

### FAQ

1. File not found when compiling the third-party library libhdfs3.a.

    - Problem Description

      During the compilation and installation process, the following error occurrs:

      > not found lib/libhdfs3.a file or directory

    - Cause

      The third-party library dependency is improperly downloaded.

    - Solution

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

   - Cause

     The system uses `python2.7`, `python3.6`, `python2`, `python3` by default to execute python commands. Doris only requires python 2.7+ to install dependencies, so you just need to add a command  `python` to connect. 

   - Solution

     Establish a soft link to the `python` command in `\usr\bin`

     ```shell
     # View python installation directory
     whereis python
     # Establish soft connection
     sudo ln -s /usr/bin/python2.7 /usr/bin/python
     ````

3. There is no output directory after compilation

   - Problem Description

     - Cannot find the output folder in the directory after the execution of build.sh.

   - Cause

     Compilation fails. Try again.

   - Solution

     ```shell
     sh build.sh --clean
     ````

4. spark-dpp compilation fails

   - Problem Description

     - After compiling build.sh, compiling to Spark-DPP fails with an error

       > Failed to execute goal on project spark-dpp

   - Cause

     This error message is caused by download failure (connection to the `repo.maven.apache.org` central repository fails).

     > Could not transfer artifact org.apache.spark:spark-sql_2.12:jar:2.4.6 from/to central (https://repo.maven.apache.org/maven2): Transfer failed for https://repo .maven.apache.org/maven2/org/apache/spark/spark-sql_2.12/2.4.6/spark-sql_2.12-2.4.6.jar: Unknown host repo.maven.apache.org

   - Solution

     - Rebuild

5. No space left, compilation fails

   - Problem Description

     - Failed to build CXX object during compilation, error message showing no space left

       > fatal error: error writing to /tmp/ccKn4nPK.s: No space left on device
       > 1112 | } // namespace doris::vectorized
       > compilation terminated.

   - Cause

     Insufficient free space on the device

   - Solution

     Expand the free space on the device by deleting files you don't need, etc.

6. Failed to start FE, transaction error -20

   - Problem Description

     When starting FE, a transaction error 20 is reported with UNKNOWN status.

     > [BDBEnvironment.setup():198] error to open replicated environment. will exit.
     > com.sleepycat.je.rep.ReplicaWriteException: (JE 18.3.12) Problem closing transaction 20. The current state is:UNKNOWN. The node transitioned to this state at:Fri Apr 22 12:48:08 CST 2022

   - Cause

     Insufficient hard disk space

   - Solution

     Free up hard disk space or mount a new hard disk

7. Abnormal BDB environment setting, disk search error

   - Problem Description

     An exception is reported when starting FE after migrating the drive letter where FE is located

     > 2022-04-22 16:21:44,092 ERROR (MASTER 172.28.7.231_9010_1650606822109(-1)|1) [BDBJEJournal.open():306] catch an exception when setup bdb environment. will exit.
     > com.sleepycat.je.DiskLimitException: (JE 18.3.12) Disk usage is not within je.maxDisk or je.freeDisk limits and write operations are prohibited: maxDiskLimit=0 freeDiskLimit=5,368,709,120 adjustedMaxDiskLimit=0 maxDiskOverage=0 freeDiskShortage=1,536,552,960 diskFreeSpace =3,832,156,160 availableLogSize=-1,536,552,960 totalLogSize=4,665 activeLogSize=4,665 reservedLogSize=0 protectedLogSize=0 protectedLogSizeMap={}

   - Cause

     FE has been migrated to another location, which doesn't match the hard disk information stored in the metadata; or the hard disk is damaged or not mounted

   - Solution

     - Check if the hard disk is normal, initialized and mounted correctly
     - Fix FE metadata
     - If it is a test machine, you can delete the metadata directory and restart

8. Could not find pkg.m4 file in pkg.config

   - Problem Description

     - A "file not found" error occurs during compilation:

       > Couldn't find pkg.m4 from pkg-config. Install the appropriate package for your distribution or set ACLOCAL_PATH to the directory containing pkg.m4.

   - Cause

     There is something wrong with the compilation of the third-party library `libxml2` .

     ***Possible Reasons:***

     1. An exception occurs when the Ubuntu system loads the environment variables so the index under the ldb directory is not successfully loaded.
     2. The retrieval of environment variables during libxml2 compilation fails, so the ldb/aclocal directory is not retrieved.
   
   - Solution

     Copy the `pkg.m4` file in the ldb/aclocal directory into the libxml2/m4 directory, and recompile the third-party library.

     ```shell
     cp /opt/software/ldb_toolchain/share/aclocal/pkg.m4 /opt/incubator-doris/thirdparty/src/libxml2-v2.9.10/m4
     sh /opt/incubator-doris/thirdparty/build-thirdparty.sh
     ````
   
9. Failed to execute test CURL_HAS_TLS_PROXY

   - Problem Description

     - An error is reported during the compilation process of the third-party package:

       > -- Performing Test CURL_HAS_TLS_PROXY - Failed
       > CMake Error at cmake/dependencies.cmake:15 (get_property):
       > INTERFACE_LIBRARY targets may only have whitelisted properties. The
       > property "LINK_LIBRARIES_ALL" is not allowed.

     - The log shows: curl `No such file or directory`

       > fatal error: curl/curl.h: No such file or directory
       > 2 | #include <curl/curl.h>
       > compilation terminated.
       > ninja: build stopped: subcommand failed.

   - Cause

     There is an error in the compilation environment. The gcc is of version 9.3.0 that comes with the system, so it is not compiled with ldb, so you need to configure the ldb environment variable.

   - Solution

     Configure ldb environment variables

     ```shell
     # Configure environment variables
     vim /etc/profile.d/ldb.sh
     export LDB_HOME=/opt/software/ldb_toolchain
     export PATH=$LDB_HOME/bin:$PATH
     # Save, exit, and refresh environment variables
     source /etc/profile.d/ldb.sh
     # Test
     gcc --version
     > gcc-11
     ````

10. Other problems

   - Problem Description
  
     The follow error prompts are all due to one root cause.
  
     - bison related
       1. fseterr.c error when installing bison-3.0.4
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
  
   - Cause
  
     Not compiled with ldb-toolchain
  
   - Solution
  
     - Check if the ldb-toolchain environment variable is configured
     - Check if gcc version is `gcc-11`
     - Delete the ldb directory after the `ldb_toolchain_gen.aarch64.sh` script is executed, re-execute and configure the environment variables, and verify the gcc version

