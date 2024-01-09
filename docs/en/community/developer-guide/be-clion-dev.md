---
{ 'title': 'Development and Debugging of Apache Doris BE -- Clion', 'language': 'en' }
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

# Using Clion for Remote Development and Debugging of Apache Doris BE

## Downloading and Compiling Code on Remote Server

1. Download Doris source code on the remote server, such as in the directory `/mnt/datadisk0/chenqi/doris`.

```
git clone https://github.com/apache/doris.git
```

2. Modify the `env.sh` file located in the root directory of the Doris code on the remote server.
Add the configuration for `DORIS_HOME` at the beginning, for example, `DORIS_HOME=/mnt/datadisk0/chenqi/doris.`

3. Execute commands to compile the code. The detailed compilation process [compilation-with-ldb-toolchain](https://doris.apache.org/zh-CN/docs/dev/install/source-install/compilation-with-ldb-toolchain).

```
cd /mnt/datadisk0/chenqi/doris
./build.sh
```

## Clion Installation and Configuration for Remote Development Environment

1. Download and install Clion on your local env, then import the Doris BE source code.

2. Set up a remote development environment on your local env by navigating to **Preferences -> Build, Execution, Deployment -> Deployment** in Clion.
Add the connection and login information for the remote development server using **SFTP** and set the **Mappings** paths.
For example, where Local Path is the local path `/User/kaka/Programs/doris/be` and Deployment Path is the remote server path `/mnt/datadisk0/chenqi/clion/doris/be`.

![Deployment1](/images/clion-deployment1.png)

![Deployment2](/images/clion-deployment2.png)

3. Copy the `gensrc` path on the remote server, for example `/mnt/datadisk0/chenqi/doris/gensrc`, to the parent directory of the **Deployment Path**.
For example, the final directory path on the remote server should be `/mnt/datadisk0/chenqi/clion/doris/gensrc`.

```
cp -R /mnt/datadisk0/chenqi/doris/gensrc /mnt/datadisk0/chenqi/clion/doris/gensrc
```

4. In Clion, navigate to **Preferences -> Build, Execution, Deployment -> Toolchains** and add the necessary remote environment toolchains such as cmake, gcc, g++, gdb, etc.
**The most important step is to add the path of **env.sh** on the remote server to **Environment file**.**

![Toolchains](/images/clion-toolchains.png)

5. In Clion, navigate to **Preferences -> Build, Execution, Deployment -> CMake** and add the compilation option -DDORIS_JAVA_HOME=/path/to/remote/JAVA_HOME in CMake options, set DORIS_JAVA_HOME to the JAVA_HOME path of the remote server, otherwise jni.h will not be found.

6. Right-click on **Load Cmake Project** in Clion. This will synchronize the code to the remote server and generate the Cmake build files.

## Running and debugging Doris BE remotely in Clion

1. Configure CMake in **Preferences -> Build, Execution, Deployment -> CMake**. Different targets such as Debug / Release can be configured, and the **ToolChain** should be set to the previously configured.
If you want to run and debug Unit Tests, you need to add ·-DMAKE_TEST=ON· to CMake Options (this option is disabled by default, and needs to be enabled to compile the Test code)
Copy the output directory in Doris source code to a separate path on the remote server, such as /mnt/datadisk0/chenqi/clion/doris/doris_be/.

2. Copy the output directory in Doris source code to a separate path on the remote server, such as `/mnt/datadisk0/chenqi/clion/doris/doris_be/`.

```
cp -R /mnt/datadisk0/chenqi/doris/output /mnt/datadisk0/chenqi/clion/doris/doris_be
```

![Output Tree](/images/doris-dist-output-tree.png)

3. Select the relevant Target for doris_be in Clion, such as Debug or Release, and configure the run.

![Run Debug Conf1](/images/clion-run-debug-conf1.png)

Refer to the environment variables exported in `be/bin/start_be.sh` in the Doris root directory for environment variable configuration. The value of each environment variable should point to the corresponding path on the remote server.**
Environment variables reference:

![Run Debug Conf2](/images/clion-run-debug-conf2.png)

4. Click **Run** to compile and run the BE, or click **Debug** to compile and debug the BE.
