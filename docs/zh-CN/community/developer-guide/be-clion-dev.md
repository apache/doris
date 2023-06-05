---
{ 'title': 'Doris BE 开发调试环境 -- clion', 'language': 'zh-CN' }
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

# 使用 Clion 进行 Apache Doris BE 远程开发调试

## 远程服务器代码下载编译

1. 在远程服务器上下载一份 Doris 代码。比如 Doris 根目录为 `/mnt/datadisk0/chenqi/doris`。

```
git clone https://github.com/apache/doris.git
```

2. 修改远程服务器上 Doris 代码根目录下的 env.sh 文件，在开头增加 `DORIS_HOME` 的配置，比如 `DORIS_HOME=/mnt/datadisk0/chenqi/doris`。

3. 执行相关命令进行编译。其中详细编译过程可参考[编译文档](https://doris.apache.org/zh-CN/docs/dev/install/source-install/compilation-with-ldb-toolchain)。

```
cd /mnt/datadisk0/chenqi/doris
./build.sh
```

## 本地 Clion 安装配置远程开发环境

1. 在本地下载安装 Clion，导入 Doris BE 代码。

2. 在本地设置远程开发环境。 在 Clion 中打开 **Preferences -> Build, Execution, Deployment -> Deployment** 中添加远程开发环境。
使用 **SFTP** 来添加一个远程开发服务器的相关连接登陆信息。设置 **Mappings** 路径。
比如 Local Path 为本地路径 `/User/kaka/Programs/doris/be`，Deployment Path 为远程服务器路径 `/mnt/datadisk0/chenqi/clion/doris/be`。

![Deployment1](/images/clion-deployment1.png)

![Deployment2](/images/clion-deployment2.png)

3. 将远程服务器上编译完成的 `gensrc` 路径，比如 `/mnt/datadisk0/chenqi/doris/gensrc` 拷贝到 **Deployment Path** 的上一级目录。
比如拷贝完最终的目录为远程服务器路径 `/mnt/datadisk0/chenqi/clion/doris/gensrc`。

```
cp -R /mnt/datadisk0/chenqi/doris/gensrc /mnt/datadisk0/chenqi/clion/doris/gensrc
```

4. 在 Clion 中打开 **Preferences -> Build, Execution, Deployment -> Toolchains** 中添加远程环境的相关 Toolchains，比如 cmake、gcc、g++、gdb 等。
**其中最关键的一点是需要在 **Environment file** 中 填写远程服务器 Doris 代码中的 **env.sh** 文件路径。**

![Toolchains](/images/clion-toolchains.png)

5. 在 Clion 中打开 **Preferences -> Build, Execution, Deployment -> CMake** ，在CMake options中添加编译选项-DDORIS_JAVA_HOME=/path/to/remote/JAVA_HOME，将DORIS_JAVA_HOME设置为远程服务器的JAVA_HOME路径，否则会找不到 jni.h。

6. 在 Clion 中右键点击 **Load Cmake Project**。此操作会同步代码到远程服务器上，并且调用生成相关 Cmake Build Files。

## 本地 Clion 运行调试远程 BE

1. 在 **Preferences -> Build, Execution, Deployment -> CMake** 中配置 CMake。可以配置类似于 Debug / Release 等不同的 Target， 其中 **ToolChain** 需要选择刚才配置的。
**如果要运行调试 Unit Test 的话，需要在 CMake Options 中配置上 `-DMAKE_TEST=ON`（该选项默认关闭，需要打开才会编译 Test 代码）**

2. 在远程服务器上将 Doris 源代码中的 `output` 目录拷贝到一个单独的路径下，比如 `/mnt/datadisk0/chenqi/clion/doris/doris_be/`。

```
cp -R /mnt/datadisk0/chenqi/doris/output /mnt/datadisk0/chenqi/clion/doris/doris_be
```

![Output Tree](/images/doris-dist-output-tree.png)

3. 在 Clion 中选择 doris_be 相关的 Target，比如 **Debug** 或者 **Release**，进行配置运行。

![Run Debug Conf1](/images/clion-run-debug-conf1.png)

参照 Doris 根目录下的 `be/bin/start_be.sh` 中 export 的环境变量进行环境变量配置。其中环境变量的值指向远程服务器对应的路径。
环境变量参考：

![Run Debug Conf2](/images/clion-run-debug-conf2.png)

4. 点击运行或者调试 BE。其中点击 **Run** 可以编译运行 BE，而点击 **Debug** 可以编译调试 BE。
