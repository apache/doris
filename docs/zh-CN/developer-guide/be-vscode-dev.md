---
{
    "title": "Doris BE开发调试环境 -- vscode",
    "language": "zh-CN"
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

# Apache Doris Be 开发调试

## 前期准备工作

**本教程是在 Ubuntu 20.04 下进行的**

1. 下载 doris 源代码

   下载地址为：[apache/incubator-doris: Apache Doris (Incubating) (github.com)](https://github.com/apache/incubator-doris)

2. 安装 GCC 8.3.1+，Oracle JDK 1.8+，Python 2.7+，确认 gcc, java, python 命令指向正确版本, 设置  JAVA_HOME 环境变量

3. 安装其他依赖包

```
sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python brotli
sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
sudo apt update
sudo apt install gcc-10 g++-10 
sudo apt-get install autoconf automake libtool autopoint
```

4. 安装 openssl-devel

```
sudo apt install -y openssl-devel
```

## 编译

以下操作步骤在 /home/workspace 目录下进行

1. 下载源码

```
git clone https://github.com/apache/incubator-doris.git 
```

2. 编译第三方依赖包

```
 cd /home/workspace/incubator-doris/thirdparty
 ./build-thirdparty.sh
```

3. 编译doris产品代码

```
cd /home/workspace/incubator-doris
./build.sh
```

注意：这个编译有以下几条指令：

```
./build.sh  #同时编译be 和fe
./build.sh  --be #只编译be
./build.sh  --fe #只编译fe
./build.sh  --fe --be#同时编译be fe
./build.sh  --fe --be --clean#删除并同时编译be fe
./build.sh  --fe  --clean#删除并编译fe
./build.sh  --be  --clean#删除并编译be
./build.sh  --be --fe  --clean#删除并同时编译be fe
```

如果不出意外，应该会编译成功，最终的部署文件将产出到  /home/workspace/incubator-doris/output/ 目录下。如果还遇到其他问题，可以参照 doris 的安装文档 http://doris.apache.org。

## 部署调试

1. 给be编译结果文件授权

```
chmod  /home/workspace/incubator-doris/output/be/lib/palo_be
```

注意： /home/workspace/incubator-doris/output/be/lib/palo_be为be的执行文件。

2. 创建数据存放目录

通过查看/home/workspace/incubator-doris/output/be/conf/be.conf

```
# INFO, WARNING, ERROR, FATAL
sys_log_level = INFO
be_port = 9060
be_rpc_port = 9070
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060

# Note that there should at most one ip match this list.
# If no ip match this rule, will choose one randomly.
# use CIDR format, e.g. 10.10.10.0/
# Default value is empty.
priority_networks = 192.168.59.0/24 # data root path, seperate by ';'
storage_root_path = /soft/be/storage 
# sys_log_dir = ${PALO_HOME}/log
# sys_log_roll_mode = SIZE-MB-
# sys_log_roll_num =
# sys_log_verbose_modules =
# log_buffer_level = -
# palo_cgroups 
```

需要创建一个文件夹，这是be数据存放的地方

```
mkdir -p /soft/be/storage
```

3. 打开 vscode，并打开 be 源码所在目录，在本案例中打开目录为 **/home/workspace/incubator-doris/**

4. 安装 vscode ms c++ 调试插件

![](/images/image-20210618104004956.png)

5. 创建 launch.json 文件，文件内容如下：

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "(gdb) Launch",
            "type": "cppdbg",
            "request": "launch",
            "program": "/home/workspace/incubator-doris/output/be/lib/palo_be",
            "args": [],
            "stopAtEntry": false,
            "cwd": "/home/workspace/incubator-doris/",
            "environment": [{"name":"PALO_HOME","value":"/home/workspace/incubator-doris/output/be/"},
                            {"name":"UDF_RUNTIME_DIR","value":"/home/workspace/incubator-doris/output/be/lib/udf-runtime"},
                            {"name":"LOG_DIR","value":"/home/workspace/incubator-doris/output/be/log"},
                            {"name":"PID_DIR","value":"/home/workspace/incubator-doris/output/be/bin"}
                           ],
            "externalConsole": true,
            "MIMode": "gdb",
            "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ]
        }
    ]
}
```

其中，environment 定义了几个环境变量 DORIS_HOME  UDF_RUNTIME_DIR LOG_DIR  PID_DIR，这是 palo_be 运行时需要的环境变量，如果没有设置，启动会失败。

**注意：如果希望是attach(附加进程）调试，配置代码如下：**

```
{
    "version": "0.2.0",
    "configurations": [
      	{
          "name": "(gdb) Launch",
          "type": "cppdbg",
          "request": "attach",
          "program": "/home/workspace/incubator-doris/output/lib/palo_be",
          "processId":,
          "MIMode": "gdb",
          "internalConsoleOptions":"openOnSessionStart",
          "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ]
        }
    ]
}
```

配置中 **"request": "attach"，   "processId":PID**，这两个配置是重点： 分别设置 gdb 的调试模式为 attach，附加进程的processId，否则会失败。如何查找进程id，可以在命令行中输入以下命令：

```
ps -ef | grep palo*
```

如图：

![](/images/image-20210618095240216.png)

其中的15200即为当前运行的 be 的进程 id.

一个完整的 lainch.json 的例子如下：

```
 {
    "version": "0.2.0",
    "configurations": [
        {
            "name": "(gdb) Attach",
            "type": "cppdbg",
            "request": "attach",
            "program": "/home/workspace/incubator-doris/output/be/lib/palo_be",
            "processId": 17016,
            "MIMode": "gdb",
            "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ]
        },
        {
            "name": "(gdb) Launch",
            "type": "cppdbg",
            "request": "launch",
            "program": "/home/workspace/incubator-doris/output/be/lib/palo_be",
            "args": [],
            "stopAtEntry": false,
            "cwd": "/home/workspace/incubator-doris/output/be",
            "environment": [
                {
                    "name": "DORIS_HOME",
                    "value": "/home/workspace/incubator-doris/output/be"
                },
                {
                    "name": "UDF_RUNTIME_DIR",
                    "value": "/home/workspace/incubator-doris/output/be/lib/udf-runtime"
                },
                {
                    "name": "LOG_DIR",
                    "value": "/home/workspace/incubator-doris/output/be/log"
                },
                {
                    "name": "PID_DIR",
                    "value": "/home/workspace/incubator-doris/output/be/bin"
                }
            ],
            "externalConsole": false,
            "MIMode": "gdb",
            "setupCommands": [
                {
                    "description": "Enable pretty-printing for gdb",
                    "text": "-enable-pretty-printing",
                    "ignoreFailures": true
                }
            ]
        }
    ]
}
```

6. 点击调试即可

   下面就可以开始你的 Doris DEBUG 之旅了

![](/images/image-20210618091006146.png)

