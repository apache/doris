---
{ 'title': 'Doris BE开发调试环境 -- vscode', 'language': 'zh-CN' }
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

**文中的出现的 BE 二进制文件名称 `doris_be`，在之前的版本中为 `palo_be`。**

1. 下载 doris 源代码

    下载地址为：[apache/doris: Apache Doris (github.com)](https://github.com/apache/doris)

2. 安装 GCC 8.3.1+，Oracle JDK 1.8+，Python 2.7+，确认 gcc, java, python 命令指向正确版本, 设置 JAVA_HOME 环境变量

3. 安装其他依赖包

```
sudo apt install build-essential openjdk-8-jdk maven cmake byacc flex automake libtool-bin bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build python brotli
sudo add-apt-repository ppa:ubuntu-toolchain-r/ppa
sudo apt update
sudo apt install gcc-10 g++-10
sudo apt-get install autoconf automake libtool autopoint
```

4. 安装 openssl libssl-dev

```
sudo apt install -y openssl libssl-dev
```

## 编译

以下操作步骤在 /home/workspace 目录下进行

1. 下载源码

```
git clone https://github.com/apache/doris.git
cd doris
git submodule update --init --recursive
```

2. 编译第三方依赖包

```
 cd /home/workspace/doris/thirdparty
 ./build-thirdparty.sh
```

3. 编译 doris 产品代码

```
cd /home/workspace/doris
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

如果不出意外，应该会编译成功，最终的部署文件将产出到 /home/workspace/doris/output/ 目录下。如果还遇到其他问题，可以参照 doris 的安装文档 http://doris.apache.org。

## 部署调试(GDB)

1. 给 be 编译结果文件授权

```
chmod  /home/workspace/doris/output/be/lib/doris_be
```

注意： /home/workspace/doris/output/be/lib/doris_be 为 be 的执行文件。

2. 创建数据存放目录

通过查看/home/workspace/doris/output/be/conf/be.conf

```
# INFO, WARNING, ERROR, FATAL
sys_log_level = INFO
be_port = 9060
be_rpc_port = 9070
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
arrow_flight_sql_port = -1

# Note that there should at most one ip match this list.
# If no ip match this rule, will choose one randomly.
# use CIDR format, e.g. 10.10.10.0/
# Default value is empty.
priority_networks = 192.168.59.0/24 # data root path, separate by ';'
storage_root_path = /soft/be/storage
# sys_log_dir = ${PALO_HOME}/log
# sys_log_roll_mode = SIZE-MB-
# sys_log_roll_num =
# sys_log_verbose_modules =
# log_buffer_level = -
# palo_cgroups
```

需要创建一个文件夹，这是 be 数据存放的地方

```
mkdir -p /soft/be/storage
```

3. 打开 vscode，并打开 be 源码所在目录，在本案例中打开目录为 **/home/workspace/doris/**

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
            "program": "/home/workspace/doris/output/be/lib/doris_be",
            "args": [],
            "stopAtEntry": false,
            "cwd": "/home/workspace/doris/",
            "environment": [{"name":"PALO_HOME","value":"/home/workspace/doris/output/be/"},
                            {"name":"UDF_RUNTIME_DIR","value":"/home/workspace/doris/output/be/lib/udf-runtime"},
                            {"name":"LOG_DIR","value":"/home/workspace/doris/output/be/log"},
                            {"name":"PID_DIR","value":"/home/workspace/doris/output/be/bin"}
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

其中，environment 定义了几个环境变量 DORIS_HOME UDF_RUNTIME_DIR LOG_DIR PID_DIR，这是 doris_be 运行时需要的环境变量，如果没有设置，启动会失败。

**注意：如果希望是 attach(附加进程）调试，配置代码如下：**

```
{
    "version": "0.2.0",
    "configurations": [
      	{
          "name": "(gdb) Launch",
          "type": "cppdbg",
          "request": "attach",
          "program": "/home/workspace/doris/output/lib/doris_be",
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

配置中 **"request": "attach"， "processId":PID**，这两个配置是重点： 分别设置 gdb 的调试模式为 attach，附加进程的 processId，否则会失败。以下命令可以直接提取进程ID：

```
lsof -i | grep -m 1 doris_be | awk "{print $2}"
```

或者写作 **"processId": "${command:pickProcess}"**，可在启动attach时指定pid.

如图：

![](/images/image-20210618095240216.png)

其中的 15200 即为当前运行的 be 的进程 id.

一个完整的 launch.json 的例子如下：

```
 {
    "version": "0.2.0",
    "configurations": [
        {
            "name": "(gdb) Attach",
            "type": "cppdbg",
            "request": "attach",
            "program": "/home/workspace/doris/output/be/lib/doris_be",
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
            "program": "/home/workspace/doris/output/be/lib/doris_be",
            "args": [],
            "stopAtEntry": false,
            "cwd": "/home/workspace/doris/output/be",
            "environment": [
                {
                    "name": "DORIS_HOME",
                    "value": "/home/workspace/doris/output/be"
                },
                {
                    "name": "UDF_RUNTIME_DIR",
                    "value": "/home/workspace/doris/output/be/lib/udf-runtime"
                },
                {
                    "name": "LOG_DIR",
                    "value": "/home/workspace/doris/output/be/log"
                },
                {
                    "name": "PID_DIR",
                    "value": "/home/workspace/doris/output/be/bin"
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

## 调试(LLDB)

lldb的attach比gdb更快，使用方式和gdb类似。vscode需要安装的插件改为`CodeLLDB`，然后在launch中加入如下配置:
```json
{
    "name": "CodeLLDB attach",
    "type": "lldb",
    "request": "attach",
    "program": "${workspaceFolder}/output/be/lib/doris_be",
    "pid":"${command:pickMyProcess}"
}
```
需要注意的是，此方式要求系统`glibc`版本为`2.18+`。如果没有则可以参考 [如何使CodeLLDB在CentOS7下工作](https://gist.github.com/JaySon-Huang/63dcc6c011feb5bd6deb1ef0cf1a9b96) 安装高版本glibc并将其链接到插件。

## 调试core dump文件

有时我们需要调试程序崩溃产生的core文件，这同样可以在vscode中完成，此时只需要在对应的configuration项中添加
```json
    "coreDumpPath": "/PATH/TO/CORE/DUMP/FILE"
```
即可。

## 常用调试技巧

### 函数执行路径

当对BE的执行细节不熟悉时，可以使用`perf`等相关工具追踪函数调用，找出调用链。`perf`的使用可以在[调试工具](./debug-tool.md)中找到。这时候我们可以在较大的表上执行需要追踪的sql语句，然后增大采样频率（例如，`perf -F 999`）。观察结果可以大致得到sql在BE执行的关键路径。

### 调试CRTP对象

BE代码为了提高运行效率，在基础类型中大量采用了CRTP（奇异递归模板模式），导致debugger无法按照派生类型调试对象。此时我们可以使用GDB这样解决这一问题：

假设我们需要调试`IColumn`类型的对象`col`，不知道它的实际类型，那么可以：

```powershell
set print object on # 按照派生类型输出对象
p *col.t # 此时使用col.t即可得到col的具体类型
p col.t->size() # 可以按照派生类型去使用它，例如ColumnString我们可以调用size()
......
```

注意：具有多态效果的是指针`COW::t`而非`IColumn`类对象，所以我们需要在GDB中将所有对`col`的使用替换为`col.t`才可以真正得到派生类型对象。
