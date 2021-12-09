---
{
    "title": "FE 开发环境搭建 - IntelliJ IDEA",
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

# 使用 IntelliJ IDEA 搭建 FE 开发环境

## 1.环境准备

JDK1.8+, IntelliJ IDEA

1. 从 https://github.com/apache/incubator-doris.git 下载源码到本地

2. 使用IntelliJ IDEA 打开代码根目录

3. 如果仅进行fe开发而没有编译过thirdparty，则需要安装thrift，并将thrift 复制或者连接到 `thirdparty/installed/bin` 目录下

        安装 `thrift` 0.13.0 版本(注意：`Doris` 0.15 和最新的版本基于 `thrift` 0.13.0 构建, 之前的版本依然使用`thrift` 0.9.3 构建)

        Windows: 
           1. 下载：`http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
           2. 拷贝：将文件拷贝至 `./thirdparty/installed/bin`
        
        MacOS: 
           1. 下载：`brew install thrift@0.13.0`
           2. 建立软链接： 
              `mkdir -p ./thirdparty/installed/bin`
              `ln -s /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`
        
        注：MacOS执行 `brew install thrift@0.13.0` 可能会报找不到版本的错误，解决方法如下，在终端执行：
           1. `brew tap-new $USER/local-tap`
           2. `brew extract --version='0.13.0' thrift $USER/local-tap`
           3. `brew install thrift@0.13.0`
        参考链接: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`

4. 如果是Mac 或者 Linux 环境 可以通过 如下命令生成自动生成代码：

    ```
    cd fe
    mvn generate-sources
    ```

    如果出现错误，则执行：

    ```
    cd fe && mvn clean install -DskipTests
    ```

或者通过图形界面运行 maven 命令生成

![](/images/gen_code.png)

如果使用windows环境可能会有make命令和sh脚本无法执行的情况 可以通过拷贝linux上的 `fe/fe-core/target/generated-sources` 目录拷贝到相应的目录的方式实现，也可以通过docker 镜像挂载本地目录之后，在docker 内部生成自动生成代码，可以参照编译一节

## 2.调试

1. 用idea导入fe工程

2. 在fe目录下创建下面红框标出的目录（在新版本中该目录可能存在，如存在则跳过，否则创建）

![](/images/DEBUG4.png)

3. 编译`ui`项目，将 `ui/dist/`目录中的文件拷贝到`webroot`中（如果你不需要看`Doris` UI，这一步可以跳过）

## 3.配置conf/fe.conf

下面是我自己的配置，你可以根据自己的需要进行修改

```
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#####################################################################
## The uppercase properties are read and exported by bin/start_fe.sh.
## To see all Frontend configurations,
## see fe/src/org/apache/doris/common/Config.java
#####################################################################

# the output dir of stderr and stdout 
LOG_DIR = ${DORIS_HOME}/log

DATE = `date +%Y%m%d-%H%M%S`
JAVA_OPTS="-Xmx2048m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$DATE"

# For jdk 9+, this JAVA_OPTS will be used as default JVM options
JAVA_OPTS_FOR_JDK_9="-Xmx4096m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$DATE:time"

##
## the lowercase properties are read by main program.
##

# INFO, WARN, ERROR, FATAL
sys_log_level = INFO

# store metadata, create it if it is not exist.
# Default value is ${DORIS_HOME}/doris-meta
# meta_dir = ${DORIS_HOME}/doris-meta

http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
mysql_service_nio_enabled = true

# Choose one if there are more than one ip except loopback address. 
# Note that there should at most one ip match this list.
# If no ip match this rule, will choose one randomly.
# use CIDR format, e.g. 10.10.10.0/24
# Default value is empty.
# priority_networks = 10.10.10.0/24;192.168.0.0/16

# Advanced configurations 
# log_roll_size_mb = 1024
# sys_log_dir = ${DORIS_HOME}/log
# sys_log_roll_num = 10
# sys_log_verbose_modules = 
# audit_log_dir = ${DORIS_HOME}/log
# audit_log_modules = slow_query, query
# audit_log_roll_num = 10
# meta_delay_toleration_second = 10
# qe_max_connection = 1024
# max_conn_per_user = 100
# qe_query_timeout_second = 300
# qe_slow_log_ms = 5000

```



## 4.设置环境变量

在IDEA中设置运行环境变量

![](/images/DEBUG5.png)

## 5.启动fe

下面你就可以愉快的启动，调试你的FE了

