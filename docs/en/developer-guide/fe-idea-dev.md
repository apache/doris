---
{
    "title": "Setting Up dev env for FE - IntelliJ IDEA",
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

# Setting Up Development Environment for FE using IntelliJ IDEA

## 1. Environmental Preparation

* Git
* JDK1.8+
* IntelliJ IDEA
* Maven (Optional, IDEA shipped embedded Maven3)

1. Git clone codebase from https://github.com/apache/incubator-doris.git


2. Use IntelliJ IDEA to open the code root directory


3. If your are only interested in FE module, and for some reason you can't or don't want to compile full thirdparty libraries,
   the minimum tool required for FE module is `thrift`, so you can manually install `thrift` and copy or create a link of
   the executable `thrift` command to `./thirdparty/installed/bin`.
   ```
   Doris build against `thrift` 0.13.0 ( note : `Doris` 0.15 and later version build against `thrift` 0.13.0 , the previous version is still `thrift` 0.9.3)   
   
   Windows: 
      1. Download：`http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
      2. Copy：copy the file to `./thirdparty/installed/bin`
      
   MacOS: 
      1. Download：`brew install thrift@0.13.0`
      2. Establish soft connection： 
        `mkdir -p ./thirdparty/installed/bin`
        `ln -s /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`
      
   Note：The error that the version cannot be found may be reported when MacOS execute `brew install thrift@0.13.0`. The solution is execute at the terminal as follows:
      1. `brew tap-new $USER/local-tap`
      2. `brew extract --version='0.13.0' thrift $USER/local-tap`
      3. `brew install thrift@0.13.0`
   Reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
   ```

4. Go to `./fe` folder and run the following maven command to generate sources.

   ```
   mvn generate-sources
   ```
   
   If fails, run following command.
   
   ```
   mvn clean install -DskipTests
   ```
   
   You can also use IDE embedded GUI tools to run maven command to generate sources

![](/images/gen_code.png)

If you are developing on the OS which lack of support to run `shell script` and `make` such as Windows, a workround here 
is generate codes in Linux and copy them back. Using Docker should also be an option.

## 2. Debug

1. Import `./fe` into IDEA

2. Follow the picture to create the folders (The directory may exist in the new version. If it exists, skip it, otherwise create it.)

![](/images/DEBUG4.png)

3. Build `ui` project , and copy files from directory `ui/dist` into directory `webroot` ( you can skip this step , if you don't need `Doris` UI )

## 3. Custom FE configuration

Copy below content into `conf/fe.conf` and tune it to fit your environment.

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

## 4. Setting Environment Variables

Follow the picture to set runtime Environment Variables in IDEA

![](/images/DEBUG5.png)

## 5. Start FE

Having fun with Doris FE!
