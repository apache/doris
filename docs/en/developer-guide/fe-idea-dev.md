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

## Prerequisites

* Git
* JDK1.8+
* IntelliJ IDEA
* Maven (Optional, IDEA shipped embedded Maven3)

## Clone Code

Git clone codebase from https://github.com/apache/incubator-doris.git

## Code Generation

If your are only interested in FE module, and for some reason you can't or don't want to compile full thirdparty libraries, 
the minimum tool required for FE module is `thrift`, so you can manually install `thrift` and copy or create a link of 
the executable `thrift` command to `./thirdparty/installed/bin`.

Doris build against `thrift` 0.9.3, and `thrift` 0.9.3.1 should also work well, but the newer version will not.

If your are using macOS, try `brew install thrift@0.9` and will get `thrift` 0.9.3.1 installed at `/usr/local/opt/thrift@0.9/bin/thrift`, 
then create a soft link to `./thirdparty/installed/bin/thrift`.
        
For Windows users, download `thrift` 0.9.3 from `http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.exe`, 
and put it into `thirdparty/installed/bin/` folder.

Go to `./fe` folder and run the following maven command to generate sources.

```
mvn generate-sources
```

If fails, run following command first to try to install modules into maven local repository, then re-run above command.

```
mvn install -DskipTests
```

You can also use IDE embedded GUI tools to run maven command to generate sources

![](/images/gen_code.png)

If you are developing on the OS which lack of support to run `shell script` and `make` such as Windows, a workround here 
is generate codes in Linux and copy them back. Using Docker should also be an option.

## Import into IDEA

1. Import `./fe` into IDEA

2. Follow the picture to create the folders and copy files under `webroot` into it.

![](/images/DEBUG4.png)

## Custom FE configuration

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

## Setting Environment Variables

Follow the picture to set runtime Environment Variables in IDEA

![](/images/DEBUG5.png)

## Start FE

Having fun with Doris FE!
