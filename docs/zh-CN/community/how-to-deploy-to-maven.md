---
{
"title": "如何部署到Maven 仓库",
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

## 准备

### 0. Requirements

1. apache 账号ID
2. apache 账号密码
3. gpg key

### 1. 准备本地maven 环境

1. 生成主密码： `mvn --encrypt-master-password <password>`   这个密码仅用作加密后续的其他密码使用, 输出类似 `{VSb+6+76djkH/43...}` 之后创建 `~/.m2/settings-security.xml` 文件，内容是

   ```
   <settingsSecurity>
     <master>{VSb+6+76djkH/43...}</master>
   </settingsSecurity>
   ```

2. 加密 apache 密码： `mvn --encrypt-password <password>` 这个密码是apache 账号的密码 输出和上面类似`{GRKbCylpwysHfV...}` 在`~/.m2/settings.xml` 中加入

   ```
     <servers>
       <!-- To publish a snapshot of your project -->
       <server>
         <id>apache.snapshots.https</id>
         <username>yangzhg</username>
         <password>{GRKbCylpwysHfV...}</password>
       </server>
       <!-- To stage a release of your project -->
       <server>
         <id>apache.releases.https</id>
         <username>yangzhg</username>
         <password>{GRKbCylpwysHfV...}</password>
       </server>
     </servers>
   ```

3. 可选，（也可以在部署时传递-Darguments="-Dgpg.passphrase=xxxx"），在`~/.m2/settings.xml` 中加入如下内容，如果已经存在profiles 标签， 这只需要将profile  加入profiles 中即可，activeProfiles 同上， xxxx 是gpg 密钥的passphrase

   ```
     <profiles>
       <profile>
         <id>gpg</id>
         <properties>
           <gpg.executable>gpg</gpg.executable>
           <gpg.passphrase>xxxx</gpg.passphrase>
         </properties>
       </profile>
     </profiles>
     <activeProfiles>
       <activeProfile>gpg</activeProfile>
     </activeProfiles>
   ```
### 发布到SNAPSHOT
### 1. 部署 flink connector

切换到 flink connector 目录， 我们以 flink 版本 1.11.6， scalar 2.12 为例

   ```
   cd incubator-doris/extension/flink-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   export FLINK_VERSION="1.11.6"
   export SCALA_VERSION="2.12"
   mvn deploy
   ```



### 2. 部署 Spark connector

切换到 spark connector 目录， 我们以 spark 版本 2.3.4， scalar 2.11 为例

   ```
   cd incubator-doris/extension/spark-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   export SPARK_VERSION="2.3.4"
   export SCALA_VERSION="2.11"
   mvn deploy
   ```
