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
## 发布到SNAPSHOT
### 1. 部署 flink connector

切换到 flink connector 目录， 我们以 flink 版本 1.11.6， scalar 2.12 为例

   ```
   cd incubator-doris/extension/flink-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   mvn deploy -Dflink.version=1.11.6 -Dscala.version=2.12
   ```



### 2. 部署 Spark connector

切换到 spark connector 目录， 我们以 spark 版本 2.3.4， scalar 2.11 为例

   ```
   cd incubator-doris/extension/spark-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   mvn deploy -Dscala.version=2.11 -Dspark.version=2.3.4
   ```

## 发布到 Release

### 1. 准备GitHub权限
加密 github token 密码： `mvn --encrypt-password <token>` 这个token 是github 用户生成的访问apache仓库的token 输出cat类似`{bSSA+TC6wzEHNKukcOn...}` 在`~/.m2/settings.xml` 中的`<servers>` 标签中加入

```xml
    <server>
      <id>github</id>
      <username>github user name</username>
      <password>{bSSA+TC6wzEHNKukcOn...}</password>
    </server>
```

### 2. 发布到 maven staging
以发布Doris Flink Connector 1.0.0 为例， flink 版本是 1.13.5 scala 版本是2.12, 其他类似
```bash
cd extension/flink-doris-connector/
export DORIS_HOME=$PWD/../../
source ${DORIS_HOME}/env.sh
mvn release:clean -Dflink.version=1.13.5 -Dscala.version=2.12 
mvn release:prepare -Dflink.version=1.13.5 -Dscala.version=2.12
```
之后maven 需要输入三个信息 
   1. Doris Flink Connector 的版本信息， 我们默认就可以，可以直接回车或者输入自己想要的版本
   2. Doris Flink Connector 的release tag, 因为release 过程会在apache/incubator-doris中添加一个tag, 因此需要一个tag名称，如果默认，直接回车即可
   3. Doris Flink Connector 下一个版本的版本号，这里需要注意，由于我们的版本号是 {flink_version}_{scala_version}_1.0.0 因此 maven 的算法会将scala 版本号加一，如果我们下次想用1.0.1 这个版本 我们可以改成1.13.5-2.12-1.0.1-SNAPSHOT
```
...
[INFO] [prepare] 3/17 check-dependency-snapshots
[INFO] Checking dependencies and plugins for snapshots ...
[INFO] [prepare] 4/17 create-backup-poms
[INFO] [prepare] 5/17 map-release-versions
What is the release version for "Doris Flink Connector"? (org.apache.doris:doris-flink-connector) 1.13.5-2.12-1.0.0: :
[INFO] [prepare] 6/17 input-variables
What is the SCM release tag or label for "Doris Flink Connector"? (org.apache.doris:doris-flink-connector) doris-flink-connector-1.13.5-2.12-1.0.0: :
[INFO] [prepare] 7/17 map-development-versions
What is the new development version for "Doris Flink Connector"? (org.apache.doris:doris-flink-connector) 1.13.5-3.12-1.0.0-SNAPSHOT: : 1.13.5-2.12-1.0.1-SNAPSHOT
...
[INFO] [prepare] 17/17 end-release
[INFO] Release preparation complete.
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  01:00 min
[INFO] Finished at: 2022-01-05T15:01:55+08:00
[INFO] ------------------------------------------------------------------------
```
prepare 之后 perform即可， perform 之后就在`https://repository.apache.org/` 里面的stagingRepositories 找到刚刚发布的版本
![](/images/staging_repo.png)

### 3. 投票
投票前需要先close staging
![](/images/close_staging.png)
之后就可以在dev邮件组发起投票， 下面是一个邮件示例
```
Hi,

We are ready to deploy Doris connectors to Maven Central Repository, as the version 1.0.0, and the next version will be 1.0.1
This Release contains Spark connectors and Flink connectors for spark 2/3 and flink 1.11/1.12/1.13

GitHub tags:
https://github.com/apache/incubator-doris/releases/tag/doris-spark-connector-3.1.2-2.12-1.0.0
https://github.com/apache/incubator-doris/releases/tag/doris-spark-connector-2.3.4-2.11-1.0.0
https://github.com/apache/incubator-doris/releases/tag/doris-flink-connector-1.13.5-2.12-1.0.0
https://github.com/apache/incubator-doris/releases/tag/doris-flink-connector-1.12.7-2.12-1.0.0
https://github.com/apache/incubator-doris/releases/tag/doris-flink-connector-1.11.6-2.12-1.0.0

Staging repo:
https://repository.apache.org/content/repositories/orgapachedoris-1000

Vote open for at least 72 hours.

[ ] +1
[ ] +0
[ ] -1
```

投票通过后就可以发布到 Maven Central 了
![](/images/release-stage.png)
