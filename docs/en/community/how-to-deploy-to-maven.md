---
{
"title": "How to deploy to Maven Central Repository",
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

## Prepare

### 0. Requirements

1. apache account ID
2. apache password
3. gpg key

### 1. Prepare the local maven environment

1. Generate the master password: `mvn --encrypt-master-password <password>` This password is only used to encrypt other subsequent passwords, and the output is similar to `{VSb+6+76djkH/43...}` and then create` ~/.m2/settings-security.xml` file, the content is

   ```
   <settingsSecurity>
     <master>{VSb+6+76djkH/43...}</master>
   </settingsSecurity>
   ```

2. Encrypt apache password: `mvn --encrypt-password <password>` This password is the password of the apache account. The output is similar to the above `{GRKbCylpwysHfV...}` and added in `~/.m2/settings.xml`
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

3. Optional, (you can also pass -Darguments="-Dgpg.passphrase=xxxx" during deployment), add the following content in `~/.m2/settings.xml`, if the profiles tag already exists, this is only required Just add profile to profiles, activeProfiles is the same as above, xxxx is the passphrase of the gpg key
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
### Publish to SNAPSHOT
### 1. Deploy flink connector

Switch to the flink connector directory, let’s take flink version 1.11.6 and scalar 2.12 as examples

   ```
   cd incubator-doris/extension/flink-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   export FLINK_VERSION="1.11.6"
   export SCALA_VERSION="2.12"
   mvn deploy
   ```



### 2. Deploy Spark connector

Switch to the spark connector directory, let’s take spark version 2.3.4 and scalar 2.11 as examples

   ```
   cd incubator-doris/extension/spark-doris-connector
   export DORIS_HOME=$PWD/../../
   source ${DORIS_HOME}/env.sh
   if [ -f ${DORIS_HOME}/custom_env.sh ]; then source ${DORIS_HOME}/custom_env.sh; fi
   export SPARK_VERSION="2.3.4"
   export SCALA_VERSION="2.11"
   mvn deploy
   ```
