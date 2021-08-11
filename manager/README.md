
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

# Apache Doris Manager (incubating)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Apache Doris Manager is used to manage the doris cluster, such as installing the cluster, upgrading the cluster, starting and stopping services, etc.

## 1. Compile and install

### Step1: build
```
$ mvn clean package -DskipTests
```
After compilation, a tar package will be generated in the assembly/output/ directory, For example doris-manager-1.0.0.tar.gz

### Step2: install manager server
Copy tar package to the the machine where the manager server needs to be installed, Start the manager service after decompression
```
$ tar zxf doris-manager-1.0.0.tar.gz
$ sh bin/startup.sh
```

### Step3: install agent service
After decompressing the tar package in the second step, there will be a directory called agent. copy the agent directory to the
machine where the agent service is installed,
```
$ sh bin/agent_start.sh --agent ${agentIp} --server ${serverIp}:9601
```
${agentIp} is the IP of the machine where the agent is located, ${serverIp} is the IP of the machine where the manager server is located

