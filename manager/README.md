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

Apache Doris Manager is used to manage the doris cluster, such as monitoring cluster, installing the cluster, upgrading the cluster, starting and stopping services, etc.

## 1. Compile and install

### Step1: build
```
$ sh build.sh
```
Compiles the front and back ends of the project. After compilation, a tar package will be generated in the output/ directory and doris-manager-1.0.0.tar.gz package.The content of the compiled output is:
```
agent/, doris manger agent
    bin/
        agent_start.sh, doris manger agent startup script
        agent_stop.sh, doris manger agent stop script
        install_be.sh, doris be install script
        install_fe.sh, doris fe install script
        install_broker.sh, doris broker install script
        process_exist.sh, doris process detection script
    lib/
        dm-agent.jar, executable package of doris manger agent
server/, doris manger server
    conf/
        manager.conf, doris manger server configuration file
    web-resource/, doris manger server front static resources
    lib/
        doris-manager.jar, executable package of doris manger server
    bin/
        start_manager.sh, doris manger server startup script
        stop_manager.sh, doris manger server stop script
```

### Step2: install manager server
#### 1) Unzip the installation package
Copy doris-manager-1.0.0.tar.gz tar package to the the machine where the manager server needs to be installed.
```
$ tar -zxvf doris-manager-1.0.0.tar.gz
$ cd server
```
#### 2) Modify profile
Edit conf/manager.conf
```$xslt
Start HTTP port of the service
STUDIO_PORT=8080

The type of database where back-end data is stored, including MySQL / H2 / PostgreSQL. MySQL is supported by default
MB_DB_TYPE=mysql

Database connection information

If you are configuring a H2 type database, you do not need to configure this information, and the data will be stored locally as a local file

For MySQL / PostgreSQL, you need to configure the following connection information

Database address
MB_DB_HOST=

Database port
MB_DB_PORT=3306

Database access port
MB_DB_USER=

Database access password
MB_DB_PASS=123456

Database name of the database
MB_DB_DBNAME
```

#### 3) Start manager server
Start the manager service after decompression and configuration.
```
$ sh bin/start_manager.sh
```
#### 4) User manager server
Browser access ${serverIp}:8080, Manger server has a preset super administrator user. The information is as follows:
```
user name: Admin
password: Admin@123
(Case sensitive)
```