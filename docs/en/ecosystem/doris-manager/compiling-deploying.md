---
{
    "title": "Compile and deploy",
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

# Compile and deploy

## Compile

Running the build.sh script under the manager path directly will generate the installation and running package -- output under the manager path, including:
1. Doris Manager's running package doris-manager.jar
2. The running configuration folder conf
3. Start the script start_manager.sh
4. Stop the script stop_manager.sh

## Run

### 1 Configuration

Enter the generated installation and running package, view the configuration file conf path, and open the configuration file manager.conf in the path. The configuration items to focus on are as follows:

````$xslt
The service's startup http port
STUDIO_PORT=8080

The type of database where the backend data is stored, including mysql/h2/postgresql. The default is to support mysql
MB_DB_TYPE=mysql

Database connection information
If it is a configured h2 type database, you do not need to configure this information, and the data will be stored locally as a local file
h2 data file storage path, directly stored in the current path by default
H2_FILE_PATH=

If it is mysql/postgresql, you need to configure the following connection information
database address
MB_DB_HOST=

database port
MB_DB_PORT=3306

database access port
MB_DB_USER=

Database access password
MB_DB_PASS=

database name of the database
MB_DB_DBNAME=

The path where the service runs, which is directly stored in the log folder of the current running path by default.
LOG_PATH=

The length of the waiting queue of the web container, the default is 100. The queue is also used as a buffer pool, but it cannot be infinitely long. It not only consumes memory, but also consumes CPU when entering the queue.
WEB_ACCEPT_COUNT=100

The maximum number of worker threads for the web container, 200 by default. (usually the number of CPU cores * 200)
WEB_MAX_THREADS=200

The minimum number of working idle threads for the web container, the default is 10. (Appropriately increase some to cope with the sudden increase in traffic)
WEB_MIN_SPARE_THREADS=10

The maximum number of connections for the web container, the default is 10000. (Appropriately increase some to cope with the sudden increase in traffic)
WEB_MAX_CONNECTIONS=10000

The maximum number of connections to access the database connection pool, the default is 10
DB_MAX_POOL_SIZE=20

The minimum number of idle connections to access the database connection pool, the default is 10
DB_MIN_IDLE=10
````

### 2 Start

After the configuration modification is completed, go back to the installation and run package and run the following command directly

````$xslt
nohup sh ./start_manager.sh > start.log 2>&1 &
````

Check the logs in the logs to determine whether the program started successfully

### 3 Use

Doris Manager presets a super administrator user with the following information:

````$xslt
Username: Admin
Password: Admin@123
````

To ensure safe use, please change your password after logging in!
