---
{
    "title": "CREATE REPOSITORY",
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

# CREATE REPOSITORY
## Description
This statement is used to create the warehouse. The warehouse is used for backup or recovery. Only root or superuser users can create warehouses.
Grammar:
CREATE [READ ONLY] REPOSITORY `repo_name`
WITH BROKER `broker_name`
ON LOCATION `repo_location`
PROPERTIES ("key"="value", ...);

Explain:
1. The creation of warehouses depends on existing brokers
2. If it is a read-only warehouse, it can only be restored on the warehouse. If not, you can backup and restore operations.
3. According to the different types of broker, PROPERTIES is different, see the example.

## example
1. Create a warehouse named bos_repo, which relies on BOS broker "bos_broker", and the data root directory is: bos://palo_backup.
CREATE REPOSITORY `bos_repo`
WITH BROKER `bos_broker`
ON LOCATION "bos://palo_backup"
PROPERTIES
(
"bosu endpoint" ="http://gz.bcebos.com",
"bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
"bos_secret_accesskey"="70999999999999de274d59eaa980a"
);

2. Create the same warehouse as in Example 1, but with read-only attributes:
CREATE READ ONLY REPOSITORY `bos_repo`
WITH BROKER `bos_broker`
ON LOCATION "bos://palo_backup"
PROPERTIES
(
"bosu endpoint" ="http://gz.bcebos.com",
"bos_accesskey" = "069fc2786e664e63a5f111111114ddbs22",
"bos_secret_accesskey"="70999999999999de274d59eaa980a"
);

3. Create a warehouse named hdfs_repo, which relies on Baidu HDFS broker "hdfs_broker", and the data root directory is: hdfs://hadoop-name-node:54310/path/to/repo./
CREATE REPOSITORY `hdfs_repo`
WITH BROKER `hdfs_broker`
ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
PROPERTIES
(
"Username" = "User"
"password" = "password"
);

## keyword
CREATE REPOSITORY
