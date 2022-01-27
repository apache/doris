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
        WITH [BROKER `broker_name`|S3]
        ON LOCATION `repo_location`
        PROPERTIES ("key"="value", ...);

    Explain:
        1. The creation of warehouses depends on existing brokers, or use aws s3 protocl to connet cloud storage directly.
        2. If it is a read-only warehouse, it can only be restored on the warehouse. If not, you can backup and restore operations.
        3. According to the different types of broker or S3, PROPERTIES is different, see the example.

## example
    1. Create a warehouse named bos_repo, which relies on BOS broker "bos_broker", and the data root directory is: bos://palo_backup.
        CREATE REPOSITORY `bos_repo`
        WITH BROKER `bos_broker`
        ON LOCATION "bos://palo_backup"
        PROPERTIES
        (
        "bos_endpoint" ="http://gz.bcebos.com",
        "bos_accesskey" = "bos_accesskey",
        "bos_secret_accesskey"="bos_accesskey"
        );
    
    2. Create the same warehouse as in Example 1, but with read-only attributes:
        CREATE READ ONLY REPOSITORY `bos_repo`
        WITH BROKER `bos_broker`
        ON LOCATION "bos://palo_backup"
        PROPERTIES
        (
        "bos_endpoint" ="http://gz.bcebos.com",
        "bos_accesskey" = "bos_accesskey",
        "bos_secret_accesskey"="bos_secret_accesskey"
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

    4. 创建名为 s3_repo 的仓库, 直接链接云存储, 而不通过broker.
        CREATE REPOSITORY `s3_repo`
        WITH S3
        ON LOCATION "s3://s3-repo"
        PROPERTIES
        (
        "AWS_ENDPOINT" = "http://s3-REGION.amazonaws.com",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "REGION"
        );

## keyword
CREATE, REPOSITORY
