---
{
    "title": "CREATE-REPOSITORY",
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

## CREATE-REPOSITORY

### Name

CREATE REPOSITORY

### Description

```text
This statement is used to create a repository. Repositories are used for backup or restore. Only root or superuser users can create repositories.
grammar:
     CREATE [READ ONLY] REPOSITORY `repo_name`
     WITH [BROKER `broker_name`|S3]
     ON LOCATION `repo_location`
     PROPERTIES ("key"="value", ...);
        
illustrate:
     1. The creation of the warehouse depends on the existing broker or directly access the cloud storage through the AWS s3 protocol
     2. If it is a read-only warehouse, recovery can only be performed on the warehouse. If not, backup and restore operations are available.
     3. According to the different types of broker or S3, PROPERTIES are different, see the example for details.
```

### Example

```text
1. Create a warehouse named bos_repo, rely on BOS broker "bos_broker", and the data root directory is: bos://palo_backup
    CREATE REPOSITORY `bos_repo`
    WITH BROKER `bos_broker`
    ON LOCATION "bos://palo_backup"
    PROPERTIES
    (
        "bos_endpoint" = "http://gz.bcebos.com",
        "bos_accesskey" = "bos_accesskey",
        "bos_secret_accesskey"="bos_secret_accesskey"
    );
 
2. Create the same repository as Example 1, but with read-only properties:
    CREATE READ ONLY REPOSITORY `bos_repo`
    WITH BROKER `bos_broker`
    ON LOCATION "bos://palo_backup"
    PROPERTIES
    (
        "bos_endpoint" = "http://gz.bcebos.com",
        "bos_accesskey" = "bos_accesskey",
        "bos_secret_accesskey"="bos_accesskey"
    );

3. Create a warehouse named hdfs_repo, rely on Baidu hdfs broker "hdfs_broker", the data root directory is: hdfs://hadoop-name-node:54310/path/to/repo/
    CREATE REPOSITORY `hdfs_repo`
    WITH BROKER `hdfs_broker`
    ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
    PROPERTIES
    (
        "username" = "user",
        "password" = "password"
    );

4. Create a repository named s3_repo to link cloud storage directly without going through the broker.
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
```

### Keywords

    CREATE, REPOSITORY

### Best Practice

1. A cluster can create multiple warehouses. Only users with ADMIN privileges can create repositories.
2. Any user can view the created repositories through the [SHOW REPOSITORIES](../../Show-Statements/SHOW-REPOSITORIES.html) command.
3. When performing data migration operations, it is necessary to create the exact same warehouse in the source cluster and the destination cluster, so that the destination cluster can view the data snapshots backed up by the source cluster through this warehouse.
