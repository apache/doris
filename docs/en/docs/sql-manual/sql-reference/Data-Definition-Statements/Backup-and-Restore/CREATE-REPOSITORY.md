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

This statement is used to create a repository. Repositories are used for backup or restore. Only root or superuser users can create repositories.

grammar:

```sql
CREATE [READ ONLY] REPOSITORY `repo_name`
WITH [BROKER `broker_name`|S3|hdfs]
ON LOCATION `repo_location`
PROPERTIES ("key"="value", ...);
```

illustrate:

- Creation of repositories, relying on existing brokers or accessing cloud storage directly through AWS s3 protocol, or accessing HDFS directly.
- If it is a read-only repository, restores can only be done on the repository. If not, backup and restore operations are available.
- PROPERTIES are different according to different types of broker or S3 or hdfs, see the example for details.
- ON LOCATION ： if it is S3 , here followed by the Bucket Name.

### Example

1. Create a warehouse named bos_repo, rely on BOS broker "bos_broker", and the data root directory is: bos://palo_backup

```sql
CREATE REPOSITORY `bos_repo`
WITH BROKER `bos_broker`
ON LOCATION "bos://palo_backup"
PROPERTIES
(
    "bos_endpoint" = "http://gz.bcebos.com",
    "bos_accesskey" = "bos_accesskey",
    "bos_secret_accesskey"="bos_secret_accesskey"
);
```

2. Create the same repository as Example 1, but with read-only properties:

```sql
CREATE READ ONLY REPOSITORY `bos_repo`
WITH BROKER `bos_broker`
ON LOCATION "bos://palo_backup"
PROPERTIES
(
    "bos_endpoint" = "http://gz.bcebos.com",
    "bos_accesskey" = "bos_accesskey",
    "bos_secret_accesskey"="bos_accesskey"
);
```

3. Create a warehouse named hdfs_repo, rely on Baidu hdfs broker "hdfs_broker", the data root directory is: hdfs://hadoop-name-node:54310/path/to/repo/

```sql
CREATE REPOSITORY `hdfs_repo`
WITH BROKER `hdfs_broker`
ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
PROPERTIES
(
    "username" = "user",
    "password" = "password"
);
```

4. Create a repository named s3_repo to link cloud storage directly without going through the broker.

```sql
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

5. Create a repository named hdfs_repo to link HDFS directly without going through the broker.

```sql
CREATE REPOSITORY `hdfs_repo`
WITH hdfs
ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
PROPERTIES
(
    "fs.defaultFS"="hdfs://hadoop-name-node:54310",
    "hadoop.username"="user"
);

### Keywords

```
6. Create a repository named minio_repo to link minio storage directly through the s3 protocol.

```
CREATE REPOSITORY `minio_repo`
WITH S3
ON LOCATION "s3://minio_repo"
PROPERTIES
(
    "AWS_ENDPOINT" = "http://minio.com",
    "AWS_ACCESS_KEY" = "MINIO_USER",
    "AWS_SECRET_KEY"="MINIO_PASSWORD",
    "AWS_REGION" = "REGION",
    "use_path_style" = "true"
);
```

<version since="1.2">

7. Create a repository named minio_repo via temporary security credentials.

```
CREATE REPOSITORY `minio_repo`
WITH S3
ON LOCATION "s3://minio_repo"
PROPERTIES
( 
    "AWS_ENDPOINT" = "AWS_ENDPOINT",
    "AWS_ACCESS_KEY" = "AWS_TEMP_ACCESS_KEY",
    "AWS_SECRET_KEY" = "AWS_TEMP_SECRET_KEY",
    "AWS_TOKEN" = "AWS_TEMP_TOKEN",
    "AWS_REGION" = "AWS_REGION"
)
```

</version>

### Keywords

    CREATE, REPOSITORY

### Best Practice

1. A cluster can create multiple warehouses. Only users with ADMIN privileges can create repositories.
2. Any user can view the created repositories through the [SHOW REPOSITORIES](../../Show-Statements/SHOW-REPOSITORIES.md) command.
3. When performing data migration operations, it is necessary to create the exact same warehouse in the source cluster and the destination cluster, so that the destination cluster can view the data snapshots backed up by the source cluster through this warehouse.
