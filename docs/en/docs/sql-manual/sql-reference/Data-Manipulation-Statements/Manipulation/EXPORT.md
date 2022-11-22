---
{
    "title": "EXPORT",
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

## EXPORT

### Name

EXPORT

### Description

This statement is used to export the data of the specified table to the specified location.

This is an asynchronous operation that returns if the task is submitted successfully. After execution, you can use the [SHOW EXPORT](../../Show-Statements/SHOW-EXPORT) command to view the progress.

```sql
EXPORT TABLE table_name
[PARTITION (p1[,p2])]
TO export_path
[opt_properties]
WITH BROKER
[broker_properties];
```

illustrate:

- `table_name`

  The table name of the table currently being exported. Only the export of Doris local table data is supported.

- `partition`

  It is possible to export only some specified partitions of the specified table

- `export_path`

  The exported path must be a directory.

- `opt_properties`

  Used to specify some export parameters.

  ```sql
  [PROPERTIES ("key"="value", ...)]
  ````

  The following parameters can be specified:

  - `column_separator`: Specifies the exported column separator, default is \t. Only single byte is supported.
  - `line_delimiter`: Specifies the line delimiter for export, the default is \n. Only single byte is supported.
  - `exec_mem_limit`: Export the upper limit of the memory usage of a single BE node, the default is 2GB, and the unit is bytes.
  - `timeout`: The timeout period of the import job, the default is 2 hours, the unit is seconds.
  - `tablet_num_per_task`: The maximum number of tablets each subtask can allocate to scan.

- `WITH BROKER`

  The export function needs to write data to the remote storage through the Broker process. Here you need to define the relevant connection information for the broker to use.

  ```sql
  WITH BROKER hdfs|s3 ("key"="value"[,...])
  ````

 1. If the export is to Amazon S3, you need to provide the following properties

````
fs.s3a.access.key: AmazonS3 access key
fs.s3a.secret.key: AmazonS3 secret key
fs.s3a.endpoint: AmazonS3 endpoint
````

 2. If you use the S3 protocol to directly connect to the remote storage, you need to specify the following properties

    (
        "AWS_ENDPOINT" = "",
        "AWS_ACCESS_KEY" = "",
        "AWS_SECRET_KEY"="",
        "AWS_REGION" = ""
    )

### Example

1. Export all data in the test table to hdfs

```sql
EXPORT TABLE test TO "hdfs://hdfs_host:port/a/b/c" 
WITH BROKER "broker_name" 
(
  "username"="xxx",
  "password"="yyy"
);
```

2. Export partitions p1, p2 in the testTbl table to hdfs

```sql
EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" 
WITH BROKER "broker_name" 
(
  "username"="xxx",
  "password"="yyy"
);
```

3. Export all data in the testTbl table to hdfs, with "," as the column separator, and specify the label

```sql
EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" 
PROPERTIES ("label" = "mylabel", "column_separator"=",") 
WITH BROKER "broker_name" 
(
  "username"="xxx",
  "password"="yyy"
);
```

4. Export the row with k1 = 1 in the testTbl table to hdfs.

```sql
EXPORT TABLE testTbl WHERE k1=1 TO "hdfs://hdfs_host:port/a/b/c" 
WITH BROKER "broker_name" 
(
  "username"="xxx",
  "password"="yyy"
);
```

5. Export all data in the testTbl table to local.

```sql
EXPORT TABLE testTbl TO "file:///home/data/a";
```

6. Export all data in the testTbl table to hdfs with invisible character "\x07" as column or row separator.

```sql
EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" 
PROPERTIES (
  "column_separator"="\\x07", 
  "line_delimiter" = "\\x07"
) 
WITH BROKER "broker_name" 
(
  "username"="xxx", 
  "password"="yyy"
)
```

7. Export the k1, v1 columns of the testTbl table to the local.

```sql
EXPORT TABLE testTbl TO "file:///home/data/a" PROPERTIES ("columns" = "k1,v1");
```

8. Export all data in the testTbl table to s3 with invisible characters "\x07" as column or row separators.

```sql
EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" 
PROPERTIES (
  "column_separator"="\\x07", 
  "line_delimiter" = "\\x07"
) WITH s3 (
  "AWS_ENDPOINT" = "xxxxx",
  "AWS_ACCESS_KEY" = "xxxxx",
  "AWS_SECRET_KEY"="xxxx",
  "AWS_REGION" = "xxxxx"
)
```

9. Export all data in the testTbl table to cos(Tencent Cloud Object Storage).

```sql
EXPORT TABLE testTbl TO "cosn://my_bucket/export/a/b/c"
PROPERTIES (
  "column_separator"=",",
  "line_delimiter" = "\n"
) WITH BROKER "broker_name"
(
  "fs.cosn.userinfo.secretId" = "xxx",
  "fs.cosn.userinfo.secretKey" = "xxxx",
  "fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxxxxx.myqcloud.com"
)
```

### Keywords

    EXPORT

### Best Practice

- Splitting of subtasks

  An Export job will be split into multiple subtasks (execution plans) to execute. How many query plans need to be executed depends on how many tablets there are in total, and how many tablets can be allocated to a query plan.

  Because multiple query plans are executed serially, the execution time of the job can be reduced if one query plan handles more shards.

  However, if there is an error in the query plan (such as the failure of the RPC calling the broker, the jitter in the remote storage, etc.), too many Tablets will lead to a higher retry cost of a query plan.

  Therefore, it is necessary to reasonably arrange the number of query plans and the number of shards that each query plan needs to scan, so as to balance the execution time and the execution success rate.

  It is generally recommended that the amount of data scanned by a query plan is within 3-5 GB.

  #### memory limit

  Usually, the query plan of an Export job has only two parts of `scan-export`, which does not involve calculation logic that requires too much memory. So usually the default memory limit of 2GB suffices.

  However, in some scenarios, such as a query plan, too many Tablets need to be scanned on the same BE, or too many Tablet data versions may cause insufficient memory. At this point, you need to set a larger memory, such as 4GB, 8GB, etc., through the `exec_mem_limit` parameter.

  #### Precautions

  - Exporting a large amount of data at one time is not recommended. The maximum recommended export data volume for an Export job is several tens of GB. An overly large export results in more junk files and higher retry costs. If the amount of table data is too large, it is recommended to export by partition.
  - If the Export job fails, the `__doris_export_tmp_xxx` temporary directory generated in the remote storage and the generated files will not be deleted, and the user needs to delete it manually.
  - If the Export job runs successfully, the `__doris_export_tmp_xxx` directory generated in the remote storage may be preserved or cleared according to the file system semantics of the remote storage. For example, in S3 object storage, after the last file in a directory is removed by the rename operation, the directory will also be deleted. If the directory is not cleared, the user can clear it manually.
  - The Export job only exports the data of the Base table, not the data of the materialized view.
  - The export job scans data and occupies IO resources, which may affect the query latency of the system.
  - The maximum number of export jobs running simultaneously in a cluster is 5. Only jobs submitted after that will be queued.
