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

The `EXPORT` command is used to export the data of a specified table to a designated location as a file. Currently, it supports exporting to remote storage such as HDFS, S3, BOS, and COS (Tencent Cloud) through Broker process, S3 protocol, or HDFS protocol.

`EXPORT` is an asynchronous operation, and the command submits an `EXPORT JOB` to Doris. The task will be successfully submitted and returns immediately. After execution, you can use the [SHOW EXPORT](../../Show-Statements/SHOW-EXPORT.md) to view the progress.

**grammar**

  ```sql
  EXPORT TABLE table_name
  [PARTITION (p1[,p2])]
  [WHERE]
  TO export_path
  [opt_properties]
  WITH BROKER/S3/HDFS
  [broker_properties];
  ```

**principle**

The bottom layer of the `Export` statement actually executes the `select...outfile..` statement. The `Export` task will be decomposed into multiple `select...outfile..` statements to execute concurrently according to the value of the `parallelism` parameter. Each `select...outfile..` is responsible for exporting some tablets of table.

**illustrate:**

- `table_name`

  The table name of the table currently being exported. Only the export of Doris local table / View / External Table data is supported.

- `partition`

  It is possible to export only some specified partitions of the specified table

- `export_path`

  The exported file path can be a directory or a file directory with a file prefix, for example: `hdfs://path/to/my_file_`

- `opt_properties`

  Used to specify some export parameters.

  ```sql
  [PROPERTIES ("key"="value", ...)]
  ````

  The following parameters can be specified:

  - `label`: This parameter is optional, specifies the label of the export task. If this parameter is not specified, the system randomly assigns a label to the export task.
  - `column_separator`: Specifies the exported column separator, default is `\t`, mulit-bytes is supported. This parameter is only used for `CSV` file format.
  - `line_delimiter`: Specifies the line delimiter for export, the default is `\n`, mulit-bytes is supported. This parameter is only used for `CSV` file format.
  - `timeout`: The timeout period of the export job, the default is 2 hours, the unit is seconds.
  - `columns`: Specifies certain columns of the export job table
  - `format`: Specifies the file format, support: parquet, orc, csv, csv_with_names, csv_with_names_and_types.The default is csv format.
  - `parallelism`: The concurrency degree of the `export` job, the default is `1`. The export job will be divided into `select..outfile..` statements of the number of `parallelism` to execute concurrently. (If the value of `parallelism` is greater than the number of tablets in the table, the system will automatically set `parallelism` to the number of tablets, that is, each `select..outfile..` statement is responsible for one tablet)
  - `delete_existing_files`: default `false`. If it is specified as true, you will first delete all files specified in the directory specified by the file_path, and then export the data to the directory.For example: "file_path" = "/user/tmp", then delete all files and directory under "/user/"; "file_path" = "/user/tmp/", then delete all files and directory under "/user/tmp/"
  - `max_file_size`: it is the limit for the size of a single file in the export job. If the result file exceeds this value, it will be split into multiple files. The valid range for `max_file_size` is [5MB, 2GB], with a default value of 1GB. (When exporting to the ORC file format, the actual size of the split files will be multiples of 64MB, for example, if max_file_size is specified as 5MB, the actual split size will be 64MB; if max_file_size is specified as 65MB, the actual split size will be 128MB.)
  - `timeout`: This is the timeout parameter of the export job, the default timeout is 2 hours, and the unit is seconds.

  > Note that to use the `delete_existing_files` parameter, you also need to add the configuration `enable_delete_existing_files = true` to the fe.conf file and restart the FE. Only then will the `delete_existing_files` parameter take effect. Setting `delete_existing_files = true` is a dangerous operation and it is recommended to only use it in a testing environment.

- `WITH BROKER`

  The export function needs to write data to the remote storage through the Broker process. Here you need to define the relevant connection information for the broker to use.

  ```sql
  WITH BROKER "broker_name"
  ("key"="value"[,...])

  Broker related properties:
        username: user name
        password: password
        hadoop.security.authentication: specify the authentication method as kerberos
        kerberos_principal: specifies the principal of kerberos
        kerberos_keytab: specifies the path to the keytab file of kerberos. The file must be the absolute path to the file on the server where the broker process is located. and can be accessed by the Broker process
  ````

- `WITH HDFS`

  You can directly write data to the remote HDFS.


  ```sql
  WITH HDFS ("key"="value"[,...])

  HDFS related properties:
        fs.defaultFS: namenode address and port
        hadoop.username: hdfs username
        dfs.nameservices: if hadoop enable HA, please set fs nameservice. See hdfs-site.xml
        dfs.ha.namenodes.[nameservice ID]：unique identifiers for each NameNode in the nameservice. See hdfs-site.xml
        dfs.namenode.rpc-address.[nameservice ID].[name node ID]: the fully-qualified RPC address for each NameNode to listen on. See hdfs-site.xml
        dfs.client.failover.proxy.provider.[nameservice ID]：the Java class that HDFS clients use to contact the Active NameNode, usually it is org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

        For a kerberos-authentication enabled Hadoop cluster, additional properties need to be set:
        dfs.namenode.kerberos.principal: HDFS namenode service principal
        hadoop.security.authentication: kerberos
        hadoop.kerberos.principal: the Kerberos pincipal that Doris will use when connectiong to HDFS.
        hadoop.kerberos.keytab: HDFS client keytab location.
  ```

- `WITH S3`

  You can directly write data to a remote S3 object store

  ```sql
  WITH S3 ("key"="value"[,...])

  S3 related properties:
    AWS_ENDPOINT
    AWS_ACCESS_KEY
    AWS_SECRET_KEY
    AWS_REGION
    use_path_stype: (optional) default false . The S3 SDK uses the virtual-hosted style by default. However, some object storage systems may not be enabled or support virtual-hosted style access. At this time, we can add the use_path_style parameter to force the use of path style access method.
  ```

### Example

#### export to local

> Export data to the local file system needs to add `enable_outfile_to_local = true` to the fe.conf and restart the Fe.

1. You can export the `test` table to a local store. Export csv format file by default.

```sql
EXPORT TABLE test TO "file:///home/user/tmp/";
```

2. You can export the k1 and k2 columns in `test` table to a local store, and set export label. Export csv format file by default.

```sql
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "label" = "label1",
  "columns" = "k1,k2"
);
```

3. You can export the rows where `k1 < 50` in `test` table to a local store, and set column_separator to `,`. Export csv format file by default.

```sql
EXPORT TABLE test WHERE k1 < 50 TO "file:///home/user/tmp/"
PROPERTIES (
  "columns" = "k1,k2",
  "column_separator"=","
);
```

4. Export partitions p1 and p2 from the test table to local storage, with the default exported file format being csv.

```sql
EXPORT TABLE test PARTITION (p1,p2) TO "file:///home/user/tmp/" 
PROPERTIES ("columns" = "k1,k2");
  ```

5. Export all data in the test table to local storage with a non-default file format.

```sql
// parquet file format
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "columns" = "k1,k2",
  "format" = "parquet"
);

// orc file format
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "columns" = "k1,k2",
  "format" = "orc"
);

// csv_with_names file format. Using 'AA' as the column delimiter and 'zz' as the line delimiter.
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "format" = "csv_with_names",
  "column_separator"="AA",
  "line_delimiter" = "zz"
);

// csv_with_names_and_types file format
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "format" = "csv_with_names_and_types"
);

```

6. set max_file_sizes

```sql
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "format" = "parquet",
  "max_file_size" = "5MB"
);
```

When the exported file size is larger than 5MB, the data will be split into multiple files, with each file containing a maximum of 5MB.

7. set parallelism
```sql
EXPORT TABLE test TO "file:///home/user/tmp/"
PROPERTIES (
  "format" = "parquet",
  "max_file_size" = "5MB",
  "parallelism" = "5"
);
```

8. set delete_existing_files

```sql
EXPORT TABLE test TO "file:///home/user/tmp"
PROPERTIES (
  "format" = "parquet",
  "max_file_size" = "5MB",
  "delete_existing_files" = "true"
)
```

Before exporting data, all files and directories in the `/home/user/` directory will be deleted, and then the data will be exported to that directory.

#### export with S3

1. Export all data from the `testTbl` table to S3 using invisible character '\x07' as a delimiter for columns and rows.If you want to export data to minio, you also need to specify use_path_style=true.

  ```sql
  EXPORT TABLE testTbl TO "s3://bucket/a/b/c" 
  PROPERTIES (
    "column_separator"="\\x07", 
    "line_delimiter" = "\\x07"
  ) WITH s3 (
    "s3.endpoint" = "xxxxx",
    "s3.region" = "xxxxx",
    "s3.secret_key"="xxxx",
    "s3.access_key" = "xxxxx"
  )
  ```

2. Export all data in the test table to HDFS in the format of parquet, limit the size of a single file to 1024MB, and reserve all files in the specified directory.

#### export with HDFS
1. Export all data from the `test` table to HDFS in `Parquet` format, with a limit of 512MB for the size of a single file in the export job, and retain all files under the specified directory.

  ```sql
  EXPORT TABLE test TO "hdfs://hdfs_host:port/a/b/c/" 
  PROPERTIES(
      "format" = "parquet",
      "max_file_size" = "512MB",
      "delete_existing_files" = "false"
  )
  with HDFS (
  "fs.defaultFS"="hdfs://hdfs_host:port",
  "hadoop.username" = "hadoop"
  );
  ```

#### export with Broker
You need to first start the broker process and add it to the FE before proceeding.
1. Export the `test` table to hdfs

  ```sql
  EXPORT TABLE test TO "hdfs://hdfs_host:port/a/b/c" 
  WITH BROKER "broker_name" 
  (
    "username"="xxx",
    "password"="yyy"
  );
  ```

2. Export partitions 'p1' and 'p2' from the 'testTbl' table to HDFS using ',' as the column delimiter and specifying a label.

  ```sql
  EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" 
  PROPERTIES (
    "label" = "mylabel",
    "column_separator"=","
  ) 
  WITH BROKER "broker_name" 
  (
    "username"="xxx",
    "password"="yyy"
  );
  ```

3. Export all data from the 'testTbl' table to HDFS using the non-visible character '\x07' as the column and row delimiter.

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

### Keywords

    EXPORT

### Best Practice

  #### Concurrent Export

  An Export job can be configured with the `parallelism` parameter to concurrently export data. The `parallelism` parameter specifies the number of threads to execute the `EXPORT Job`. Each thread is responsible for exporting a subset of the total tablets.

  The underlying execution logic of an `Export Job `is actually the `SELECT INTO OUTFILE` statement. Each thread specified by the `parallelism` parameter executes independent `SELECT INTO OUTFILE` statements.

  The specific logic for splitting an `Export Job` into multiple `SELECT INTO OUTFILE` is, to evenly distribute all the tablets of the table among all parallel threads. For example:

  - If num(tablets) = 40 and parallelism = 3, then the three threads will be responsible for 14, 13, and 13 tablets, respectively.
  - If num(tablets) = 2 and parallelism = 3, then Doris automatically sets the parallelism to 2, and each thread is responsible for one tablet.

  When the number of tablets responsible for a thread exceeds the `maximum_tablets_of_outfile_in_export` value (default is 10, and can be modified by adding the `maximum_tablets_of_outfile_in_export` parameter in fe.conf), the thread will split the tablets which are responsibled for this thread into multiple `SELECT INTO OUTFILE` statements. For example:

  - If a thread is responsible for 14 tablets and `maximum_tablets_of_outfile_in_export = 10`, then the thread will be responsible for two `SELECT INTO OUTFILE` statements. The first `SELECT INTO OUTFILE` statement exports 10 tablets, and the second `SELECT INTO OUTFILE` statement exports 4 tablets. The two `SELECT INTO OUTFILE` statements are executed serially by this thread.

  #### memory limit

  The query plan for an `Export Job` typically involves only `scanning and exporting`, and does not involve compute logic that requires a lot of memory. Therefore, the default memory limit of 2GB is usually sufficient to meet the requirements.

  However, in certain scenarios, such as a query plan that requires scanning too many tablets on the same BE, or when there are too many data versions of tablets, it may result in insufficient memory. In these cases, you can adjust the session variable `exec_mem_limit` to increase the memory usage limit.

  #### Precautions

  - Exporting a large amount of data at one time is not recommended. The maximum recommended export data volume for an Export job is several tens of GB. An overly large export results in more junk files and higher retry costs. If the amount of table data is too large, it is recommended to export by partition.

  - If the Export job fails, the generated files will not be deleted, and the user needs to delete it manually.

  - The Export job only exports the data of the Base table / View / External table, not the data of the materialized view.

  - The export job scans data and occupies IO resources, which may affect the query latency of the system.

  - Currently, The `Export Job` is simply check whether the `Tablets version` is the same, it is recommended not to import data during the execution of the `Export Job`.

  - The maximum number of partitions that an `Export job` allows is 2000. You can add a parameter to the fe.conf `maximum_number_of_export_partitions` and restart FE to modify the setting.
