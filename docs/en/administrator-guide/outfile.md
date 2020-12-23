---
{
    "title": "Export Query Result",
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

# Export Query Result

This document describes how to use the `SELECT INTO OUTFILE` command to export query results.

## Syntax

The `SELECT INTO OUTFILE` statement can export the query results to a file. Currently, it only supports exporting to remote storage such as HDFS, S3, BOS and COS(Tencent Cloud) through the Broker process. The syntax is as follows:

```
query_stmt
INTO OUTFILE "file_path"
[format_as]
WITH BROKER `broker_name`
[broker_properties]
[other_properties]
```

* `file_path`

    `file_path` specify the file path and file name prefix. Like: `hdfs://path/to/my_file_`.
    
    The final file name will be assembled as `my_file_`, file seq no and the format suffix. File seq no starts from 0, determined by the number of split.
    
    ```
    my_file_0.csv
    my_file_1.csv
    my_file_2.csv
    ```

* `[format_as]`

    ```
    FORMAT AS CSV
    ```
    
    Specify the export format. The default is CSV.

* `[properties]`

    Specify the relevant attributes. Currently only export through Broker process is supported. Broker related attributes need to be prefixed with `broker.`. For details, please refer to [Broker](./broker.html).

    ```
    PROPERTIES
    ("broker.prop_key" = "broker.prop_val", ...)
    ```

    Other properties

    ```
    PROPERTIES
    ("key1" = "val1", "key2" = "val2", ...)
    ```

    currently supports the following properties:

    * `column_separator`: Column separator, only applicable to CSV format. The default is `\t`.
    * `line_delimiter`: Line delimiter, only applicable to CSV format. The default is `\n`.
    * `max_file_size`：The max size of a single file. Default is 1GB. Range from 5MB to 2GB. Files exceeding this size will be splitted.

1. Example 1

    Export simple query results to the file `hdfs:/path/to/result.txt`. Specify the export format as CSV. Use `my_broker` and set kerberos authentication information. Specify the column separator as `,` and the line delimiter as `\n`.
    
    ```
    SELECT * FROM tbl
    INTO OUTFILE "hdfs:/path/to/result_"
    FORMAT AS CSV
    PROPERTIES
    (
        "broker.name" = "my_broker",
        "broker.hadoop.security.authentication" = "kerberos",
        "broker.kerberos_principal" = "doris@YOUR.COM",
        "broker.kerberos_keytab" = "/home/doris/my.keytab",
        "column_separator" = ",",
        "line_delimiter" = "\n",
        "max_file_size" = "100MB"
    );
    ```
    
    If the result is less than 100MB, file will be: `result_0.csv`.
    
    If larger than 100MB, may be: `result_0.csv, result_1.csv, ...`.

2. Example 2

    Export the query result of the CTE statement to the file `hdfs:/path/to/result.txt`. The default export format is CSV. Use `my_broker` and set hdfs high availability information. Use the default column separators and line delimiter.

    ```
    WITH
    x1 AS
    (SELECT k1, k2 FROM tbl1),
    x2 AS
    (SELECT k3 FROM tbl2)
    SELEC k1 FROM x1 UNION SELECT k3 FROM x2
    INTO OUTFILE "hdfs:/path/to/result_"
    PROPERTIES
    (
        "broker.name" = "my_broker",
        "broker.username"="user",
        "broker.password"="passwd",
        "broker.dfs.nameservices" = "my_ha",
        "broker.dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "broker.dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "broker.dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "broker.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    );
    ```
    
    If the result is less than 1GB, file will be: `result_0.csv`.
    
    If larger than 1GB, may be: `result_0.csv, result_1.csv, ...`.
    
3. Example 3

    Export the query results of the UNION statement to the file `bos://bucket/result.parquet`. Specify the export format as PARQUET. Use `my_broker` and set hdfs high availability information. PARQUET format does not need to specify the column separator and line delimiter.
    
    ```
    SELECT k1 FROM tbl1 UNION SELECT k2 FROM tbl1
    INTO OUTFILE "bos://bucket/result_"
    FORMAT AS PARQUET
    PROPERTIES
    (
        "broker.name" = "my_broker",
        "broker.bos_endpoint" = "http://bj.bcebos.com",
        "broker.bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "broker.bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyyyyyyyy"
    );
    ```
    
    If the result is less than 1GB, file will be: `result_0.parquet`.
    
    If larger than 1GB, may be: `result_0.parquet, result_1.parquet, ...`.

4. Example 4

    Export simple query results to the file `cos://${bucket_name}/path/result.txt`. Specify the export format as CSV.
    
    ```
   select k1,k2,v1 from tbl1 limit 100000
   into outfile "s3a://my_bucket/export/my_file_"
   FORMAT AS CSV
   PROPERTIES
   (
       "broker.name" = "hdfs_broker",
       "broker.fs.s3a.access.key" = "xxx",
       "broker.fs.s3a.secret.key" = "xxxx",
       "broker.fs.s3a.endpoint" = "https://cos.xxxxxx.myqcloud.com/",
       "column_separator" = ",",
       "line_delimiter" = "\n",
       "max_file_size" = "1024MB"
    )
    ```
    
    If the result is less than 1GB, file will be: `my_file_0.csv`.
    
    If larger than 1GB, may be: `my_file_0.csv, result_1.csv, ...`.
    
    Please Note: 
    1. Paths that do not exist are automatically created.
    2. These parameters(access.key/secret.key/endpointneed) need to be confirmed with `Tecent Cloud COS`. In particular, the value of endpoint does not need to be filled in bucket_name。

## Return result

The command is a synchronization command. The command returns, which means the operation is over.

If it exports and returns normally, the result is as follows:

```
mysql> SELECT * FROM tbl INTO OUTFILE ...                                                                                                                                                                                                                                                                  Query OK, 100000 row affected (5.86 sec)
```

`100000 row affected` Indicates the size of the exported result set.

If the execution is incorrect, an error message will be returned, such as:

```
mysql> SELECT * FROM tbl INTO OUTFILE ...                                                                                                                                                                                                                                                                  ERROR 1064 (HY000): errCode = 2, detailMessage = Open broker writer failed ...
```

## Notice

* The CSV format does not support exporting binary types, such as BITMAP and HLL types. These types will be output as `\N`, which is null.
* The query results are exported from a single BE node and a single thread. Therefore, the export time and the export result set size are positively correlated.
* The export command does not check whether the file and file path exist. Whether the path will be automatically created or whether the existing file will be overwritten is entirely determined by the semantics of the remote storage system.
* If an error occurs during the export process, the exported file may remain on the remote storage system. Doris will not clean these files. The user needs to manually clean up.
* The timeout of the export command is the same as the timeout of the query. It can be set by `SET query_timeout = xxx`.
* For empty result query, there will be an empty file.
* File spliting will ensure that a row of data is stored in a single file. Therefore, the size of the file is not strictly equal to `max_file_size`.
* For functions whose output is invisible characters, such as BITMAP and HLL types, the output is `\N`, which is NULL.
* At present, the output type of some geo functions, such as `ST_Point` is VARCHAR, but the actual output value is an encoded binary character. Currently these functions will output garbled characters. For geo functions, use `ST_AsText` for output.
