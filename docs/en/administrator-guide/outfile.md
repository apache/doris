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

The `SELECT INTO OUTFILE` statement can export the query results to a file. Currently supports export to remote storage through Broker process, or directly through S3, HDFS  protocol such as HDFS, S3, BOS and COS(Tencent Cloud) through the Broker process. The syntax is as follows:

```
query_stmt
INTO OUTFILE "file_path"
[format_as]
[properties]
```

* `file_path`

    `file_path` specify the file path and file name prefix. Like: `hdfs://path/to/my_file_`.
    
    The final file name will be assembled as `my_file_`, file seq no and the format suffix. File seq no starts from 0, determined by the number of split.
    
    ```
    my_file_abcdefg_0.csv
    my_file_abcdefg_1.csv
    my_file_abcdegf_2.csv
    ```

* `[format_as]`

    ```
    FORMAT AS CSV
    ```
    
    Specify the export format. The default is CSV.

* `[properties]`

    Specify the relevant attributes. Currently it supports exporting through the Broker process, or through the S3, HDFS protocol.

    + Broker related attributes need to be prefixed with `broker.`. For details, please refer to [Broker Document](./broker.html).
    + HDFS protocal can directly execute HDFS protocal configuration. hdfs.fs.defaultFS is used to fill in the namenode address and port. It is required.
    + S3 protocol can directly execute S3 protocol configuration.

    ```
    PROPERTIES
    ("broker.prop_key" = "broker.prop_val", ...)
    or 
    ("hdfs.fs.defaultFS" = "xxx", "hdfs.user" = "xxx")
    or
    ("AWS_ENDPOINT" = "xxx", ...)
    ```

    Other properties

    ```
    PROPERTIES
    ("key1" = "val1", "key2" = "val2", ...)
    ```

    currently supports the following properties:

    * `column_separator`: Column separator, only applicable to CSV format. The default is `\t`.
    * `line_delimiter`: Line delimiter, only applicable to CSV format. The default is `\n`.
    * `max_file_size`: The max size of a single file. Default is 1GB. Range from 5MB to 2GB. Files exceeding this size will be splitted.
    * `schema`: schema infomation for PARQUET, only applicable to PARQUET format. If the exported file format is PARQUET, `schema` must be specified.

## Concurrent export

By default, the export of the query result set is non-concurrent, that is, a single point of export. If the user wants the query result set to be exported concurrently, the following conditions need to be met:

1. session variable 'enable_parallel_outfile' to enable concurrent export: ```set enable_parallel_outfile = true;```
2. The export method is S3, HDFS instead of using a broker
3. The query can meet the needs of concurrent export, for example, the top level does not contain single point nodes such as sort. (I will give an example later, which is a query that does not export the result set concurrently)

If the above three conditions are met, the concurrent export query result set can be triggered. Concurrency = ```be_instacne_num * parallel_fragment_exec_instance_num```

### How to verify that the result set is exported concurrently

After the user enables concurrent export through the session variable setting, if you want to verify whether the current query can be exported concurrently, you can use the following method.

```
explain select xxx from xxx where xxx into outfile "s3://xxx" format as csv properties ("AWS_ENDPOINT" = "xxx", ...);
```

After explaining the query, Doris will return the plan of the query. If you find that ```RESULT FILE SINK``` appears in ```PLAN FRAGMENT 1```, it means that the export concurrency has been opened successfully.
If ```RESULT FILE SINK``` appears in ```PLAN FRAGMENT 0```, it means that the current query cannot be exported concurrently (the current query does not satisfy the three conditions of concurrent export at the same time).

```
Planning example for concurrent export:
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:<slot 2> | <slot 3> | <slot 4> | <slot 5>                     |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   1:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:`k1` + `k2`                                                   |
|   PARTITION: HASH_PARTITIONED: `default_cluster:test`.`multi_tablet`.`k1`   |
|                                                                             |
|   RESULT FILE SINK                                                          |
|   FILE PATH: s3://ml-bd-repo/bpit_test/outfile_1951_                        |
|   STORAGE TYPE: S3                                                          |
|                                                                             |
|   0:OlapScanNode                                                            |
|      TABLE: multi_tablet                                                    |
+-----------------------------------------------------------------------------+
```

## Usage example

For details, please refer to [OUTFILE Document](../sql-reference/sql-statements/Data%20Manipulation/OUTFILE.md).

## Return result

The command is a synchronization command. The command returns, which means the operation is over.
At the same time, a row of results will be returned to show the exported execution result.

If it exports and returns normally, the result is as follows:

```
mysql> select * from tbl1 limit 10 into outfile "file:///home/work/path/result_";
+------------+-----------+----------+--------------------------------------------------------------------+
| FileNumber | TotalRows | FileSize | URL                                                                |
+------------+-----------+----------+--------------------------------------------------------------------+
|          1 |         2 |        8 | file:///192.168.1.10/home/work/path/result_{fragment_instance_id}_ |
+------------+-----------+----------+--------------------------------------------------------------------+
1 row in set (0.05 sec)
```

* FileNumber: The number of files finally generated.
* TotalRows: The number of rows in the result set.
* FileSize: The total size of the exported file. Unit byte.
* URL: If it is exported to a local disk, the Compute Node to which it is exported is displayed here.

If a concurrent export is performed, multiple rows of data will be returned.

```
+------------+-----------+----------+--------------------------------------------------------------------+
| FileNumber | TotalRows | FileSize | URL                                                                |
+------------+-----------+----------+--------------------------------------------------------------------+
|          1 |         3 |        7 | file:///192.168.1.10/home/work/path/result_{fragment_instance_id}_ |
|          1 |         2 |        4 | file:///192.168.1.11/home/work/path/result_{fragment_instance_id}_ |
+------------+-----------+----------+--------------------------------------------------------------------+
2 rows in set (2.218 sec)
```

If the execution is incorrect, an error message will be returned, such as:

```
mysql> SELECT * FROM tbl INTO OUTFILE ...
ERROR 1064 (HY000): errCode = 2, detailMessage = Open broker writer failed ...
```

## Notice

* The CSV format does not support exporting binary types, such as BITMAP and HLL types. These types will be output as `\N`, which is null.
* If you do not enable concurrent export, the query result is exported by a single BE node in a single thread. Therefore, the export time and the export result set size are positively correlated. Turning on concurrent export can reduce the export time.
* The export command does not check whether the file and file path exist. Whether the path will be automatically created or whether the existing file will be overwritten is entirely determined by the semantics of the remote storage system.
* If an error occurs during the export process, the exported file may remain on the remote storage system. Doris will not clean these files. The user needs to manually clean up.
* The timeout of the export command is the same as the timeout of the query. It can be set by `SET query_timeout = xxx`.
* For empty result query, there will be an empty file.
* File spliting will ensure that a row of data is stored in a single file. Therefore, the size of the file is not strictly equal to `max_file_size`.
* For functions whose output is invisible characters, such as BITMAP and HLL types, the output is `\N`, which is NULL.
* At present, the output type of some geo functions, such as `ST_Point` is VARCHAR, but the actual output value is an encoded binary character. Currently these functions will output garbled characters. For geo functions, use `ST_AsText` for output.
