---
{
    "title": "导出查询结果集",
    "language": "zh-CN"
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

# 导出查询结果集

本文档介绍如何使用 `SELECT INTO OUTFILE` 命令进行查询结果的导出操作。

## 语法

`SELECT INTO OUTFILE` 语句可以将查询结果导出到文件中。目前支持通过 Broker 进程, 通过 S3 协议, 或直接通过 HDFS 协议，导出到远端存储，如 HDFS，S3，BOS，COS（腾讯云）上。语法如下

```
query_stmt
INTO OUTFILE "file_path"
[format_as]
[properties]
```

* `file_path`

    `file_path` 指向文件存储的路径以及文件前缀。如 `hdfs://path/to/my_file_`。
    
    最终的文件名将由 `my_file_`，文件序号以及文件格式后缀组成。其中文件序号由0开始，数量为文件被分割的数量。如：
    
    ```
    my_file_abcdefg_0.csv
    my_file_abcdefg_1.csv
    my_file_abcdegf_2.csv
    ```

* `[format_as]`

    ```
    FORMAT AS CSV
    ```
    
    指定导出格式。默认为 CSV。


* `[properties]`

    指定相关属性。目前支持通过 Broker 进程, 或通过 S3 协议进行导出。

    + Broker 相关属性需加前缀 `broker.`。具体参阅[Broker 文档](./broker.html)。
    + HDFS 相关属性需加前缀 `hdfs.` 其中 hdfs.fs.defaultFS 用于填写 namenode 地址和端口。属于必填项。。
    + S3 协议则直接执行 S3 协议配置即可。

    ```
    ("broker.prop_key" = "broker.prop_val", ...)
    or
    ("hdfs.fs.defaultFS" = "xxx", "hdfs.hdfs_user" = "xxx")
    or 
    ("AWS_ENDPOINT" = "xxx", ...)
    ``` 

    其他属性：

    ```
    ("key1" = "val1", "key2" = "val2", ...)
    ```

    目前支持以下属性：

    * `column_separator`：列分隔符，仅对 CSV 格式适用。默认为 `\t`。
    * `line_delimiter`：行分隔符，仅对 CSV 格式适用。默认为 `\n`。
    * `max_file_size`：单个文件的最大大小。默认为 1GB。取值范围在 5MB 到 2GB 之间。超过这个大小的文件将会被切分。
    * `schema`：PARQUET 文件schema信息。仅对 PARQUET 格式适用。导出文件格式为PARQUET时，必须指定`schema`。

## 并发导出

默认情况下，查询结果集的导出是非并发的，也就是单点导出。如果用户希望查询结果集可以并发导出，需要满足以下条件：

1. session variable 'enable_parallel_outfile' 开启并发导出: ```set enable_parallel_outfile = true;```
2. 导出方式为 S3 , 或者 HDFS， 而不是使用 broker
3. 查询可以满足并发导出的需求，比如顶层不包含 sort 等单点节点。（后面会举例说明，哪种属于不可并发导出结果集的查询）

满足以上三个条件，就能触发并发导出查询结果集了。并发度 = ```be_instacne_num * parallel_fragment_exec_instance_num```

### 如何验证结果集被并发导出

用户通过 session 变量设置开启并发导出后，如果想验证当前查询是否能进行并发导出，则可以通过下面这个方法。

```
explain select xxx from xxx where xxx  into outfile "s3://xxx" format as csv properties ("AWS_ENDPOINT" = "xxx", ...);
```

对查询进行 explain 后，Doris 会返回该查询的规划，如果你发现 ```RESULT FILE SINK``` 出现在 ```PLAN FRAGMENT 1``` 中，就说明导出并发开启成功了。
如果 ```RESULT FILE SINK``` 出现在 ```PLAN FRAGMENT 0``` 中，则说明当前查询不能进行并发导出 (当前查询不同时满足并发导出的三个条件)。

```
并发导出的规划示例：
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

## 使用示例

具体参阅[OUTFILE 文档](../sql-reference/sql-statements/Data%20Manipulation/OUTFILE.md)。

## 返回结果

导出命令为同步命令。命令返回，即表示操作结束。同时会返回一行结果来展示导出的执行结果。

如果正常导出并返回，则结果如下：

```
mysql> select * from tbl1 limit 10 into outfile "file:///home/work/path/result_";
+------------+-----------+----------+--------------------------------------------------------------------+
| FileNumber | TotalRows | FileSize | URL                                                                |
+------------+-----------+----------+--------------------------------------------------------------------+
|          1 |         2 |        8 | file:///192.168.1.10/home/work/path/result_{fragment_instance_id}_ |
+------------+-----------+----------+--------------------------------------------------------------------+
1 row in set (0.05 sec)
```

* FileNumber：最终生成的文件个数。
* TotalRows：结果集行数。
* FileSize：导出文件总大小。单位字节。
* URL：如果是导出到本地磁盘，则这里显示具体导出到哪个 Compute Node。

如果进行了并发导出，则会返回多行数据。

```
+------------+-----------+----------+--------------------------------------------------------------------+
| FileNumber | TotalRows | FileSize | URL                                                                |
+------------+-----------+----------+--------------------------------------------------------------------+
|          1 |         3 |        7 | file:///192.168.1.10/home/work/path/result_{fragment_instance_id}_ |
|          1 |         2 |        4 | file:///192.168.1.11/home/work/path/result_{fragment_instance_id}_ |
+------------+-----------+----------+--------------------------------------------------------------------+
2 rows in set (2.218 sec)
```

如果执行错误，则会返回错误信息，如：

```
mysql> SELECT * FROM tbl INTO OUTFILE ...
ERROR 1064 (HY000): errCode = 2, detailMessage = Open broker writer failed ...
```

## 注意事项

* 如果不开启并发导出，查询结果是由单个 BE 节点，单线程导出的。因此导出时间和导出结果集大小正相关。开启并发导出可以降低导出的时间。
* 导出命令不会检查文件及文件路径是否存在。是否会自动创建路径、或是否会覆盖已存在文件，完全由远端存储系统的语义决定。
* 如果在导出过程中出现错误，可能会有导出文件残留在远端存储系统上。Doris 不会清理这些文件。需要用户手动清理。
* 导出命令的超时时间同查询的超时时间。可以通过 `SET query_timeout=xxx` 进行设置。
* 对于结果集为空的查询，依然会产生一个大小为0的文件。
* 文件切分会保证一行数据完整的存储在单一文件中。因此文件的大小并不严格等于 `max_file_size`。
* 对于部分输出为非可见字符的函数，如 BITMAP、HLL 类型，输出为 `\N`，即 NULL。
* 目前部分地理信息函数，如 `ST_Point` 的输出类型为 VARCHAR，但实际输出值为经过编码的二进制字符。当前这些函数会输出乱码。对于地理函数，请使用 `ST_AsText` 进行输出。
