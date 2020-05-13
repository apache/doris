---
{
    "title": "查询结果集导出",
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

# 查询结果集导出

本文档介绍如何使用 `SELECT INTO OUTFILE` 命令进行查询结果的导入操作。

## 语法

`SELECT INTO OUTFILE` 语句可以将查询结果导出到文件中。目前仅支持通过 Broker 进程导出到远端存储，如 HDFS，S3，BOS 上。语法如下

```
query_stmt
INTO OUFILE "file_path"
[format_as]
WITH BROKER `broker_name`
[broker_properties]
[other_properties]
```

* `[format_as]`

    ```
    FORMAT AS CSV
    ```
    
    指定导出格式。默认为 CSV。

* `[broker_properties]`

    ```
    ("broker_prop_key" = "broker_prop_val", ...)
    ``` 

    Broker 相关的一些参数，如 HDFS 的 认证信息等。具体参阅[Broker 文档](./broker.html)。

* `[other_properties]`

    ```
    ("key1" = "val1", "key2" = "val2", ...)
    ```

    其他属性，目前支持以下属性：

    * `column_separator`：列分隔符，仅对 CSV 格式适用。默认为 `\t`。
    * `line_delimiter`：行分隔符，仅对 CSV 格式适用。默认为 `\n`。

1. 示例1

    将简单查询结果导出到文件 `hdfs:/path/to/result.txt`。指定导出格式为 CSV。使用 `my_broker` 并设置 kerberos 认证信息。指定列分隔符为 `,`，行分隔符为 `\n`。
    ```
    SELECT * FROM tbl
    INTO OUTFILE "hdfs:/path/to/result.txt"
    FORMAT AS CSV
    WITH BROKER "my_broker"
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal" = "doris@YOUR.COM",
        "kerberos_keytab" = "/home/doris/my.keytab"
    )
    PROPERTIELS
    (
        "column_separator" = ",",
        "line_delimiter" = "\n"
    );
    ```

2. 示例2

    将 CTE 语句的查询结果导出到文件 `hdfs:/path/to/result.txt`。默认导出格式为 CSV。使用 `my_broker` 并设置 hdfs 高可用信息。使用默认的行列分隔符。

    ```
    WITH
    x1 AS
    (SELECT k1, k2 FROM tbl1),
    x2 AS
    (SELECT k3 FROM tbl2)
    SELEC k1 FROM x1 UNION SELECT k3 FROM x2
    INTO OUTFILE "hdfs:/path/to/result.txt"
    WITH BROKER "my_broker"
    (
        "username"="user",
        "password"="passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    );
    ```
    
3. 示例3

    将 UNION 语句的查询结果导出到文件 `bos://bucket/result.txt`。指定导出格式为 PARQUET。使用 `my_broker` 并设置 hdfs 高可用信息。PARQUET 格式无需指定列分割符。
    
    ```
    SELECT k1 FROM tbl1 UNION SELECT k2 FROM tbl1
    INTO OUTFILE "bos://bucket/result.txt"
    FORMAT AS PARQUET
    WITH BROKER "my_broker"
    (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyyyyyyyy"
    )
    ```
    
## 返回结果

导出命令为同步命令。命令返回，即表示操作结束。

如果正常导出并返回，则结果如下：

```
mysql> SELECT * FROM tbl INTO OUTFILE ...                                                                                                                                                                                                                                                                  Query OK, 100000 row affected (5.86 sec)
```

其中 `100000 row affected` 表示导出的结果集大小。

如果执行错误，则会返回错误信息，如：

```
mysql> SELECT * FROM tbl INTO OUTFILE ...                                                                                                                                                                                                                                                                  ERROR 1064 (HY000): errCode = 2, detailMessage = Open broker writer failed ...
```

## 注意事项

* 查询结果是由单个 BE 节点，单线程导出的。因此导出时间和导出结果集大小正相关。
* 导出命令不会检查文件及文件路径是否存在。是否会自动创建路径、或是否会覆盖已存在文件，完全由远端存储系统的语义决定。
* 如果在导入过程中出现错误，可能会有导出文件残留在远端存储系统上。Doris 不会清理这些文件。需要用户手动清理。
* 导出命令的超时时间同查询的超时时间。可以通过 `SET query_timeout=xxx` 进行设置。
