---
{
    "title": "BROKER-LOAD",
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

## BROKER-LOAD

### Name

BROKER LOAD

### Description

该命令主要用于通过 Broker 服务进程读取远端存储（如S3、HDFS）上的数据导入到 Doris 表里。

```sql
LOAD LABEL load_label
(
data_desc1[, data_desc2, ...]
)
WITH BROKER broker_name
[broker_properties]
[load_properties]
[COMMENT "comments"];
```

- `load_label`

  每个导入需要指定一个唯一的 Label。后续可以通过这个 label 来查看作业进度。

  `[database.]label_name`

- `data_desc1`

  用于描述一组需要导入的文件。

  ```sql
  [MERGE|APPEND|DELETE]
  DATA INFILE
  (
  "file_path1"[, file_path2, ...]
  )
  [NEGATIVE]
  INTO TABLE `table_name`
  [PARTITION (p1, p2, ...)]
  [COLUMNS TERMINATED BY "column_separator"]
  [LINES TERMINATED BY "line_delimiter"]
  [FORMAT AS "file_type"]
  [COMPRESS_TYPE AS "compress_type"]
  [(column_list)]
  [COLUMNS FROM PATH AS (c1, c2, ...)]
  [SET (column_mapping)]
  [PRECEDING FILTER predicate]
  [WHERE predicate]
  [DELETE ON expr]
  [ORDER BY source_sequence]
  [PROPERTIES ("key1"="value1", ...)]
  ```

  - `[MERGE|APPEND|DELETE]`

    数据合并类型。默认为 APPEND，表示本次导入是普通的追加写操作。MERGE 和 DELETE 类型仅适用于 Unique Key 模型表。其中 MERGE 类型需要配合 `[DELETE ON]` 语句使用，以标注 Delete Flag 列。而 DELETE 类型则表示本次导入的所有数据皆为删除数据。

  - `DATA INFILE`

    指定需要导入的文件路径。可以是多个。可以使用通配符。路径最终必须匹配到文件，如果只匹配到目录则导入会失败。

  - `NEGATIVE`

    该关键词用于表示本次导入为一批”负“导入。这种方式仅针对具有整型 SUM 聚合类型的聚合数据表。该方式会将导入数据中，SUM 聚合列对应的整型数值取反。主要用于冲抵之前导入错误的数据。

  - `PARTITION(p1, p2, ...)`

    可以指定仅导入表的某些分区。不在分区范围内的数据将被忽略。

  - `COLUMNS TERMINATED BY`

    指定列分隔符。仅在 CSV 格式下有效。仅能指定单字节分隔符。

  - `LINES TERMINATED BY`

    指定行分隔符。仅在 CSV 格式下有效。仅能指定单字节分隔符。

  - `FORMAT AS`

    指定文件类型，支持 CSV、PARQUET 和 ORC 格式。默认为 CSV。

  - `COMPRESS_TYPE AS`
    指定文件压缩类型, 支持GZ/BZ2/LZ4FRAME。

  - `column list`

    用于指定原始文件中的列顺序。关于这部分详细介绍，可以参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

    `(k1, k2, tmpk1)`

  - `COLUMNS FROM PATH AS`

    指定从导入文件路径中抽取的列。

  - `SET (column_mapping)`

    指定列的转换函数。

  - `PRECEDING FILTER predicate`

    前置过滤条件。数据首先根据 `column list` 和 `COLUMNS FROM PATH AS` 按顺序拼接成原始数据行。然后按照前置过滤条件进行过滤。关于这部分详细介绍，可以参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

  - `WHERE predicate`

    根据条件对导入的数据进行过滤。关于这部分详细介绍，可以参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

  - `DELETE ON expr`

    需配合 MEREGE 导入模式一起使用，仅针对 Unique Key 模型的表。用于指定导入数据中表示 Delete Flag 的列和计算关系。

  - `ORDER BY`

    仅针对 Unique Key 模型的表。用于指定导入数据中表示 Sequence Col 的列。主要用于导入时保证数据顺序。

  - `PROPERTIES ("key1"="value1", ...)`

    指定导入的format的一些参数。如导入的文件是`json`格式，则可以在这里指定`json_root`、`jsonpaths`、`fuzzy_parse`等参数。

- `WITH BROKER broker_name`

  指定需要使用的 Broker 服务名称。在公有云 Doris 中。Broker 服务名称为 `bos`

- `broker_properties`

  指定 broker 所需的信息。这些信息通常被用于 Broker 能够访问远端存储系统。如 BOS 或 HDFS。关于具体信息，可参阅 [Broker](../../../../advanced/broker.md) 文档。

  ```text
  (
      "key1" = "val1",
      "key2" = "val2",
      ...
  )
  ```

  - `load_properties`

    指定导入的相关参数。目前支持以下参数：

    - `timeout`

      导入超时时间。默认为 4 小时。单位秒。

    - `max_filter_ratio`

      最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。取值范围为 0 到 1。

    - `exec_mem_limit`

      导入内存限制。默认为 2GB。单位为字节。

    - `strict_mode`

      是否对数据进行严格限制。默认为 false。

    - `partial_columns`

      布尔类型，为 true 表示使用部分列更新，默认值为 false，该参数只允许在表模型为 Unique 且采用 Merge on Write 时设置。

    - `timezone`

      指定某些受时区影响的函数的时区，如 `strftime/alignment_timestamp/from_unixtime` 等等，具体请查阅 [时区](../../../../advanced/time-zone.md) 文档。如果不指定，则使用 "Asia/Shanghai" 时区

    - `load_parallelism`

      导入并发度，默认为1。调大导入并发度会启动多个执行计划同时执行导入任务，加快导入速度。 

    - `send_batch_parallelism`
    
      用于设置发送批处理数据的并行度，如果并行度的值超过 BE 配置中的 `max_send_batch_parallelism_per_job`，那么作为协调点的 BE 将使用 `max_send_batch_parallelism_per_job` 的值。
    
    - `load_to_single_tablet`
      
      布尔类型，为true表示支持一个任务只导入数据到对应分区的一个tablet，默认值为false，作业的任务数取决于整体并发度。该参数只允许在对带有random分桶的olap表导数的时候设置。

    - <version since="dev" type="inline"> priority </version>

      设置导入任务的优先级，可选 `HIGH/NORMAL/LOW` 三种优先级，默认为 `NORMAL`，对于处在 `PENDING` 状态的导入任务，更高优先级的任务将优先被执行进入 `LOADING` 状态。

-  <version since="1.2.3" type="inline"> comment </version>

   指定导入任务的备注信息。可选参数。

### Example

1. 从 HDFS 导入一批数据

   ```sql
   LOAD LABEL example_db.label1
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file.txt")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY ","
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ```

   导入文件 `file.txt`，按逗号分隔，导入到表 `my_table`。

2. 从 HDFS 导入数据，使用通配符匹配两批文件。分别导入到两个表中。

   ```sql
   LOAD LABEL example_db.label2
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file-10*")
       INTO TABLE `my_table1`
       PARTITION (p1)
       COLUMNS TERMINATED BY ","
       (k1, tmp_k2, tmp_k3)
       SET (
           k2 = tmp_k2 + 1,
           k3 = tmp_k3 + 1
       )
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file-20*")
       INTO TABLE `my_table2`
       COLUMNS TERMINATED BY ","
       (k1, k2, k3)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ```

   使用通配符匹配导入两批文件 `file-10*` 和 `file-20*`。分别导入到 `my_table1` 和 `my_table2` 两张表中。其中 `my_table1` 指定导入到分区 `p1` 中，并且将导入源文件中第二列和第三列的值 +1 后导入。

3. 从 HDFS 导入一批数据。

   ```sql
   LOAD LABEL example_db.label3
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/user/doris/data/*/*")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY "\\x01"
   )
   WITH BROKER my_hdfs_broker
   (
       "username" = "",
       "password" = "",
       "fs.defaultFS" = "hdfs://my_ha",
       "dfs.nameservices" = "my_ha",
       "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
       "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
       "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
       "dfs.client.failover.proxy.provider.my_ha" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   );
   ```

   指定分隔符为 Hive 的默认分隔符 `\\x01`，并使用通配符 * 指定 `data` 目录下所有目录的所有文件。使用简单认证，同时配置 namenode HA。

4. 导入 Parquet 格式数据，指定 FORMAT 为 parquet。默认是通过文件后缀判断

   ```sql
   LOAD LABEL example_db.label4
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file")
       INTO TABLE `my_table`
       FORMAT AS "parquet"
       (k1, k2, k3)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ```

5. 导入数据，并提取文件路径中的分区字段

   ```sql
   LOAD LABEL example_db.label10
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/city=beijing/*/*")
       INTO TABLE `my_table`
       FORMAT AS "csv"
       (k1, k2, k3)
       COLUMNS FROM PATH AS (city, utc_date)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ```

   `my_table` 表中的列为 `k1, k2, k3, city, utc_date`。

   其中 `hdfs://hdfs_host:hdfs_port/user/doris/data/input/dir/city=beijing` 目录下包括如下文件：

   ```text
   hdfs://hdfs_host:hdfs_port/input/city=beijing/utc_date=2020-10-01/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=beijing/utc_date=2020-10-02/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=tianji/utc_date=2020-10-03/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=tianji/utc_date=2020-10-04/0000.csv
   ```

   文件中只包含 `k1, k2, k3` 三列数据，`city, utc_date` 这两列数据会从文件路径中提取。

6. 对待导入数据进行过滤。

   ```sql
   LOAD LABEL example_db.label6
   (
       DATA INFILE("hdfs://host:port/input/file")
       INTO TABLE `my_table`
       (k1, k2, k3)
       SET (
           k2 = k2 + 1
       )
       PRECEDING FILTER k1 = 1
       WHERE k1 > k2
   )
   WITH BROKER hdfs
   (
       "username"="user",
       "password"="pass"
   );
   ```

   只有原始数据中，k1 = 1，并且转换后，k1 > k2 的行才会被导入。

7. 导入数据，提取文件路径中的时间分区字段，并且时间包含 %3A (在 hdfs 路径中，不允许有 ':'，所有 ':' 会由 %3A 替换)

   ```sql
   LOAD LABEL example_db.label7
   (
       DATA INFILE("hdfs://host:port/user/data/*/test.txt") 
       INTO TABLE `tbl12`
       COLUMNS TERMINATED BY ","
       (k2,k3)
       COLUMNS FROM PATH AS (data_time)
       SET (
           data_time=str_to_date(data_time, '%Y-%m-%d %H%%3A%i%%3A%s')
       )
   )
   WITH BROKER hdfs
   (
       "username"="user",
       "password"="pass"
   );
   ```

   路径下有如下文件：

   ```text
   /user/data/data_time=2020-02-17 00%3A00%3A00/test.txt
   /user/data/data_time=2020-02-18 00%3A00%3A00/test.txt
   ```

   表结构为：

   ```text
   data_time DATETIME,
   k2        INT,
   k3        INT
   ```

8. 从 HDFS 导入一批数据，指定超时时间和过滤比例。使用明文 my_hdfs_broker 的 broker。简单认证。并且将原有数据中与 导入数据中v2 大于100 的列相匹配的列删除，其他列正常导入

   ```sql
   LOAD LABEL example_db.label8
   (
       MERGE DATA INFILE("HDFS://test:802/input/file")
       INTO TABLE `my_table`
       (k1, k2, k3, v2, v1)
       DELETE ON v2 > 100
   )
   WITH HDFS
   (
       "hadoop.username"="user",
       "password"="pass"
   )
   PROPERTIES
   (
       "timeout" = "3600",
       "max_filter_ratio" = "0.1"
   );
   ```

   使用 MERGE 方式导入。`my_table` 必须是一张 Unique Key 的表。当导入数据中的 v2 列的值大于 100 时，该行会被认为是一个删除行。

   导入任务的超时时间是 3600 秒，并且允许错误率在 10% 以内。

9. 导入时指定source_sequence列，保证UNIQUE_KEYS表中的替换顺序：

   ```sql
   LOAD LABEL example_db.label9
   (
       DATA INFILE("HDFS://test:802/input/file")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY ","
       (k1,k2,source_sequence,v1,v2)
       ORDER BY source_sequence
   ) 
   WITH HDFS
   (
       "hadoop.username"="user",
       "password"="pass"
   )
   ```

   `my_table` 必须是 Unique Key 模型表，并且指定了 Sequcence Col。数据会按照源数据中 `source_sequence` 列的值来保证顺序性。

10. 从 HDFS 导入一批数据，指定文件格式为 `json` 并指定 `json_root`、`jsonpaths`

    ```sql
    LOAD LABEL example_db.label10
    (
        DATA INFILE("HDFS://test:port/input/file.json")
        INTO TABLE `my_table`
        FORMAT AS "json"
        PROPERTIES(
          "json_root" = "$.item",
          "jsonpaths" = "[$.id, $.city, $.code]"
        )       
    )
    with HDFS (
    "hadoop.username" = "user"
    "password" = ""
    )
    PROPERTIES
    (
    "timeout"="1200",
    "max_filter_ratio"="0.1"
    );
    ```

    `jsonpaths` 可与 `column list` 及 `SET (column_mapping)`配合：

    ```sql
    LOAD LABEL example_db.label10
    (
        DATA INFILE("HDFS://test:port/input/file.json")
        INTO TABLE `my_table`
        FORMAT AS "json"
        (id, code, city)
        SET (id = id * 10)
        PROPERTIES(
          "json_root" = "$.item",
          "jsonpaths" = "[$.id, $.code, $.city]"
        )       
    )
    with HDFS (
    "hadoop.username" = "user"
    "password" = ""
    )
    PROPERTIES
    (
    "timeout"="1200",
    "max_filter_ratio"="0.1"
    );

11. 从腾讯云cos中以csv格式导入数据。

    ```SQL
    LOAD LABEL example_db.label10
    (
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    )
    WITH BROKER "broker_name"
    (
        "fs.cosn.userinfo.secretId" = "xxx",
        "fs.cosn.userinfo.secretKey" = "xxxx",
        "fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxxxxx.myqcloud.com"
    )
    ```

12. 导入CSV数据时去掉双引号, 并跳过前5行。

    ```SQL
    LOAD LABEL example_db.label12
    (
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    PROPERTIES("trim_double_quotes" = "true", "skip_lines" = "5")
    )
    WITH BROKER "broker_name"
    (
        "fs.cosn.userinfo.secretId" = "xxx",
        "fs.cosn.userinfo.secretKey" = "xxxx",
        "fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxxxxx.myqcloud.com"
    )
    ```

### Keywords

    BROKER, LOAD

### Best Practice

1. 查看导入任务状态

   Broker Load 是一个异步导入过程，语句执行成功仅代表导入任务提交成功，并不代表数据导入成功。导入状态需要通过 [SHOW LOAD](../../Show-Statements/SHOW-LOAD.md) 命令查看。

2. 取消导入任务

   已提交切尚未结束的导入任务可以通过 [CANCEL LOAD](./CANCEL-LOAD.md) 命令取消。取消后，已写入的数据也会回滚，不会生效。

3. Label、导入事务、多表原子性

   Doris 中所有导入任务都是原子生效的。并且在同一个导入任务中对多张表的导入也能够保证原子性。同时，Doris 还可以通过 Label 的机制来保证数据导入的不丢不重。具体说明可以参阅 [导入事务和原子性](../../../../data-operate/import/import-scenes/load-atomicity.md) 文档。

4. 列映射、衍生列和过滤

   Doris 可以在导入语句中支持非常丰富的列转换和过滤操作。支持绝大多数内置函数和 UDF。关于如何正确的使用这个功能，可参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

5. 错误数据过滤

   Doris 的导入任务可以容忍一部分格式错误的数据。容忍了通过 `max_filter_ratio` 设置。默认为0，即表示当有一条错误数据时，整个导入任务将会失败。如果用户希望忽略部分有问题的数据行，可以将次参数设置为 0~1 之间的数值，Doris 会自动跳过哪些数据格式不正确的行。

   关于容忍率的一些计算方式，可以参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

6. 严格模式

   `strict_mode` 属性用于设置导入任务是否运行在严格模式下。该格式会对列映射、转换和过滤的结果产生影响。关于严格模式的具体说明，可参阅 [严格模式](../../../../data-operate/import/import-scenes/load-strict-mode.md) 文档。

7. 超时时间

   Broker Load 的默认超时时间为 4 小时。从任务提交开始算起。如果在超时时间内没有完成，则任务会失败。

8. 数据量和任务数限制

   Broker Load 适合在一个导入任务中导入100GB以内的数据。虽然理论上在一个导入任务中导入的数据量没有上限。但是提交过大的导入会导致运行时间较长，并且失败后重试的代价也会增加。

   同时受限于集群规模，我们限制了导入的最大数据量为 ComputeNode 节点数 * 3GB。以保证系统资源的合理利用。如果有大数据量需要导入，建议分成多个导入任务提交。

   Doris 同时会限制集群内同时运行的导入任务数量，通常在 3-10 个不等。之后提交的导入作业会排队等待。队列最大长度为 100。之后的提交会直接拒绝。注意排队时间也被计算到了作业总时间中。如果超时，则作业会被取消。所以建议通过监控作业运行状态来合理控制作业提交频率。
