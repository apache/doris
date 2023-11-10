---
{
    "title": "Broker Load",
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

# Broker Load

Broker load 是一个异步的导入方式，支持的数据源取决于 [Broker](../../../advanced/broker.md) 进程支持的数据源。

因为 Doris 表里的数据是有序的，所以 Broker load 在导入数据的时是要利用doris 集群资源对数据进行排序，相对于 Spark load 来完成海量历史数据迁移，对 Doris 的集群资源占用要比较大，这种方式是在用户没有 Spark 这种计算资源的情况下使用，如果有 Spark 计算资源建议使用   [Spark load](../../../data-operate/import/import-way/spark-load-manual.md)。

用户需要通过 MySQL 协议 创建 [Broker load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 导入，并通过查看导入命令检查导入结果。

## 适用场景

* 源数据在 Broker 可以访问的存储系统中，如 HDFS。
* 数据量在 几十到百GB 级别。

## 基本原理

用户在提交导入任务后，FE 会生成对应的 Plan 并根据目前 BE 的个数和文件的大小，将 Plan 分给 多个 BE 执行，每个 BE 执行一部分导入数据。

BE 在执行的过程中会从 Broker 拉取数据，在对数据 transform 之后将数据导入系统。所有 BE 均完成导入，由 FE 最终决定导入是否成功。

```
                 +
                 | 1. user create broker load
                 v
            +----+----+
            |         |
            |   FE    |
            |         |
            +----+----+
                 |
                 | 2. BE etl and load the data
    +--------------------------+
    |            |             |
+---v---+     +--v----+    +---v---+
|       |     |       |    |       |
|  BE   |     |  BE   |    |   BE  |
|       |     |       |    |       |
+---+-^-+     +---+-^-+    +--+-^--+
    | |           | |         | |
    | |           | |         | | 3. pull data from broker
+---v-+-+     +---v-+-+    +--v-+--+
|       |     |       |    |       |
|Broker |     |Broker |    |Broker |
|       |     |       |    |       |
+---+-^-+     +---+-^-+    +---+-^-+
    | |           | |          | |
+---v-+-----------v-+----------v-+-+
|       HDFS/BOS/AFS cluster       |
|                                  |
+----------------------------------+

```

## 开始导入

下面我们通过几个实际的场景示例来看 [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 的使用

### Hive 分区表的数据导入

1. 创建 Hive 表

```sql
##数据格式是：默认，分区字段是：day
CREATE TABLE `ods_demo_detail`(
  `id` string, 
  `store_id` string, 
  `company_id` string, 
  `tower_id` string, 
  `commodity_id` string, 
  `commodity_name` string, 
  `commodity_price` double, 
  `member_price` double, 
  `cost_price` double, 
  `unit` string, 
  `quantity` double, 
  `actual_price` double
)
PARTITIONED BY (day string)
row format delimited fields terminated by ','
lines terminated by '\n'
```

然后使用 Hive 的 Load 命令将你的数据导入到 Hive 表中

```
load data local inpath '/opt/custorm' into table ods_demo_detail;
```

2. 创建 Doris 表，具体建表语法参照：[CREATE TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)

```
CREATE TABLE `doris_ods_test_detail` (
  `rq` date NULL,
  `id` varchar(32) NOT NULL,
  `store_id` varchar(32) NULL,
  `company_id` varchar(32) NULL,
  `tower_id` varchar(32) NULL,
  `commodity_id` varchar(32) NULL,
  `commodity_name` varchar(500) NULL,
  `commodity_price` decimal(10, 2) NULL,
  `member_price` decimal(10, 2) NULL,
  `cost_price` decimal(10, 2) NULL,
  `unit` varchar(50) NULL,
  `quantity` int(11) NULL,
  `actual_price` decimal(10, 2) NULL
) ENGINE=OLAP
UNIQUE KEY(`rq`, `id`, `store_id`)
PARTITION BY RANGE(`rq`)
(
PARTITION P_202204 VALUES [('2022-04-01'), ('2022-05-01')))
DISTRIBUTED BY HASH(`store_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.end" = "2",
"dynamic_partition.prefix" = "P_",
"dynamic_partition.buckets" = "1",
"in_memory" = "false",
"storage_format" = "V2"
);
```

3. 开始导入数据

   具体语法参照： [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 
```sql
LOAD LABEL broker_load_2022_03_23
(
    DATA INFILE("hdfs://192.168.20.123:8020/user/hive/warehouse/ods.db/ods_demo_detail/*/*")
    INTO TABLE doris_ods_test_detail
    COLUMNS TERMINATED BY ","
  (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price) 
    COLUMNS FROM PATH AS (`day`)
   SET 
   (rq = str_to_date(`day`,'%Y-%m-%d'),id=id,store_id=store_id,company_id=company_id,tower_id=tower_id,commodity_id=commodity_id,commodity_name=commodity_name,commodity_price=commodity_price,member_price=member_price,cost_price=cost_price,unit=unit,quantity=quantity,actual_price=actual_price)
	)
WITH BROKER "broker_name_1" 
	( 
	  "username" = "hdfs", 
	  "password" = "" 
	)
PROPERTIES
(
    "timeout"="1200",
    "max_filter_ratio"="0.1"
);
```
### Hive 分区表导入(ORC格式)

1. 创建Hive分区表，ORC格式

```sql
#数据格式:ORC 分区:day
CREATE TABLE `ods_demo_orc_detail`(
  `id` string, 
  `store_id` string, 
  `company_id` string, 
  `tower_id` string, 
  `commodity_id` string, 
  `commodity_name` string, 
  `commodity_price` double, 
  `member_price` double, 
  `cost_price` double, 
  `unit` string, 
  `quantity` double, 
  `actual_price` double
)
PARTITIONED BY (day string)
row format delimited fields terminated by ','
lines terminated by '\n'
STORED AS ORC
```

2. 创建Doris表，这里的建表语句和上面的Doris建表语句一样，请参考上面的.

3. 使用 Broker Load 导入数据

   ```sql
   LOAD LABEL dish_2022_03_23
   (
       DATA INFILE("hdfs://10.220.147.151:8020/user/hive/warehouse/ods.db/ods_demo_orc_detail/*/*")
       INTO TABLE doris_ods_test_detail
       COLUMNS TERMINATED BY ","
       FORMAT AS "orc"
   (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price) 
       COLUMNS FROM PATH AS (`day`)
      SET 
      (rq = str_to_date(`day`,'%Y-%m-%d'),id=id,store_id=store_id,company_id=company_id,tower_id=tower_id,commodity_id=commodity_id,commodity_name=commodity_name,commodity_price=commodity_price,member_price=member_price,cost_price=cost_price,unit=unit,quantity=quantity,actual_price=actual_price)
   	)
   WITH BROKER "broker_name_1" 
   	( 
   	  "username" = "hdfs", 
   	  "password" = "" 
   	)
   PROPERTIES
   (
       "timeout"="1200",
       "max_filter_ratio"="0.1"
   );
   ```

   **注意：**

   -  `FORMAT AS "orc"` : 这里我们指定了要导入的数据格式
   - `SET` : 这里我们定义了 Hive 表和 Doris 表之间的字段映射关系及字段转换的一些操作

### HDFS文件系统数据导入

我们继续以上面创建好的 Doris 表为例，演示通过 Broker Load 从 HDFS 上导入数据。

导入作业的语句如下：

```sql
LOAD LABEL demo.label_20220402
        (
            DATA INFILE("hdfs://10.220.147.151:8020/tmp/test_hdfs.txt")
            INTO TABLE `ods_dish_detail_test`
            COLUMNS TERMINATED BY "\t"            (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price)
        ) 
        with HDFS (
            "fs.defaultFS"="hdfs://10.220.147.151:8020",
            "hadoop.username"="root"
        )
        PROPERTIES
        (
            "timeout"="1200",
            "max_filter_ratio"="0.1"
        );
```

这里的具体 参数可以参照：  [Broker](../../../advanced/broker.md)  及 [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 文档

## 查看导入状态

我们可以通过下面的命令查看上面导入任务的状态信息，

具体的查看导入状态的语法参考 [SHOW LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD.md)

```sql
mysql> show load order by createtime desc limit 1\G;
*************************** 1. row ***************************
         JobId: 41326624
         Label: broker_load_2022_03_23
         State: FINISHED
      Progress: ETL:100%; LOAD:100%
          Type: BROKER
       EtlInfo: unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=27
      TaskInfo: cluster:N/A; timeout(s):1200; max_filter_ratio:0.1
      ErrorMsg: NULL
    CreateTime: 2022-04-01 18:59:06
  EtlStartTime: 2022-04-01 18:59:11
 EtlFinishTime: 2022-04-01 18:59:11
 LoadStartTime: 2022-04-01 18:59:11
LoadFinishTime: 2022-04-01 18:59:11
           URL: NULL
    JobDetails: {"Unfinished backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[]},"ScannedRows":27,"TaskNumber":1,"All backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[36728051]},"FileNumber":1,"FileSize":5540}
1 row in set (0.01 sec)
```

## 取消导入

当 Broker load 作业状态不为 CANCELLED 或 FINISHED 时，可以被用户手动取消。取消时需要指定待取消导入任务的 Label 。取消导入命令语法可执行 [CANCEL LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CANCEL-LOAD.md) 查看。

例如：撤销数据库 demo 上， label 为 broker_load_2022_03_23 的导入作业

```sql
CANCEL LOAD FROM demo WHERE LABEL = "broker_load_2022_03_23";
```

## 相关系统配置

###  Broker 参数

Broker Load 需要借助 Broker 进程访问远端存储，不同的 Broker 需要提供不同的参数，具体请参阅 [Broker文档](../../../advanced/broker.md) 。

### FE 配置

下面几个配置属于 Broker load 的系统级别配置，也就是作用于所有 Broker load 导入任务的配置。主要通过修改 `fe.conf`来调整配置值。

- min_bytes_per_broker_scanner/max_bytes_per_broker_scanner/max_broker_concurrency

  前两个配置限制了单个 BE 处理的数据量的最小和最大值。第三个配置限制了一个作业的最大的导入并发数。最小处理的数据量，最大并发数，源文件的大小和当前集群 BE 的个数 **共同决定了本次导入的并发数**。

  ```text
  本次导入并发数 = Math.min(源文件大小/最小处理量，最大并发数，当前BE节点个数)
  本次导入单个BE的处理量 = 源文件大小/本次导入的并发数
  ```

  通常一个导入作业支持的最大数据量为 `max_bytes_per_broker_scanner * BE 节点数`。如果需要导入更大数据量，则需要适当调整 `max_bytes_per_broker_scanner` 参数的大小。

  默认配置：

  ```text
  参数名：min_bytes_per_broker_scanner， 默认 64MB，单位bytes。
  参数名：max_broker_concurrency， 默认 10。
  参数名：max_bytes_per_broker_scanner，默认 500G，单位bytes。
  ```

## 最佳实践

### 应用场景

使用 Broker load 最适合的场景就是原始数据在文件系统（HDFS，BOS，AFS）中的场景。其次，由于 Broker load 是单次导入中唯一的一种异步导入的方式，所以如果用户在导入大文件中，需要使用异步接入，也可以考虑使用 Broker load。

### 数据量

这里仅讨论单个 BE 的情况，如果用户集群有多个 BE 则下面标题中的数据量应该乘以 BE 个数来计算。比如：如果用户有3个 BE，则 3G 以下（包含）则应该乘以 3，也就是 9G 以下（包含）。

- 3G 以下（包含）

  用户可以直接提交 Broker load 创建导入请求。

- 3G 以上

  由于单个导入 BE 最大的处理量为 3G，超过 3G 的待导入文件就需要通过调整 Broker load 的导入参数来实现大文件的导入。

  1. 根据当前 BE 的个数和原始文件的大小修改单个 BE 的最大扫描量和最大并发数。

     ```text
     修改 fe.conf 中配置
     max_broker_concurrency = BE 个数
     当前导入任务单个 BE 处理的数据量 = 原始文件大小 / max_broker_concurrency
     max_bytes_per_broker_scanner >= 当前导入任务单个 BE 处理的数据量
     
     比如一个 100G 的文件，集群的 BE 个数为 10 个
     max_broker_concurrency = 10
     # >= 10G = 100G / 10
     max_bytes_per_broker_scanner = 1069547520
     ```

     修改后，所有的 BE 会并发的处理导入任务，每个 BE 处理原始文件的一部分。

     *注意：上述两个 FE 中的配置均为系统配置，也就是说其修改是作用于所有的 Broker load的任务的。*

  2. 在创建导入的时候自定义当前导入任务的 timeout 时间

     ```text
     当前导入任务单个 BE 处理的数据量 / 用户 Doris 集群最慢导入速度(MB/s) >= 当前导入任务的 timeout 时间 >= 当前导入任务单个 BE 处理的数据量 / 10M/s
     
     比如一个 100G 的文件，集群的 BE 个数为 10个
     # >= 1000s = 10G / 10M/s
     timeout = 1000
     ```

  3. 当用户发现第二步计算出的 timeout 时间超过系统默认的导入最大超时时间 4小时

     这时候不推荐用户将导入最大超时时间直接改大来解决问题。单个导入时间如果超过默认的导入最大超时时间4小时，最好是通过切分待导入文件并且分多次导入来解决问题。主要原因是：单次导入超过4小时的话，导入失败后重试的时间成本很高。

     可以通过如下公式计算出 Doris 集群期望最大导入文件数据量：

     ```text
     期望最大导入文件数据量 = 14400s * 10M/s * BE 个数
     比如：集群的 BE 个数为 10个
     期望最大导入文件数据量 = 14400s * 10M/s * 10 = 1440000M ≈ 1440G
     
     注意：一般用户的环境可能达不到 10M/s 的速度，所以建议超过 500G 的文件都进行文件切分，再导入。
     ```

### 作业调度

系统会限制一个集群内，正在运行的 Broker Load 作业数量，以防止同时运行过多的 Load 作业。

首先， FE 的配置参数：`desired_max_waiting_jobs` 会限制一个集群内，未开始或正在运行（作业状态为 PENDING 或 LOADING）的 Broker Load 作业数量。默认为 100。如果超过这个阈值，新提交的作业将会被直接拒绝。

一个 Broker Load 作业会被分为 pending task 和 loading task 阶段。其中 pending task 负责获取导入文件的信息，而 loading task 会发送给BE执行具体的导入任务。

FE 的配置参数 `async_pending_load_task_pool_size` 用于限制同时运行的 pending task 的任务数量。也相当于控制了实际正在运行的导入任务数量。该参数默认为 10。也就是说，假设用户提交了100个Load作业，同时只会有10个作业会进入 LOADING 状态开始执行，而其他作业处于 PENDING 等待状态。

FE 的配置参数 `async_loading_load_task_pool_size` 用于限制同时运行的 loading task 的任务数量。一个 Broker Load 作业会有 1 个 pending task 和多个 loading task （等于 LOAD 语句中 DATA INFILE 子句的个数）。所以 `async_loading_load_task_pool_size` 应该大于等于 `async_pending_load_task_pool_size`。

### 性能分析

可以在提交 LOAD 作业前，先执行 `set enable_profile=true` 打开会话变量。然后提交导入作业。待导入作业完成后，可以在 FE 的 web 页面的 `Queris` 标签中查看到导入作业的 Profile。

可以查看 [SHOW LOAD PROFILE](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD-PROFILE.md) 帮助文档，获取更多使用帮助信息。

这个 Profile 可以帮助分析导入作业的运行状态。

当前只有作业成功执行后，才能查看 Profile

## 常见问题

- 导入报错：`Scan bytes per broker scanner exceed limit:xxx`

  请参照文档中最佳实践部分，修改 FE 配置项 `max_bytes_per_broker_scanner` 和 `max_broker_concurrency`

- 导入报错：`failed to send batch` 或 `TabletWriter add batch with unknown id`

  适当修改 `query_timeout` 和 `streaming_load_rpc_max_alive_time_sec`。

  streaming_load_rpc_max_alive_time_sec：

  在导入过程中，Doris 会为每一个 Tablet 开启一个 Writer，用于接收数据并写入。这个参数指定了 Writer 的等待超时时间。如果在这个时间内，Writer 没有收到任何数据，则 Writer 会被自动销毁。当系统处理速度较慢时，Writer 可能长时间接收不到下一批数据，导致导入报错：`TabletWriter add batch with unknown id`。此时可适当增大这个配置。默认为 600 秒

- 导入报错：`LOAD_RUN_FAIL; msg:Invalid Column Name:xxx`

  如果是PARQUET或者ORC格式的数据，则文件头的列名需要与doris表中的列名保持一致，如:

  ```text
  (tmp_c1,tmp_c2)
  SET
  (
      id=tmp_c2,
      name=tmp_c1
  )
  ```

  代表获取在 parquet 或 orc 中以(tmp_c1, tmp_c2)为列名的列，映射到 doris 表中的(id, name)列。如果没有设置set, 则以column中的列作为映射。

  注：如果使用某些 hive 版本直接生成的 orc 文件，orc 文件中的表头并非 hive meta 数据，而是（_col0, _col1, _col2, ...）, 可能导致 Invalid Column Name 错误，那么则需要使用 set 进行映射

- 导入报错：`Failed to get S3 FileSystem for bucket is null/empty`
  1. bucket信息填写不正确或者不存在。
  2. bucket的格式不受支持。使用GCS创建带`_`的桶名时，比如：`s3://gs_bucket/load_tbl`，S3 Client访问GCS会报错，建议创建bucket路径时不使用`_`。

## 更多帮助

关于 Broker Load 使用的更多详细语法及最佳实践，请参阅 [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP BROKER LOAD` 获取更多帮助信息。
