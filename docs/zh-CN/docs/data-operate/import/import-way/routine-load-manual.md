---
{
    "title": "Routine Load",
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

# Routine Load

例行导入（Routine Load）功能，支持用户提交一个常驻的导入任务，通过不断的从指定的数据源读取数据，将数据导入到 Doris 中。

本文档主要介绍该功能的实现原理、使用方式以及最佳实践。

## 基本原理

```text
         +---------+
         |  Client |
         +----+----+
              |
+-----------------------------+
| FE          |               |
| +-----------v------------+  |
| |                        |  |
| |   Routine Load Job     |  |
| |                        |  |
| +---+--------+--------+--+  |
|     |        |        |     |
| +---v--+ +---v--+ +---v--+  |
| | task | | task | | task |  |
| +--+---+ +---+--+ +---+--+  |
|    |         |        |     |
+-----------------------------+
     |         |        |
     v         v        v
 +---+--+   +--+---+   ++-----+
 |  BE  |   |  BE  |   |  BE  |
 +------+   +------+   +------+
```

如上图，Client 向 FE 提交一个Routine Load 作业。

1. FE 通过 JobScheduler 将一个导入作业拆分成若干个 Task。每个 Task 负责导入指定的一部分数据。Task 被 TaskScheduler 分配到指定的 BE 上执行。

2. 在 BE 上，一个 Task 被视为一个普通的导入任务，通过 Stream Load 的导入机制进行导入。导入完成后，向 FE 汇报。

3. FE 中的 JobScheduler 根据汇报结果，继续生成后续新的 Task，或者对失败的 Task 进行重试。

4. 整个 Routine Load 作业通过不断的产生新的 Task，来完成数据不间断的导入。


## Kafka例行导入

当前我们仅支持从 Kafka 进行例行导入。该部分会详细介绍 Kafka 例行导入使用方式和最佳实践。

### 使用限制

1. 支持无认证的 Kafka 访问，以及通过 SSL 方式认证的 Kafka 集群。
2. 支持的消息格式为 csv， json 文本格式。csv 每一个 message 为一行，且行尾**不包含**换行符。
3. 默认支持 Kafka 0.10.0.0(含) 以上版本。如果要使用 Kafka 0.10.0.0 以下版本 (0.9.0, 0.8.2, 0.8.1, 0.8.0)，需要修改 be 的配置，将 kafka_broker_version_fallback 的值设置为要兼容的旧版本，或者在创建routine load的时候直接设置 property.broker.version.fallback的值为要兼容的旧版本，使用旧版本的代价是routine load 的部分新特性可能无法使用，如根据时间设置 kafka 分区的 offset。

### 创建任务

创建例行导入任务的详细语法可以连接到 Doris 后，查看[CREATE ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md)命令手册，或者执行 `HELP ROUTINE LOAD;` 查看语法帮助。

下面我们以几个例子说明如何创建Routine Load任务：

1. 为example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务。指定列分隔符和 group.id 和 client.id，并且自动默认消费所有分区，且从有数据的位置（OFFSET_BEGINNING）开始订阅。 

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS TERMINATED BY ",",
        COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100)
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "false"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "property.group.id" = "xxx",
            "property.client.id" = "xxx",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        );
```

2. 以 **严格模式** 为 example_db 的 example_tbl 创建一个名为 test1 的 Kafka 例行导入任务；

```sql
CREATE ROUTINE LOAD example_db.test1 ON example_tbl
        COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
        WHERE k1 > 100 and k2 like "%doris%"
        PROPERTIES
        (
            "desired_concurrent_number"="3",
            "max_batch_interval" = "20",
            "max_batch_rows" = "300000",
            "max_batch_size" = "209715200",
            "strict_mode" = "true"
        )
        FROM KAFKA
        (
            "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
            "kafka_topic" = "my_topic",
            "kafka_partitions" = "0,1,2,3",
            "kafka_offsets" = "101,0,0,200"
        );

```

3. 导入Json格式数据使用示例

   Routine Load导入的json格式仅支持以下两种

   第一种只有一条记录，且为json对象：

   ```json
   {"category":"a9jadhx","author":"test","price":895}
   ```

   第二种为json数组，数组中可含多条记录

   ```json
   [
       {   
           "category":"11",
           "author":"4avc",
           "price":895,
           "timestamp":1589191587
       },
       {
           "category":"22",
           "author":"2avc",
           "price":895,
           "timestamp":1589191487
       },
       {
           "category":"33",
           "author":"3avc",
           "price":342,
           "timestamp":1589191387
       }
   ]
   ```
   
   创建待导入的Doris数据表
   
   ```sql
   CREATE TABLE `example_tbl` (
      `category` varchar(24) NULL COMMENT "",
      `author` varchar(24) NULL COMMENT "",
      `timestamp` bigint(20) NULL COMMENT "",
      `dt` int(11) NULL COMMENT "",
      `price` double REPLACE
   ) ENGINE=OLAP
   AGGREGATE KEY(`category`,`author`,`timestamp`,`dt`)
   COMMENT "OLAP"
   PARTITION BY RANGE(`dt`)
   (
     PARTITION p0 VALUES [("-2147483648"), ("20200509")),
   	PARTITION p20200509 VALUES [("20200509"), ("20200510")),
   	PARTITION p20200510 VALUES [("20200510"), ("20200511")),
   	PARTITION p20200511 VALUES [("20200511"), ("20200512"))
   )
   DISTRIBUTED BY HASH(`category`,`author`,`timestamp`) BUCKETS 4
   PROPERTIES (
       "replication_num" = "1"
   );
   ```
   
   以简单模式导入json数据
   
   ```sql
   CREATE ROUTINE LOAD example_db.test_json_label_1 ON table1
   COLUMNS(category,price,author)
   PROPERTIES
   (
   	"desired_concurrent_number"="3",
   	"max_batch_interval" = "20",
   	"max_batch_rows" = "300000",
   	"max_batch_size" = "209715200",
   	"strict_mode" = "false",
   	"format" = "json"
   )
   FROM KAFKA
   (
   	"kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
   	"kafka_topic" = "my_topic",
   	"kafka_partitions" = "0,1,2",
   	"kafka_offsets" = "0,0,0"
    );
   ```
   
   精准导入json格式数据
   
   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false",
       "format" = "json",
       "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
       "strip_outer_array" = "true"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2",
       "kafka_offsets" = "0,0,0"
   );
   ```

**注意：** 表里的分区字段 `dt`  在我们的数据里并没有，而是在我们Routine load 语句里通过 `dt=from_unixtime(timestamp, '%Y%m%d')` 转换出来的



**strict mode 与 source data 的导入关系**

这里以列类型为 TinyInt 来举例

> 注：当表中的列允许导入空值时

| source data | source data example | string to int | strict_mode   | result                 |
| ----------- | ------------------- | ------------- | ------------- | ---------------------- |
| 空值        | \N                  | N/A           | true or false | NULL                   |
| not null    | aaa or 2000         | NULL          | true          | invalid data(filtered) |
| not null    | aaa                 | NULL          | false         | NULL                   |
| not null    | 1                   | 1             | true or false | correct data           |

这里以列类型为 Decimal(1,0) 举例

> 注：当表中的列允许导入空值时

| source data | source data example | string to int | strict_mode   | result                 |
| ----------- | ------------------- | ------------- | ------------- | ---------------------- |
| 空值        | \N                  | N/A           | true or false | NULL                   |
| not null    | aaa                 | NULL          | true          | invalid data(filtered) |
| not null    | aaa                 | NULL          | false         | NULL                   |
| not null    | 1 or 10             | 1             | true or false | correct data           |

> 注意：10 虽然是一个超过范围的值，但是因为其类型符合 decimal的要求，所以 strict mode对其不产生影响。10 最后会在其他 ETL 处理流程中被过滤。但不会被 strict mode 过滤。

**访问 SSL 认证的 Kafka 集群**

访问 SSL 认证的 Kafka 集群需要用户提供用于认证 Kafka Broker 公钥的证书文件（ca.pem）。如果 Kafka 集群同时开启了客户端认证，则还需提供客户端的公钥（client.pem）、密钥文件（client.key），以及密钥密码。这里所需的文件需要先通过 `CREAE FILE` 命令上传到 Doris 中，**并且 catalog 名称为 `kafka`**。`CREATE FILE` 命令的具体帮助可以参见 `HELP CREATE FILE;`。这里给出示例：

1. 上传文件

   ```sql
   CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
   CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
   CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
   ```

2. 创建例行导入作业

   ```sql
   CREATE ROUTINE LOAD db1.job1 on tbl1
   PROPERTIES
   (
       "desired_concurrent_number"="1"
   )
   FROM KAFKA
   (
       "kafka_broker_list"= "broker1:9091,broker2:9091",
       "kafka_topic" = "my_topic",
       "property.security.protocol" = "ssl",
       "property.ssl.ca.location" = "FILE:ca.pem",
       "property.ssl.certificate.location" = "FILE:client.pem",
       "property.ssl.key.location" = "FILE:client.key",
       "property.ssl.key.password" = "abcdefg"
   );
   ```

> Doris 通过 Kafka 的 C++ API `librdkafka` 来访问 Kafka 集群。`librdkafka` 所支持的参数可以参阅
>
> [https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

### 查看作业状态

查看**作业**状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD;` 命令查看。

查看**任务**运行状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD TASK;` 命令查看。

只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

### 修改作业属性

用户可以修改已经创建的作业。具体说明可以通过 `HELP ALTER ROUTINE LOAD;` 命令查看或参阅 [ALTER ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/ALTER-ROUTINE-LOAD.md)。

### 作业控制

用户可以通过 `STOP/PAUSE/RESUME` 三个命令来控制作业的停止，暂停和重启。可以通过 `HELP STOP ROUTINE LOAD;` `HELP PAUSE ROUTINE LOAD;` 以及 `HELP RESUME ROUTINE LOAD;` 三个命令查看帮助和示例。

## 其他说明

1. 例行导入作业和 ALTER TABLE 操作的关系

   - 例行导入不会阻塞 SCHEMA CHANGE 和 ROLLUP 操作。但是注意如果 SCHEMA CHANGE 完成后，列映射关系无法匹配，则会导致作业的错误数据激增，最终导致作业暂停。建议通过在例行导入作业中显式指定列映射关系，以及通过增加 Nullable 列或带 Default 值的列来减少这类问题。
   - 删除表的 Partition 可能会导致导入数据无法找到对应的 Partition，作业进入暂停。

2. 例行导入作业和其他导入作业的关系（LOAD, DELETE, INSERT）

   - 例行导入和其他 LOAD 作业以及 INSERT 操作没有冲突。
   - 当执行 DELETE 操作时，对应表分区不能有任何正在执行的导入任务。所以在执行 DELETE 操作前，可能需要先暂停例行导入作业，并等待已下发的 task 全部完成后，才可以执行 DELETE。

3. 例行导入作业和 DROP DATABASE/TABLE 操作的关系

   当例行导入对应的 database 或 table 被删除后，作业会自动 CANCEL。

4. kafka 类型的例行导入作业和 kafka topic 的关系

   当用户在创建例行导入声明的 `kafka_topic` 在kafka集群中不存在时。

   - 如果用户 kafka 集群的 broker 设置了 `auto.create.topics.enable = true`，则 `kafka_topic` 会先被自动创建，自动创建的 partition 个数是由**用户方的kafka集群**中的 broker 配置 `num.partitions` 决定的。例行作业会正常的不断读取该 topic 的数据。
   - 如果用户 kafka 集群的 broker 设置了 `auto.create.topics.enable = false`, 则 topic 不会被自动创建，例行作业会在没有读取任何数据之前就被暂停，状态为 `PAUSED`。

   所以，如果用户希望当 kafka topic 不存在的时候，被例行作业自动创建的话，只需要将**用户方的kafka集群**中的 broker 设置 `auto.create.topics.enable = true` 即可。

5. 在网络隔离的环境中可能出现的问题 在有些环境中存在网段和域名解析的隔离措施，所以需要注意

   1. 创建Routine load 任务中指定的 Broker list 必须能够被Doris服务访问
   2. Kafka 中如果配置了`advertised.listeners`, `advertised.listeners` 中的地址必须能够被Doris服务访问

6. 关于指定消费的 Partition 和 Offset

   Doris 支持指定 Partition 和 Offset 开始消费。新版中还支持了指定时间点进行消费的功能。这里说明下对应参数的配置关系。

   有三个相关参数：

   - `kafka_partitions`：指定待消费的 partition 列表，如："0, 1, 2, 3"。
   - `kafka_offsets`：指定每个分区的起始offset，必须和 `kafka_partitions` 列表个数对应。如："1000, 1000, 2000, 2000"
   - `property.kafka_default_offset`：指定分区默认的起始offset。

   在创建导入作业时，这三个参数可以有以下组合：

   | 组合 | `kafka_partitions` | `kafka_offsets` | `property.kafka_default_offset` | 行为                                                         |
   | ---- | ------------------ | --------------- | ------------------------------- | ------------------------------------------------------------ |
   | 1    | No                 | No              | No                              | 系统会自动查找topic对应的所有分区并从 OFFSET_END 开始消费    |
   | 2    | No                 | No              | Yes                             | 系统会自动查找topic对应的所有分区并从 default offset 指定的位置开始消费 |
   | 3    | Yes                | No              | No                              | 系统会从指定分区的 OFFSET_END 开始消费                       |
   | 4    | Yes                | Yes             | No                              | 系统会从指定分区的指定offset 处开始消费                      |
   | 5    | Yes                | No              | Yes                             | 系统会从指定分区，default offset 指定的位置开始消费          |

7. STOP和PAUSE的区别

   FE会自动定期清理STOP状态的ROUTINE LOAD，而PAUSE状态的则可以再次被恢复启用。

## 相关参数

一些系统配置参数会影响例行导入的使用。

1. max_routine_load_task_concurrent_num

   FE 配置项，默认为 5，可以运行时修改。该参数限制了一个例行导入作业最大的子任务并发数。建议维持默认值。设置过大，可能导致同时并发的任务数过多，占用集群资源。

2. max_routine_load_task_num_per_be

   FE 配置项，默认为5，可以运行时修改。该参数限制了每个 BE 节点最多并发执行的子任务个数。建议维持默认值。如果设置过大，可能导致并发任务数过多，占用集群资源。

3. max_routine_load_job_num

   FE 配置项，默认为100，可以运行时修改。该参数限制的例行导入作业的总数，包括 NEED_SCHEDULED, RUNNING, PAUSE 这些状态。超过后，不能在提交新的作业。

4. max_consumer_num_per_group

   BE 配置项，默认为 3。该参数表示一个子任务中最多生成几个 consumer 进行数据消费。对于 Kafka 数据源，一个 consumer 可能消费一个或多个 kafka partition。假设一个任务需要消费 6 个 kafka partition，则会生成 3 个 consumer，每个 consumer 消费 2 个 partition。如果只有 2 个 partition，则只会生成 2 个 consumer，每个 consumer 消费 1 个 partition。

5. max_tolerable_backend_down_num FE 配置项，默认值是0。在满足某些条件下，Doris可PAUSED的任务重新调度，即变成RUNNING。该参数为0代表只有所有BE节点是alive状态才允许重新调度。

6. period_of_auto_resume_min FE 配置项，默认是5分钟。Doris重新调度，只会在5分钟这个周期内，最多尝试3次. 如果3次都失败则锁定当前任务，后续不在进行调度。但可通过人为干预，进行手动恢复。

## 更多帮助

关于 **Routine Load** 使用的更多详细语法，可以在Mysql客户端命令行下输入 `HELP ROUTINE LOAD` 获取更多帮助信息。
