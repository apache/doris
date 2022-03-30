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

例行导入（Routine Load）功能为用户提供了一种自动从指定数据源进行数据导入的功能。

本文档主要介绍该功能的实现原理、使用方式以及最佳实践。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* RoutineLoadJob：用户提交的一个例行导入作业。
* JobScheduler：例行导入作业调度器，用于调度和拆分一个 RoutineLoadJob 为多个 Task。
* Task：RoutineLoadJob 被 JobScheduler 根据规则拆分的子任务。
* TaskScheduler：任务调度器。用于调度 Task 的执行。

## 原理

```
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

如上图，Client 向 FE 提交一个例行导入作业。

FE 通过 JobScheduler 将一个导入作业拆分成若干个 Task。每个 Task 负责导入指定的一部分数据。Task 被 TaskScheduler 分配到指定的 BE 上执行。

在 BE 上，一个 Task 被视为一个普通的导入任务，通过 Stream Load 的导入机制进行导入。导入完成后，向 FE 汇报。

FE 中的 JobScheduler 根据汇报结果，继续生成后续新的 Task，或者对失败的 Task 进行重试。

整个例行导入作业通过不断的产生新的 Task，来完成数据不间断的导入。

## Kafka 例行导入

当前我们仅支持从 Kafka 系统进行例行导入。该部分会详细介绍 Kafka 例行导入使用方式和最佳实践。

### 使用限制

1. 支持无认证的 Kafka 访问，以及通过 SSL 方式认证的 Kafka 集群。
2. 支持的消息格式为 csv, json 文本格式。csv 每一个 message 为一行，且行尾**不包含**换行符。
3. 默认支持 Kafka 0.10.0.0(含) 以上版本。如果要使用 Kafka 0.10.0.0 以下版本 (0.9.0, 0.8.2, 0.8.1, 0.8.0)，需要修改 be 的配置，将 kafka_broker_version_fallback 的值设置为要兼容的旧版本，或者在创建routine load的时候直接设置 property.broker.version.fallback的值为要兼容的旧版本，使用旧版本的代价是routine load 的部分新特性可能无法使用，如根据时间设置 kafka 分区的 offset。

### 创建例行导入任务

创建例行导入任务的的详细语法可以连接到 Doris 后，执行 `HELP ROUTINE LOAD;` 查看语法帮助。这里主要详细介绍，创建作业时的注意事项。

* columns_mapping

    `columns_mapping` 主要用于指定表结构和 message 中的列映射关系，以及一些列的转换。如果不指定，Doris 会默认 message 中的列和表结构的列按顺序一一对应。虽然在正常情况下，如果源数据正好一一对应，则不指定也可以进行正常的数据导入。但是我们依然强烈建议用户**显式的指定列映射关系**。这样当表结构发生变化（比如增加一个 nullable 的列），或者源文件发生变化（比如增加了一列）时，导入任务依然可以继续进行。否则，当发生上述变动后，因为列映射关系不再一一对应，导入将报错。

    在 `columns_mapping` 中我们同样可以使用一些内置函数进行列的转换。但需要注意函数参数对应的实际列类型。举例说明：

    假设用户需要导入只包含 `k1` 一列的表，列类型为 `int`。并且需要将源文件中的 null 值转换为 0。该功能可以通过 `ifnull` 函数实现。正确的使用方式如下：

    `COLUMNS (xx, k1=ifnull(xx, "0"))`

    注意这里我们使用 `"0"` 而不是 `0`，虽然 `k1` 的类型为 `int`。因为对于导入任务来说，源数据中的列类型都为 `varchar`，所以这里 `xx` 虚拟列的类型也为 `varchar`。所以我们需要使用 `"0"` 来进行对应的匹配，否则 `ifnull` 函数无法找到参数为 `(varchar, int)` 的函数签名，将出现错误。

    再举例，假设用户需要导入只包含 `k1` 一列的表，列类型为 `int`。并且需要将源文件中的对应列进行处理：将负数转换为正数，而将正数乘以 100。这个功能可以通过 `case when` 函数实现，正确写法应如下：

    `COLUMNS (xx, k1 = case when xx < 0 then cast(-xx as varchar) else cast((xx + '100') as varchar) end)`

    注意这里我们需要将 `case when` 中所有的参数都最终转换为 varchar，才能得到期望的结果。

* where_predicates

    `where_predicates` 中的的列的类型，已经是实际的列类型了，所以无需向 `columns_mapping` 那样强制的转换为 varchar 类型。按照实际的列类型书写即可。

* desired\_concurrent\_number

    `desired_concurrent_number` 用于指定一个例行作业期望的并发度。即一个作业，最多有多少 task 同时在执行。对于 Kafka 导入而言，当前的实际并发度计算如下：

    ```
    Min(partition num, desired_concurrent_number, Config.max_routine_load_task_concurrrent_num)
    ```

    其中 `Config.max_routine_load_task_concurrrent_num` 是系统的一个默认的最大并发数限制。这是一个 FE 配置，可以通过改配置调整。默认为 5。

    其中 partition num 指订阅的 Kafka topic 的 partition 数量。

* max\_batch\_interval/max\_batch\_rows/max\_batch\_size

    这三个参数用于控制单个任务的执行时间。其中任意一个阈值达到，则任务结束。其中 `max_batch_rows` 用于记录从 Kafka 中读取到的数据行数。`max_batch_size` 用于记录从 Kafka 中读取到的数据量，单位是字节。目前一个任务的消费速率大约为 5-10MB/s。

    那么假设一行数据 500B，用户希望每 100MB 或 10 秒为一个 task。100MB 的预期处理时间是 10-20 秒，对应的行数约为 200000 行。则一个合理的配置为：

    ```
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000",
    "max_batch_size" = "104857600"
    ```

    以上示例中的参数也是这些配置的默认参数。

* max\_error\_number

    `max_error_number` 用于控制错误率。在错误率过高的时候，作业会自动暂停。因为整个作业是面向数据流的，且由于数据流的无边界性，我们无法像其他导入任务一样，通过一个错误比例来计算错误率。因此这里提供了一种新的计算方式，来计算数据流中的错误比例。

    我们设定了一个采样窗口。窗口的大小为 `max_batch_rows * 10`。在一个采样窗口内，如果错误行数超过 `max_error_number`，则作业被暂停。如果没有超过，则下一个窗口重新开始计算错误行数。

    我们假设 `max_batch_rows` 为 200000，则窗口大小为 2000000。设 `max_error_number` 为 20000，即用户预期每 2000000 行的错误行为 20000。即错误率为 1%。但是因为不是每批次任务正好消费 200000 行，所以窗口的实际范围是 [2000000, 2200000]，即有 10% 的统计误差。

    错误行不包括通过 where 条件过滤掉的行。但是包括没有对应的 Doris 表中的分区的行。

* data\_source\_properties

    `data_source_properties` 中可以指定消费具体的 Kafka partition。如果不指定，则默认消费所订阅的 topic 的所有 partition。

    注意，当显式的指定了 partition，则导入作业不会再动态的检测 Kafka partition 的变化。如果没有指定，则会根据 kafka partition 的变化，动态调整需要消费的 partition。

* strict\_mode

    Routine load 导入可以开启 strict mode 模式。开启方式为在 job\_properties 中增加 ```"strict_mode" = "true"``` 。默认的 strict mode 为关闭。

    strict mode 模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

    1. 对于列类型转换来说，如果 strict mode 为true，则错误的数据将被 filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

    2. 对于导入的某列由函数变换生成时，strict mode 对其不产生影响。

    3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict mode 对其也不产生影响。例如：如果类型是 decimal(1,0), 原始数据为 10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。
* merge\_type
    数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete 条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理

#### strict mode 与 source data 的导入关系

这里以列类型为 TinyInt 来举例

>注：当表中的列允许导入空值时

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|---------|
|空值        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa or 2000         | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1                   | 1               | true or false      | correct data|

这里以列类型为 Decimal(1,0) 举例

>注：当表中的列允许导入空值时

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|--------|
|空值        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa                 | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1 or 10             | 1               | true or false      | correct data|

> 注意：10 虽然是一个超过范围的值，但是因为其类型符合 decimal的要求，所以 strict mode对其不产生影响。10 最后会在其他 ETL 处理流程中被过滤。但不会被 strict mode 过滤。

#### 访问 SSL 认证的 Kafka 集群

访问 SSL 认证的 Kafka 集群需要用户提供用于认证 Kafka Broker 公钥的证书文件（ca.pem）。如果 Kafka 集群同时开启了客户端认证，则还需提供客户端的公钥（client.pem）、密钥文件（client.key），以及密钥密码。这里所需的文件需要先通过 `CREAE FILE` 命令上传到 Doris 中，**并且 catalog 名称为 `kafka`**。`CREATE FILE` 命令的具体帮助可以参见 `HELP CREATE FILE;`。这里给出示例：

1. 上传文件

    ```
    CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
    CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
    CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
    ```

2. 创建例行导入作业

    ```
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
> <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>


### 查看导入作业状态

查看**作业**状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD;` 命令查看。

查看**任务**运行状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD TASK;` 命令查看。

只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

### 修改作业属性

用户可以修改已经创建的作业。具体说明可以通过 `HELP ALTER ROUTINE LOAD;` 命令查看。或参阅 [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/Data%20Manipulation/alter-routine-load.md)。

### 作业控制

用户可以通过 `STOP/PAUSE/RESUME` 三个命令来控制作业的停止，暂停和重启。可以通过 `HELP STOP ROUTINE LOAD;`, `HELP PAUSE ROUTINE LOAD;` 以及 `HELP RESUME ROUTINE LOAD;` 三个命令查看帮助和示例。

## 其他说明

1. 例行导入作业和 ALTER TABLE 操作的关系

    * 例行导入不会阻塞 SCHEMA CHANGE 和 ROLLUP 操作。但是注意如果 SCHEMA CHANGE 完成后，列映射关系无法匹配，则会导致作业的错误数据激增，最终导致作业暂停。建议通过在例行导入作业中显式指定列映射关系，以及通过增加 Nullable 列或带 Default 值的列来减少这类问题。
    * 删除表的 Partition 可能会导致导入数据无法找到对应的 Partition，作业进入暂停。

2. 例行导入作业和其他导入作业的关系（LOAD, DELETE, INSERT）

    * 例行导入和其他 LOAD 作业以及 INSERT 操作没有冲突。
    * 当执行 DELETE 操作时，对应表分区不能有任何正在执行的导入任务。所以在执行 DELETE 操作前，可能需要先暂停例行导入作业，并等待已下发的 task 全部完成后，才可以执行 DELETE。

3. 例行导入作业和 DROP DATABASE/TABLE 操作的关系

    当例行导入对应的 database 或 table 被删除后，作业会自动 CANCEL。

4. kafka 类型的例行导入作业和 kafka topic 的关系

    当用户在创建例行导入声明的 `kafka_topic` 在kafka集群中不存在时。

    * 如果用户 kafka 集群的 broker 设置了 `auto.create.topics.enable = true`，则 `kafka_topic` 会先被自动创建，自动创建的 partition 个数是由**用户方的kafka集群**中的 broker 配置 `num.partitions` 决定的。例行作业会正常的不断读取该 topic 的数据。
    * 如果用户 kafka 集群的 broker 设置了 `auto.create.topics.enable = false`, 则 topic 不会被自动创建，例行作业会在没有读取任何数据之前就被暂停，状态为 `PAUSED`。

    所以，如果用户希望当 kafka topic 不存在的时候，被例行作业自动创建的话，只需要将**用户方的kafka集群**中的 broker 设置 `auto.create.topics.enable = true` 即可。
 5. 在网络隔离的环境中可能出现的问题
    在有些环境中存在网段和域名解析的隔离措施，所以需要注意
     1. 创建Routine load 任务中指定的 Broker list 必须能够被Doris服务访问
     2. Kafka 中如果配置了`advertised.listeners`, `advertised.listeners` 中的地址必须能够被Doris服务访问

6. 关于指定消费的 Partition 和 Offset

    Doris 支持指定 Partition 和 Offset 开始消费。新版中还支持了指定时间点进行消费的功能。这里说明下对应参数的配置关系。
    
    有三个相关参数：
    
    * `kafka_partitions`：指定待消费的 partition 列表，如："0, 1, 2, 3"。
    * `kafka_offsets`：指定每个分区的起始offset，必须和 `kafka_partitions` 列表个数对应。如："1000, 1000, 2000, 2000"
    * `property.kafka_default_offset`：指定分区默认的起始offset。

    在创建导入作业时，这三个参数可以有以下组合：
    
    | 组合 | `kafka_partitions` | `kafka_offsets` | `property.kafka_default_offset` | 行为 |
    |---|---|---|---|---|
    |1| No | No | No | 系统会自动查找topic对应的所有分区并从 OFFSET_END 开始消费 |
    |2| No | No | Yes | 系统会自动查找topic对应的所有分区并从 default offset 指定的位置开始消费|
    |3| Yes | No | No | 系统会从指定分区的 OFFSET_END 开始消费 |
    |4| Yes | Yes | No | 系统会从指定分区的指定offset 处开始消费 |
    |5| Yes | No | Yes | 系统会从指定分区，default offset 指定的位置开始消费 |
   
 7. STOP和PAUSE的区别
    
    FE会自动定期清理STOP状态的ROUTINE LOAD，而PAUSE状态的则可以再次被恢复启用。
    
## 相关参数

一些系统配置参数会影响例行导入的使用。

1. max\_routine\_load\_task\_concurrent\_num

    FE 配置项，默认为 5，可以运行时修改。该参数限制了一个例行导入作业最大的子任务并发数。建议维持默认值。设置过大，可能导致同时并发的任务数过多，占用集群资源。

2. max\_routine_load\_task\_num\_per\_be

    FE 配置项，默认为5，可以运行时修改。该参数限制了每个 BE 节点最多并发执行的子任务个数。建议维持默认值。如果设置过大，可能导致并发任务数过多，占用集群资源。

3. max\_routine\_load\_job\_num

    FE 配置项，默认为100，可以运行时修改。该参数限制的例行导入作业的总数，包括 NEED_SCHEDULED, RUNNING, PAUSE 这些状态。超过后，不能在提交新的作业。

4. max\_consumer\_num\_per\_group

    BE 配置项，默认为 3。该参数表示一个子任务中最多生成几个 consumer 进行数据消费。对于 Kafka 数据源，一个 consumer 可能消费一个或多个 kafka partition。假设一个任务需要消费 6 个 kafka partition，则会生成 3 个 consumer，每个 consumer 消费 2 个 partition。如果只有 2 个 partition，则只会生成 2 个 consumer，每个 consumer 消费 1 个 partition。

5. push\_write\_mbytes\_per\_sec

    BE 配置项。默认为 10，即 10MB/s。该参数为导入通用参数，不限于例行导入作业。该参数限制了导入数据写入磁盘的速度。对于 SSD 等高性能存储设备，可以适当增加这个限速。

6. max\_tolerable\_backend\_down\_num
    FE 配置项，默认值是0。在满足某些条件下，Doris可PAUSED的任务重新调度，即变成RUNNING。该参数为0代表只有所有BE节点是alive状态才允许重新调度。

7. period\_of\_auto\_resume\_min
    FE 配置项，默认是5分钟。Doris重新调度，只会在5分钟这个周期内，最多尝试3次. 如果3次都失败则锁定当前任务，后续不在进行调度。但可通过人为干预，进行手动恢复。

## keyword
    ROUTINE,LOAD
