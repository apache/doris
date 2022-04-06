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

- RoutineLoadJob：用户提交的一个例行导入作业。
- JobScheduler：例行导入作业调度器，用于调度和拆分一个 RoutineLoadJob 为多个 Task。
- Task：RoutineLoadJob 被 JobScheduler 根据规则拆分的子任务。
- TaskScheduler：任务调度器。用于调度 Task 的执行。

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

如上图，Client 向 FE 提交一个例行导入作业。

FE 通过 JobScheduler 将一个导入作业拆分成若干个 Task。每个 Task 负责导入指定的一部分数据。Task 被 TaskScheduler 分配到指定的 BE 上执行。

在 BE 上，一个 Task 被视为一个普通的导入任务，通过 Stream Load 的导入机制进行导入。导入完成后，向 FE 汇报。

FE 中的 JobScheduler 根据汇报结果，继续生成后续新的 Task，或者对失败的 Task 进行重试。

整个例行导入作业通过不断的产生新的 Task，来完成数据不间断的导入。

## Kafka例行导入

当前我们仅支持从 Kafka 系统进行例行导入。该部分会详细介绍 Kafka 例行导入使用方式和最佳实践。

### 使用限制

1. 支持无认证的 Kafka 访问，以及通过 SSL 方式认证的 Kafka 集群。
2. 支持的消息格式为 csv, json 文本格式。csv 每一个 message 为一行，且行尾**不包含**换行符。
3. 默认支持 Kafka 0.10.0.0(含) 以上版本。如果要使用 Kafka 0.10.0.0 以下版本 (0.9.0, 0.8.2, 0.8.1, 0.8.0)，需要修改 be 的配置，将 kafka_broker_version_fallback 的值设置为要兼容的旧版本，或者在创建routine load的时候直接设置 property.broker.version.fallback的值为要兼容的旧版本，使用旧版本的代价是routine load 的部分新特性可能无法使用，如根据时间设置 kafka 分区的 offset。

### 创建例行导入任务

创建例行导入任务的的详细语法可以连接到 Doris 后，执行 `HELP ROUTINE LOAD;` 查看语法帮助。这里主要详细介绍，创建作业时的注意事项。

- columns_mapping

  `columns_mapping` 主要用于指定表结构和 message 中的列映射关系，以及一些列的转换。如果不指定，Doris 会默认 message 中的列和表结构的列按顺序一一对应。虽然在正常情况下，如果源数据正好一一对应，则不指定也可以进行正常的数据导入。但是我们依然强烈建议用户**显式的指定列映射关系**。这样当表结构发生变化（比如增加一个 nullable 的列），或者源文件发生变化（比如增加了一列）时，导入任务依然可以继续进行。否则，当发生上述变动后，因为列映射关系不再一一对应，导入将报错。

  在 `columns_mapping` 中我们同样可以使用一些内置函数进行列的转换。但需要注意函数参数对应的实际列类型。举例说明：

  假设用户需要导入只包含 `k1` 一列的表，列类型为 `int`。并且需要将源文件中的 null 值转换为 0。该功能可以通过 `ifnull` 函数实现。正确的使用方式如下：

  `COLUMNS (xx, k1=ifnull(xx, "0"))`

  注意这里我们使用 `"0"` 而不是 `0`，虽然 `k1` 的类型为 `int`。因为对于导入任务来说，源数据中的列类型都为 `varchar`，所以这里 `xx` 虚拟列的类型也为 `varchar`。所以我们需要使用 `"0"` 来进行对应的匹配，否则 `ifnull` 函数无法找到参数为 `(varchar, int)` 的函数签名，将出现错误。

  再举例，假设用户需要导入只包含 `k1` 一列的表，列类型为 `int`。并且需要将源文件中的对应列进行处理：将负数转换为正数，而将正数乘以 100。这个功能可以通过 `case when` 函数实现，正确写法应如下：

  `COLUMNS (xx, k1 = case when xx < 0 then cast(-xx as varchar) else cast((xx + '100') as varchar) end)`

  注意这里我们需要将 `case when` 中所有的参数都最终转换为 varchar，才能得到期望的结果。

- where_predicates

  `where_predicates` 中的的列的类型，已经是实际的列类型了，所以无需向 `columns_mapping` 那样强制的转换为 varchar 类型。按照实际的列类型书写即可。

- desired_concurrent_number

  `desired_concurrent_number` 用于指定一个例行作业期望的并发度。即一个作业，最多有多少 task 同时在执行。对于 Kafka 导入而言，当前的实际并发度计算如下：

  ```text
  Min(partition num, desired_concurrent_number, Config.max_routine_load_task_concurrrent_num)
  ```

  其中 `Config.max_routine_load_task_concurrrent_num` 是系统的一个默认的最大并发数限制。这是一个 FE 配置，可以通过改配置调整。默认为 5。

  其中 partition num 指订阅的 Kafka topic 的 partition 数量。

- max_batch_interval/max_batch_rows/max_batch_size

  这三个参数用于控制单个任务的执行时间。其中任意一个阈值达到，则任务结束。其中 `max_batch_rows` 用于记录从 Kafka 中读取到的数据行数。`max_batch_size` 用于记录从 Kafka 中读取到的数据量，单位是字节。目前一个任务的消费速率大约为 5-10MB/s。

  那么假设一行数据 500B，用户希望每 100MB 或 10 秒为一个 task。100MB 的预期处理时间是 10-20 秒，对应的行数约为 200000 行。则一个合理的配置为：

  ```text
  "max_batch_interval" = "10",
  "max_batch_rows" = "200000",
  "max_batch_size" = "104857600"
  ```

  以上示例中的参数也是这些配置的默认参数。

- max_error_number

  `max_error_number` 用于控制错误率。在错误率过高的时候，作业会自动暂停。因为整个作业是面向数据流的，且由于数据流的无边界性，我们无法像其他导入任务一样，通过一个错误比例来计算错误率。因此这里提供了一种新的计算方式，来计算数据流中的错误比例。

  我们设定了一个采样窗口。窗口的大小为 `max_batch_rows * 10`。在一个采样窗口内，如果错误行数超过 `max_error_number`，则作业被暂停。如果没有超过，则下一个窗口重新开始计算错误行数。

  我们假设 `max_batch_rows` 为 200000，则窗口大小为 2000000。设 `max_error_number` 为 20000，即用户预期每 2000000 行的错误行为 20000。即错误率为 1%。但是因为不是每批次任务正好消费 200000 行，所以窗口的实际范围是 [2000000, 2200000]，即有 10% 的统计误差。

  错误行不包括通过 where 条件过滤掉的行。但是包括没有对应的 Doris 表中的分区的行。

- data_source_properties

  `data_source_properties` 中可以指定消费具体的 Kafka partition。如果不指定，则默认消费所订阅的 topic 的所有 partition。

  注意，当显式的指定了 partition，则导入作业不会再动态的检测 Kafka partition 的变化。如果没有指定，则会根据 kafka partition 的变化，动态调整需要消费的 partition。

- strict_mode

  Routine load 导入可以开启 strict mode 模式。开启方式为在 job_properties 中增加 `"strict_mode" = "true"` 。默认的 strict mode 为关闭。

  strict mode 模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

  1. 对于列类型转换来说，如果 strict mode 为true，则错误的数据将被 filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。
  2. 对于导入的某列由函数变换生成时，strict mode 对其不产生影响。
  3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict mode 对其也不产生影响。例如：如果类型是 decimal(1,0), 原始数据为 10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。

- merge_type 数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete 条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理

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

   ```text
   CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
   CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
   CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
   ```

2. 创建例行导入作业

   ```text
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
> [https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md(opens new window)](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

### 查看导入作业状态

查看**作业**状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD;` 命令查看。

查看**任务**运行状态的具体命令和示例可以通过 `HELP SHOW ROUTINE LOAD TASK;` 命令查看。

只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

### 修改作业属性

用户可以修改已经创建的作业。具体说明可以通过 `HELP ALTER ROUTINE LOAD;` 命令查看或参阅 [ALTER ROUTINE LOAD](../../sql-manual/sql-reference-v2/Data-Manipulation-Statements/Load/ALTER-ROUTINE-LOAD.html)。

## 更多帮助

关于 **Routine Load** 使用的更多详细语法，可以在Mysql客户端命令行下输入 `HELP ROUTINE LOAD` 获取更多帮助信息。
