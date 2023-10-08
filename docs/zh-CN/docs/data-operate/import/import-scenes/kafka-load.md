---
{
    "title": "订阅 Kafka 日志",
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

# 订阅Kafka日志

用户可以通过提交例行导入作业，直接订阅Kafka中的消息数据，以近实时的方式进行数据同步。

Doris 自身能够保证不丢不重的订阅 Kafka 中的消息，即 `Exactly-Once` 消费语义。

## 订阅 Kafka 消息

订阅 Kafka 消息使用了 Doris 中的例行导入（Routine Load）功能。

用户首先需要创建一个**例行导入作业**。作业会通过例行调度，不断地发送一系列的**任务**，每个任务会消费一定数量 Kafka 中的消息。

请注意以下使用限制：

1. 支持无认证的 Kafka 访问，以及通过 SSL 方式认证的 Kafka 集群。
2. 支持的消息格式如下：
   - csv 文本格式。每一个 message 为一行，且行尾**不包含**换行符。
   - Json 格式，详见 [导入 Json 格式数据](../import-way/load-json-format.md)。
3. 仅支持 Kafka 0.10.0.0(含) 以上版本。

### 访问 SSL 认证的 Kafka 集群

例行导入功能支持无认证的 Kafka 集群，以及通过 SSL 认证的 Kafka 集群。

访问 SSL 认证的 Kafka 集群需要用户提供用于认证 Kafka Broker 公钥的证书文件（ca.pem）。如果 Kafka 集群同时开启了客户端认证，则还需提供客户端的公钥（client.pem）、密钥文件（client.key），以及密钥密码。这里所需的文件需要先通过 `CREAE FILE` 命令上传到 Doris 中，并且 catalog 名称为 `kafka`。`CREATE FILE` 命令的具体帮助可以参见 [CREATE FILE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-FILE.md) 命令手册。这里给出示例：

- 上传文件

  ```sql
  CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
  CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
  CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
  ```

上传完成后，可以通过 [SHOW FILES](../../../sql-manual/sql-reference/Show-Statements/SHOW-FILE.md) 命令查看已上传的文件。

### 创建例行导入作业

创建例行导入任务的具体命令，请参阅 [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md) 命令手册。这里给出示例：

1. 访问无认证的 Kafka 集群

   ```sql
   CREATE ROUTINE LOAD demo.my_first_routine_load_job ON test_1
   COLUMNS TERMINATED BY ","
   PROPERTIES
   (
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
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
   
   - `max_batch_interval/max_batch_rows/max_batch_size` 用于控制一个子任务的运行周期。一个子任务的运行周期由最长运行时间、最多消费行数和最大消费数据量共同决定。

2. 访问 SSL 认证的 Kafka 集群

   ```sql
   CREATE ROUTINE LOAD demo.my_first_routine_load_job ON test_1
   COLUMNS TERMINATED BY ",",
   PROPERTIES
   (
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
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

### 查看导入作业状态

查看**作业**状态的具体命令和示例请参阅 [SHOW ROUTINE LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-ROUTINE-LOAD.md) 命令文档。

查看某个作业的**任务**运行状态的具体命令和示例请参阅 [SHOW ROUTINE LOAD TASK](../../../sql-manual/sql-reference/Show-Statements/SHOW-ROUTINE-LOAD-TASK.md) 命令文档。

只能查看当前正在运行中的任务，已结束和未开始的任务无法查看。

### 修改作业属性

用户可以修改已经创建的作业的部分属性。具体说明请参阅 [ALTER ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/ALTER-ROUTINE-LOAD.md) 命令手册。

### 作业控制

用户可以通过 `STOP/PAUSE/RESUME` 三个命令来控制作业的停止，暂停和重启。

具体命令请参阅 [STOP ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STOP-ROUTINE-LOAD.md)，[PAUSE ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/PAUSE-ROUTINE-LOAD.md)，[RESUME ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/RESUME-ROUTINE-LOAD.md) 命令文档。

## 更多帮助

关于 ROUTINE LOAD 的更多详细语法和最佳实践，请参阅 [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md) 命令手册。
