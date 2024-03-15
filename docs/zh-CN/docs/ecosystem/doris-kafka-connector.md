---
{
"title": "Doris Kafka Connector",
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

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) 是一款可扩展、可靠的在 Apache Kafka 和其他系统之间进行数据传输的工具，可以定义 Connectors 将大量数据迁入迁出 Kafka。

Doris 社区提供了 [doris-kafka-connector](https://github.com/apache/doris-kafka-connector) 插件，可以将 Kafka topic 中的数据写入到 Doris 中。

## Doris Kafka Connector 使用

### 下载
[doris-kafka-connector](https://dist.apache.org/repos/dist/dev/doris/doris-kafka-connector/)

maven 依赖
```xml
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>doris-kafka-connector</artifactId>
  <version>1.0.0</version>
</dependency> 
```

### Standalone 模式启动

配置 connect-standalone.properties

```properties
# 修改 broker 地址
bootstrap.servers=127.0.0.1:9092
```

配置 doris-connector-sink.properties
在 config 目录下创建 doris-connector-sink.properties，并配置如下内容：

```properties
name=test-doris-sink
connector.class=org.apache.doris.kafka.connector.DorisSinkConnector
topics=topic_test
doris.topic2table.map=topic_test:test_kafka_tbl
buffer.count.records=10000
buffer.flush.time=120
buffer.size.bytes=5000000
doris.urls=10.10.10.1
doris.http.port=8030
doris.query.port=9030
doris.user=root
doris.password=
doris.database=test_db
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

启动 Standalone

```shell
$KAFKA_HOME/bin/connect-standalone.sh -daemon $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/doris-connector-sink.properties
```
:::note
注意：一般不建议在生产环境中使用 standalone 模式
:::


### Distributed 模式启动

配置 connect-distributed.properties

```properties
# 修改 broker 地址
bootstrap.servers=127.0.0.1:9092

# 修改 group.id，同一集群的需要一致
group.id=connect-cluster
```



启动 Distributed

```shell
$KAFKA_HOME/bin/connect-distributed.sh -daemon $KAFKA_HOME/config/connect-distributed.properties
```


增加 Connector

```shell
curl -i http://127.0.0.1:8083/connectors -H "Content-Type: application/json" -X POST -d '{
  "name":"test-doris-sink-cluster",
  "config":{
    "connector.class":"org.apache.doris.kafka.connector.DorisSinkConnector",
    "topics":"topic_test",
    "doris.topic2table.map": "topic_test:test_kafka_tbl",
    "buffer.count.records":"10000",
    "buffer.flush.time":"120",
    "buffer.size.bytes":"5000000",
    "doris.urls":"10.10.10.1",
    "doris.user":"root",
    "doris.password":"",
    "doris.http.port":"8030",
    "doris.query.port":"9030",
    "doris.database":"test_db",
    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
    "value.converter":"org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "value.converter.schemas.enable":"false",
  }
}'
```

操作 Connector
```
# 查看 connector 状态
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/status -X GET
# 删除当前 connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster -X DELETE
# 暂停当前 connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/pause -X PUT
# 重启当前 connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/resume -X PUT
# 重启 connector 内的 tasks
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/tasks/0/restart -X POST
```
参考：[Connect REST Interface](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface)

:::note
注意 kafka-connect 首次启动时，会往 kafka 集群中创建 `config.storage.topic` `offset.storage.topic` `status.storage.topic` 三个 topic 用于记录 kafka-connect 的共享连接器配置、偏移数据和状态更新。[How to Use Kafka Connect - Get Started](https://docs.confluent.io/platform/current/connect/userguide.html)
:::

### 访问 SSL 认证的 Kafka 集群
通过 kafka-connect 访问 SSL 认证的 Kafka 集群需要用户提供用于认证 Kafka Broker 公钥的证书文件（client.truststore.jks）。您可以在 `connect-distributed.properties` 文件中增加以下配置：
```
# Connect worker
security.protocol=SSL
ssl.truststore.location=/var/ssl/private/client.truststore.jks
ssl.truststore.password=test1234

# Embedded consumer for sink connectors
consumer.security.protocol=SSL
consumer.ssl.truststore.location=/var/ssl/private/client.truststore.jks
consumer.ssl.truststore.password=test1234
```
关于通过 kafka-connect 连接 SSL 认证的 Kafka 集群配置说明可以参考：[Configure Kafka Connect](https://docs.confluent.io/5.1.2/tutorials/security_tutorial.html#configure-kconnect-long)


### 死信队列
默认情况下，转换过程中或转换过程中遇到的任何错误都会导致连接器失败。每个连接器配置还可以通过跳过它们来容忍此类错误，可选择将每个错误和失败操作的详细信息以及有问题的记录（具有不同级别的详细信息）写入死信队列以便记录。
```
errors.tolerance=all
errors.deadletterqueue.topic.name=test_error_topic
errors.deadletterqueue.context.headers.enable=true
errors.deadletterqueue.topic.replication.factor=1
```


## 配置项


| Key                   | Default Value                                                                        | **Required** | **Description**                                                                                                                                                                                                        |
|-----------------------|--------------------------------------------------------------------------------------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                  | -                                                                                    | Y            | Connect 应用名称，必须是在 Kafka Connect 环境中唯一                                                                                                                                                                                  |
| connector.class       | -                                                                                    | Y            | org.apache.doris.kafka.connector.DorisSinkConnector                                                                                                                                                                    |
| topics                | -                                                                                    | Y            | 订阅的 topic 列表，逗号分隔: topic1,topic2                                                                                                                                                                                       |
| doris.urls            | -                                                                                    | Y            | Doris FE 连接地址。如果有多个，中间用逗号分割: 10.20.30.1,10.20.30.2,10.20.30.3                                                                                                                                                          |
| doris.http.port       | -                                                                                    | Y            | Doris HTTP 协议端口                                                                                                                                                                                                        |
| doris.query.port      | -                                                                                    | Y            | Doris MySQL 协议端口                                                                                                                                                                                                       |
| doris.user            | -                                                                                    | Y            | Doris 用户名                                                                                                                                                                                                              |
| doris.password        | -                                                                                    | Y            | Doris 密码                                                                                                                                                                                                               |
| doris.database        | -                                                                                    | Y            | 要写入的数据库。多个库时可以为空，同时在 topic2table.map 需要配置具体的库名称                                                                                                                                                                        |
| doris.topic2table.map | -                                                                                    | N            | topic 和 table 表的对应关系，例：topic1:tb1,topic2:tb2<br />默认为空，表示 topic 和 table 名称一一对应。 <br />  多个库的格式为 topic1:db1.tbl1,topic2:db2.tbl2                                                                                        |
| buffer.count.records  | 10000                                                                                | N            | 在 flush 到 doris 之前，每个 Kafka 分区在内存中缓冲的记录数。默认 10000 条记录                                                                                                                                                                  |
| buffer.flush.time     | 120                                                                                  | N            | buffer 刷新间隔，单位秒，默认120秒                                                                                                                                                                                                 |
| buffer.size.bytes     | 5000000(5MB)                                                                         | N            | 每个 Kafka 分区在内存中缓冲的记录的累积大小，单位字节，默认5MB                                                                                                                                                                                   |
| jmx                   | true                                                                                 | N            | 通过 JMX 获取 Connector 内部监控指标, 请参考: [Doris-Connector-JMX](https://github.com/apache/doris-kafka-connector/blob/master/docs/zh-CN/Doris-Connector-JMX.md)                                                                  |
| enable.delete         | false                                                                                | N            | 是否同步删除记录, 默认 false                                                                                                                                                                                                     |
| label.prefix          | ${name}                                                                              | N            | Stream load 导入数据时的 label 前缀。默认为 Connector 应用名称。                                                                                                                                                                        |
| auto.redirect         | true                                                                                 | N            | 是否重定向 StreamLoad 请求。开启后 StreamLoad 将通过 FE 重定向到需要写入数据的 BE，并且不再显示获取 BE 信息                                                                                                                                                |
| load.model            | stream_load                                                                          | N            | 导入数据的方式。支持 `stream_load` 直接数据导入到 Doris 中；同时支持 `copy_into` 的方式导入数据至对象存储中，然后将数据加载至 Doris 中                                                                                                                               |
| sink.properties.*     | `'sink.properties.format':'json'`, <br/>`'sink.properties.read_json_by_line':'true'` | N            | Stream Load 的导入参数。<br />例如: 定义列分隔符`'sink.properties.column_separator':','`  <br />详细参数参考[这里](https://doris.apache.org/zh-CN/docs/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/#description)。 |
| delivery.guarantee    | at_least_once                                                                        | N            | 消费 Kafka 数据导入至 doris 时，数据一致性的保障方式。 支持 `at_least_once` `exactly_once`，默认为 `at_least_once` 。Doris 需要升级至 2.1.0 以上，才能保障数据的 `exactly_once`                                                                                  |

其他Kafka Connect Sink通用配置项可参考：[Kafka Connect Sink Configuration Properties](https://docs.confluent.io/platform/current/installation/configuration/connect/sink-connect-configs.html#kconnect-long-sink-configuration-properties-for-cp)