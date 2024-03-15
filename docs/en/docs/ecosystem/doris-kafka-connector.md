---
{
"title": "Doris Kafka Connector",
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

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) is a scalable and reliable tool for data transmission between Apache Kafka and other systems. Connectors can be defined Move large amounts of data in and out of Kafka.

The Doris community provides the [doris-kafka-connector](https://github.com/apache/doris-kafka-connector) plug-in, which can write data in the Kafka topic to Doris.
## Usage Doris Kafka Connector

### Download
[doris-kafka-connector](https://dist.apache.org/repos/dist/dev/doris/doris-kafka-connector/)

maven dependencies
```xml
<dependency>
  <groupId>org.apache.doris</groupId>
  <artifactId>doris-kafka-connector</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Standalone mode startup

Configure connect-standalone.properties

```properties
# Modify broker address
bootstrap.servers=127.0.0.1:9092
```

Configure doris-connector-sink.properties
Create doris-connector-sink.properties in the config directory and configure the following content:

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

Start Standalone

```shell
$KAFKA_HOME/bin/connect-standalone.sh -daemon $KAFKA_HOME/config/connect-standalone.properties $KAFKA_HOME/config/doris-connector-sink.properties
```
:::note
Note: It is generally not recommended to use standalone mode in a production environment.
:::

### Distributed mode startup

Configure connect-distributed.properties

```properties
# Modify broker address
bootstrap.servers=127.0.0.1:9092

# Modify group.id, the same cluster needs to be consistent
group.id=connect-cluster
```



Start Distributed

```shell
$KAFKA_HOME/bin/connect-distributed.sh -daemon $KAFKA_HOME/config/connect-distributed.properties
```


Add Connector

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

Operation Connector
```
# View connector status
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/status -X GET
# Delete connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster -X DELETE
# Pause connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/pause -X PUT
# Restart connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/resume -X PUT
# Restart tasks within the connector
curl -i http://127.0.0.1:8083/connectors/test-doris-sink-cluster/tasks/0/restart -X POST
```
Refer to: [Connect REST Interface](https://docs.confluent.io/platform/current/connect/references/restapi.html#kconnect-rest-interface)

:::note
Note that when kafka-connect is started for the first time, three topics `config.storage.topic` `offset.storage.topic` and `status.storage.topic` will be created in the kafka cluster to record the shared connector configuration of kafka-connect. Offset data and status updates. [How to Use Kafka Connect - Get Started](https://docs.confluent.io/platform/current/connect/userguide.html)
:::

### Access an SSL-certified Kafka cluster
Accessing an SSL-certified Kafka cluster through kafka-connect requires the user to provide a certificate file (client.truststore.jks) used to authenticate the Kafka Broker public key. You can add the following configuration in the `connect-distributed.properties` file:
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
For instructions on configuring a Kafka cluster connected to SSL authentication through kafka-connect, please refer to: [Configure Kafka Connect](https://docs.confluent.io/5.1.2/tutorials/security_tutorial.html#configure-kconnect-long)


### Dead letter queue
By default, any errors encountered during or during the conversion will cause the connector to fail. Each connector configuration can also tolerate such errors by skipping them, optionally writing the details of each error and failed operation as well as the records in question (with varying levels of detail) to a dead-letter queue for logging.
```
errors.tolerance=all
errors.deadletterqueue.topic.name=test_error_topic
errors.deadletterqueue.context.headers.enable=true
errors.deadletterqueue.topic.replication.factor=1
```


## Configuration items


| Key                    | Default Value                                                                        | **Required** | **Description**                                                                                                                                                                                                                                                         |
|------------------------|--------------------------------------------------------------------------------------|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                   | -                                                                                    | Y            | Connect application name, must be unique within the Kafka Connect environment                                                                                                                                                                                           |
| connector.class        | -                                                                                    | Y            | org.apache.doris.kafka.connector.DorisSinkConnector                                                                                                                                                                                                                     |
| topics                 | -                                                                                    | Y            | List of subscribed topics, separated by commas. like: topic1, topic2                                                                                                                                                                                                    |
| doris.urls             | -                                                                                    | Y            | Doris FE connection address. If there are multiple, separate them with commas. like: 10.20.30.1,10.20.30.2,10.20.30.3                                                                                                                                                   |
| doris.http.port        | -                                                                                    | Y            | Doris HTTP protocol port                                                                                                                                                                                                                                                |
| doris.query.port       | -                                                                                    | Y            | Doris MySQL protocol port                                                                                                                                                                                                                                               |
| doris.user             | -                                                                                    | Y            | Doris username                                                                                                                                                                                                                                                          |
| doris.password         | -                                                                                    | Y            | Doris password                                                                                                                                                                                                                                                          |
| doris.database         | -                                                                                    | Y            | The database to write to. It can be empty when there are multiple libraries. At the same time, the specific library name needs to be configured in topic2table.map.                                                                                                     |
| doris.topic2table.map  | -                                                                                    | N            | The corresponding relationship between topic and table table, for example: topic1:tb1,topic2:tb2<br />The default is empty, indicating that topic and table names correspond one to one. <br />The format of multiple libraries is topic1:db1.tbl1,topic2:db2.tbl2      |
| buffer.count.records   | 10000                                                                                | N            | The number of records each Kafka partition buffers in memory before flushing to doris. Default 10000 records                                                                                                                                                            |
| buffer.flush.time      | 120                                                                                  | N            | Buffer refresh interval, in seconds, default 120 seconds                                                                                                                                                                                                                |
| buffer.size.bytes      | 5000000(5MB)                                                                         | N            | The cumulative size of records buffered in memory for each Kafka partition, in bytes, default 5MB                                                                                                                                                                       |
| jmx                    | true                                                                                 | N            | To obtain connector internal monitoring indicators through JMX, please refer to: [Doris-Connector-JMX](https://github.com/apache/doris-kafka-connector/blob/master/docs/en/Doris-Connector-JMX.md)                                                                      |
| enable.delete          | false                                                                                | N            | Whether to delete records synchronously, default false                                                                                                                                                                                                                  |
| label.prefix           | ${name}                                                                              | N            | Stream load label prefix when importing data. Defaults to the Connector application name.                                                                                                                                                                               |
| auto.redirect          | true                                                                                 | N            | Whether to redirect StreamLoad requests. After being turned on, StreamLoad will redirect to the BE where data needs to be written through FE, and the BE information will no longer be displayed.                                                                       |
| load.model             | stream_load                                                                          | N            | How to import data. Supports `stream_load` to directly import data into Doris; also supports `copy_into` to import data into object storage, and then load the data into Doris.                                                                                         |
| sink.properties.*      | `'sink.properties.format':'json'`, <br/>`'sink.properties.read_json_by_line':'true'` | N            | Import parameters for Stream Load. <br />For example: define column separator `'sink.properties.column_separator':','` <br />Detailed parameter reference [here](https://doris.apache.org/docs/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD/) |
| delivery.guarantee     | at_least_once                                                                        | N            | How to ensure data consistency when consuming Kafka data is imported into Doris. Supports `at_least_once` `exactly_once`, default is `at_least_once`. Doris needs to be upgraded to 2.1.0 or above to ensure data `exactly_once`                                        |

For other Kafka Connect Sink common configuration items, please refer to: [Kafka Connect Sink Configuration Properties](https://docs.confluent.io/platform/current/installation/configuration/connect/sink-connect-configs.html#kconnect-long-sink-configuration-properties-for-cp)