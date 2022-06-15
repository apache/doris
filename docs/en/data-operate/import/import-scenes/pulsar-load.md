---
{
"title": "Pulsar Data Subscription",
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


#### Kafka-on-Pulsar (KoP)

---
KoP (Kafka on Pulsar) brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. By adding the KoP protocol handler to your existing Pulsar cluster, you can migrate your existing Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s powerful features, such as:
- Streamlined operations with enterprise-grade multi-tenancy
- Simplified operations with a rebalance-free architecture
- Infinite event stream retention with Apache BookKeeper and tiered storage
- Serverless event processing with Pulsar Functions
  ![](https://github.com/streamnative/kop/raw/master/docs/kop-architecture.png)
#### Pulsar Data Subscription with Routine Load By KoP

---
Apache Doris supports subscription Kafka Data by Routine Load,and  ensures transactional operations during subscription.  Apache pulsar as an enterprise  message publishing and subscription system in the cloud native,has been used in many online services.How can Apache Doris Subscription Apache Pulsar data? The answer is through KoP.

Because Kop provides compatibility subscription pulsar data by kafka-client,so  Apache Doris can be subscription pulsar data by Routine Load with librdkafka , and ensure transaction consistency by Routine Load.
```bash
--------------------------
|     Apache Doris       |
|     ---------------    |
|     | Routine Load |   |
|     ---------------    |
--------------------------
            |Kafka Protocol(librdkafka)
------------v--------------
|     ---------------    |
|     |     KoP      |   |
|     ---------------    |
|       Apache Pulsar    |
--------------------------
```

#### Quick Start

---
##### 1. Prepare Pulsar Standalone environment

1. make sure JDK environment
2. download Pulsar binary package and decompression:
```bash
#download binary package
wget https://archive.apache.org/dist/pulsar/pulsar-2.10.0/apache-pulsar-2.10.0-bin.tar.gz
#decompression
tar xvfz apache-pulsar-2.10.0-bin.tar.gz
cd apache-pulsar-2.10.0
```

##### 2. KoP compile

1. Clone the KoP GitHub project to your local.

```bash
git clone https://github.com/streamnative/kop.git
cd kop
```

2. Build the project.

```bash
mvn clean install -DskipTests
```

3. Get the .nar file in the following directory and copy it your Pulsar protocols directory. You need to create the protocols folder in Pulsar if it's the first time you use protocol handlers.

```bash
mkdir apache-pulsar-2.10.0/protocols
# mv kop/kafka-impl/target/pulsar-protocol-handler-kafka-{{protocol:version}}.nar apache-pulsar-2.10.0/protocols
cp kop/kafka-impl/target/pulsar-protocol-handler-kafka-2.11.0-SNAPSHOT.nar apache-pulsar-2.10.0/protocols
```

4. View results：

```bash
[root@17a5da45700b apache-pulsar-2.10.0]# ls protocols/
pulsar-protocol-handler-kafka-2.11.0-SNAPSHOT.nar
```

##### 3. Configuration KoP：

After you copy the .nar file to your Pulsar /protocols directory, you need to configure the Pulsar broker to run the KoP protocol handler as a plugin by adding configurations in the Pulsar configuration file broker.conf or standalone.conf.

1.Set the configuration of the KoP protocol handler in broker.conf or standalone.conf file.

```bash
#kop Protocols
messagingProtocols=kafka
#kop nar file directory
protocolHandlerDirectory=./protocols
#if allow auth create topic
allowAutoTopicCreationType=partitioned
```

2. Set Kafka listeners、brokerEntryMetadataInterceptors and brokerDeleteInactiveTopicsEnabled

```bash
# Use `kafkaListeners` here for KoP 2.8.0 because `listeners` is marked as deprecated from KoP 2.8.0 
kafkaListeners=PLAINTEXT://127.0.0.1:9092# This config is not required unless you want to expose another address to the Kafka client.
# If it’s not configured, it will be the same with `kafkaListeners` config by default
kafkaAdvertisedListeners=PLAINTEXT://127.0.0.1:9092
brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
brokerDeleteInactiveTopicsEnabled=false
```

if you get the follow exception,

```
java.lang.IllegalArgumentException: Broker has disabled transaction coordinator, please enable it before using transaction.
```

please set  the transactionCoordinatorEnabled to true

```bash
kafkaTransactionCoordinatorEnabled=true
transactionCoordinatorEnabled=true
```

> if this error not fixed，you will find use bin/kafka-console-producer.sh and bin/kafka-console-consumer.sh tools publishing and subscription is success on pulsar, but Apache Doris can not subscription data on pulsar.

##### 4. Start Pulsar

```bash
#foreground start
#bin/pulsar standalone
#daemon start 
pulsar-daemon start standalone
```

##### 5. Create Doris database and table

```bash
#Connected to Doris
mysql -u root  -h 127.0.0.1 -P 9030
# Create database
create database pulsar_doris;
#Change to pulsar_doris database
use pulsar_doris;
#Create clicklog table
CREATE TABLE IF NOT EXISTS pulsar_doris.clicklog
(
    `clickTime` DATETIME NOT NULL COMMENT "clickTime",
    `type` String NOT NULL COMMENT "clickType",
    `id`  VARCHAR(100) COMMENT "id",
    `user` VARCHAR(100) COMMENT "username",
    `city` VARCHAR(50) COMMENT "city"
)
DUPLICATE KEY(`clickTime`, `type`)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

##### 6. Create Routine Load Task

```bash
CREATE ROUTINE LOAD pulsar_doris.load_from_pulsar_test ON clicklog
COLUMNS(clickTime,id,type,user)
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
    "kafka_broker_list" = "127.0.0.1:9092",
    "kafka_topic" = "test",
    "property.group.id" = "doris"
 );
```

PROPERTIES Desc：
- pulsar_doris ：database name

- load_from_pulsar_test：Routine Load Task name

- clicklog：target table of Routine Load Task insert into

- strict_mode：is strict_mode or not

- format：datasource format

- kafka_broker_list：kafka broker address

- kafka_broker_list：kafka topic name

- property.group.id：consumer group id

##### 7. Data Publishing and Subscription

1. Data Publishing

   Create data structure of ClickLog,and use Kafka Producer send 5000 rows  to pulsar.

ClickLog data structure:

```bash
public class ClickLog {
    private String id;
    private String user;
    private String city;
    private String clickTime;
    private String type;
    ... //getter and  setter
   }
```

create and send data code:
```bash
       String strDateFormat = "yyyy-MM-dd HH:mm:ss";
       @Autowired
       private Producer producer;
        try {
            for(int j =0 ; j<50000;j++){
              int batchSize = 1000;
                for(int i = 0 ; i<batchSize ;i++){
                    ClickLog clickLog  = new ClickLog();
                    clickLog.setId(UUID.randomUUID().toString());
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(strDateFormat);
                    clickLog.setClickTime(simpleDateFormat.format(new Date()));
                    clickLog.setType("webset");
                    clickLog.setUser("user"+ new Random().nextInt(1000) +i);
                    producer.sendMessage(Constant.topicName, JSONObject.toJSONString(clickLog));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
```

2. View ROUTINE LOAD Task

execute SHOW ALL ROUTINE LOAD FOR load_from_pulsar_test \G; command，and view ROUTINE LOAD Task。


```bash
mysql>  SHOW ALL ROUTINE LOAD FOR load_from_pulsar_test \G;
*************************** 1. row ***************************
                  Id: 87873
                Name: load_from_pulsar_test
          CreateTime: 2022-05-31 12:03:34
           PauseTime: NULL
             EndTime: NULL
              DbName: default_cluster:pulsar_doris
           TableName: clicklog1
               State: RUNNING
      DataSourceType: KAFKA
      CurrentTaskNum: 1
       JobProperties: {"partitions":"*","columnToColumnExpr":"clickTime,id,type,user","maxBatchIntervalS":"20","whereExpr":"*","dataFormat":"json","timezone":"Europe/London","send_batch_parallelism":"1","precedingFilter":"*","mergeType":"APPEND","format":"json","json_root":"","maxBatchSizeBytes":"209715200","exec_mem_limit":"2147483648","strict_mode":"false","jsonpaths":"","deleteCondition":"*","desireTaskConcurrentNum":"3","maxErrorNum":"0","strip_outer_array":"false","currentTaskConcurrentNum":"1","execMemLimit":"2147483648","num_as_string":"false","fuzzy_parse":"false","maxBatchRows":"300000"}
DataSourceProperties: {"topic":"test","currentKafkaPartitions":"0","brokerList":"127.0.0.1:9092"}
    CustomProperties: {"group.id":"doris","kafka_default_offsets":"OFFSET_END","client.id":"doris.client"}
           Statistic: {"receivedBytes":5739001913,"runningTxns":[],"errorRows":0,"committedTaskNum":168,"loadedRows":50000000,"loadRowsRate":23000,"abortedTaskNum":1,"errorRowsAfterResumed":0,"totalRows":50000000,"unselectedRows":0,"receivedBytesRate":2675000,"taskExecuteTimeMs":2144799}
            Progress: {"0":"51139566"}
                 Lag: {"0":0}
ReasonOfStateChanged: 
        ErrorLogUrls: 
            OtherMsg: 
1 row in set (0.00 sec)

ERROR: 
No query specified
```

As can be seen from the above results totalRows is 50000000，errorRows is 0. This indicates that the data can be imported into Apache Doris without loss or duplication.

3. count data

    Execute the following command to count the data in the table. It is found that the statistical result is also 50000000, which is in line with expectations.

```bash
mysql> select count(*) from clicklog;
+----------+
| count(*) |
+----------+
| 50000000 |
+----------+
1 row in set (3.73 sec)

mysql> 
```

8. ##### SSL Access Pulsar
To be improved


