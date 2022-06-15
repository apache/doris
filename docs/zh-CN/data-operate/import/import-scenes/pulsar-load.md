---
{
"title": "订阅Pulsar日志",
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


#### Kop架构介绍：

---
KoP是Kafka on Pulsar的简写，顾名思义就是如何在Pulsar上实现对Kafka数据的读写。KoP将Kafka协议处理插件引入Pulsar Broker来实现Apache Pulsar对Apache Kafka 协议的支持。将 KoP 协议处理插件添加到现有 Pulsar 集群后，用户不用修改代码就可以将现有的 Kafka 应用程序和服务迁移到 Pulsar。

Apache Pulsar主要特点如下：

- 利用企业级多租户特性简化运营。

- 避免数据搬迁，简化操作。

- 利用 Apache BookKeeper 和分层存储持久保留事件流。

- 利用 Pulsar Functions 进行无服务器化事件处理。

- KoP架构如下图，通过图可以看到KoP引入一个新的协议处理插件，该协议处理插件利用 Pulsar 的现有组件（例如 Topic 发现、分布式日志库-ManagedLedger、cursor 等）来实现 Kafka 传输协议。

  ![](https://github.com/streamnative/kop/raw/master/docs/kop-architecture.png)

  

  #### Routine Load 订阅Pulsar数据思路

---
 Apache Doris Routine Load支持了将Kafka数据接入Apache Doris，并保障了数据接入过程中的事务性操作。Apache Pulsar定位为一个云原生时代企业级的消息发布和订阅系统，已经在很多线上服务使用。那么Apache Pulsar用户如何将数据数据接入Apache Doris呢，答案是通过KoP实现。

由于Kop直接在Pulsar侧提供了对Kafka的兼容，那么对于Apache Doris来说可以像使用Kafka一样使用Plusar。整个过程对于Apache Doris来说无需任务改变，就能将Pulsar数据接入Apache Doris，并且可以获得Routine Load的事务性保障。

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

#### 操作实战

---
##### 1. Pulsar Standalone安装环境准备:

1. JDK安装：略
2. 下载Pulsar二进制包，并解压：
```bash
#下载
wget https://archive.apache.org/dist/pulsar/pulsar-2.10.0/apache-pulsar-2.10.0-bin.tar.gz
#解压并进入安装目录
tar xvfz apache-pulsar-2.10.0-bin.tar.gz
cd apache-pulsar-2.10.0
```

##### 2. KoP组件编译和安装:

1. 下载KoP源码

```bash
git clone https://github.com/streamnative/kop.git
cd kop
```

2. 编译KoP项目：

```bash
mvn clean install -DskipTests
```

3. protocols配置：在解压后的apache-pulsar目录下创建protocols文件夹，并把编译好的nar包复制到protocols文件夹中。

```bash
mkdir apache-pulsar-2.10.0/protocols
# mv kop/kafka-impl/target/pulsar-protocol-handler-kafka-{{protocol:version}}.nar apache-pulsar-2.10.0/protocols
cp kop/kafka-impl/target/pulsar-protocol-handler-kafka-2.11.0-SNAPSHOT.nar apache-pulsar-2.10.0/protocols
```

4. 添加后的结果查看：

```bash
[root@17a5da45700b apache-pulsar-2.10.0]# ls protocols/
pulsar-protocol-handler-kafka-2.11.0-SNAPSHOT.nar
```

##### 3. KoP配置添加：

1. 在standalone.conf或者broker.conf添加如下配置

```bash
#kop适配的协议
messagingProtocols=kafka
#kop 的NAR文件路径
protocolHandlerDirectory=./protocols
#是否允许自动创建topic
allowAutoTopicCreationType=partitioned
```

2. 添加如下服务监听配置

```bash
# Use `kafkaListeners` here for KoP 2.8.0 because `listeners` is marked as deprecated from KoP 2.8.0 
kafkaListeners=PLAINTEXT://127.0.0.1:9092# This config is not required unless you want to expose another address to the Kafka client.
# If it’s not configured, it will be the same with `kafkaListeners` config by default
kafkaAdvertisedListeners=PLAINTEXT://127.0.0.1:9092
brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
brokerDeleteInactiveTopicsEnabled=false
```

当出现如下错误：

```
java.lang.IllegalArgumentException: Broker has disabled transaction coordinator, please enable it before using transaction.
```

添加如下配置，开启transactionCoordinatorEnabled

```bash
kafkaTransactionCoordinatorEnabled=true
transactionCoordinatorEnabled=true
```

> 这个错误一定要修复，不然看到的现象就是使用kafka自带的工具：bin/kafka-console-producer.sh和bin/kafka-console-consumer.sh在Pulsar上进行数据的生产和消费正常，但是在Apache Doris中数据无法同步过来。

##### 4. Pulsar启动

```bash
#前台启动
#bin/pulsar standalone
#后台启动
pulsar-daemon start standalone
```

##### 5. 创建Doris数据库和建表

```bash
#进入Doris
mysql -u root  -h 127.0.0.1 -P 9030
# 创建数据库
create database pulsar_doris;
#切换数据库
use pulsar_doris;
#创建clicklog表
CREATE TABLE IF NOT EXISTS pulsar_doris.clicklog
(
    `clickTime` DATETIME NOT NULL COMMENT "点击时间",
    `type` String NOT NULL COMMENT "点击类型",
    `id`  VARCHAR(100) COMMENT "唯一id",
    `user` VARCHAR(100) COMMENT "用户名称",
    `city` VARCHAR(50) COMMENT "所在城市"
)
DUPLICATE KEY(`clickTime`, `type`)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

##### 6. 创建Routine Load任务

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

上述命令中的参数解释如下：
  - pulsar_doris ：Routine Load 任务所在的数据库

  - load_from_pulsar_test：Routine Load 任务名称

  - clicklog：Routine Load 任务的目标表，也就是配置Routine Load 任务将数据导入到Doris哪个表中。

  - strict_mode：导入是否为严格模式，这里设置为false。

  - format：导入数据的类型，这里配置为json。

  - kafka_broker_list：kafka broker服务的地址

  - kafka_broker_list：kafka topic名称，也就是同步哪个topic上的数据。

  - property.group.id：消费组id

  ##### 7. 数据导入和测试

  1. 数据导入

     构造一个ClickLog的数据结构，并调用Kafka的Producer发送5000万条数据到Pulsar。

  ClickLog数据结构如下：

```bash
public class ClickLog {
    private String id;
    private String user;
    private String city;
    private String clickTime;
    private String type;
    ... //省略getter和setter
   }
```

消息构造和发送的核心代码逻辑如下：
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

2. ROUTINE LOAD任务查看

执行SHOW ALL ROUTINE LOAD FOR load_from_pulsar_test \G;命令，查看导入任务的状态。


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

从上面结果可以看到totalRows为50000000，errorRows为0。说明数据不丢不重的导入Apache Doris了。

3. 数据统计验证
      执行如下命令统计表中的数据，发现统计的结果也是50000000，符合预期。

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

8. ##### SSL方式接入Pulsar

待完善

