---
{
    "title": "Use Flink CDC to realize real-time MySQL data into Apache Doris",
    "description": "This article uses examples to demonstrate how to use Flink CDC and Doris' Flink Connector to monitor data from the Mysql database and store it in the corresponding table of the Doris data warehouse in real time.",
    "date": "2021-11-11",
    "metaTitle": "Use Flink CDC to realize real-time MySQL data into Apache Doris",
    "language": "en",
    "author": "张家锋",
    "layout": "Article",
    "isArticle": true,
    "sidebar": false
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
# Use Flink CDC to realize real-time MySQL data into Apache Doris

This article uses examples to demonstrate how to use Flink CDC and Doris' Flink Connector to monitor data from the Mysql database and store it in the corresponding table of the Doris data warehouse in real time.

## 1. What is CDC

CDC is the abbreviation of Change Data Capture technology. It can record incremental changes of the source database (Source) and synchronize it to one or more data destinations (Sink). In the synchronization process, you can also perform certain processing on the data, such as grouping (GROUP BY), multi-table association (JOIN), and so on.

For example, for an e-commerce platform, a user’s order will be written to a source database in real time; Department A needs to simply aggregate the real-time data every minute and then save it to Redis for query, and Department B needs to temporarily store the data of the day in Elasticsearch will use a copy for report display, and Department C also needs a copy of data to ClickHouse for real-time data warehouse. As time goes by, the subsequent D and E departments will also have data analysis requirements. In this scenario, the traditional copying and distributing multiple copies is very inflexible, while CDC can realize a change record, process and deliver it in real time To multiple destinations.

### 1.1 Application Scenarios of CDC

-**Data synchronization: **Used for backup and disaster recovery;
-**Data distribution:** One data source is distributed to multiple downstream systems;
-**Data Collection:** ETL data integration for data warehouse/data lake is a very important data source.

There are many technical solutions for CDC, and the current mainstream implementation mechanisms in the industry can be divided into two types:

-CDC based on query:
  -Offline scheduling query jobs, batch processing. Synchronize a table to other systems, and obtain the latest data in the table through query each time;
  -Data consistency cannot be guaranteed, and the data may have been changed many times during the inspection process;
  -Real-time performance is not guaranteed, and there is a natural delay based on offline scheduling.
-Log-based CDC:
  -Real-time consumption log, stream processing, for example, MySQL's binlog log completely records the changes in the database, and the binlog file can be used as the data source of the stream;
  -Ensure data consistency, because the binlog file contains details of all historical changes;
  -Guarantee real-time performance, because log files like binlog can be streamed for consumption and provide real-time data.

## 2.Flink CDC

Flink added the feature of CDC in version 1.11, referred to as change data capture. The name is a bit messy, let's look at the content of CDC from the previous data structure.

The above is the log processing flow of the previous `mysq binlog`, for example, canal listens to binlog and writes the log to Kafka. And Apache Flink consumes Kakfa data in real time to achieve mysql data synchronization or other content. As a whole, it can be divided into the following stages.

1. mysql open binlog
2. Canal synchronize binlog data write to kafka
3. Flink reads the binlog data in kakfa for related business processing.

The overall processing link is longer and requires more components. Apache Flink CDC can obtain binlog directly from the database for downstream business calculation analysis

### 2.1 Flink Connector Mysql CDC 2.0 Features

Provide MySQL CDC 2.0, core features include

-Concurrent reading, the reading performance of the full amount of data can be horizontally expanded;
-No locks throughout the process, no risk of locks on online business;
-Resumable transfers at breakpoints, support full-stage checkpoints.

There are test documents on the Internet showing that the customer table in the TPC-DS data set was used for testing. The Flink version is 1.13.1, the data volume of the customer table is 65 million, the Source concurrency is 8, and the full read stage:

-MySQL CDC 2.0 takes **13** minutes;
-MySQL CDC 1.4 takes **89** minutes;
-Reading performance improved by **6.8** times.

## 3. What is Doris Flink Connector

Flink Doris Connector is an extension of the doris community in order to facilitate users to use Flink to read and write Doris data tables.

Currently doris supports Flink 1.11.x, 1.12.x, 1.13.x, Scala version: 2.12.x

Currently, the Flink doris connector currently controls the warehousing through two parameters:

1. sink.batch.size: How many pieces are written once, the default is 100 pieces
2. sink.batch.interval: how many seconds to write each time, the default is 1 second

These two parameters work at the same time, and that condition will trigger the write doris table operation when the condition comes first.

**Notice:**

**Note** here is to enable the http v2 version, specifically configure `enable_http_server_v2=true` in fe.conf, and because the be list is obtained through fe http rest api, the users who need to be configured have admin permissions.

## 4. Usage example

### 4.1 Flink Doris Connector compilation

First, we need to compile Doris's Flink connector, which can also be downloaded from the following address:

https://github.com/hf200012/hf200012.github.io/raw/main/lib/doris-flink-1.0-SNAPSHOT.jar

>Note:
>
>Here because Doris' Flink Connector is developed based on Scala 2.12.x version, so when you use Flink, please choose the version corresponding to Scala 2.12.
>
>If you downloaded the corresponding jar using the above address, please ignore the compiled content part below

Compile under the doris docker compilation environment `apache/incubator-doris:build-env-1.2`, because the JDK version below 1.3 is 11, there will be compilation problems.

Execute in the extension/flink-doris-connector/ source directory:

```
sh build.sh
```

After the compilation is successful, the file `doris-flink-1.0.0-SNAPSHOT.jar` will be generated in the `output/` directory. Copy this file to the `ClassPath` of `Flink` to use `Flink-Doris-Connector`. For example, for `Flink` running in `Local` mode, put this file in the `jars/` folder. For `Flink` running in `Yarn` cluster mode, put this file into the pre-deployment package.

**For Flink 1.13.x version adaptation issues**

```xml
   <properties>
        <scala.version>2.12</scala.version>
        <flink.version>1.11.2</flink.version>
        <libthrift.version>0.9.3</libthrift.version>
        <arrow.version>0.15.1</arrow.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <doris.home>${basedir}/../../</doris.home>
        <doris.thirdparty>${basedir}/../../thirdparty</doris.thirdparty>
    </properties>
```

Just change the `flink.version` here to be the same as your Flink cluster version, and edit again

### 4.2 Configure Flink

Here we use Flink Sql Client to operate.

Here we demonstrate the software version used:

1. Mysql 8.x
2. Apache Flink: 1.13.3
3. Apache Doris: 0.14.13.1

#### 4.2.1 Install Flink

First download and install Flink:

https://dlcdn.apache.org/flink/flink-1.13.3/flink-1.13.3-bin-scala_2.12.tgz


Download Flink CDC related Jar packages:

https://repo1.maven.org/maven2/com/ververica/flink-connector-mysql-cdc/2.0.2/flink-connector-mysql-cdc-2.0.2.jar

Pay attention to the version correspondence between Flink CDC and Flink here

![image-20211025170642628](/images/cdc/image-20211025170642628.png)

-Copy the Flink Doris Connector jar package downloaded or compiled above to the lib directory under the Flink root directory
-The Flink cdc jar package is also copied to the lib directory under the Flink root directory

The demonstration here uses the local stand-alone mode,

```shell
# wget https://dlcdn.apache.org/flink/flink-1.13.3/flink-1.13.3-bin-scala_2.12.tgz
# tar zxvf flink-1.13.3-bin-scala_2.12.tgz
# cd flink-1.13.3
# wget https://repo1.maven.org/maven2/com/ververica/flink-connector-mysql-cdc/2.0.2/flink-connector-mysql-cdc-2.0.2.jar -P ./lib/
# wget https://github.com/hf200012/hf200012.github.io/raw/main/lib/doris-flink-1.0-SNAPSHOT.jar -P ./lib/
```

![image-20211026095513892](/images/cdc/image-20211026095513892.png)

#### 4.2.2 Start Flink

Here we are using the local stand-alone mode

```
# bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host doris01.
Starting taskexecutor daemon on host doris01.
```

We start the Flink cluster through web access (the default port is 8081), and we can see that the cluster starts normally

![image-20211025162831632](/images/cdc/image-20211025162831632.png)

### 4.3 Install Apache Doris

For the specific method of installing and deploying Doris, refer to the following link:

https://hf200012.github.io/2021/09/Apache-Doris-Environment installation and deployment

### 4.3 Installation and Configuration Mysql

1. Install Mysql

   Quickly use Docker to install and configure Mysql, refer to the following link for details

   https://segmentfault.com/a/1190000021523570

2. Open Mysql binlog

   Enter the Docker container to modify the /etc/my.cnf file, and add the following content under [mysqld],

   ```
   log_bin=mysql_bin
   binlog-format=Row
   server-id=1
   ```

   Then restart Mysql

   ```
   systemctl restart mysqld
   ```

3. Create Mysql database table

```sql
 CREATE TABLE `test_cdc` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
 ) ENGINE=InnoDB
```

### 4.4 Create doris table

```sql
CREATE TABLE `doris_test` (
  `id` int NULL COMMENT "",
  `name` varchar(100) NULL COMMENT ""
 ) ENGINE=OLAP
 UNIQUE KEY(`id`)
 COMMENT "OLAP"
 DISTRIBUTED BY HASH(`id`) BUCKETS 1
 PROPERTIES (
 "replication_num" = "3",
 "in_memory" = "false",
 "storage_format" = "V2"
 );
```

### 4.5 Start Flink Sql Client

```shell
./bin/sql-client.sh embedded
> set execution.result-mode=tableau;
```

![image-20211025165547903](/images/cdc/image-20211025165547903.png)

#### 4.5.1 Create Flink CDC Mysql mapping table

```sql
CREATE TABLE test_flink_cdc (
  id INT,
  name STRING,
  primary key(id) NOT ENFORCED
) WITH (
  'connector' ='mysql-cdc',
  'hostname' ='localhost',
  'port' = '3306',
  'username' ='root',
  'password' ='password',
  'database-name' ='demo',
  'table-name' ='test_cdc'
);
```

The Mysql mapping table created by the query is displayed normally

```
select * from test_flink_cdc;
```

![image-20211026100505972](/images/cdc/image-20211026100505972.png)

#### 4.5.2 Create Flink Doris Table Mapping Table

Use Doris Flink Connector to create Doris mapping table

```sql
CREATE TABLE doris_test_sink (
   id INT,
   name STRING
)
WITH (
  'connector' ='doris',
  'fenodes' ='localhost:8030',
  'table.identifier' ='db_audit.doris_test',
  'sink.batch.size' = '2',
  'sink.batch.interval'='1',
  'username' ='root',
  'password' =''
)
```

Execute the above statement on the command line, you can see that the table is created successfully, and then execute the query statement to verify whether it is normal

```sql
select * from doris_test_sink;
```

![image-20211026100804091](/images/cdc/image-20211026100804091.png)

Perform an insert operation, insert the data in Mysql into Doris through Flink CDC combined with Doris Flink Connector

```sql
INSERT INTO doris_test_sink select id,name from test_flink_cdc
```

![image-20211026101004547](/images/cdc/image-20211026101004547.png)

After the submission is successful, we can see the related job task information on the Flink web interface

![image-20211026100943474](/images/cdc/image-20211026100943474.png)

#### 4.5.3 Insert data into Mysql table

```sql
INSERT INTO test_cdc VALUES (123,'this is a update');
INSERT INTO test_cdc VALUES (1212,'Test flink CDC');
INSERT INTO test_cdc VALUES (1234,'This is a test');
INSERT INTO test_cdc VALUES (11233,'zhangfeng_1');
INSERT INTO test_cdc VALUES (21233,'zhangfeng_2');
INSERT INTO test_cdc VALUES (31233,'zhangfeng_3');
INSERT INTO test_cdc VALUES (41233,'zhangfeng_4');
INSERT INTO test_cdc VALUES (51233,'zhangfeng_5');
INSERT INTO test_cdc VALUES (61233,'zhangfeng_6');
INSERT INTO test_cdc VALUES (71233,'zhangfeng_7');
INSERT INTO test_cdc VALUES (81233,'zhangfeng_8');
INSERT INTO test_cdc VALUES (91233,'zhangfeng_9');
```

#### 4.5.4 Observe the data in the Doris table

First stop the Insert into task, because I am in the local stand-alone mode, there is only one task task, so I have to stop it, and then execute the query statement on the command line to see the data

![image-20211026101203629](/images/cdc/image-20211026101203629.png)

#### 4.5.5 Modify Mysql data

Restart the Insert into task

![image-20211025182341086](/images/cdc/image-20211025182341086.png)

Modify the data in the Mysql table

```sql
update test_cdc set name='This is an operation to verify the modification' where id =123
```

Look at the data in the Doris table again, you will find that it has been modified

Note that if you want to modify the data in the Mysql table, the data in Doris is also modified. If the model of the Doris data table is a unique key model, other data models (Aggregate Key and Duplicate Key) cannot update data.

![image-20211025182435827](/images/cdc/image-20211025182435827.png)

#### 4.5.6 Delete data operation

Currently Doris Flink Connector does not support delete operation, it is planned to add this operation later

