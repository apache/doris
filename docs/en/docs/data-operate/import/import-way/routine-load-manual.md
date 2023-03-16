---
{
    "title": "Routine Load",
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

# Routine Load

The Routine Load feature provides users with a way to automatically load data from a specified data source.

This document describes the implementation principles, usage, and best practices of this feature.

## Glossary

* RoutineLoadJob: A routine load job submitted by the user.
* JobScheduler: A routine load job scheduler for scheduling and dividing a RoutineLoadJob into multiple Tasks.
* Task: RoutineLoadJob is divided by JobScheduler according to the rules.
* TaskScheduler: Task Scheduler. Used to schedule the execution of a Task.

## Principle

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

As shown above, the client submits a routine load job to FE.

FE splits an load job into several Tasks via JobScheduler. Each Task is responsible for loading a specified portion of the data. The Task is assigned by the TaskScheduler to the specified BE.

On the BE, a Task is treated as a normal load task and loaded via the Stream Load load mechanism. After the load is complete, report to FE.

The JobScheduler in the FE continues to generate subsequent new Tasks based on the reported results, or retry the failed Task.

The entire routine load job completes the uninterrupted load of data by continuously generating new Tasks.

## Kafka Routine load

Currently we only support routine load from the Kafka system. This section details Kafka's routine use and best practices.

### Usage restrictions

1. Support unauthenticated Kafka access and Kafka clusters certified by SSL.
2. The supported message format is csv text or json format. Each message is a line in csv format, and the end of the line does not contain a ** line break.
3. Kafka 0.10.0.0 (inclusive) or above is supported by default. If you want to use Kafka versions below 0.10.0.0 (0.9.0, 0.8.2, 0.8.1, 0.8.0), you need to modify the configuration of be, set the value of kafka_broker_version_fallback to be the older version, or directly set the value of property.broker.version.fallback to the old version when creating routine load. The cost of the old version is that some of the new features of routine load may not be available, such as setting the offset of the kafka partition by time.

### Create a routine load task

The detailed syntax for creating a routine import task can be found in [CREATE ROUTINE LOAD](... /... /... /sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md) after connecting to Doris  command manual, or execute `HELP ROUTINE LOAD;` for syntax help.

Here we illustrate how to create Routine Load tasks with a few examples.

1. Create a Kafka example import task named test1 for example_tbl of example_db. Specify the column separator and group.id and client.id, and automatically consume all partitions by default and subscribe from the location where data is available (OFFSET_BEGINNING). 

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

2. Create a Kafka example import task named test1 for example_tbl of example_db in **strict mode**.

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

>Notes：
>
>"strict_mode" = "true"

3. Example of importing data in Json format

   Routine Load only supports the following two types of json formats

   The first one has only one record and is a json object.

```json
{"category":"a9jadhx","author":"test","price":895}
```

The second one is a json array, which can contain multiple records

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

Create the Doris data table to be imported

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

Import json data in simple mode

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

Accurate import of data in json format

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

>**Notes:** 
>
>The partition field `dt` in the table is not in our data, but is converted in our Routine load statement by `dt=from_unixtime(timestamp, '%Y%m%d')`

**strict mode import relationship with source data**

Here is an example with a column type of TinyInt

> Notes: When a column in the table allows importing null values

> 

| source data | source data example | string to int | strict_mode   | result                 |
| ----------- | ------------------- | ------------- | ------------- | ---------------------- |
| Null value  | \N                  | N/A           | true or false | NULL                   |
| not null    | aaa or 2000         | NULL          | true          | invalid data(filtered) |
| not null    | aaa                 | NULL          | false         | NULL                   |
| not null    | 1                   | 1             | true or false | correct data           |

Here is an example with the column type Decimal(1,0)

> Notes:
>
>  When the columns in the table allow importing null values

| source data | source data example | string to int | strict_mode   | result                 |
| ----------- | ------------------- | ------------- | ------------- | ---------------------- |
| Null value  | \N                  | N/A           | true or false | NULL                   |
| not null    | aaa                 | NULL          | true          | invalid data(filtered) |
| not null    | aaa                 | NULL          | false         | NULL                   |
| not null    | 1 or 10             | 1             | true or false | correct data           |

> Notes:
>
>  Although 10 is an out-of-range value, it is not affected by strict mode because its type meets the decimal requirement. 10 will eventually be filtered in other ETL processing processes. But it will not be filtered by strict mode.

**Accessing an SSL-certified Kafka cluster**

Accessing an SSL-certified Kafka cluster requires the user to provide the certificate file (ca.pem) used to authenticate the Kafka Broker's public key. If the Kafka cluster also has client authentication enabled, the client's public key (client.pem), the key file (client.key), and the key password are also required. The required files need to be uploaded to Doris first via the `CREAE FILE` command, **and the catalog name is `kafka`**. See `HELP CREATE FILE;` for help with the `CREATE FILE` command. Here are some examples.

1. uploading a file

```sql
CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
```

2. Create routine import jobs

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

>Doris accesses Kafka clusters through Kafka's C++ API `librdkafka`. The parameters supported by `librdkafka` can be found in
>
>[https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/ CONFIGURATION.md)
>
>

<version since="1.2">

**Accessing a Kerberos-certified Kafka cluster**

Accessing a Kerberos-certified Kafka cluster. The following configurations need to be added:

   - security.protocol=SASL_PLAINTEXT : Use SASL plaintext
   - sasl.kerberos.service.name=$SERVICENAME : Broker service name
   - sasl.kerberos.keytab=/etc/security/keytabs/${CLIENT_NAME}.keytab : Client keytab location
   - sasl.kerberos.principal=${CLIENT_NAME}/${CLIENT_HOST} : sasl.kerberos.principal

1. Create routine import jobs

   ```sql
   CREATE ROUTINE LOAD db1.job1 on tbl1
   PROPERTIES (
   "desired_concurrent_number"="1",
    )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092",
       "kafka_topic" = "my_topic",
       "property.security.protocol" = "SASL_PLAINTEXT",
       "property.sasl.kerberos.service.name" = "kafka",
       "property.sasl.kerberos.keytab" = "/etc/krb5.keytab",
       "property.sasl.kerberos.principal" = "doris@YOUR.COM"
   );
   ```

**Note:**
- To enable Doris to access the Kafka cluster with Kerberos authentication enabled, you need to deploy the Kerberos client kinit on all running nodes of the Doris cluster, configure krb5.conf, and fill in KDC service information.
- Configure property.sasl.kerberos The value of keytab needs to specify the absolute path of the keytab local file and allow Doris processes to access the local file.

</version>

### Viewing Job Status

Specific commands and examples to view the status of **jobs** can be viewed with the `HELP SHOW ROUTINE LOAD;` command.

Specific commands and examples to view the status of **tasks** running can be viewed with the `HELP SHOW ROUTINE LOAD TASK;` command.

Only currently running tasks can be viewed; closed and unstarted tasks cannot be viewed.

### Modify job properties

Users can modify jobs that have already been created. The details can be viewed with the `HELP ALTER ROUTINE LOAD;` command or see [ALTER ROUTINE LOAD](... /... /... /sql-manual/sql-reference/Data-Manipulation-Statements/Load/ALTER-ROUTINE-LOAD.md).

### Job Control

The user can control the stop, pause and restart of jobs with the `STOP/PAUSE/RESUME` commands. Help and examples can be viewed with the `HELP STOP ROUTINE LOAD;` `HELP PAUSE ROUTINE LOAD;` and `HELP RESUME ROUTINE LOAD;` commands.

## Other notes

1. The relationship between a routine import job and an ALTER TABLE operation

   - Example import does not block SCHEMA CHANGE and ROLLUP operations. However, note that if the column mapping relationships do not match after the SCHEMA CHANGE completes, it can cause a spike in error data for the job and eventually cause the job to pause. It is recommended that you reduce this problem by explicitly specifying column mapping relationships in routine import jobs and by adding Nullable columns or columns with Default values.
   - Deleting a Partition of a table may cause the imported data to fail to find the corresponding Partition and the job to enter a pause. 2.

2. Relationship between routine import jobs and other import jobs (LOAD, DELETE, INSERT)

   - There is no conflict between the routine import and other LOAD operations and INSERT operations.
   - When the DELETE operation is executed, the corresponding table partition cannot have any ongoing import jobs. Therefore, before executing DELETE operation, you may need to suspend the routine import job and wait until all the issued tasks are completed before executing DELETE. 3.

3. The relationship between routine import and DROP DATABASE/TABLE operations

   When the database or table corresponding to the routine import is deleted, the job will automatically CANCEL.

4. The relationship between kafka type routine import jobs and kafka topic

   When the `kafka_topic` declared by the user in the create routine import statement does not exist in the kafka cluster.

   - If the broker of the user's kafka cluster has `auto.create.topics.enable = true` set, then `kafka_topic` will be created automatically first, and the number of partitions created automatically is determined by the configuration of the broker in the **user's kafka cluster** with `num. partitions`. The routine job will keep reading data from the topic as normal.
   - If the broker in the user's kafka cluster has `auto.create.topics.enable = false` set, the topic will not be created automatically and the routine job will be suspended with a status of `PAUSED` before any data is read.

   So, if you want the kafka topic to be automatically created by the routine when it does not exist, just set `auto.create.topics.enable = true` for the broker in the **user's kafka cluster**.

5. Problems that may arise in network isolated environments In some environments there are isolation measures for network segments and domain name resolution, so care needs to be taken

   1. the Broker list specified in the Create Routine load task must be accessible by the Doris service
   2. If `advertised.listeners` is configured in Kafka, the addresses in `advertised.listeners` must be accessible to the Doris service

6. Specify the Partition and Offset for consumption

   Doris supports specifying a Partition and Offset to start consumption. The new version also supports the ability to specify time points for consumption. The configuration of the corresponding parameters is explained here.

   There are three relevant parameters.

   - `kafka_partitions`: Specify the list of partitions to be consumed, e.g., "0, 1, 2, 3".
   - `kafka_offsets`: specifies the starting offset of each partition, which must correspond to the number of `kafka_partitions` list. For example: "1000, 1000, 2000, 2000"
   - `property.kafka_default_offset`: specifies the default starting offset of the partitions.

   When creating an import job, these three parameters can have the following combinations.

   | combinations | `kafka_partitions` | `kafka_offsets` | `property.kafka_default_offset` | behavior                                                     |
   | ------------ | ------------------ | --------------- | ------------------------------- | ------------------------------------------------------------ |
   | 1            | No                 | No              | No                              | The system will automatically find all partitions corresponding to the topic and start consuming them from OFFSET_END |
   | 2            | No                 | No              | Yes                             | The system will automatically find all the partitions corresponding to the topic and start consuming them from the default offset location. |
   | 3            | Yes                | No              | No                              | The system will start consuming from the OFFSET_END of the specified partition. |
   | 4            | Yes                | Yes             | No                              | The system will start consuming at the specified offset of the specified partition. |
   | 5            | Yes                | No              | Yes                             | The system will start consuming at the default offset of the specified partition |
   
7. The difference between STOP and PAUSE

   FE will automatically clean up the ROUTINE LOAD in STOP status periodically, while the PAUSE status can be restored to enable again.

## Related Parameters

Some system configuration parameters can affect the use of routine import.

1. max_routine_load_task_concurrent_num

   FE configuration item, defaults to 5 and can be modified at runtime. This parameter limits the maximum number of concurrent subtasks for a routine import job. It is recommended to keep the default value. Setting it too large may result in too many concurrent tasks and consume cluster resources.

2. max_routine_load_task_num_per_be

   FE configuration item, default is 5, can be modified at runtime. This parameter limits the maximum number of concurrently executed subtasks per BE node. It is recommended to keep the default value. If set too large, it may lead to too many concurrent tasks and consume cluster resources.

3. max_routine_load_job_num

   FE configuration item, default is 100, can be modified at runtime. This parameter limits the total number of routine import jobs, including the states NEED_SCHEDULED, RUNNING, PAUSE. After this, no new jobs can be submitted.

4. max_consumer_num_per_group

   BE configuration item, default is 3. This parameter indicates the maximum number of consumers that can be generated for data consumption in a subtask. For a Kafka data source, a consumer may consume one or more kafka partitions. If there are only 2 partitions, only 2 consumers are generated, each consuming 1 partition. 5. push_write_mby

5. max_tolerable_backend_down_num 

   FE configuration item, the default value is 0. Doris can PAUSED job rescheduling to RUNNING if certain conditions are met. 0 means rescheduling is allowed only if all BE nodes are ALIVE.

6. period_of_auto_resume_min 

   FE configuration item, the default is 5 minutes, Doris rescheduling will only be attempted up to 3 times within the 5 minute period. If all 3 attempts fail, the current task is locked and no further scheduling is performed. However, manual recovery can be done through human intervention.

## More help

For more detailed syntax on the use of **Routine Load**, you can type `HELP ROUTINE LOAD` at the Mysql client command line for more help.
