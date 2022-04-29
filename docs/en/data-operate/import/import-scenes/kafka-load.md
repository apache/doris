---
{
    "title": "Kafka Data Subscription",
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

# Subscribe to Kafka logs

Users can directly subscribe to message data in Kafka by submitting routine import jobs to synchronize data in near real-time.

Doris itself can ensure that messages in Kafka are subscribed without loss or weight, that is, `Exactly-Once` consumption semantics.

## Subscribe to Kafka messages

Subscribing to Kafka messages uses the Routine Load feature in Doris.

The user first needs to create a **routine import job**. The job will send a series of **tasks** continuously through routine scheduling, and each task will consume a certain number of messages in Kafka.

Please note the following usage restrictions:

1. Support unauthenticated Kafka access and SSL-authenticated Kafka clusters.
2. The supported message formats are as follows:
   - csv text format. Each message is a line, and the end of the line **does not contain** a newline.
   - Json format, see [Import Json Format Data](../import-way/load-json-format.html).
3. Only supports Kafka 0.10.0.0 (inclusive) and above.

### Accessing SSL-authenticated Kafka clusters

The routine import feature supports unauthenticated Kafka clusters, as well as SSL-authenticated Kafka clusters.

Accessing an SSL-authenticated Kafka cluster requires the user to provide a certificate file (ca.pem) for authenticating the Kafka Broker public key. If client authentication is also enabled in the Kafka cluster, the client's public key (client.pem), key file (client.key), and key password must also be provided. The files required here need to be uploaded to Plao through the `CREAE FILE` command, and the catalog name is `kafka`. The specific help of the `CREATE FILE` command can be found in the [CREATE FILE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-FILE.html) command manual . Here is an example:

- upload files

  ```sql
  CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
  CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
  CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
  ````

After the upload is complete, you can view the uploaded files through the [SHOW FILES](../../../sql-manual/sql-reference/Show-Statements/SHOW-FILE.html) command.

### Create a routine import job

For specific commands to create routine import tasks, see [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.html ) command manual. Here is an example:

1. Access the Kafka cluster without authentication

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
   ````

   - `max_batch_interval/max_batch_rows/max_batch_size` is used to control the running period of a subtask. The running period of a subtask is determined by the longest running time, the maximum number of rows consumed, and the maximum amount of data consumed.

2. Access an SSL-authenticated Kafka cluster

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
   ````

### View import job status

See [SHOW ROUTINE LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-ROUTINE-LOAD.html) for specific commands and examples for checking the status of a **job** ) command documentation.

See [SHOW ROUTINE LOAD TASK](../../../sql-manual/sql-reference/Show-Statements/SHOW-ROUTINE-LOAD-TASK.html) command documentation.

Only the currently running tasks can be viewed, and the completed and unstarted tasks cannot be viewed.

### Modify job properties

The user can modify some properties of the job that has been created. For details, please refer to the [ALTER ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/ALTER-ROUTINE-LOAD.html) command manual.

### Job Control

The user can control the stop, pause and restart of the job through the `STOP/PAUSE/RESUME` three commands.

For specific commands, please refer to [STOP ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STOP-ROUTINE-LOAD.html) , [PAUSE ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/PAUSE-ROUTINE-LOAD.html), [RESUME ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/RESUME-ROUTINE-LOAD.html) command documentation.

## more help

For more detailed syntax and best practices for ROUTINE LOAD, see [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.html) command manual.
