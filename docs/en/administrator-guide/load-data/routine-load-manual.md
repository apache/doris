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

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, the backend node of Doris. Responsible for query execution and data storage.
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

The detailed syntax for creating a routine load task can be connected to Doris and execute `HELP ROUTINE LOAD;` to see the syntax help. Here is a detailed description of the precautions when creating a job.

* columns_mapping

    `columns_mapping` is mainly used to specify the column structure of the table structure and message, as well as the conversion of some columns. If not specified, Doris will default to the columns in the message and the columns of the table structure in a one-to-one correspondence. Although under normal circumstances, if the source data is exactly one-to-one, normal data load can be performed without specifying. However, we still strongly recommend that users ** explicitly specify column mappings**. This way, when the table structure changes (such as adding a nullable column), or the source file changes (such as adding a column), the load task can continue. Otherwise, after the above changes occur, the load will report an error because the column mapping relationship is no longer one-to-one.

    In `columns_mapping` we can also use some built-in functions for column conversion. But you need to pay attention to the actual column type corresponding to the function parameters. for example:

    Suppose the user needs to load a table containing only a column of `k1` with a column type of `int`. And you need to convert the null value in the source file to 0. This feature can be implemented with the `ifnull` function. The correct way to use is as follows:

    `COLUMNS (xx, k1=ifnull(xx, "3"))`

    Note that we use `"3"` instead of `3`, although `k1` is of type `int`. Because the column type in the source data is `varchar` for the load task, the `xx` virtual column is also of type `varchar`. So we need to use `"3"` to match the match, otherwise the `ifnull` function can't find the function signature with the parameter `(varchar, int)`, and an error will occur.

    As another example, suppose the user needs to load a table containing only a column of `k1` with a column type of `int`. And you need to process the corresponding column in the source file: convert the negative number to a positive number and the positive number to 100. This function can be implemented with the `case when` function. The correct wording should be as follows:

    `COLUMNS (xx, k1 = case when xx < 0 then cast(-xx as varchar) else cast((xx + '100') as varchar) end)`

    Note that we need to convert all the parameters in `case when` to varchar in order to get the desired result.

* where_predicates

    The type of the column in `where_predicates` is already the actual column type, so there is no need to cast to the varchar type as `columns_mapping`. Write according to the actual column type.

* desired\_concurrent\_number

    `desired_concurrent_number` is used to specify the degree of concurrency expected for a routine job. That is, a job, at most how many tasks are executing at the same time. For Kafka load, the current actual concurrency is calculated as follows:

    ```
    Min(partition num, desired_concurrent_number, Config.max_routine_load_task_concurrrent_num)
    ```

    Where `Config.max_routine_load_task_concurrrent_num` is a default maximum concurrency limit for the system. This is a FE configuration that can be adjusted by changing the configuration. The default is 5.

    Where partition num refers to the number of partitions for the Kafka topic subscribed to.

* max\_batch\_interval/max\_batch\_rows/max\_batch\_size

    These three parameters are used to control the execution time of a single task. If any of the thresholds is reached, the task ends. Where `max_batch_rows` is used to record the number of rows of data read from Kafka. `max_batch_size` is used to record the amount of data read from Kafka in bytes. The current consumption rate for a task is approximately 5-10MB/s.

    So assume a row of data 500B, the user wants to be a task every 100MB or 10 seconds. The expected processing time for 100MB is 10-20 seconds, and the corresponding number of rows is about 200000 rows. Then a reasonable configuration is:

    ```
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000",
    "max_batch_size" = "104857600"
    ```

    The parameters in the above example are also the default parameters for these configurations.

* max\_error\_number

    `max_error_number` is used to control the error rate. When the error rate is too high, the job will automatically pause. Because the entire job is stream-oriented, and because of the borderless nature of the data stream, we can't calculate the error rate with an error ratio like other load tasks. So here is a new way of calculating to calculate the proportion of errors in the data stream.

    We have set up a sampling window. The size of the window is `max_batch_rows * 10`. Within a sampling window, if the number of error lines exceeds `max_error_number`, the job is suspended. If it is not exceeded, the next window restarts counting the number of error lines.

    We assume that `max_batch_rows` is 200000 and the window size is 2000000. Let `max_error_number` be 20000, that is, the user expects an error behavior of 20000 for every 2000000 lines. That is, the error rate is 1%. But because not every batch of tasks consumes 200000 rows, the actual range of the window is [2000000, 2200000], which is 10% statistical error.

    The error line does not include rows that are filtered out by the where condition. But include rows that do not have a partition in the corresponding Doris table.

* data\_source\_properties

    The specific Kafka partition can be specified in `data_source_properties`. If not specified, all partitions of the subscribed topic are consumed by default.

    Note that when partition is explicitly specified, the load job will no longer dynamically detect changes to Kafka partition. If not specified, the partitions that need to be consumed are dynamically adjusted based on changes in the kafka partition.

* strict\_mode

    Routine load load can turn on strict mode mode. The way to open it is to add ```"strict_mode" = "true"``` to job\_properties. The default strict mode is off.

    The strict mode mode means strict filtering of column type conversions during the load process. The strict filtering strategy is as follows:

    1. For column type conversion, if strict mode is true, the wrong data will be filtered. The error data here refers to the fact that the original data is not null, and the result is a null value after participating in the column type conversion.

    2. When a loaded column is generated by a function transformation, strict mode has no effect on it.

    3. For a column type loaded with a range limit, if the original data can pass the type conversion normally, but cannot pass the range limit, strict mode will not affect it. For example, if the type is decimal(1,0) and the original data is 10, it is eligible for type conversion but not for column declarations. This data strict has no effect on it.

* merge\_type
     The type of data merging supports three types: APPEND, DELETE, and MERGE. APPEND is the default value, which means that all this batch of data needs to be appended to the existing data. DELETE means to delete all rows with the same key as this batch of data. MERGE semantics Need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics


#### strict mode and load relationship of source data

Here is an example of a column type of TinyInt.

> Note: When a column in a table allows a null value to be loaded

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|---------|
|null        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa or 2000         | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1                   | 1               | true or false      | correct data|

Here the column type is Decimal(1,0)
 
> Note: When a column in a table allows a null value to be loaded

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|--------|
|null        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa                 | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1 or 10             | 1               | true or false      | correct data|

> Note: 10 Although it is a value that is out of range, because its type meets the requirements of decimal, strict mode has no effect on it. 10 will eventually be filtered in other ETL processing flows. But it will not be filtered by strict mode.

#### Accessing SSL-certified Kafka clusters

Accessing the SSL-certified Kafka cluster requires the user to provide a certificate file (ca.pem) for authenticating the Kafka Broker public key. If the Kafka cluster has both client authentication enabled, you will also need to provide the client's public key (client.pem), key file (client.key), and key password. The files needed here need to be uploaded to Doris via the `CREAE FILE` command, ** and the catalog name is `kafka`**. See `HELP CREATE FILE;` for specific help on the `CREATE FILE` command. Here is an example:

1. Upload file

    ```
    CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
    CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
    CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
    ```

2. Create a routine load job

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

> Doris accesses Kafka clusters via Kafka's C++ API `librdkafka`. The parameters supported by `librdkafka` can be found.
>
> <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>

### Viewing the status of the load job

Specific commands and examples for viewing the status of the ** job** can be viewed with the `HELP SHOW ROUTINE LOAD;` command.

Specific commands and examples for viewing the **Task** status can be viewed with the `HELP SHOW ROUTINE LOAD TASK;` command.

You can only view tasks that are currently running, and tasks that have ended and are not started cannot be viewed.

### Alter job

Users can modify jobs that have been created. Specific instructions can be viewed through the `HELP ALTER ROUTINE LOAD;` command. Or refer to [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/Data%20Manipulation/alter-routine-load.md).

### Job Control

The user can control the stop, pause and restart of the job by the three commands `STOP/PAUSE/RESUME`. You can view help and examples with the three commands `HELP STOP ROUTINE LOAD;`, `HELP PAUSE ROUTINE LOAD;` and `HELP RESUME ROUTINE LOAD;`.

## other instructions

1. The relationship between a routine load job and an ALTER TABLE operation

    * Routine load does not block SCHEMA CHANGE and ROLLUP operations. Note, however, that if the column mappings are not matched after SCHEMA CHANGE is completed, the job's erroneous data will spike and eventually cause the job to pause. It is recommended to reduce this type of problem by explicitly specifying column mappings in routine load jobs and by adding Nullable columns or columns with Default values.
    * Deleting a Partition of a table may cause the loaded data to fail to find the corresponding Partition and the job will be paused.

2. Relationship between routine load jobs and other load jobs (LOAD, DELETE, INSERT)

    * Routine load does not conflict with other LOAD jobs and INSERT operations.
    * When performing a DELETE operation, the corresponding table partition cannot have any load tasks being executed. Therefore, before performing the DELETE operation, you may need to pause the routine load job and wait for the delivered task to complete before you can execute DELETE.

3. Relationship between routine load jobs and DROP DATABASE/TABLE operations

    When the corresponding database or table is deleted, the job will automatically CANCEL.

4. The relationship between the kafka type routine load job and kafka topic

    When the user creates a routine load declaration, the `kafka_topic` does not exist in the kafka cluster.

    * If the broker of the user kafka cluster has `auto.create.topics.enable = true` set, `kafka_topic` will be automatically created first, and the number of partitions created automatically will be in the kafka cluster** of the user side. The broker is configured with `num.partitions`. The routine job will continue to read the data of the topic continuously.
    * If the broker of the user kafka cluster has `auto.create.topics.enable = false` set, topic will not be created automatically, and the routine will be paused before any data is read, with the status `PAUSED`.

    So, if the user wants to be automatically created by the routine when the kafka topic does not exist, just set the broker in the kafka cluster** of the user's side to set auto.create.topics.enable = true` .
    
5. Problems that may occur in the some environment
     In some environments, there are isolation measures for network segment and domain name resolution. So should pay attention to:
        1. The broker list specified in the routine load task must be accessible on the doris environment. 
        2. If `advertised.listeners` is configured in kafka, The addresses in `advertised.listeners` need to be accessible on the doris environment.

6. About specified Partition and Offset

    Doris supports specifying Partition and Offset to start consumption. The new version also supports the consumption function at a specified time point. The configuration relationship of the corresponding parameters is explained here.
    
    There are three relevant parameters:
    
    * `kafka_partitions`: Specify the list of partitions to be consumed, such as: "0, 1, 2, 3".
    * `kafka_offsets`: Specify the starting offset of each partition, which must correspond to the number of `kafka_partitions` lists. Such as: "1000, 1000, 2000, 2000"
    * `property.kafka_default_offset`: Specify the default starting offset of the partition.

    When creating an routine load job, these three parameters can have the following combinations:
    
    | Combinations | `kafka_partitions` | `kafka_offsets` | `property.kafka_default_offset` | Behavior |
    |---|---|---|---|---|
    |1| No | No | No | The system will automatically find all the partitions corresponding to the topic and start consumption from OFFSET_END |
    |2| No | No | Yes | The system will automatically find all the partitions corresponding to the topic and start consumption from the position specified by the default offset |
    |3| Yes | No | No | The system will start consumption from the OFFSET_END of the specified partition |
    |4| Yes | Yes | No | The system will start consumption from the specified offset of the specified partition |
    |5| Yes | No | Yes | The system will start consumption from the specified partition and the location specified by the default offset |
   
 7. The difference between STOP and PAUSE

    the FE will automatically clean up stopped ROUTINE LOAD，while paused ROUTINE LOAD can be resumed

## Related parameters

Some system configuration parameters can affect the use of routine loads.

1. max\_routine\_load\_task\_concurrent\_num

    The FE configuration item, which defaults to 5, can be modified at runtime. This parameter limits the maximum number of subtask concurrency for a routine load job. It is recommended to maintain the default value. If the setting is too large, it may cause too many concurrent tasks and occupy cluster resources.

2. max\_routine_load\_task\_num\_per\_be

    The FE configuration item, which defaults to 5, can be modified at runtime. This parameter limits the number of subtasks that can be executed concurrently by each BE node. It is recommended to maintain the default value. If the setting is too large, it may cause too many concurrent tasks and occupy cluster resources.

3. max\_routine\_load\_job\_num

    The FE configuration item, which defaults to 100, can be modified at runtime. This parameter limits the total number of routine load jobs, including NEED_SCHEDULED, RUNNING, PAUSE. After the overtime, you cannot submit a new assignment.

4. max\_consumer\_num\_per\_group

    BE configuration item, the default is 3. This parameter indicates that up to several consumers are generated in a subtask for data consumption. For a Kafka data source, a consumer may consume one or more kafka partitions. Suppose a task needs to consume 6 kafka partitions, it will generate 3 consumers, and each consumer consumes 2 partitions. If there are only 2 partitions, only 2 consumers will be generated, and each consumer will consume 1 partition.

5. push\_write\_mbytes\_per\_sec

    BE configuration item. The default is 10, which is 10MB/s. This parameter is to load common parameters, not limited to routine load jobs. This parameter limits the speed at which loaded data is written to disk. For high-performance storage devices such as SSDs, this speed limit can be appropriately increased.

6. max\_tolerable\_backend\_down\_num
    FE configuration item, the default is 0. Under certain conditions, Doris can reschedule PAUSED tasks, that becomes RUNNING?This parameter is 0, which means that rescheduling is allowed only when all BE nodes are in alive state.

7. period\_of\_auto\_resume\_min
    FE configuration item, the default is 5 mins. Doris reschedules will only try at most 3 times in the 5 minute period. If all 3 times fail, the current task will be locked, and auto-scheduling will not be performed. However, manual intervention can be performed.
