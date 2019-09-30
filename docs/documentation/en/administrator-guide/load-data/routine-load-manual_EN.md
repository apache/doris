# Routine Load

Routine Load provides users with a function to automatically import data from a specified data source.

This document mainly introduces the realization principle, usage and best practices of this function.

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.
* Routine LoadJob: A routine import job submitted by the user.
* Job Scheduler: Import job scheduler routinely for scheduling and splitting a Routine LoadJob into multiple Tasks.
* Task：RoutineLoadJob 被 JobScheduler 根据规则拆分的子任务。
* Task Scheduler: Task Scheduler. It is used to schedule the execution of Task.

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

As shown above, Client submits a routine import job to FE.

FE splits an import job into several Tasks through Job Scheduler. Each Task is responsible for importing a specified portion of the data. Task is assigned by Task Scheduler to execute on the specified BE.

On BE, a Task is considered a common import task, which is imported through the import mechanism of Stream Load. After importing, report to FE.

Job Scheduler in FE continues to generate subsequent new Tasks based on the results reported, or retries failed Tasks.

The whole routine import job completes the data uninterrupted import by continuously generating new Tasks.

## Kafka routine load

Currently we only support routine imports from Kafka systems. This section details Kafka's routine introduction usage and best practices.

### Use restrictions

1. Supporting unauthenticated Kafka access and the Kafka cluster authenticated by SSL.
2. The supported message format is CSV text format. Each message is a line, and line end **does not contain** newline characters.
3. Only Kafka version 0.10.0.0 (including) is supported.

### Create routine import tasks

After creating a detailed grammar for routine import tasks, you can connect to Doris and execute `HELP CREATE ROUTINE LOAD'; `Check grammar help. Here is a detailed description of the creation of the job note.

* columns_mapping

	`colum_mapping` is mainly used to specify table structure and column mapping relationships in message, as well as transformation of some columns. If not specified, Doris defaults that columns in message and columns in table structure correspond one by one in order. Although under normal circumstances, if the source data is exactly one-to-one correspondence, it is not specified that normal data import can also be carried out. However, we strongly recommend that users **explicitly specify column mapping relationships**. In this way, when the table structure changes (such as adding a nullable column) or the source file changes (such as adding a column), the import task can continue. Otherwise, when the above changes occur, the import will report an error because the column mapping relationship no longer corresponds one to one.

	In `columns_mapping`, we can also use some built-in functions to convert columns. But you need to pay attention to the actual column types corresponding to the function parameters. Examples are given to illustrate:

	Assume that the user needs to import a table containing only `k1` columns of `int` type. And the null value in the source file needs to be converted to 0. This function can be implemented through the `ifnull` function. The correct way to use it is as follows:

	`COLUMNS (xx, k1=ifnull(xx, "3"))`

	Note that we use `3` instead of `3`, although the type of `k1` is `int`. Because the column types in the source data are `varchar` for the import task, the `xx` virtual column types here are also `varchar`. So we need to use `3` for matching, otherwise `ifnull` function can not find the function signature with parameter `(varchar, int)`, and there will be an error.

	Another example is to assume that the user needs to import a table containing only `k1` columns of `int` type. And the corresponding columns in the source file need to be processed: the negative number is converted to positive, and the positive number is multiplied by 100. This function can be implemented by the `case when` function, which should be correctly written as follows:

    `COLUMNS (xx, case when xx < 0 than cast(-xx as varchar) else cast((xx + '100') as varchar) end)`

	Note that we need to convert all the parameters in `case when'to varchar in order to get the desired results.

* where_predicates

	The column type in `where_predicates` is already the actual column type, so there is no need to force the conversion to varchar type as `columns_mapping'. Write according to the actual column type.

* desired\_concurrent\_number

	`desired_concurrent_number` is used to specify the expected concurrency of a routine job. That is, how many tasks are executed simultaneously for a job at most. For Kafka import, the current actual concurrency calculation is as follows:

    ```
    Min(partition num, desired_concurrent_number, alive_backend_num, Config.max_routine_load_task_concurrrent_num)
    ```

	Where `Config.max_routine_load_task_concurrent_num` is a default maximum concurrency limit for the system. This is a FE configuration, which can be adjusted by reconfiguration. The default is 5.

	The partition num refers to the number of partitions subscribed to Kafka topic. ` alive_backend_num` is the current normal number of BE nodes.

* max\_batch\_interval/max\_batch\_rows/max\_batch\_size

	These three parameters are used to control the execution time of a single task. If any of these thresholds is reached, the task ends. Where `max_batch_rows` is used to record the number of data rows read from Kafka. ` Max_batch_size` is used to record the amount of data read from Kafka in bytes. At present, the consumption rate of a task is about 5-10MB/s.

	Assuming a row of data is 500B, the user wants a task every 100MB or 10 seconds. The expected processing time of 100MB is 10-20 seconds, and the corresponding number of rows is about 200,000 rows. A reasonable configuration is:

    ```
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000",
    "max_batch_size" = "104857600"
    ```

	The parameters in the examples above are also default parameters for these configurations.

* max\_error\_number

	` max_error_number` is used to control the error rate. When the error rate is too high, the job will be automatically suspended. Because the whole job is oriented to data flow, and because of the boundless nature of data flow, we can not calculate the error rate through an error ratio like other import tasks. Therefore, a new computing method is provided to calculate the error ratio in the data stream.

	We set up a sampling window. The size of the window is `max_batch_rows * 10`. In a sampling window, if the number of error lines exceeds `max_error_number`, the job is suspended. If it does not exceed, the next window starts to recalculate the number of erroneous rows.

	Let's assume `max_batch_rows` is 200,000, and the window size is 2,000,000. Let `max_error_number` be 20,000, that is, 20,000 erroneous actions per 2,000,000 lines expected by the user. That is, the error rate is 1%. But because not every batch of tasks consumes 200,000 rows, the actual range of windows is [2000000, 2200000], that is, 10% statistical error.

	Error rows do not include rows filtered through where conditions. But it includes rows that do not have partitions in the corresponding Doris table.

* data\_source\_properties

	` Data_source_properties` can specify consumption specific Kakfa partition. If not specified, all partitions of the topic subscribed are consumed by default.

	Note that when partition is explicitly specified, the import job will no longer dynamically detect changes in Kafka partition. If not specified, the consumption partition will be dynamically adjusted according to the change of Kafka partition.

#### Accessing the Kafka Cluster of SSL Authentication

Accessing the Kafka cluster for SSL authentication requires the user to provide a certificate file (ca.pem) to authenticate the Kafka Broker public key. If the Kafka cluster opens client authentication at the same time, the client's public key (client. pem), key file (client. key), and key password are also required. The required files need to be uploaded to Doris by the `CREAE FILE` command,** and the catalog name is `kafka`**. The specific help of the `CREATE FILE` command can be found in `HELP CREATE FILE;'. An example is given here:

1. Upload files

    ```
    CREATE FILE "ca.pem" PROPERTIES("url" = "https://example_url/kafka-key/ca.pem", "catalog" = "kafka");
    CREATE FILE "client.key" PROPERTIES("url" = "https://example_urlkafka-key/client.key", "catalog" = "kafka");
    CREATE FILE "client.pem" PROPERTIES("url" = "https://example_url/kafka-key/client.pem", "catalog" = "kafka");
    ```

2. Create routine import jobs

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

> Doris accesses the Kafka cluster through Kafka's C++ API `librdkafka`. The parameters supported by `librdkafka` can be consulted
>
> `https://github.com /edenhill /librdkafka /blob /master /CONFIGURATION.md `


### View the status of import jobs

Specific commands and examples for viewing the status of ** job ** can be viewed through `HELP SHOW ROUTINE LOAD;`commands.

Specific commands and examples for viewing the running status of ** tasks ** can be viewed through `HELP SHOW ROUTINE LOAD TASK;`commands.

You can only view tasks that are currently running. Tasks that have ended or not started cannot be viewed.

### Job Control

用户可以通过 `STOP/PAUSE/RESUME` 三个命令来控制作业的停止，暂停和重启。可以通过 `HELP STOP ROUTINE LOAD;`, `HELP PAUSE ROUTINE LOAD;` 以及 `HELP RESUME ROUTINE LOAD;` 三个命令查看帮助和示例。

## Other notes

1. The relationship between routine import operations and ALTER TABLE operations

	* Routine imports do not block SCHEMA CHANGE and ROLLUP operations. But note that if SCHEMA CHANGE is completed, the column mapping relationship does not match, which will lead to a sharp increase in error data for jobs and eventually to job pause. It is recommended that such problems be reduced by explicitly specifying column mapping relationships in routine import operations and by adding Nullable columns or columns with Default values.
	* Deleting the Partition of a table may cause the imported data to fail to find the corresponding Partition and the job to pause.

2. Relations between routine import jobs and other import jobs (LOAD, DELETE, INSERT)

	* Routine imports do not conflict with other LOAD and INSERT operations.
	* When performing a DELETE operation, the corresponding table partition cannot have any import tasks being performed. Therefore, before performing the DELETE operation, it may be necessary to suspend the routine import operation and wait for the task that has been issued to complete before DELETE can be executed.

3. The relationship between routine import jobs and DROP DATABASE/TABLE operations

	When the corresponding database or table is deleted from the routine import, the job will automatically CANCEL.

4. The relationship between Kafka type routine import and Kafka topic

	When the `kafka_topic'of the user creating the routine import declaration does not exist in the Kafka cluster.

	* If the broker of user Kafka cluster sets `auto.create.topics.enable = true`, then `kafka_topic` will be created automatically first. The number of partitions created automatically is determined by the broker configuration `num.partitions` in **user's Kafka cluster**. Routine jobs will read the topic data regularly and continuously.
	* If the broker of the user Kafka cluster sets `auto.create.topics.enable = false`, topic will not be created automatically, and routine jobs will be suspended before any data is read, in the state of `PAUSED`.

	So, if users want to be created automatically by routine jobs when Kafka topic does not exist, they just need to set `auto.create.topics.enable = true` to the broker in **user's Kafka cluster**.

## Relevant parameters

Some system configuration parameters affect the use of routine imports.

1. max\_routine\_load\_task\_concurrent\_num

	FE configuration item, default 5, can be modified at run time. This parameter limits the maximum number of subtasks concurrently imported routinely. It is recommended that the default values be maintained. Setting too large may lead to too many concurrent tasks and occupy cluster resources.

2. max\_consumer\_num\_per\_group

	BE configuration item, default 3. This parameter represents the maximum number of consumers generated in a sub-task for data consumption. For Kafka data sources, a consumer may consume one or more Kafka partitions. Assuming that a task needs to consume six Kafka partitions, three consumers will be generated, and each consumer consumes two partitions. If there are only two partitions, only two consumers will be generated, and each consumer consumes one partition.

3. push\_write\_mbytes\_per\_sec

	BE configuration item. The default is 10, or 10MB/s. This parameter is a general import parameter and is not limited to routine import operations. This parameter limits the speed at which imported data is written to disk. For high performance storage devices such as SSD, this speed limit can be increased appropriately.
