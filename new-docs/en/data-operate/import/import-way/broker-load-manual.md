---
{
    "title": "Broker Load",
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

# Broker Load

Broker load is an asynchronous import method, and the data source supported depends on the data source supported by the Broker process.

Users need to create Broker load imports through MySQL protocol and check the import results by viewing the import commands.

## Applicable scenarios

* Source data in Broker accessible storage systems, such as HDFS.
* Data volumes range from tens to hundreds of GB.

## Noun Interpretation

1. Frontend (FE): Metadata and scheduling nodes of Doris system. In the import process, it is mainly responsible for the generation of import plan and the scheduling of import tasks.
2. Backend (BE): The computing and storage nodes of Doris system. In the import process, it is mainly responsible for ETL and storage of data.
3. Broker: Broker is an independent stateless process. It encapsulates the file system interface and provides Doris with the ability to read files in the remote storage system.
4. Plan: Import the execution plan, and BE executes the import execution plan to import data into Doris system.

## Basic Principles

After the user submits the import task, FE generates the corresponding plan and distributes the plan to several BEs according to the number of BEs and the size of the file. Each BE performs part of the import data.

BE pulls data from Broker and imports it into the system after transforming the data. All BEs complete the import, and the FE decides whether the import is successful or not.

```
                 +
                 | 1. user create broker load
                 v
            +----+----+
            |         |
            |   FE    |
            |         |
            +----+----+
                 |
                 | 2. BE etl and load the data
    +--------------------------+
    |            |             |
+---v---+     +--v----+    +---v---+
|       |     |       |    |       |
|  BE   |     |  BE   |    |   BE  |
|       |     |       |    |       |
+---+-^-+     +---+-^-+    +--+-^--+
    | |           | |         | |
    | |           | |         | | 3. pull data from broker
+---v-+-+     +---v-+-+    +--v-+--+
|       |     |       |    |       |
|Broker |     |Broker |    |Broker |
|       |     |       |    |       |
+---+-^-+     +---+-^-+    +---+-^-+
    | |           | |          | |
+---v-+-----------v-+----------v-+-+
|       HDFS/BOS/AFS cluster       |
|                                  |
+----------------------------------+

```

## Basic operations

### Create a load

Broker load create a data load job

Grammar:

```
LOAD LABEL db_name.label_name 
(data_desc, ...)
WITH BROKER broker_name broker_properties
[PROPERTIES (key1=value1, ... )]

* data_desc:

    DATA INFILE ('file_path', ...)
    [NEGATIVE]
    INTO TABLE tbl_name
    [PARTITION (p1, p2)]
    [COLUMNS TERMINATED BY separator ]
    [(col1, ...)]
    [PRECEDING FILTER predicate]
    [SET (k1=f1(xx), k2=f2(xx))]
    [WHERE predicate]

* broker_properties: 

    (key1=value1, ...)
```
Examples:

```
LOAD LABEL db1.label1
(
    DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file1")
    INTO TABLE tbl1
    COLUMNS TERMINATED BY ","
    (tmp_c1,tmp_c2)
    SET
    (
        id=tmp_c2,
        name=tmp_c1)
    ),
    DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/file2")
    INTO TABLE tbl2
    COLUMNS TERMINATED BY ","
    (col1, col2)
    where col1 > 1
)
WITH BROKER 'broker'
(
    "username"="user",
    "password"="pass"
)
PROPERTIES
(
    "timeout" = "3600"
);

```

Create the imported detailed grammar execution ``HELP BROKER LOAD `` View grammar help. This paper mainly introduces the parametric meaning and points for attention in Broker load's creation import grammar.

#### Label

Identity of import task. Each import task has a unique Label within a single database. Label is a user-defined name in the import command. With this Label, users can view the execution of the corresponding import task.

Another function of Label is to prevent users from repeatedly importing the same data. **It is strongly recommended that users use the same label for the same batch of data. Thus, repeated requests for the same batch of data can only be accepted once, guaranteeing at-Most-One semantics**

When the corresponding import job status of Label is CANCELLED, it can be used again to submit the import job.

#### Data Description Class Parameters

Data description class parameters mainly refer to the parameters belonging to ``data_desc`` in Broker load creating import statements. Each group of ```data_desc``` mainly describes the data source address, ETL function, target table and partition information involved in this import.

The following is a detailed explanation of some parameters of the data description class:

+ Multi-table import

	Broker load supports a single import task involving multiple tables, and each Broker load import task can implement multiple tables import by declaring multiple tables in multiple ``data_desc``. Each individual ```data_desc``` can also specify the data source address belonging to the table. Broker load guarantees atomic success or failure between multiple tables imported at a single time.

+ negative

	```data_desc``` can also set up data fetching and anti-importing. This function is mainly used when aggregated columns in data tables are of SUM type. If you want to revoke a batch of imported data. The `negative` parameter can be used as a batch of data. Doris automatically retrieves this batch of data on aggregated columns to eliminate the same batch of data.

+ partition

	In `data_desc`, you can specify the partition information of the table to be imported, but it will not be imported if the data to be imported does not belong to the specified partition. At the same time, data that does not specify a Partition is considered error data.

+ preceding filter predicate

    Used to filter original data. The original data is the data without column mapping and transformation. The user can filter the data before conversion, select the desired data, and then perform the conversion.

+ where predicate

        The where statement in ```data_desc``` is responsible for filtering the data that has been transformed. The unselected rows which is filtered by where predicate will not be calculated in ```max_filter_ratio``` . If there are more than one where predicate of the same table , the multi where predicate will be merged from different ```data_desc``` and the policy is AND.

+ merge\_type
     The type of data merging supports three types: APPEND, DELETE, and MERGE. APPEND is the default value, which means that all this batch of data needs to be appended to the existing data. DELETE means to delete all rows with the same key as this batch of data. MERGE semantics Need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics


#### Import job parameters

Import job parameters mainly refer to the parameters in Broker load creating import statement that belong to ``opt_properties``. Import operation parameters act on the whole import operation.

The following is a detailed explanation of some parameters of the import operation parameters:

+ time out

	The time-out of the import job (in seconds) allows the user to set the time-out of each import by himself in ``opt_properties``. If the import task is not completed within the set timeout time, it will be cancelled by the system and become CANCELLED. The default import timeout for Broker load is 4 hours.

	Usually, the user does not need to manually set the timeout of the import task. When the import cannot be completed within the default timeout time, the task timeout can be set manually.

	> Recommended timeout
	>
	> Total File Size (MB) / Slowest Import Speed (MB/s) > timeout 	>((MB) * Number of tables to be imported and related Roll up tables) / (10 * Number of concurrent imports)

	> The concurrency of imports can be seen in the final configuration of the import system in the document. The current import speed limit is 10MB/s in 10 of the formulas.

	> For example, a 1G data to be imported contains three Rollup tables, and the current concurrency of imports is 3. The minimum value of timeout is ```(1 * 1024 * 3) / (10 * 3) = 102 seconds.```

	Because the machine environment of each Doris cluster is different and the concurrent query tasks of the cluster are different, the slowest import speed of the user Doris cluster requires the user to guess the import task speed according to the history.

+ max\_filter\_ratio

	The maximum tolerance rate of the import task is 0 by default, and the range of values is 0-1. When the import error rate exceeds this value, the import fails.

	If the user wishes to ignore the wrong row, the import can be successful by setting this parameter greater than 0.

	The calculation formula is as follows:

	``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) ) > max_filter_ratio ```

	``` dpp.abnorm.ALL``` denotes the number of rows whose data quality is not up to standard. Such as type mismatch, column mismatch, length mismatch and so on.

	``` dpp.norm.ALL ``` refers to the number of correct data in the import process. The correct amount of data for the import task can be queried by the ``SHOW LOAD`` command.

	The number of rows in the original file = `dpp.abnorm.ALL + dpp.norm.ALL`

* exec\_mem\_limit

	Memory limit. Default is 2GB. Unit is Bytes.

+ strict\_mode

	Broker load can use `strict mode`. Use ```properties ("strict_mode" = "true")```  to enable `strict mode`, default is false

	The strict mode means that the column type conversion in the import process is strictly filtered. The strategy of strict filtering is as follows:

	1. For column type conversion, if strict mode is true, the wrong data will be filtered. Error data here refers to the kind of data that the original data is not null and the result is null after participating in column type conversion.

	2. Strict mode does not affect the imported column when it is generated by a function transformation.

	3. For a column type imported that contains scope restrictions, strict mode does not affect it if the original data can normally pass type conversion, but cannot pass scope restrictions. For example, if the type is decimal (1,0) and the original data is 10, it falls within the scope of type conversion but not column declaration. This data strict has no effect on it.

#### Import Relation between strict mode source data

Here's an example of a column type TinyInt

> Note: When columns in a table allow null values to be imported

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|---------|
|null        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa or 2000         | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1                   | 1               | true or false      | correct data|

Here's an example of column type Decimal (1,0)

> Note: When columns in a table allow null values to be imported

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|--------|
|null        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa                 | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1 or 10             | 1               | true or false      | correct data|

> Note: Although 10 is a value beyond the range, strict mode does not affect it because its type meets the requirements of decimal. 10 will eventually be filtered in other ETL processes. But it will not be filtered by strict mode.

### View load

Broker load import mode is asynchronous, so the user must create the imported Label record and use Label in the **view Import command to view the import result**. View import commands are common in all import modes. The specific syntax can be `HELP SHOW LOAD`.

Examples:

```
mysql> show load order by createtime desc limit 1\G
*************************** 1. row ***************************
         JobId: 76391
         Label: label1
         State: FINISHED
      Progress: ETL:N/A; LOAD:100%
          Type: BROKER
       EtlInfo: dpp.abnorm.ALL=15; dpp.norm.ALL=28133376
      TaskInfo: cluster:N/A; timeout(s):10800; max_filter_ratio:5.0E-5
      ErrorMsg: N/A
    CreateTime: 2019-07-27 11:46:42
  EtlStartTime: 2019-07-27 11:46:44
 EtlFinishTime: 2019-07-27 11:46:44
 LoadStartTime: 2019-07-27 11:46:44
LoadFinishTime: 2019-07-27 11:50:16
           URL: http://192.168.1.1:8040/api/_load_error_log?file=__shard_4/error_log_insert_stmt_4bb00753932c491a-a6da6e2725415317_4bb00753932c491a_a6da6e2725415317
    JobDetails: {"Unfinished backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"ScannedRows":2390016,"TaskNumber":1,"All backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"FileNumber":1,"FileSize":1073741824}
```

The following is mainly about the significance of viewing the parameters in the return result set of the import command:

+ JobId

	The unique ID of the import task is different for each import task, which is automatically generated by the system. Unlike Label, JobId will never be the same, while Label can be reused after the import task fails.

+ Label

	Identity of import task.

+ State

	Import the current phase of the task. In the Broker load import process, PENDING and LOADING are the two main import states. If the Broker load is in the PENDING state, it indicates that the current import task is waiting to be executed; the LOADING state indicates that it is executing.

	There are two final stages of the import task: CANCELLED and FINISHED. When Load job is in these two stages, the import is completed. CANCELLED is the import failure, FINISHED is the import success.

+ Progress

	Import the progress description of the task. There are two kinds of progress: ETL and LOAD, which correspond to the two stages of the import process, ETL and LOADING. At present, Broker load only has the LOADING stage, so ETL will always be displayed as `N/A`.

	The progress range of LOAD is 0-100%.

	``` LOAD Progress = Number of tables currently completed / Number of tables designed for this import task * 100%```

	**If all import tables complete the import, then the progress of LOAD is 99%** import enters the final effective stage, and the progress of LOAD will only be changed to 100% after the entire import is completed.

	Import progress is not linear. So if there is no change in progress over a period of time, it does not mean that the import is not being implemented.

+ Type

	Types of import tasks. The type value of Broker load is only BROKER.
+ Etlinfo

	It mainly shows the imported data quantity indicators `unselected.rows`, `dpp.norm.ALL` and `dpp.abnorm.ALL`. The first value shows the rows which has been filtered by where predicate. Users can verify that the error rate of the current import task exceeds max\_filter\_ratio based on these two indicators.

+ TaskInfo

	It mainly shows the current import task parameters, that is, the user-specified import task parameters when creating the Broker load import task, including `cluster`, `timeout`, and `max_filter_ratio`.

+ ErrorMsg

	When the import task status is CANCELLED, the reason for the failure is displayed in two parts: type and msg. If the import task succeeds, the `N/A` is displayed.

	The value meaning of type:

    ```
    USER_CANCEL: User Canceled Tasks
    ETL_RUN_FAIL: Import tasks that failed in the ETL phase
    ETL_QUALITY_UNSATISFIED: Data quality is not up to standard, that is, the error rate exceedsmax_filter_ratio
    LOAD_RUN_FAIL: Import tasks that failed in the LOADING phase
    TIMEOUT: Import task not completed in overtime
    UNKNOWN: Unknown import error
    ```

+ CreateTime /EtlStartTime /EtlFinishTime /LoadStartTime /LoadFinishTime

	These values represent the creation time of the import, the beginning time of the ETL phase, the completion time of the ETL phase, the beginning time of the Loading phase and the completion time of the entire import task, respectively.

	Broker load import has no ETL stage, so its EtlStartTime, EtlFinishTime, LoadStartTime are set to the same value.

	Import tasks stay in CreateTime for a long time, while LoadStartTime is N/A, which indicates that import tasks are heavily stacked at present. Users can reduce the frequency of import submissions.

    ```
    LoadFinishTime - CreateTime = Time consumed by the entire import task
    LoadFinishTime - LoadStartTime = The entire Broker load import task execution time = the time consumed by the entire import task - the time the import task waits
    ```

+ URL

	The error data sample of the import task can be obtained by accessing the URL address. When there is no error data in this import, the URL field is N/A.

+ JobDetails

    Display some details of the running status of the job. Including file number, total file size(Bytes), num of sub tasks, scanned rows, related backend ids and unfinished backend ids.

    ```
    {"Unfinished backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"ScannedRows":2390016,"TaskNumber":1,"All backends":{"9c3441027ff948a0-8287923329a2b6a7":[10002]},"FileNumber":1,"FileSize":1073741824}
    ```

    This info will be updated every 5 seconds. the ScannedRows only for displaying the job progress, not indicate the real numbers.

### Cancel load

When the Broker load job status is not CANCELLED or FINISHED, it can be manually cancelled by the user. When canceling, you need to specify a Label for the import task to be cancelled. Canceling Import command syntax can perform `HELP CANCEL LOAD` view.

## Relevant System Configuration

### FE configuration

The following configurations belong to the Broker load system-level configuration, which acts on all Broker load import tasks. Configuration values are adjusted mainly by modifying `fe.conf`.

+ min\_bytes\_per\_broker\_scanner/max\_bytes\_per\_broker\_scanner/max\_broker\_concurrency

	The first two configurations limit the minimum and maximum amount of data processed by a single BE. The third configuration limits the maximum number of concurrent imports for a job. The minimum amount of data processed, the maximum number of concurrency, the size of source files and the number of BEs in the current cluster **together determine the concurrency of this import**.

	```
	The number of concurrent imports = Math. min (source file size / minimum throughput, maximum concurrency, current number of BE nodes)
	Processing capacity of this import of a single BE = source file size / concurrency of this import
	```

	Usually the maximum amount of data supported by an import job is `max_bytes_per_broker_scanner * number of BE nodes`. If you need to import a larger amount of data, you need to adjust the size of the `max_bytes_per_broker_scanner` parameter appropriately.

Default configuration:

	```
	Parameter name: min_bytes_per_broker_scanner, default 64MB, unit bytes.
	Parameter name: max_broker_concurrency, default 10.
	Parameter name: max_bytes_per_broker_scanner, default 3G, unit bytes.
	```

## Best Practices

### Application scenarios

The most appropriate scenario to use Broker load is the scenario of raw data in a file system (HDFS, BOS, AFS). Secondly, since Broker load is the only way of asynchronous import in a single import, users can also consider using Broker load if they need to use asynchronous access in importing large files.

### Data volume

We will only discuss the case of a single BE. If the user cluster has more than one BE, the amount of data in the heading below should be multiplied by the number of BEs. For example, if the user has three BEs, then the number below 3G (including) should be multiplied by 3, that is, under 9G (including).

+ Below 3G (including)

	Users can submit Broker load to create import requests directly.

+ Over 3G

	Since the maximum processing capacity of a single imported BE is 3G, the imported files over 3G need to be imported by adjusting the import parameters of Broker load to achieve the import of large files.

	1. Modify the maximum number of scans and concurrency of a single BE according to the current number of BEs and the size of the original file.

		```
		Modify the configuration in fe.conf
		
		max_broker_concurrency = BE number
		The amount of data processed by a single BE for the current import task = the original file size / max_broker_concurrency
		Max_bytes_per_broker_scanner >= the amount of data processed by a single BE of the current import task
		
		For example, a 100G file with 10 BEs in the cluster
		max_broker_concurrency = 10
		Max================
		
		```

		After modification, all BEs process import tasks concurrently, and each BE processes part of the original file.

		*Note: The configurations in both FEs are system configurations, that is to say, their modifications work on all Broker load tasks.*

	2. Customize the timeout time of the current import task when creating the import

		```
		Current import task single BE processing data volume / user Doris cluster slowest import speed (MB/s) >= current import task timeout time >= current import task single BE processing data volume / 10M/s
		
		For example, a 100G file with 10 BEs in the cluster
		Timeout > 1000s = 10G / 10M /s
		
		```

	3. When the user finds that the timeout time calculated in the second step exceeds the default maximum time-out time for importing the system by 4 hours.

		At this time, it is not recommended that users directly increase the maximum time-out to solve the problem. If the single import time exceeds the default maximum import timeout of 4 hours, it is better to solve the problem by splitting the file to be imported and importing it several times. The main reason is that if a single import exceeds 4 hours, the time cost of retry after import failure is very high.

		The maximum amount of imported file data expected by the Doris cluster can be calculated by the following formula:

		```
		Expected maximum imported file data = 14400s * 10M / s * BE number
		For example, the BE number of clusters is 10.
		Expected maximum imported file data volume = 14400 * 10M / s * 10 = 1440000M 1440G
		
		Note: The average user's environment may not reach the speed of 10M/s, so it is recommended that more than 500G files be split and imported.
		
		```
		
### Job Scheduling

The system limits the number of Broker Load jobs running in a cluster to prevent too many Load jobs from running at the same time.

First, the configuration parameter of FE: `desired_max_waiting_jobs` will limit the number of Broker Load jobs that are pending or running (the job status is PENDING or LOADING) in a cluster. The default is 100. If this threshold is exceeded, the newly submitted job will be rejected directly.

A Broker Load job will be divided into pending task and loading task phases. Among them, the pending task is responsible for obtaining the information of the imported file, and the loading task will be sent to BE to perform specific import tasks.

The configuration parameter `async_pending_load_task_pool_size` of FE is used to limit the number of pending tasks running at the same time. It is also equivalent to controlling the number of import tasks that are actually running. This parameter defaults to 10. In other words, assuming that the user submits 100 Load jobs, only 10 jobs will enter the LOADING state and start execution, while other jobs are in the PENDING waiting state.

The FE configuration parameter `async_loading_load_task_pool_size` is used to limit the number of loading tasks that run at the same time. A Broker Load job will have 1 pending task and multiple loading tasks (equal to the number of DATA INFILE clauses in the LOAD statement). So `async_loading_load_task_pool_size` should be greater than or equal to `async_pending_load_task_pool_size`.

Because the work of pending tasks is relatively lightweight (for example, just accessing hdfs to obtain file information), `async_pending_load_task_pool_size` does not need to be large, and the default 10 is usually sufficient. And `async_loading_load_task_pool_size` is really used to limit the import tasks that can be run at the same time. It can be adjusted appropriately according to the cluster size.

### Performance analysis

You can execute `set enable_profile=true` to open the load job profile before submitting the import job. After the import job is completed, you can view the profile of the import job in the `Queris` tab of the FE web page.

This profile can help analyze the running status of the import job.

Currently, the profile can only be viewed after the job is successfully executed.

### Complete examples

Data situation: User data in HDFS, file address is hdfs://abc.com:8888/store_sales, HDFS authentication user name is root, password is password, data size is about 30G, hope to import into database bj_sales table store_sales.

Cluster situation: The number of BEs in the cluster is about 3, and the Broker name is broker.

+ Step 1: After the calculation of the above method, the single BE import quantity is 10G, then the configuration of FE needs to be modified first, and the maximum amount of single BE import is changed to:

	```
	max_bytes_per_broker_scanner = 10737418240
	
	```

+ Step 2: Calculated, the import time is about 1000s, which does not exceed the default timeout time. No custom timeout time for import can be configured.

+ Step 3: Create import statements

    ```
    LOAD LABEL bj_sales.store_sales_broker_load_01
    (
        DATA INFILE("hdfs://abc.com:8888/store_sales")
        INTO TABLE store_sales
    )
    WITH BROKER 'broker'
    ("username"="root", "password"="password");
    ```

## Common Questions

* failed with: `Scan bytes per broker scanner exceed limit:xxx`

	Refer to the Best Practices section of the document to modify the FE configuration items `max_bytes_per_broker_scanner` and `max_broker_concurrency'.`

*  failed with: `failed to send batch` or `TabletWriter add batch with unknown id`

	Refer to **General System Configuration** in **BE Configuration** in the Import Manual (./load-manual.md), and modify `query_timeout` and `streaming_load_rpc_max_alive_time_sec` appropriately.
	
*  failed with: `LOAD_RUN_FAIL; msg: Invalid Column Name: xxx`
    
     If it is PARQUET or ORC format data, you need to keep the column names in the file header consistent with the column names in the doris table, such as:
     `` `
     (tmp_c1, tmp_c2)
     SET
     (
         id = tmp_c2,
         name = tmp_c1
     )
     `` `
     Represents getting the column with (tmp_c1, tmp_c2) as the column name in parquet or orc, which is mapped to the (id, name) column in the doris table. If set is not set, the column names in the column are used as the mapping relationship.

     Note: If the orc file directly generated by some hive versions is used, the table header in the orc file is not the column name in the hive meta, but (_col0, _col1, _col2, ...), which may cause the Invalid Column Name error, then You need to use set for mapping.
