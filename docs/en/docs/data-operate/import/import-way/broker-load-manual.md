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

Broker load is an asynchronous import method, and the supported data sources depend on the data sources supported by the [Broker](../../../advanced/broker.md) process.

Because the data in the Doris table is ordered, Broker load uses the doris cluster resources to sort the data when importing data. Complete massive historical data migration relative to Spark load, the Doris cluster resource usage is relatively large. , this method is used when the user does not have Spark computing resources. If there are Spark computing resources, it is recommended to use [Spark load](../../../data-operate/import/import-way/spark-load-manual.md).

Users need to create [Broker load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) import through MySQL protocol and import by viewing command to check the import result.

## Applicable scene

* The source data is in a storage system that the broker can access, such as HDFS.
* The amount of data is at the level of tens to hundreds of GB.

## Fundamental

After the user submits the import task, FE will generate the corresponding Plan and distribute the Plan to multiple BEs for execution according to the current number of BEs and file size, and each BE executes a part of the imported data.

BE pulls data from the broker during execution, and imports the data into the system after transforming the data. All BEs are imported, and FE ultimately decides whether the import is successful.

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

## start import

Let's look at [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) through several actual scenario examples. use

### Data import of Hive partition table

1. Create Hive table

```sql
##Data format is: default, partition field is: day
CREATE TABLE `ods_demo_detail`(
  `id` string,
  `store_id` string,
  `company_id` string,
  `tower_id` string,
  `commodity_id` string,
  `commodity_name` string,
  `commodity_price` double,
  `member_price` double,
  `cost_price` double,
  `unit` string,
  `quantity` double,
  `actual_price` double
)
PARTITIONED BY (day string)
row format delimited fields terminated by ','
lines terminated by '\n'
````

Then use Hive's Load command to import your data into the Hive table

````
load data local inpath '/opt/custorm' into table ods_demo_detail;
````

2. Create a Doris table, refer to the specific table syntax: [CREATE TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)

````
CREATE TABLE `doris_ods_test_detail` (
  `rq` date NULL,
  `id` varchar(32) NOT NULL,
  `store_id` varchar(32) NULL,
  `company_id` varchar(32) NULL,
  `tower_id` varchar(32) NULL,
  `commodity_id` varchar(32) NULL,
  `commodity_name` varchar(500) NULL,
  `commodity_price` decimal(10, 2) NULL,
  `member_price` decimal(10, 2) NULL,
  `cost_price` decimal(10, 2) NULL,
  `unit` varchar(50) NULL,
  `quantity` int(11) NULL,
  `actual_price` decimal(10, 2) NULL
) ENGINE=OLAP
UNIQUE KEY(`rq`, `id`, `store_id`)
PARTITION BY RANGE(`rq`)
(
PARTITION P_202204 VALUES [('2022-04-01'), ('2022-05-01')))
DISTRIBUTED BY HASH(`store_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.end" = "2",
"dynamic_partition.prefix" = "P_",
"dynamic_partition.buckets" = "1",
"in_memory" = "false",
"storage_format" = "V2"
);
````

3. Start importing data

   Specific syntax reference: [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md)

```sql
LOAD LABEL broker_load_2022_03_23
(
    DATA INFILE("hdfs://192.168.20.123:8020/user/hive/warehouse/ods.db/ods_demo_detail/*/*")
    INTO TABLE doris_ods_test_detail
    COLUMNS TERMINATED BY ","
  (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price)
    COLUMNS FROM PATH AS (`day`)
   SET
   (rq = str_to_date(`day`,'%Y-%m-%d'),id=id,store_id=store_id,company_id=company_id,tower_id=tower_id,commodity_id=commodity_id,commodity_name=commodity_name,commodity_price=commodity_price,member_price =member_price,cost_price=cost_price,unit=unit,quantity=quantity,actual_price=actual_price)
)
WITH BROKER "broker_name_1"
(
"username" = "hdfs",
"password" = ""
)
PROPERTIES
(
    "timeout"="1200",
    "max_filter_ratio"="0.1"
);
````

### Hive partition table import (ORC format)
1. Create Hive partition table, ORC format

```sql
#Data format: ORC partition: day
CREATE TABLE `ods_demo_orc_detail`(
  `id` string,
  `store_id` string,
  `company_id` string,
  `tower_id` string,
  `commodity_id` string,
  `commodity_name` string,
  `commodity_price` double,
  `member_price` double,
  `cost_price` double,
  `unit` string,
  `quantity` double,
  `actual_price` double
)
PARTITIONED BY (day string)
row format delimited fields terminated by ','
lines terminated by '\n'
STORED AS ORC
````

2. Create a Doris table. The table creation statement here is the same as the Doris table creation statement above. Please refer to the above .

3. Import data using Broker Load

   ```sql
   LOAD LABEL dish_2022_03_23
   (
       DATA INFILE("hdfs://10.220.147.151:8020/user/hive/warehouse/ods.db/ods_demo_orc_detail/*/*")
       INTO TABLE doris_ods_test_detail
       COLUMNS TERMINATED BY ","
       FORMAT AS "orc"
   (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price)
       COLUMNS FROM PATH AS (`day`)
      SET
      (rq = str_to_date(`day`,'%Y-%m-%d'),id=id,store_id=store_id,company_id=company_id,tower_id=tower_id,commodity_id=commodity_id,commodity_name=commodity_name,commodity_price=commodity_price,member_price =member_price,cost_price=cost_price,unit=unit,quantity=quantity,actual_price=actual_price)
   )
   WITH BROKER "broker_name_1"
   (
   "username" = "hdfs",
   "password" = ""
   )
   PROPERTIES
   (
       "timeout"="1200",
       "max_filter_ratio"="0.1"
   );
   ````

   **Notice:**

   - `FORMAT AS "orc"` : here we specify the data format to import
   - `SET` : Here we define the field mapping relationship between the Hive table and the Doris table and some operations for field conversion

### HDFS file system data import

Let's continue to take the Doris table created above as an example to demonstrate importing data from HDFS through Broker Load.

The statement to import the job is as follows:

```sql
LOAD LABEL demo.label_20220402
        (
            DATA INFILE("hdfs://10.220.147.151:8020/tmp/test_hdfs.txt")
            INTO TABLE `ods_dish_detail_test`
            COLUMNS TERMINATED BY "\t" (id,store_id,company_id,tower_id,commodity_id,commodity_name,commodity_price,member_price,cost_price,unit,quantity,actual_price)
        )
        with HDFS (
            "fs.defaultFS"="hdfs://10.220.147.151:8020",
            "hadoop.username"="root"
        )
        PROPERTIES
        (
            "timeout"="1200",
            "max_filter_ratio"="0.1"
        );
````

The specific parameters here can refer to: [Broker](../../../advanced/broker.md) and [Broker Load](../../../sql-manual/sql-reference-v2 /Data-Manipulation-Statements/Load/BROKER-LOAD.md) documentation

## View import status

We can view the status information of the above import task through the following command,

The specific syntax reference for viewing the import status [SHOW LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD.md)

```sql
mysql> show load order by createtime desc limit 1\G;
**************************** 1. row ******************** ******
         JobId: 41326624
         Label: broker_load_2022_03_23
         State: FINISHED
      Progress: ETL: 100%; LOAD: 100%
          Type: BROKER
       EtlInfo: unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=27
      TaskInfo: cluster:N/A; timeout(s):1200; max_filter_ratio:0.1
      ErrorMsg: NULL
    CreateTime: 2022-04-01 18:59:06
  EtlStartTime: 2022-04-01 18:59:11
 EtlFinishTime: 2022-04-01 18:59:11
 LoadStartTime: 2022-04-01 18:59:11
LoadFinishTime: 2022-04-01 18:59:11
           URL: NULL
    JobDetails: {"Unfinished backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[]},"ScannedRows":27,"TaskNumber":1,"All backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[36728051]},"FileNumber ":1,"FileSize":5540}
1 row in set (0.01 sec)
````

## Cancel import

When the broker load job status is not CANCELLED or FINISHED, it can be manually canceled by the user. When canceling, you need to specify the Label of the import task to be canceled. Cancel the import command syntax to execute [CANCEL LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CANCEL-LOAD.md) view.

For example: cancel the import job with the label broker_load_2022_03_23 on the database demo

```sql
CANCEL LOAD FROM demo WHERE LABEL = "broker_load_2022_03_23";
````
## Relevant system configuration

### Broker parameters

Broker Load needs to use the Broker process to access remote storage. Different brokers need to provide different parameters. For details, please refer to [Broker documentation](../../../advanced/broker.md).

### FE configuration

The following configurations belong to the system-level configuration of Broker load, that is, the configurations that apply to all Broker load import tasks. The configuration values are adjusted mainly by modifying `fe.conf`.

- min_bytes_per_broker_scanner/max_bytes_per_broker_scanner/max_broker_concurrency

  The first two configurations limit the minimum and maximum amount of data processed by a single BE. The third configuration limits the maximum number of concurrent imports for a job. The minimum amount of data processed, the maximum number of concurrency, the size of the source file and the number of BEs in the current cluster ** together determine the number of concurrent imports**.

  ````text
  The number of concurrent imports this time = Math.min (source file size/minimum processing capacity, maximum concurrent number, current number of BE nodes)
  The processing volume of a single BE imported this time = the size of the source file / the number of concurrent imports this time
  ````

  Usually the maximum amount of data supported by an import job is `max_bytes_per_broker_scanner * number of BE nodes`. If you need to import a larger amount of data, you need to adjust the size of the `max_bytes_per_broker_scanner` parameter appropriately.

  default allocation:

  ````text
  Parameter name: min_bytes_per_broker_scanner, the default is 64MB, the unit is bytes.
  Parameter name: max_broker_concurrency, default 10.
  Parameter name: max_bytes_per_broker_scanner, the default is 500G, the unit is bytes.
  ````

## Best Practices

### Application scenarios

The most suitable scenario for using Broker load is the scenario where the original data is in the file system (HDFS, BOS, AFS). Secondly, since Broker load is the only way of asynchronous import in a single import, if users need to use asynchronous access when importing large files, they can also consider using Broker load.

### The amount of data

Only the case of a single BE is discussed here. If the user cluster has multiple BEs, the amount of data in the title below should be multiplied by the number of BEs. For example: if the user has 3 BEs, the value below 3G (inclusive) should be multiplied by 3, that is, below 9G (inclusive).

- Below 3G (included)

  Users can directly submit Broker load to create import requests.

- Above 3G

  Since the maximum processing capacity of a single import BE is 3G, the import of files exceeding 3G needs to be adjusted by adjusting the import parameters of Broker load to realize the import of large files.

  1. Modify the maximum scan amount and maximum concurrent number of a single BE according to the current number of BEs and the size of the original file.

     ````text
     Modify the configuration in fe.conf
     max_broker_concurrency = number of BEs
     The amount of data processed by a single BE of the current import task = original file size / max_broker_concurrency
     max_bytes_per_broker_scanner >= the amount of data processed by a single BE of the current import task
     
     For example, for a 100G file, the number of BEs in the cluster is 10
     max_broker_concurrency = 10
     # >= 10G = 100G / 10
     max_bytes_per_broker_scanner = 1069547520
     ````

     After modification, all BEs will process the import task concurrently, each BE processing part of the original file.

     *Note: The configurations in the above two FEs are all system configurations, that is to say, their modifications are applied to all Broker load tasks. *

  2. Customize the timeout time of the current import task when creating an import

     ````text
     The amount of data processed by a single BE of the current import task / the slowest import speed of the user Doris cluster (MB/s) >= the timeout time of the current import task >= the amount of data processed by a single BE of the current import task / 10M/s
     
     For example, for a 100G file, the number of BEs in the cluster is 10
     # >= 1000s = 10G / 10M/s
     timeout = 1000
     ````

  3. When the user finds that the timeout time calculated in the second step exceeds the default import timeout time of 4 hours

     At this time, it is not recommended for users to directly increase the maximum import timeout to solve the problem. If the single import time exceeds the default import maximum timeout time of 4 hours, it is best to divide the files to be imported and import them in multiple times to solve the problem. The main reason is: if a single import exceeds 4 hours, the time cost of retrying after the import fails is very high.

     The expected maximum import file data volume of the Doris cluster can be calculated by the following formula:

     ````text
     Expected maximum import file data volume = 14400s * 10M/s * number of BEs
     For example: the number of BEs in the cluster is 10
     Expected maximum import file data volume = 14400s * 10M/s * 10 = 1440000M ≈ 1440G
     
     Note: The average user's environment may not reach the speed of 10M/s, so it is recommended that files over 500G be divided and imported.
     ````

### Job scheduling

The system limits the number of running Broker Load jobs in a cluster to prevent too many Load jobs from running at the same time.

First, the configuration parameter of FE: `desired_max_waiting_jobs` will limit the number of Broker Load jobs that have not started or are running (job status is PENDING or LOADING) in a cluster. Default is 100. If this threshold is exceeded, newly submitted jobs will be rejected outright.

A Broker Load job is divided into pending task and loading task phases. Among them, the pending task is responsible for obtaining the information of the imported file, and the loading task will be sent to the BE to execute the specific import task.

The FE configuration parameter `async_pending_load_task_pool_size` is used to limit the number of pending tasks running at the same time. It is also equivalent to controlling the number of import tasks that are actually running. This parameter defaults to 10. That is to say, assuming that the user submits 100 Load jobs, at the same time only 10 jobs will enter the LOADING state and start execution, while other jobs are in the PENDING waiting state.

The configuration parameter `async_loading_load_task_pool_size` of FE is used to limit the number of tasks of loading tasks running at the same time. A Broker Load job will have one pending task and multiple loading tasks (equal to the number of DATA INFILE clauses in the LOAD statement). So `async_loading_load_task_pool_size` should be greater than or equal to `async_pending_load_task_pool_size`.

### Performance Analysis

Session variables can be enabled by executing `set enable_profile=true` before submitting the LOAD job. Then submit the import job. After the import job is completed, you can view the profile of the import job in the `Queris` tab of the FE web page.

You can check the [SHOW LOAD PROFILE](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD-PROFILE.md) help document for more usage help information.

This Profile can help analyze the running status of import jobs.

Currently the Profile can only be viewed after the job has been successfully executed

## common problem

- Import error: `Scan bytes per broker scanner exceed limit:xxx`

  Please refer to the Best Practices section in the document to modify the FE configuration items `max_bytes_per_broker_scanner` and `max_broker_concurrency`

- Import error: `failed to send batch` or `TabletWriter add batch with unknown id`

  Modify `query_timeout` and `streaming_load_rpc_max_alive_time_sec` appropriately.

  streaming_load_rpc_max_alive_time_sec:

  During the import process, Doris will open a Writer for each Tablet to receive data and write. This parameter specifies the Writer's wait timeout. If the Writer does not receive any data within this time, the Writer will be automatically destroyed. When the system processing speed is slow, the Writer may not receive the next batch of data for a long time, resulting in an import error: `TabletWriter add batch with unknown id`. At this time, this configuration can be appropriately increased. Default is 600 seconds

- Import error: `LOAD_RUN_FAIL; msg:Invalid Column Name:xxx`

  If it is data in PARQUET or ORC format, the column name of the file header needs to be consistent with the column name in the doris table, such as:

  ````text
  (tmp_c1,tmp_c2)
  SET
  (
      id=tmp_c2,
      name=tmp_c1
  )
  ````

  Represents getting the column with (tmp_c1, tmp_c2) as the column name in parquet or orc, which is mapped to the (id, name) column in the doris table. If set is not set, the column in column is used as the map.

  Note: If you use the orc file directly generated by some hive versions, the header in the orc file is not hive meta data, but (_col0, _col1, _col2, ...), which may cause Invalid Column Name error, then you need to use set to map

- Import error: `Failed to get S3 FileSystem for bucket is null/empty`
  1. The bucket is incorrect or does not exist.
  2. The bucket format is not supported. When creating a bucket name with `_` on GCS, like `s3://gs_bucket/load_tbl`, the S3 Client will report an error. It is recommended not to use `_` on GCS.

## more help

For more detailed syntax and best practices used by Broker Load, see [Broker Load](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md) command manual, you can also enter `HELP BROKER LOAD` in the MySql client command line for more help information.
