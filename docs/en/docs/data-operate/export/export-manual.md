---
{
    "title": "Export Overview",
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

# Export Overview

 `Export` is a feature provided by Doris that allows for the asynchronous export of data. This feature allows the user to export the data of specified tables or partitions in a specified file format through the Broker process or S3 protocol/ HDFS protocol, to remote storage such as object storage or HDFS.

Currently, `EXPORT` supports exporting Doris local tables / views / external tables and supports exporting to file formats including parquet, orc, csv, csv_with_names, and csv_with_names_and_types.

This document mainly introduces the basic principles, usage, best practices and precautions of Export.

## Principles

After a user submits an `Export Job`, Doris will calculate all the Tablets involved in this job. Then, based on the `parallelism` parameter specified by the user, these tablets will be grouped. Each thread is responsible for a group of tablets, generating multiple `SELECT INTO OUTFILE` query plans. The query plan will read the data from the included tablets and then write the data to the specified path in remote storage through S3 protocol/ HDFS protocol/ Broker.

The overall execution process is as follows:

1. The user submits an Export job to FE.
2. FE calculates all the tablets to be exported and groups them based on the `parallelism` parameter. Each group generates multiple `SELECT INTO OUTFILE` query plans based on the `maximum_number_of_export_partitions` parameter.

3. Based on the parallelism parameter, an equal number of `ExportTaskExecutor` are generated, and each `ExportTaskExecutor` is responsible for a thread, which is scheduled and executed by FE's `Job scheduler` framework.
4. FE's `Job scheduler` schedules and executes the `ExportTaskExecutor`, and each `ExportTaskExecutor` serially executes the multiple `SELECT INTO OUTFILE` query plans it is responsible for.

## Start Export

For detailed usage of Export, please refer to [EXPORT](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/EXPORT.md).

Export's detailed commands can be passed through `HELP EXPORT;` in mysql client. Examples are as follows:

### Export to HDFS

```sql
EXPORT TABLE db1.tbl1 
PARTITION (p1,p2)
[WHERE [expr]]
TO "hdfs://host/path/to/export/" 
PROPERTIES
(
    "label" = "mylabel",
    "column_separator"=",",
    "columns" = "col1,col2",
    "parallelusm" = "3"
)
WITH BROKER "hdfs"
(
    "username" = "user",
    "password" = "passwd"
);
```

* `label`: The identifier of this export job. You can use this identifier to view the job status later.
* `column_separator`: Column separator. The default is `\t`. Supports invisible characters, such as'\x07'.
* `column`: columns to be exported, separated by commas, if this parameter is not filled in, all columns of the table will be exported by default.
* `line_delimiter`: Line separator. The default is `\n`. Supports invisible characters, such as'\x07'.
* `parallelusm`：Exporting with 3 concurrent threads.

### Export to Object Storage (Supports S3 Protocol)

```sql
EXPORT TABLE test TO "s3://bucket/path/to/export/dir/"
WITH S3 (
    "s3.endpoint" = "http://host",
    "s3.access_key" = "AK",
    "s3.secret_key"="SK",
    "s3.region" = "region"
);
```

- `s3.access_key`/`s3.secret_key`：Is your key to access the object storage API.
- `s3.endpoint`：Endpoint indicates the access domain name of object storage external services.
- `s3.region`：Region indicates the region where the object storage data center is located.

### View Export Status

After submitting a job, the job status can be viewed by querying the   [SHOW EXPORT](../../sql-manual/sql-reference/Show-Statements/SHOW-EXPORT.md)  command. The results are as follows:

```sql
mysql> show EXPORT\G;
*************************** 1. row ***************************
     JobId: 14008
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":[],"max_file_size":"","delete_existing_files":"","columns":"","format":"csv","column_separator":"\t","line_delimiter":"\n","db":"default_cluster:demo","tbl":"student4","tablet_num":30}
      Path: hdfs://host/path/to/export/
CreateTime: 2019-06-25 17:08:24
 StartTime: 2019-06-25 17:08:28
FinishTime: 2019-06-25 17:08:34
   Timeout: 3600
  ErrorMsg: NULL
  OutfileInfo: [
  [
    {
      "fileNumber": "1",
      "totalRows": "4",
      "fileSize": "34bytes",
      "url": "file:///127.0.0.1/Users/fangtiewei/tmp_data/export/f1ab7dcc31744152-bbb4cda2f5c88eac_"
    }
  ]
]
1 row in set (0.01 sec)
```


* JobId: The unique ID of the job
* State: Job status:
	* PENDING: Jobs to be Scheduled
	* EXPORTING: Data Export
	* FINISHED: Operation Successful
	* CANCELLED: Job Failure
* Progress: Work progress. The schedule is based on the query plan. Assuming there are 10 threads in total and 3 have been completed, the progress will be 30%.
* TaskInfo: Job information in Json format:
	* db: database name
	* tbl: Table name
	* partitions: Specify the exported partition. `empty` Represents all partitions.
	* column separator: The column separator for the exported file.
	* line delimiter: The line separator for the exported file.
	* tablet num: The total number of tablets involved.
	* Broker: The name of the broker used.
    * max_file_size: The maximum size of an export file.
    * delete_existing_files: Whether to delete existing files and directories in the specified export directory. 
    * columns: Specifies the column names to be exported. Empty values represent exporting all columns.
    * format: The file format for export.
* Path: Export path on remote storage.
* CreateTime/StartTime/FinishTime: Creation time, start scheduling time and end time of jobs.
* Timeout: Job timeout. The unit is seconds. This time is calculated from CreateTime.
* Error Msg: If there is an error in the job, the cause of the error is shown here.
* OutfileInfo：If the export job is successful, specific `SELECT INTO OUTFILE` result information will be displayed here.

### Cancel Export Job

<version since="dev"></version>

After submitting a job, the job can be canceled by using the  [CANCEL EXPORT](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/CANCEL-EXPORT.md)  command. For example:

```sql
CANCEL EXPORT
FROM example_db
WHERE LABEL like "%example%";
````

## Best Practices

### Concurrent Export

An Export job can be configured with the `parallelism` parameter to concurrently export data. The `parallelism` parameter specifies the number of threads to execute the `EXPORT Job`. Each thread is responsible for exporting a subset of the total tablets.

The underlying execution logic of an `Export Job `is actually the `SELECT INTO OUTFILE` statement. Each thread specified by the `parallelism` parameter executes independent `SELECT INTO OUTFILE` statements.

The specific logic for splitting an `Export Job` into multiple `SELECT INTO OUTFILE` is, to evenly distribute all the tablets of the table among all parallel threads. For example:

- If num(tablets) = 40 and parallelism = 3, then the three threads will be responsible for 14, 13, and 13 tablets, respectively.
- If num(tablets) = 2 and parallelism = 3, then Doris automatically sets the parallelism to 2, and each thread is responsible for one tablet.

When the number of tablets responsible for a thread exceeds the `maximum_tablets_of_outfile_in_export` value (default is 10, and can be modified by adding the `maximum_tablets_of_outfile_in_export` parameter in fe.conf), the thread will split the tablets which are responsibled for this thread into multiple `SELECT INTO OUTFILE` statements. For example:

- If a thread is responsible for 14 tablets and `maximum_tablets_of_outfile_in_export = 10`, then the thread will be responsible for two `SELECT INTO OUTFILE` statements. The first `SELECT INTO OUTFILE` statement exports 10 tablets, and the second `SELECT INTO OUTFILE` statement exports 4 tablets. The two `SELECT INTO OUTFILE` statements are executed serially by this thread.

### exec\_mem\_limit

The query plan for an `Export Job` typically involves only `scanning and exporting`, and does not involve compute logic that requires a lot of memory. Therefore, the default memory limit of 2GB is usually sufficient to meet the requirements.

However, in certain scenarios, such as a query plan that requires scanning too many tablets on the same BE, or when there are too many data versions of tablets, it may result in insufficient memory. In these cases, you can adjust the session variable `exec_mem_limit` to increase the memory usage limit.

## Notes

* It is not recommended to export large amounts of data at one time. The maximum amount of exported data recommended by an Export job is tens of GB. Excessive export results in more junk files and higher retry costs.
* If the amount of table data is too large, it is recommended to export it by partition.
* During the operation of the Export job, if FE restarts or cuts the master, the Export job will fail, requiring the user to resubmit.
* If the Export job fails, the temporary files and directory generated in the remote storage will not be deleted, requiring the user to delete them manually.
* Export jobs scan data and occupy IO resources, which may affect the query latency of the system.
* The Export job can export data from  `Doris Base tables`, `View`, and `External tables`, but not from `Rollup Index`.
* When using the EXPORT command, please ensure that the target path exists, otherwise the export may fail.
* When concurrent export is enabled, please configure the thread count and parallelism appropriately to fully utilize system resources and avoid performance bottlenecks.
* When exporting to a local file, pay attention to file permissions and the path, ensure that you have sufficient permissions to write, and follow the appropriate file system path.
* It is possible to monitor progress and performance metrics in real-time during the export process to identify issues promptly and make optimal adjustments.
* It is recommended to verify the integrity and accuracy of the exported data after the export operation is completed to ensure the quality and integrity of the data.

## Relevant configuration

### FE

* `maximum_tablets_of_outfile_in_export`: The maximum number of tablets allowed for an OutFile statement in an ExportExecutorTask.

## More Help

For more detailed syntax and best practices used by Export, please refer to the [Export](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/EXPORT.md) command manual, You can also enter `HELP EXPORT` at the command line of the MySql client for more help.

The underlying implementation of the `EXPORT` command is the `SELECT INTO OUTFILE` statement. For more information about SELECT INTO OUTFILE, please refer to [Export Query Result](./outfile.md) and [SELECT INTO OUTFILE](../..//sql-manual/sql-reference/Data-Manipulation-Statements/OUTFILE.md).

