---
{
    "title": "Data export",
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

# Data export

Export is a function provided by Doris to export data. This function can export user-specified table or partition data in text format to remote storage through Broker process, such as HDFS/BOS.

This document mainly introduces the basic principles, usage, best practices and precautions of Export.

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.
* Broker: Doris can manipulate files for remote storage through the Broker process.
* Tablet: Data fragmentation. A table is divided into multiple data fragments.

## Principle

After the user submits an Export job. Doris counts all Tablets involved in this job. These tablets are then grouped to generate a special query plan for each group. The query plan reads the data on the included tablet and then writes the data to the specified path of the remote storage through Broker. It can also be directly exported to the remote storage that supports S3 protocol through S3 protocol.

The overall mode of dispatch is as follows:

```
+--------+
| Client |
+---+----+
    |  1. Submit Job
    |
+---v--------------------+
| FE                     |
|                        |
| +-------------------+  |
| | ExportPendingTask |  |
| +-------------------+  |
|                        | 2. Generate Tasks
| +--------------------+ |
| | ExportExporingTask | |
| +--------------------+ |
|                        |
| +-----------+          |     +----+   +------+   +---------+
| | QueryPlan +----------------> BE +--->Broker+--->         |
| +-----------+          |     +----+   +------+   | Remote  |
| +-----------+          |     +----+   +------+   | Storage |
| | QueryPlan +----------------> BE +--->Broker+--->         |
| +-----------+          |     +----+   +------+   +---------+
+------------------------+         3. Execute Tasks

```

1. The user submits an Export job to FE.
2. FE's Export scheduler performs an Export job in two stages:
	1. PENDING: FE generates Export Pending Task, sends snapshot command to BE, and takes a snapshot of all Tablets involved. And generate multiple query plans.
	2. EXPORTING: FE generates Export ExportingTask and starts executing the query plan.

### query plan splitting

The Export job generates multiple query plans, each of which scans a portion of the Tablet. The number of Tablets scanned by each query plan is specified by the FE configuration parameter `export_tablet_num_per_task`, which defaults to 5. That is, assuming a total of 100 Tablets, 20 query plans will be generated. Users can also specify this number by the job attribute `tablet_num_per_task`, when submitting a job.

Multiple query plans for a job are executed sequentially.

### Query Plan Execution

A query plan scans multiple fragments, organizes read data in rows, batches every 1024 actions, and writes Broker to remote storage.

The query plan will automatically retry three times if it encounters errors. If a query plan fails three retries, the entire job fails.

Doris will first create a temporary directory named `doris_export_tmp_12345` (where `12345` is the job id) in the specified remote storage path. The exported data is first written to this temporary directory. Each query plan generates a file with an example file name:

`export-data-c69fcf2b6db5420f-a96b94c1ff8bccef-1561453713822`

Among them, `c69fcf2b6db5420f-a96b94c1ff8bccef` is the query ID of the query plan. ` 1561453713822` Timestamp generated for the file.

When all data is exported, Doris will rename these files to the user-specified path.

## Use examples

Export's detailed commands can be passed through `HELP EXPORT;` Examples are as follows:

```
EXPORT TABLE db1.tbl1 
PARTITION (p1,p2)
[WHERE [expr]]
TO "bos://bj-test-cmy/export/" 
PROPERTIES
(
    "label"="mylabel",
    "column_separator"=",",
    "columns" = "col1,col2",
    "exec_mem_limit"="2147483648",
    "timeout" = "3600"
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
* `exec_mem_limit`: Represents the memory usage limitation of a query plan on a single BE in an Export job. Default 2GB. Unit bytes.
* `timeout`: homework timeout. Default 2 hours. Unit seconds.
* `tablet_num_per_task`: The maximum number of fragments allocated per query plan. The default is 5.

After submitting a job, the job status can be imported by querying the `SHOW EXPORT` command. The results are as follows:

```
     JobId: 14008
     Label: mylabel
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":["*"],"exec mem limit":2147483648,"column separator":",","line delimiter":"\n","tablet num":1,"broker":"hdfs","coord num":1,"db":"default_cluster:db1","tbl":"tbl3"}
      Path: bos://bj-test-cmy/export/
CreateTime: 2019-06-25 17:08:24
 StartTime: 2019-06-25 17:08:28
FinishTime: 2019-06-25 17:08:34
   Timeout: 3600
  ErrorMsg: N/A
```


* JobId: The unique ID of the job
* Label: Job identifier
* State: Job status:
	* PENDING: Jobs to be Scheduled
	* EXPORTING: Data Export
	* FINISHED: Operation Successful
	* CANCELLED: Job Failure
* Progress: Work progress. The schedule is based on the query plan. Assuming a total of 10 query plans have been completed, the progress will be 30%.
* TaskInfo: Job information in Json format:
	* db: database name
	* tbl: Table name
	* partitions: Specify the exported partition. `*` Represents all partitions.
	* exec MEM limit: query plan memory usage limit. Unit bytes.
	* column separator: The column separator for the exported file.
	* line delimiter: The line separator for the exported file.
	* tablet num: The total number of tablets involved.
	* Broker: The name of the broker used.
	* Coord num: Number of query plans.
* Path: Export path on remote storage.
* CreateTime/StartTime/FinishTime: Creation time, start scheduling time and end time of jobs.
* Timeout: Job timeout. The unit is seconds. This time is calculated from CreateTime.
* Error Msg: If there is an error in the job, the cause of the error is shown here.

## Best Practices

### Splitting Query Plans

How many query plans need to be executed for an Export job depends on the total number of Tablets and how many Tablets can be allocated for a query plan at most. Since multiple query plans are executed serially, the execution time of jobs can be reduced if more fragments are processed by one query plan. However, if the query plan fails (e.g., the RPC fails to call Broker, the remote storage jitters, etc.), too many tablets can lead to a higher retry cost of a query plan. Therefore, it is necessary to arrange the number of query plans and the number of fragments to be scanned for each query plan in order to balance the execution time and the success rate of execution. It is generally recommended that the amount of data scanned by a query plan be within 3-5 GB (the size and number of tables in a table can be viewed by `SHOW TABLET FROM tbl_name;`statement.

### exec\_mem\_limit

Usually, a query plan for an Export job has only two parts `scan`- `export`, and does not involve computing logic that requires too much memory. So usually the default memory limit of 2GB can satisfy the requirement. But in some scenarios, such as a query plan, too many Tablets need to be scanned on the same BE, or too many data versions of Tablets, may lead to insufficient memory. At this point, larger memory needs to be set through this parameter, such as 4 GB, 8 GB, etc.

## Notes

* It is not recommended to export large amounts of data at one time. The maximum amount of exported data recommended by an Export job is tens of GB. Excessive export results in more junk files and higher retry costs.
* If the amount of table data is too large, it is recommended to export it by partition.
* During the operation of the Export job, if FE restarts or cuts the master, the Export job will fail, requiring the user to resubmit.
* If the Export job fails, the `__doris_export_tmp_xxx` temporary directory generated in the remote storage and the generated files will not be deleted, requiring the user to delete them manually.
* If the Export job runs successfully, the `__doris_export_tmp_xxx` directory generated in the remote storage may be retained or cleared according to the file system semantics of the remote storage. For example, in Baidu Object Storage (BOS), after removing the last file in a directory through rename operation, the directory will also be deleted. If the directory is not cleared, the user can clear it manually.
* When the Export runs successfully or fails, the FE reboots or cuts, then some information of the jobs displayed by `SHOW EXPORT` will be lost and cannot be viewed.
* Export jobs only export data from Base tables, not Rollup Index.
* Export jobs scan data and occupy IO resources, which may affect the query latency of the system.

## Relevant configuration

### FE

* `expo_checker_interval_second`: Scheduling interval of Export job scheduler, default is 5 seconds. Setting this parameter requires restarting FE.
* `export_running_job_num_limit `: Limit on the number of Export jobs running. If exceeded, the job will wait and be in PENDING state. The default is 5, which can be adjusted at run time.
* `Export_task_default_timeout_second`: Export job default timeout time. The default is 2 hours. It can be adjusted at run time.
* `export_tablet_num_per_task`: The maximum number of fragments that a query plan is responsible for. The default is 5.
