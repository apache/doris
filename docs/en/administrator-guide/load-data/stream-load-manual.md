---
{
    "title": "Stream load",
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

# Stream load

Stream load is a synchronous way of importing. Users import local files or data streams into Doris by sending HTTP protocol requests. Stream load synchronously executes the import and returns the import result. Users can directly determine whether the import is successful by the return body of the request.

Stream load is mainly suitable for importing local files or data from data streams through procedures.

## Basic Principles

The following figure shows the main flow of Stream load, omitting some import details.

```
                         ^      +
                         |      |
                         |      | 1A. User submit load to FE
                         |      |
                         |   +--v-----------+
                         |   | FE           |
5. Return result to user |   +--+-----------+
                         |      |
                         |      | 2. Redirect to BE
                         |      |
                         |   +--v-----------+
                         +---+Coordinator BE| 1B. User submit load to BE
                             +-+-----+----+-+
                               |     |    |
                         +-----+     |    +-----+
                         |           |          | 3. Distrbute data
                         |           |          |
                       +-v-+       +-v-+      +-v-+
                       |BE |       |BE |      |BE |
                       +---+       +---+      +---+
```

In Stream load, Doris selects a node as the Coordinator node. This node is responsible for receiving data and distributing data to other data nodes.

Users submit import commands through HTTP protocol. If submitted to FE, FE forwards the request to a BE via the HTTP redirect instruction. Users can also submit import commands directly to a specified BE.

The final result of the import is returned to the user by Coordinator BE.

## Basic operations
### Create a Load

Stream load submits and transfers data through HTTP protocol. Here, the `curl` command shows how to submit an import.

Users can also operate through other HTTP clients.

```
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

The properties supported in the header are described in "Load Parameters" below
The format is: - H "key1: value1"
```

Examples:

```
curl --location-trusted -u root -T date -H "label:123" http://abc.com:8030/api/test/date/_stream_load
```
The detailed syntax for creating imports helps to execute ``HELP STREAM LOAD`` view. The following section focuses on the significance of creating some parameters of Stream load.

#### Signature parameters

+ user/passwd

	Stream load uses the HTTP protocol to create the imported protocol and signs it through the Basic Access authentication. The Doris system verifies user identity and import permissions based on signatures.

#### Load Parameters

Stream load uses HTTP protocol, so all parameters related to import tasks are set in the header. The significance of some parameters of the import task parameters of Stream load is mainly introduced below.

+ label

	Identity of import task. Each import task has a unique label inside a single database. Label is a user-defined name in the import command. With this label, users can view the execution of the corresponding import task.

	Another function of label is to prevent users from importing the same data repeatedly. **It is strongly recommended that users use the same label for the same batch of data. This way, repeated requests for the same batch of data will only be accepted once, guaranteeing at-Most-Once**

	When the corresponding import operation state of label is CANCELLED, the label can be used again.

+ max\_filter\_ratio

	The maximum tolerance rate of the import task is 0 by default, and the range of values is 0-1. When the import error rate exceeds this value, the import fails.

	If the user wishes to ignore the wrong row, the import can be successful by setting this parameter greater than 0.

	The calculation formula is as follows:

    ``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) ) > max_filter_ratio ```

	``` dpp.abnorm.ALL``` denotes the number of rows whose data quality is not up to standard. Such as type mismatch, column mismatch, length mismatch and so on.

	``` dpp.norm.ALL ``` refers to the number of correct data in the import process. The correct amount of data for the import task can be queried by the ``SHOW LOAD` command.

The number of rows in the original file = `dpp.abnorm.ALL + dpp.norm.ALL`

+ where

	Import the filter conditions specified by the task. Stream load supports filtering of where statements specified for raw data. The filtered data will not be imported or participated in the calculation of filter ratio, but will be counted as `num_rows_unselected`.

+ partition

	Partition information for tables to be imported will not be imported if the data to be imported does not belong to the specified Partition. These data will be included in `dpp.abnorm.ALL`.

+ columns

	The function transformation configuration of data to be imported includes the sequence change of columns and the expression transformation, in which the expression transformation method is consistent with the query statement.

	```
	Examples of column order transformation: There are two columns of original data, and there are also two columns (c1, c2) in the table at present. But the first column of the original file corresponds to the C2 column of the target table, while the second column of the original file corresponds to the C1 column of the target table, which is written as follows:
	columns: c2,c1
	
	Example of expression transformation: There are two columns in the original file and two columns in the target table (c1, c2). However, both columns in the original file need to be transformed by functions to correspond to the two columns in the target table.
	columns: tmp_c1, tmp_c2, c1 = year(tmp_c1), c2 = mouth(tmp_c2)
	Tmp_* is a placeholder, representing two original columns in the original file.
	```

+ exec\_mem\_limit

    Memory limit. Default is 2GB. Unit is Bytes

+ merge\_type
     The type of data merging supports three types: APPEND, DELETE, and MERGE. APPEND is the default value, which means that all this batch of data needs to be appended to the existing data. DELETE means to delete all rows with the same key as this batch of data. MERGE semantics Need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics


### Return results

Since Stream load is a synchronous import method, the result of the import is directly returned to the user by creating the return value of the import.

Examples:

```
{
    "TxnId": 1003,
    "Label": "b6f3bc78-0d2c-45d9-9e4c-faa0a0149bee",
    "Status": "Success",
    "ExistingJobStatus": "FINISHED", // optional
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 1000000,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 40888898,
    "LoadTimeMs": 2144,
    "BeginTxnTimeMs": 1,
    "StreamLoadPutTimeMs": 2,
    "ReadDataTimeMs": 325,
    "WriteDataTimeMs": 1933,
    "CommitAndPublishTimeMs": 106,
    "ErrorURL": "http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005"
}
```

The following main explanations are given for the Stream load import result parameters:

+ TxnId: The imported transaction ID. Users do not perceive.

+ Label: Import Label. User specified or automatically generated by the system.

+ Status: Import completion status.

	"Success": Indicates successful import.

	"Publish Timeout": This state also indicates that the import has been completed, except that the data may be delayed and visible without retrying.

	"Label Already Exists"：Label duplicate, need to be replaced Label.

	"Fail": Import failed.
	
+ ExistingJobStatus: The state of the load job corresponding to the existing Label.

    This field is displayed only when the status is "Label Already Exists". The user can know the status of the load job corresponding to Label through this state. "RUNNING" means that the job is still executing, and "FINISHED" means that the job is successful.

+ Message: Import error messages.

+ NumberTotalRows: Number of rows imported for total processing.

+ NumberLoadedRows: Number of rows successfully imported.

+ NumberFilteredRows: Number of rows that do not qualify for data quality.

+ NumberUnselectedRows: Number of rows filtered by where condition.

+ LoadBytes: Number of bytes imported.

+ LoadTimeMs: Import completion time. Unit milliseconds.

+ BeginTxnTimeMs: The time cost for RPC to Fe to begin a transaction, Unit milliseconds.

+ StreamLoadPutTimeMs：The time cost for RPC to Fe to get a stream load plan, Unit milliseconds.
  
+ ReadDataTimeMs：Read data time, Unit milliseconds.

+ WriteDataTimeMs：Write data time, Unit milliseconds.

+ CommitAndPublishTimeMs：The time cost for RPC to Fe to commit and publish a transaction, Unit milliseconds.

+ ErrorURL: If you have data quality problems, visit this URL to see specific error lines.

> Note: Since Stream load is a synchronous import mode, import information will not be recorded in Doris system. Users cannot see Stream load asynchronously by looking at import commands. You need to listen for the return value of the create import request to get the import result.

### Cancel Load

Users can't cancel Stream load manually. Stream load will be cancelled automatically by the system after a timeout or import error.

## Relevant System Configuration

### FE configuration

+ stream\_load\_default\_timeout\_second

	The timeout time of the import task (in seconds) will be cancelled by the system if the import task is not completed within the set timeout time, and will become CANCELLED.

	At present, Stream load does not support custom import timeout time. All Stream load import timeout time is uniform. The default timeout time is 300 seconds. If the imported source file can no longer complete the import within the specified time, the FE parameter ```stream_load_default_timeout_second``` needs to be adjusted.

### BE configuration

+ streaming\_load\_max\_mb

	The maximum import size of Stream load is 10G by default, in MB. If the user's original file exceeds this value, the BE parameter ```streaming_load_max_mb``` needs to be adjusted.

## Best Practices

### Application scenarios

The most appropriate scenario for using Stream load is that the original file is in memory or on disk. Secondly, since Stream load is a synchronous import method, users can also use this import if they want to obtain the import results in a synchronous manner.

### Data volume

Since Stream load is based on the BE initiative to import and distribute data, the recommended amount of imported data is between 1G and 10G. Since the default maximum Stream load import data volume is 10G, the configuration of BE ```streaming_load_max_mb``` needs to be modified if files exceeding 10G are to be imported.

```
For example, the size of the file to be imported is 15G
Modify the BE configuration streaming_load_max_mb to 16000
```

Stream load default timeout is 300 seconds, according to Doris currently the largest import speed limit, about more than 3G files need to modify the import task default timeout.

```
Import Task Timeout = Import Data Volume / 10M / s (Specific Average Import Speed Requires Users to Calculate Based on Their Cluster Conditions)
For example, import a 10G file
Timeout = 1000s -31561;. 20110G / 10M /s
```

### Complete examples
Data situation: In the local disk path / home / store_sales of the sending and importing requester, the imported data is about 15G, and it is hoped to be imported into the table store\_sales of the database bj_sales.

Cluster situation: The concurrency of Stream load is not affected by cluster size.

+ Step 1: Does the import file size exceed the default maximum import size of 10G

	```
	BE conf
	streaming_load_max_mb = 16000
	```
+ Step 2: Calculate whether the approximate import time exceeds the default timeout value

	```
	Import time 15000/10 = 1500s
	Over the default timeout time, you need to modify the FE configuration
	stream_load_default_timeout_second = 1500
	```

+ Step 3: Create Import Tasks

    ```
    curl --location-trusted -u user:password -T /home/store_sales -H "label:abc" http://abc.com:8000/api/bj_sales/store_sales/_stream_load
    ```

## Common Questions

* Label Already Exists

	The Label repeat checking steps of Stream load are as follows:

	1. Is there an import Label conflict that already exists with other import methods?

		Because imported Label in Doris system does not distinguish between import methods, there is a problem that other import methods use the same Label.

		Through ``SHOW LOAD WHERE LABEL = "xxx"'``, where XXX is a duplicate Label string, see if there is already a Label imported by FINISHED that is the same as the Label created by the user.

	2. Are Stream loads submitted repeatedly for the same job?

		Since Stream load is an HTTP protocol submission creation import task, HTTP Clients in various languages usually have their own request retry logic. After receiving the first request, the Doris system has started to operate Stream load, but because the result is not returned to the Client side in time, the Client side will retry to create the request. At this point, the Doris system is already operating on the first request, so the second request will be reported to Label Already Exists.

		To sort out the possible methods mentioned above: Search FE Master's log with Label to see if there are two ``redirect load action to destination = ``redirect load action to destination cases in the same Label. If so, the request is submitted repeatedly by the Client side.

		It is suggested that the user calculate the approximate import time according to the data quantity of the current request, and change the request time-out time of the Client end according to the import time-out time, so as to avoid the request being submitted by the Client end many times.
