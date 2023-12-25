---
{
    "title": "Stream Load",
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

# Stream Load

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

## Support data format

Stream Load currently supports data formats: CSV (text), JSON

<version since="1.2">supports PARQUET and ORC</version>

## Basic operations
### Create Load

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

**Signature parameters**

+ user/passwd

  Stream load uses the HTTP protocol to create the imported protocol and signs it through the Basic Access authentication. The Doris system verifies user identity and import permissions based on signatures.

**Load Parameters**

Stream load uses HTTP protocol, so all parameters related to import tasks are set in the header. The significance of some parameters of the import task parameters of Stream load is mainly introduced below.

+ label

  Identity of import task. Each import task has a unique label inside a single database. Label is a user-defined name in the import command. With this label, users can view the execution of the corresponding import task.

  Another function of label is to prevent users from importing the same data repeatedly. **It is strongly recommended that users use the same label for the same batch of data. This way, repeated requests for the same batch of data will only be accepted once, guaranteeing at-Most-Once**

  When the corresponding import operation state of label is CANCELLED, the label can be used again.


+ column_separator

    Used to specify the column separator in the load file. The default is `\t`. If it is an invisible character, you need to add `\x` as a prefix and hexadecimal to indicate the separator.

    For example, the separator `\x01` of the hive file needs to be specified as `-H "column_separator:\x01"`.

    You can use a combination of multiple characters as the column separator.

+ line_delimiter

   Used to specify the line delimiter in the load file. The default is `\n`.

   You can use a combination of multiple characters as the column separator.

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

+ partitions

    Partitions information for tables to be imported will not be imported if the data to be imported does not belong to the specified Partition. These data will be included in `dpp.abnorm.ALL`.

+ columns

    The function transformation configuration of data to be imported includes the sequence change of columns and the expression transformation, in which the expression transformation method is consistent with the query statement.

    ```
    Examples of column order transformation: There are three columns of original data (src_c1,src_c2,src_c3), and there are also three columns （dst_c1,dst_c2,dst_c3) in the doris table at present.
    when the first column src_c1 of the original file corresponds to the dst_c1 column of the target table, while the second column src_c2 of the original file corresponds to the dst_c2 column of the target table and the third column src_c3 of the original file corresponds to the dst_c3 column of the target table,which is written as follows:
    columns: dst_c1, dst_c2, dst_c3

    when the first column src_c1 of the original file corresponds to the dst_c2 column of the target table, while the second column src_c2 of the original file corresponds to the dst_c3 column of the target table and the third column src_c3 of the original file corresponds to the dst_c1 column of the target table,which is written as follows:
    columns: dst_c2, dst_c3, dst_c1

    Example of expression transformation: There are two columns in the original file and two columns in the target table (c1, c2). However, both columns in the original file need to be transformed by functions to correspond to the two columns in the target table.
    columns: tmp_c1, tmp_c2, c1 = year(tmp_c1), c2 = mouth(tmp_c2)
    Tmp_* is a placeholder, representing two original columns in the original file.
    ```
  
+ format

  Specify the import data format, support csv, json, arrow, the default is csv

  <version since="1.2">supports `csv_with_names` (csv file line header filter), `csv_with_names_and_types` (csv file first two lines filter), parquet, orc.</version>

  <version since="2.1.0">supports `arrow` format.</version>

+ exec\_mem\_limit

    Memory limit. Default is 2GB. Unit is Bytes

+ merge\_type

     The type of data merging supports three types: APPEND, DELETE, and MERGE. APPEND is the default value, which means that all this batch of data needs to be appended to the existing data. DELETE means to delete all rows with the same key as this batch of data. MERGE semantics Need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics

+ two\_phase\_commit

  Stream load import can enable two-stage transaction commit mode: in the stream load process, the data is written and the information is returned to the user. At this time, the data is invisible and the transaction status is `PRECOMMITTED`. After the user manually triggers the commit operation, the data is visible.

+ enclose
  
  When the csv data field contains row delimiters or column delimiters, to prevent accidental truncation, single-byte characters can be specified as brackets for protection. For example, the column separator is ",", the bracket is "'", and the data is "a,'b,c'", then "b,c" will be parsed as a field.

+ escape

  Used to escape characters that appear in a csv field identical to the enclosing characters. For example, if the data is "a,'b,'c'", enclose is "'", and you want "b,'c to be parsed as a field, you need to specify a single-byte escape character, such as "\", and then modify the data to "a,' b,\'c'".

  Example：

    1. Initiate a stream load pre-commit operation
  ```shell
  curl  --location-trusted -u user:passwd -H "two_phase_commit:true" -T test.txt http://fe_host:http_port/api/{db}/{table}/_stream_load
  {
      "TxnId": 18036,
      "Label": "55c8ffc9-1c40-4d51-b75e-f2265b3602ef",
      "TwoPhaseCommit": "true",
      "Status": "Success",
      "Message": "OK",
      "NumberTotalRows": 100,
      "NumberLoadedRows": 100,
      "NumberFilteredRows": 0,
      "NumberUnselectedRows": 0,
      "LoadBytes": 1031,
      "LoadTimeMs": 77,
      "BeginTxnTimeMs": 1,
      "StreamLoadPutTimeMs": 1,
      "ReadDataTimeMs": 0,
      "WriteDataTimeMs": 58,
      "CommitAndPublishTimeMs": 0
  }
  ```
    2. Trigger the commit operation on the transaction.
    Note 1) requesting to fe and be both works
    Note 2) `{table}` in url can be omit when commit
  using txn id
  ```shell
  curl -X PUT --location-trusted -u user:passwd  -H "txn_id:18036" -H "txn_operation:commit"  http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
  {
      "status": "Success",
      "msg": "transaction [18036] commit successfully."
  }
  ```
  using label
  ```shell
  curl -X PUT --location-trusted -u user:passwd  -H "label:55c8ffc9-1c40-4d51-b75e-f2265b3602ef" -H "txn_operation:commit"  http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
  {
      "status": "Success",
      "msg": "label [55c8ffc9-1c40-4d51-b75e-f2265b3602ef] commit successfully."
  }
  ```
    3. Trigger an abort operation on a transaction
    Note 1) requesting to fe and be both works
    Note 2) `{table}` in url can be omit when abort
  using txn id
  ```shell
  curl -X PUT --location-trusted -u user:passwd  -H "txn_id:18037" -H "txn_operation:abort"  http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
  {
      "status": "Success",
      "msg": "transaction [18037] abort successfully."
  }
  ```
  using label
  ```shell
  curl -X PUT --location-trusted -u user:passwd  -H "label:55c8ffc9-1c40-4d51-b75e-f2265b3602ef" -H "txn_operation:abort"  http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
  {
      "status": "Success",
      "msg": "label [55c8ffc9-1c40-4d51-b75e-f2265b3602ef] abort successfully."
  }
  ```

+ enable_profile

  <version since="1.2.7">When `enable_profile` is true, the Stream Load profile will be printed to logs (be.INFO).</version>

+ memtable_on_sink_node

  <version since="2.1.0">
  Whether to enable MemTable on DataSink node when loading data, default is false.
  </version>

  Build MemTable on DataSink node, and send segments to other backends through brpc streaming.
  It reduces duplicate work among replicas, and saves time in data serialization & deserialization.
- partial_columns
   <version since="2.0">
   Whether to enable partial column updates，Boolean type, True means that use partial column update, the default value is false, this parameter is only allowed to be set when the table model is Unique and Merge on Write is used.

   eg: `curl  --location-trusted -u root: -H "partial_columns:true" -H "column_separator:," -H "columns:id,balance,last_access_time" -T /tmp/test.csv http://127.0.0.1:48037/api/db1/user_profile/_stream_load`
  </version>

### Use stream load with SQL

You can add a `sql` parameter to the `Header` to replace the `column_separator`, `line_delimiter`, `where`, `columns` in the previous parameter, which is convenient to use.

```
curl --location-trusted -u user:passwd [-H "sql: ${load_sql}"...] -T data.file -XPUT http://fe_host:http_port/api/_http_stream


# -- load_sql
# insert into db.table (col, ...) select stream_col, ... from http_stream("property1"="value1");

# http_stream
# (
#     "column_separator" = ",",
#     "format" = "CSV",
#     ...
# )
```

Examples：

```
curl  --location-trusted -u root: -T test.csv  -H "sql:insert into demo.example_tbl_1(user_id, age, cost) select c1, c4, c7 * 2 from http_stream(\"format\" = \"CSV\", \"column_separator\" = \",\" ) where age >= 30"  http://127.0.0.1:28030/api/_http_stream
```

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

  "Label Already Exists": Label duplicate, need to be replaced Label.

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

+ StreamLoadPutTimeMs: The time cost for RPC to Fe to get a stream load plan, Unit milliseconds.

+ ReadDataTimeMs: Read data time, Unit milliseconds.

+ WriteDataTimeMs: Write data time, Unit milliseconds.

+ CommitAndPublishTimeMs: The time cost for RPC to Fe to commit and publish a transaction, Unit milliseconds.

+ ErrorURL: If you have data quality problems, visit this URL to see specific error lines.

> Note: Since Stream load is a synchronous import mode, import information will not be recorded in Doris system. Users cannot see Stream load asynchronously by looking at import commands. You need to listen for the return value of the create import request to get the import result.

### Cancel Load

Users can't cancel Stream load manually. Stream load will be cancelled automatically by the system after a timeout or import error.

### View Stream Load

Users can view completed stream load tasks through `show stream load`.

By default, BE does not record Stream Load records. If you want to view records that need to be enabled on BE, the configuration parameter is: `enable_stream_load_record=true`. For details, please refer to [BE Configuration Items](https://doris.apache. org/zh-CN/docs/admin-manual/config/be-config)

## Relevant System Configuration

### FE configuration

+ stream\_load\_default\_timeout\_second

  The timeout time of the import task (in seconds) will be cancelled by the system if the import task is not completed within the set timeout time, and will become CANCELLED.

  At present, Stream load does not support custom import timeout time. All Stream load import timeout time is uniform. The default timeout time is 600 seconds. If the imported source file can no longer complete the import within the specified time, the FE parameter ```stream_load_default_timeout_second``` needs to be adjusted.

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

Stream load default timeout is 600 seconds, according to Doris currently the largest import speed limit, about more than 3G files need to modify the import task default timeout.

```
Import Task Timeout = Import Data Volume / 10M / s (Specific Average Import Speed Requires Users to Calculate Based on Their Cluster Conditions)
For example, import a 10G file
Timeout = 1000s -31561;. 20110G / 10M /s
```

### Complete examples
Data situation: In the local disk path /home/store_sales of the sending and importing requester, the imported data is about 15G, and it is hoped to be imported into the table store\_sales of the database bj_sales.

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
    curl --location-trusted -u user:password -T /home/store_sales -H "label:abc" http://abc.com:8030/api/bj_sales/store_sales/_stream_load
    ```

### Coding with StreamLoad

You can initiate HTTP requests for Stream Load using any language. Before initiating HTTP requests, you need to set several necessary headers:

```http
Content-Type: text/plain; charset=UTF-8
Expect: 100-continue
Authorization: Basic <Base64 encoded username and password>
```

`<Base64 encoded username and password>`: a string consist with Doris's `username`, `:` and `password` and then do a base64 encode.

Additionally, it should be noted that if you directly initiate an HTTP request to FE, as Doris will redirect to BE, some frameworks will remove the `Authorization` HTTP header during this process, which requires manual processing.

Doris provides StreamLoad examples in three languages: [Java](https://github.com/apache/doris/tree/master/samples/stream_load/java), [Go](https://github.com/apache/doris/tree/master/samples/stream_load/go), and [Python](https://github.com/apache/doris/tree/master/samples/stream_load/python) for reference.

## Common Questions

* Label Already Exists

  The Label repeat checking steps of Stream load are as follows:

  1. Is there an import Label conflict that already exists with other import methods?

    Because imported Label in Doris system does not distinguish between import methods, there is a problem that other import methods use the same Label.

    Through ``SHOW LOAD WHERE LABEL = "xxx"'``, where XXX is a duplicate Label string, see if there is already a Label imported by FINISHED that is the same as the Label created by the user.

  2. Are Stream loads submitted repeatedly for the same job?

    Since Stream load is an HTTP protocol submission creation import task, HTTP Clients in various languages usually have their own request retry logic. After receiving the first request, the Doris system has started to operate Stream load, but because the result is not returned to the Client side in time, the Client side will retry to create the request. At this point, the Doris system is already operating on the first request, so the second request will be reported to Label Already Exists.

    To sort out the possible methods mentioned above: Search FE Master's log with Label to see if there are two ``redirect load action to destination = ``redirect load action to destination cases in the same Label. If so, the request is submitted repeatedly by the Client side.

    It is recommended that the user calculate the approximate import time based on the amount of data currently requested, and change the request overtime on the client side to a value greater than the import timeout time according to the import timeout time to avoid multiple submissions of the request by the client side.

  3. Connection reset abnormal

    In the community version 0.14.0 and earlier versions, the connection reset exception occurred after Http V2 was enabled, because the built-in web container is tomcat, and Tomcat has pits in 307 (Temporary Redirect). There is a problem with the implementation of this protocol. All In the case of using Stream load to import a large amount of data, a connect reset exception will occur. This is because tomcat started data transmission before the 307 jump, which resulted in the lack of authentication information when the BE received the data request. Later, changing the built-in container to Jetty solved this problem. If you encounter this problem, please upgrade your Doris or disable Http V2 (`enable_http_server_v2=false`).

    After the upgrade, also upgrade the http client version of your program to `4.5.13`，Introduce the following dependencies in your pom.xml file

    ```xml
        <dependency>
          <groupId>org.apache.httpcomponents</groupId>
          <artifactId>httpclient</artifactId>
          <version>4.5.13</version>
        </dependency>
    ```
 
* After enabling the Stream Load record on the BE, the record cannot be queried

  This is caused by the slowness of fetching records, you can try to adjust the following parameters:

  1. Increase the BE configuration `stream_load_record_batch_size`. This configuration indicates how many Stream load records can be pulled from BE each time. The default value is 50, which can be increased to 500.
  2. Reduce the FE configuration `fetch_stream_load_record_interval_second`, this configuration indicates the interval for obtaining Stream load records, the default is to fetch once every 120 seconds, and it can be adjusted to 60 seconds.
  3. If you want to save more Stream load records (not recommended, it will take up more resources of FE), you can increase the configuration `max_stream_load_record_size` of FE, the default is 5000.

## More Help

For more detailed syntax used by **Stream Load**,  you can enter `HELP STREAM LOAD` on the Mysql client command line for more help.
