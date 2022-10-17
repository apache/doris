---
{
    "title": "STREAM-LOAD",
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

## STREAM-LOAD

### Name

STREAM LOAD

### Description

stream-load: load data to table in streaming

````
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load
````

This statement is used to import data into the specified table. The difference from ordinary Load is that this import method is synchronous import.

 This import method can still ensure the atomicity of a batch of import tasks, either all data is imported successfully or all of them fail.

 This operation will update the data of the rollup table related to this base table at the same time.

 This is a synchronous operation. After the entire data import work is completed, the import result is returned to the user.

 Currently, HTTP chunked and non-chunked uploads are supported. For non-chunked methods, Content-Length must be used to indicate the length of the uploaded content, which can ensure the integrity of the data.

In addition, it is best for users to set the content of the Expect Header field to 100-continue, which can avoid unnecessary data transmission in some error scenarios.

Parameter introduction:
        Users can pass in import parameters through the Header part of HTTP

1. label: The label imported once, the data of the same label cannot be imported multiple times. Users can avoid the problem of duplicate data import by specifying Label.

   Currently, Doris retains the most recent successful label within 30 minutes.

2. column_separator: used to specify the column separator in the import file, the default is \t. If it is an invisible character, you need to add \x as a prefix and use hexadecimal to represent the separator.

    For example, the separator \x01 of the hive file needs to be specified as -H "column_separator:\x01".

    You can use a combination of multiple characters as column separators.

3. line_delimiter: used to specify the newline character in the imported file, the default is \n. Combinations of multiple characters can be used as newlines.

4. columns: used to specify the correspondence between the columns in the import file and the columns in the table. If the column in the source file corresponds exactly to the content in the table, then there is no need to specify the content of this field.

   If the source file does not correspond to the table schema, then this field is required for some data conversion. There are two forms of column, one is directly corresponding to the field in the imported file, which is directly represented by the field name;

   One is derived column, the syntax is `column_name` = expression. Give a few examples to help understand.

    Example 1: There are 3 columns "c1, c2, c3" in the table, and the three columns in the source file correspond to "c3, c2, c1" at a time; then you need to specify -H "columns: c3, c2, c1 "

    Example 2: There are 3 columns "c1, c2, c3" in the table, the first three columns in the source file correspond in turn, but there is more than 1 column; then you need to specify -H "columns: c1, c2, c3, xxx";

    The last column can be arbitrarily assigned a name and placeholder

    Example 3: There are three columns "year, month, day" in the table, and there is only one time column in the source file, which is in "2018-06-01 01:02:03" format;

    Then you can specify -H "columns: col, year = year(col), month=month(col), day=day(col)" to complete the import

5. where: used to extract part of the data. If the user needs to filter out the unnecessary data, he can achieve this by setting this option.

   Example 1: Only import data greater than k1 column equal to 20180601, then you can specify -H "where: k1 = 20180601" when importing

6. max_filter_ratio: The maximum tolerable data ratio that can be filtered (for reasons such as data irregularity). Zero tolerance by default. Data irregularities do not include rows filtered out by where conditions.

7. partitions: used to specify the partition designed for this import. If the user can determine the partition corresponding to the data, it is recommended to specify this item. Data that does not satisfy these partitions will be filtered out.

   For example, specify import to p1, p2 partition, -H "partitions: p1, p2"

8. timeout: Specify the import timeout. in seconds. The default is 600 seconds. The setting range is from 1 second to 259200 seconds.

9. strict_mode: The user specifies whether to enable strict mode for this import. The default is off. The enable mode is -H "strict_mode: true".

10. timezone: Specify the time zone used for this import. The default is Dongba District. This parameter affects the results of all time zone-related functions involved in the import.

11. exec_mem_limit: Import memory limit. Default is 2GB. The unit is bytes.

12. format: Specify the import data format, the default is csv, and csv_with_names(filter out the first row of your csv file), csv_with_names_and_types(filter out the first two lines of your csv file), json format are supported.

13. jsonpaths: The way of importing json is divided into: simple mode and matching mode.

    Simple mode: The simple mode is not set the jsonpaths parameter. In this mode, the json data is required to be an object type, for example:

       ````
    {"k1":1, "k2":2, "k3":"hello"}, where k1, k2, k3 are column names.
       ````

    Matching mode: It is relatively complex for json data and needs to match the corresponding value through the jsonpaths parameter.

14. strip_outer_array: Boolean type, true indicates that the json data starts with an array object and flattens the array object, the default value is false. E.g:

       ````
        [
         {"k1" : 1, "v1" : 2},
         {"k1" : 3, "v1" : 4}
        ]
        When strip_outer_array is true, the final import into doris will generate two rows of data.
       ````

15. json_root: json_root is a valid jsonpath string, used to specify the root node of the json document, the default value is "".

16. merge_type: The merge type of data, which supports three types: APPEND, DELETE, and MERGE. Among them, APPEND is the default value, which means that this batch of data needs to be appended to the existing data, and DELETE means to delete all the data with the same key as this batch of data. Line, the MERGE semantics need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to the DELETE semantics and the rest is processed according to the APPEND semantics, for example: `-H "merge_type: MERGE" -H "delete: flag=1"`

17. delete: Only meaningful under MERGE, indicating the deletion condition of the data
        function_column.sequence_col: Only applicable to UNIQUE_KEYS. Under the same key column, ensure that the value column is REPLACEed according to the source_sequence column. The source_sequence can be a column in the data source or a column in the table structure.

18. fuzzy_parse: Boolean type, true means that json will be parsed with the schema of the first row. Enabling this option can improve the efficiency of json import, but requires that the order of the keys of all json objects is the same as the first row, the default is false, only use in json format

19. num_as_string: Boolean type, true means that when parsing json data, the numeric type will be converted to a string, and then imported without losing precision.

20. read_json_by_line: Boolean type, true to support reading one json object per line, the default value is false.

21. send_batch_parallelism: Integer, used to set the parallelism of sending batch data. If the value of parallelism exceeds `max_send_batch_parallelism_per_job` in the BE configuration, the BE as a coordination point will use the value of `max_send_batch_parallelism_per_job`.

22. hidden_columns: Specify hidden column when no `columns` in Headers，multi hidden column shoud be
separated by commas.

       ```
           hidden_columns: __DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__
           The system will use the order specified by user. in case above, data should be ended
           with __DORIS_SEQUENCE_COL__.
       ```

    RETURN VALUES
        After the import is complete, the related content of this import will be returned in Json format. Currently includes the following fields
        Status: Import the last status.
            Success: Indicates that the import is successful and the data is already visible;
            Publish Timeout: Indicates that the import job has been successfully committed, but is not immediately visible for some reason. The user can consider the import to be successful and not have to retry the import
            Label Already Exists: Indicates that the Label has been occupied by other jobs. It may be imported successfully or it may be being imported.
            The user needs to determine the subsequent operation through the get label state command
            Others: The import failed, the user can specify the Label to retry the job
        Message: Detailed description of the import status. On failure, the specific failure reason is returned.
        NumberTotalRows: The total number of rows read from the data stream
        NumberLoadedRows: The number of data rows imported this time, only valid in Success
        NumberFilteredRows: The number of rows filtered out by this import, that is, the number of rows with unqualified data quality
        NumberUnselectedRows: This import, the number of rows filtered out by the where condition
        LoadBytes: The size of the source file data imported this time
        LoadTimeMs: The time taken for this import
        BeginTxnTimeMs: The time it takes to request Fe to start a transaction, in milliseconds.
        StreamLoadPutTimeMs: The time it takes to request Fe to obtain the execution plan for importing data, in milliseconds.
        ReadDataTimeMs: Time spent reading data, in milliseconds.
        WriteDataTimeMs: The time taken to perform the write data operation, in milliseconds.
        CommitAndPublishTimeMs: The time it takes to submit a request to Fe and publish the transaction, in milliseconds.
        ErrorURL: The specific content of the filtered data, only the first 1000 items are retained

ERRORS:
        Import error details can be viewed with the following statement:

       ```sql
        SHOW LOAD WARNINGS ON 'url
       ````

    where url is the url given by ErrorURL.

23: compress_type

    Specify compress type file. Only support compressed csv file now. Support gz, lzo, bz2, lz4, lzop, deflate.

### Example

1. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', and use Label for deduplication. Specify a timeout of 100 seconds

   ````
       curl --location-trusted -u root -H "label:123" -H "timeout:100" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ````

2. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', use Label for deduplication, and only import data whose k1 is equal to 20180601
        

       ````
       curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData http://host:port/api/testDb/testTbl/_stream_load
       ````

3. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', allowing a 20% error rate (the user is in the defalut_cluster)
        

       ````
       curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData http://host:port/api/testDb/testTbl/_stream_load
       ````

4. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', allow a 20% error rate, and specify the column name of the file (the user is in the defalut_cluster)
        

       ````
       curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -H "columns: k2, k1, v1" -T testData http://host:port/api/testDb/testTbl /_stream_load
       ````

5. Import the data in the local file 'testData' into the p1, p2 partitions of the table 'testTbl' in the database 'testDb', allowing a 20% error rate.
        

       ````
       curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -H "partitions: p1, p2" -T testData http://host:port/api/testDb/testTbl/_stream_load
       ````

6. Import using streaming (user is in defalut_cluster)
        

       ````
       seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/ _stream_load
       ````

7. Import a table containing HLL columns, which can be columns in the table or columns in the data to generate HLL columns, or use hll_empty to supplement columns that are not in the data
        

       ````
       curl --location-trusted -u root -H "columns: k1, k2, v1=hll_hash(k1), v2=hll_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load
       ````

8. Import data for strict mode filtering and set the time zone to Africa/Abidjan
        

       ````
       curl --location-trusted -u root -H "strict_mode: true" -H "timezone: Africa/Abidjan" -T testData http://host:port/api/testDb/testTbl/_stream_load
       ````

9. Import a table with a BITMAP column, which can be a column in the table or a column in the data to generate a BITMAP column, or use bitmap_empty to fill an empty Bitmap
       ````
        curl --location-trusted -u root -H "columns: k1, k2, v1=to_bitmap(k1), v2=bitmap_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load
        ````

10. Simple mode, import json data
    Table Structure:

`category` varchar(512) NULL COMMENT "",
`author` varchar(512) NULL COMMENT "",
`title` varchar(512) NULL COMMENT "",
`price` double NULL COMMENT ""

json data format:

````
{"category":"C++","author":"avc","title":"C++ primer","price":895}
````

Import command:

````
curl --location-trusted -u root -H "label:123" -H "format: json" -T testData http://host:port/api/testDb/testTbl/_stream_load
````

In order to improve throughput, it supports importing multiple pieces of json data at one time, each line is a json object, and \n is used as a newline by default. You need to set read_json_by_line to true. The json data format is as follows:
        

````
{"category":"C++","author":"avc","title":"C++ primer","price":89.5}
{"category":"Java","author":"avc","title":"Effective Java","price":95}
{"category":"Linux","author":"avc","title":"Linux kernel","price":195}
````

11. Match pattern, import json data
    json data format:

````
[
{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc"," title":"SayingsoftheCentury","price":895},
{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
]
````

Precise import by specifying jsonpath, such as importing only three attributes of category, author, and price

````
curl --location-trusted -u root -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\" $.price\",\"$.author\"]" -H "strip_outer_array: true" -T testData http://host:port/api/testDb/testTbl/_stream_load
````

illustrate:
    1) If the json data starts with an array, and each object in the array is a record, you need to set strip_outer_array to true, which means flatten the array.
    2) If the json data starts with an array, and each object in the array is a record, when setting jsonpath, our ROOT node is actually an object in the array.

12. User specified json root node
    json data format:

````
{
 "RECORDS":[
{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
{"category":"22","author":"2avc","price":895,"timestamp":1589191487},
{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
]
}
````

Precise import by specifying jsonpath, such as importing only three attributes of category, author, and price

````
curl --location-trusted -u root -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\" $.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData http://host:port/api/testDb/testTbl/_stream_load
````

13. Delete the data with the same import key as this batch

````
curl --location-trusted -u root -H "merge_type: DELETE" -T testData http://host:port/api/testDb/testTbl/_stream_load
````

14. Delete the columns in this batch of data that match the data whose flag is listed as true, and append other rows normally

````
curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv, flag" -H "merge_type: MERGE" -H "delete: flag=1" -T testData http://host:port/api/testDb/testTbl/_stream_load
````

15. Import data into UNIQUE_KEYS table with sequence column

````
curl --location-trusted -u root -H "columns: k1,k2,source_sequence,v1,v2" -H "function_column.sequence_col: source_sequence" -T testData http://host:port/api/testDb/testTbl/ _stream_load
````

### Keywords

    STREAM, LOAD

### Best Practice

1. Check the import task status

   Stream Load is a synchronous import process. The successful execution of the statement means that the data is imported successfully. The imported execution result will be returned synchronously through the HTTP return value. And display it in Json format. An example is as follows:

   ````json
   {
       "TxnId": 17,
       "Label": "707717c0-271a-44c5-be0b-4e71bfeacaa5",
       "Status": "Success",
       "Message": "OK",
       "NumberTotalRows": 5,
       "NumberLoadedRows": 5,
       "NumberFilteredRows": 0,
       "NumberUnselectedRows": 0,
       "LoadBytes": 28,
       "LoadTimeMs": 27,
       "BeginTxnTimeMs": 0,
       "StreamLoadPutTimeMs": 2,
       "ReadDataTimeMs": 0,
       "WriteDataTimeMs": 3,
       "CommitAndPublishTimeMs": 18
   }
   ````

   The field definitions are as follows:

   - TxnId: Import transaction ID, which is automatically generated by the system and is globally unique.

   - Label: Import Label, if not specified, the system will generate a UUID.

   - Status:

     Import results. Has the following values:

     - Success: Indicates that the import was successful and the data is already visible.
     - Publish Timeout: This status also means that the import has completed, but the data may be visible with a delay.
     - Label Already Exists: The Label is duplicated and needs to be replaced.
     - Fail: Import failed.

   - ExistingJobStatus:

     The status of the import job corresponding to the existing Label.

     This field is only displayed when the Status is "Label Already Exists". The user can know the status of the import job corresponding to the existing Label through this status. "RUNNING" means the job is still executing, "FINISHED" means the job was successful.

   - Message: Import error message.

   - NumberTotalRows: The total number of rows processed by the import.

   - NumberLoadedRows: The number of rows successfully imported.

   - NumberFilteredRows: The number of rows with unqualified data quality.

   - NumberUnselectedRows: The number of rows filtered by the where condition.

   - LoadBytes: Number of bytes imported.

   - LoadTimeMs: Import completion time. The unit is milliseconds.

   - BeginTxnTimeMs: The time it takes to request the FE to start a transaction, in milliseconds.

   - StreamLoadPutTimeMs: The time taken to request the FE to obtain the execution plan for importing data, in milliseconds.

   - ReadDataTimeMs: Time spent reading data, in milliseconds.

   - WriteDataTimeMs: The time spent performing the write data operation, in milliseconds.

   - CommitAndPublishTimeMs: The time it takes to submit a request to Fe and publish the transaction, in milliseconds.

   - ErrorURL: If there is a data quality problem, visit this URL to view the specific error line.

2. How to correctly submit the Stream Load job and process the returned results.

   Stream Load is a synchronous import operation, so the user needs to wait for the return result of the command synchronously, and decide the next processing method according to the return result.

   The user's primary concern is the `Status` field in the returned result.

   If it is `Success`, everything is fine and you can do other operations after that.

   If the returned result shows a large number of `Publish Timeout`, it may indicate that some resources (such as IO) of the cluster are currently under strain, and the imported data cannot take effect finally. The import task in the state of `Publish Timeout` has succeeded and does not need to be retried. However, it is recommended to slow down or stop the submission of new import tasks and observe the cluster load.

   If the returned result is `Fail`, the import failed, and you need to check the problem according to the specific reason. Once resolved, you can retry with the same Label.

   In some cases, the user's HTTP connection may be disconnected abnormally and the final returned result cannot be obtained. At this point, you can use the same Label to resubmit the import task, and the resubmitted task may have the following results:

   1. `Status` status is `Success`, `Fail` or `Publish Timeout`. At this point, it can be processed according to the normal process.
   2. The `Status` status is `Label Already Exists`. At this time, you need to continue to view the `ExistingJobStatus` field. If the value of this field is `FINISHED`, it means that the import task corresponding to this Label has been successful, and there is no need to retry. If it is `RUNNING`, it means that the import task corresponding to this Label is still running. At this time, you need to use the same Label to continue to submit repeatedly at intervals (such as 10 seconds) until `Status` is not `Label Already Exists' `, or until the value of the `ExistingJobStatus` field is `FINISHED`.

3. Cancel the import task

   Import tasks that have been submitted and not yet completed can be canceled with the CANCEL LOAD command. After cancellation, the written data will also be rolled back and will not take effect.

4. Label, import transaction, multi-table atomicity

   All import tasks in Doris are atomic. And the import of multiple tables in the same import task can also guarantee atomicity. At the same time, Doris can also use the Label mechanism to ensure that the data imported is not lost or heavy. For details, see the [Import Transactions and Atomicity](../../../data-operate/import/import-scenes/load-atomicity.md) documentation.

5. Column mapping, derived columns and filtering

   Doris can support very rich column transformation and filtering operations in import statements. Most built-in functions and UDFs are supported. For how to use this function correctly, please refer to the [Column Mapping, Conversion and Filtering](../../../data-operate/import/import-scenes/load-data-convert.md) document.

6. Error data filtering

   Doris' import tasks can tolerate a portion of malformed data. The tolerance ratio is set via `max_filter_ratio`. The default is 0, which means that the entire import task will fail when there is an error data. If the user wants to ignore some problematic data rows, the secondary parameter can be set to a value between 0 and 1, and Doris will automatically skip the rows with incorrect data format.

   For some calculation methods of the tolerance rate, please refer to the [Column Mapping, Conversion and Filtering](../../../data-operate/import/import-scenes/load-data-convert.md) document.

7. Strict Mode

   The `strict_mode` attribute is used to set whether the import task runs in strict mode. The format affects the results of column mapping, transformation, and filtering. For a detailed description of strict mode, see the [strict mode](../../../data-operate/import/import-scenes/load-strict-mode.md) documentation.

8. Timeout

   The default timeout for Stream Load is 10 minutes. from the time the task is submitted. If it does not complete within the timeout period, the task fails.

9. Limits on data volume and number of tasks

   Stream Load is suitable for importing data within a few GB. Because the data is processed by single-threaded transmission, the performance of importing excessively large data cannot be guaranteed. When a large amount of local data needs to be imported, multiple import tasks can be submitted in parallel.

   Doris also limits the number of import tasks running at the same time in the cluster, usually ranging from 10-20. Import jobs submitted after that will be rejected.

