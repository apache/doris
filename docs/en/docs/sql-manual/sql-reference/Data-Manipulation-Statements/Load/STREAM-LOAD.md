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

12. format: Specify load data format, support csv, json, <version since="1.2" type="inline"> csv_with_names(support csv file line header filter), csv_with_names_and_types(support csv file first two lines filter), parquet, orc</version>, default is csv.

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
    ````
    When strip_outer_array is true, the final import into doris will generate two rows of data.


15. json_root: json_root is a valid jsonpath string, used to specify the root node of the json document, the default value is "".

16. merge_type: The merge type of data, which supports three types: APPEND, DELETE, and MERGE. Among them, APPEND is the default value, which means that this batch of data needs to be appended to the existing data, and DELETE means to delete all the data with the same key as this batch of data. Line, the MERGE semantics need to be used in conjunction with the delete condition, which means that the data that meets the delete condition is processed according to the DELETE semantics and the rest is processed according to the APPEND semantics, for example: `-H "merge_type: MERGE" -H "delete: flag=1"`

17. delete: Only meaningful under MERGE, indicating the deletion condition of the data
    
18. function_column.sequence_col: Only applicable to UNIQUE_KEYS. Under the same key column, ensure that the value column is REPLACEed according to the source_sequence column. The source_sequence can be a column in the data source or a column in the table structure.

19. fuzzy_parse: Boolean type, true means that json will be parsed with the schema of the first row. Enabling this option can improve the efficiency of json import, but requires that the order of the keys of all json objects is the same as the first row, the default is false, only use in json format

20. num_as_string: Boolean type, true means that when parsing json data, the numeric type will be converted to a string, and then imported without losing precision.

21. read_json_by_line: Boolean type, true to support reading one json object per line, the default value is false.

22. send_batch_parallelism: Integer, used to set the parallelism of sending batch data. If the value of parallelism exceeds `max_send_batch_parallelism_per_job` in the BE configuration, the BE as a coordination point will use the value of `max_send_batch_parallelism_per_job`.

23. hidden_columns: Specify hidden column when no `columns` in Headers，multi hidden column shoud be
separated by commas.

       ```
           hidden_columns: __DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__
           The system will use the order specified by user. in case above, data should be ended
           with __DORIS_SEQUENCE_COL__.
       ```
24. load_to_single_tablet: Boolean type, True means that one task can only load data to one tablet in the corresponding partition at a time. The default value is false. This parameter can only be set when loading data into the OLAP table with random bucketing.

25. compress_type: Specify compress type file. Only support compressed csv file now. Support gz, lzo, bz2, lz4, lzop, deflate.

26. trim_double_quotes: Boolean type, The default value is false. True means that the outermost double quotes of each field in the csv file are trimmed.

27. skip_lines: <version since="dev" type="inline"> Integer type, the default value is 0. It will skip some lines in the head of csv file. It will be disabled when format is `csv_with_names` or `csv_with_names_and_types`. </version>

28. comment: <version since="1.2.3" type="inline"> String type, the default value is "". </version>

29. enclose: <version since="dev" type="inline"> When the csv data field contains row delimiters or column delimiters, to prevent accidental truncation, single-byte characters can be specified as brackets for protection. For example, the column separator is ",", the bracket is "'", and the data is "a,'b,c'", then "b,c" will be parsed as a field. </version>
  
30. escape <version since="dev" type="inline"> Used to escape characters that appear in a csv field identical to the enclosing characters. For example, if the data is "a,'b,'c'", enclose is "'", and you want "b,'c to be parsed as a field, you need to specify a single-byte escape character, such as "\", and then modify the data to "a,' b,\'c'". </version>

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
    ```
    `category` varchar(512) NULL COMMENT "",
    `author` varchar(512) NULL COMMENT "",
    `title` varchar(512) NULL COMMENT "",
    `price` double NULL COMMENT ""
    ```
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

16. csv file line header filter import

    file data:
    
    ```
       id,name,age
       1,doris,20
       2,flink,10
    ```
    Filter the first line import by specifying `format=csv_with_names`
    ```
    curl --location-trusted -u root -T test.csv  -H "label:1" -H "format:csv_with_names" -H "column_separator:," http://host:port/api/testDb/testTbl/_stream_load
    ```
    
17. Import data into a table whose table field contains DEFAULT CURRENT_TIMESTAMP

    Table Structure:
    
    ```sql
    `id` bigint(30) NOT NULL,
    `order_code` varchar(30) DEFAULT NULL COMMENT '',
    `create_time` datetimev2(3) DEFAULT CURRENT_TIMESTAMP    
    ```

    JSON data format:

    ```
    {"id":1,"order_Code":"avc"}
    ```

    Import command:

    ```
    curl --location-trusted -u root -T test.json -H "label:1" -H "format:json" -H 'columns: id, order_code, create_time=CURRENT_TIMESTAMP()' http://host:port/api/testDb/testTbl/_stream_load
    ```
    
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

   All import tasks in Doris are atomic. And the import of multiple tables in the same import task can also guarantee atomicity. At the same time, Doris can also use the Label mechanism to ensure that the data imported is not lost or heavy. For details, see the [Import Transactions and Atomicity](../../../../data-operate/import/import-scenes/load-atomicity.md) documentation.

5. Column mapping, derived columns and filtering

   Doris can support very rich column transformation and filtering operations in import statements. Most built-in functions and UDFs are supported. For how to use this function correctly, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

6. Error data filtering

   Doris' import tasks can tolerate a portion of malformed data. The tolerance ratio is set via `max_filter_ratio`. The default is 0, which means that the entire import task will fail when there is an error data. If the user wants to ignore some problematic data rows, the secondary parameter can be set to a value between 0 and 1, and Doris will automatically skip the rows with incorrect data format.

   For some calculation methods of the tolerance rate, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

7. Strict Mode

   The `strict_mode` attribute is used to set whether the import task runs in strict mode. The format affects the results of column mapping, transformation, and filtering, and it also controls the behavior of partial updates. For a detailed description of strict mode, see the [strict mode](../../../../data-operate/import/import-scenes/load-strict-mode.md) documentation.

8. Timeout

   The default timeout for Stream Load is 10 minutes. from the time the task is submitted. If it does not complete within the timeout period, the task fails.

9. Limits on data volume and number of tasks

   Stream Load is suitable for importing data within a few GB. Because the data is processed by single-threaded transmission, the performance of importing excessively large data cannot be guaranteed. When a large amount of local data needs to be imported, multiple import tasks can be submitted in parallel.

   Doris also limits the number of import tasks running at the same time in the cluster, usually ranging from 10-20. Import jobs submitted after that will be rejected.

