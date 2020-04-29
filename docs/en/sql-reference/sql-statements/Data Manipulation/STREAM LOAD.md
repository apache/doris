---
{
    "title": "STREAM LOAD",
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

# STREAM LOAD
## description

NAME

load data to table in streaming

SYNOPSIS

Curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

DESCRIPTION

This statement is used to load data to the specified table. The difference from normal load is that this load method is synchronous load.

This type of load still guarantees the atomicity of a batch of load tasks, either all data is loaded successfully or all fails.

This operation also updates the data for the rollup table associated with this base table.

This is a synchronous operation that returns the results to the user after the entire data load is completed.

Currently, HTTP chunked and non-chunked uploads are supported. For non-chunked mode, Content-Length must be used to indicate the length of the uploaded content, which ensures data integrity.

In addition, the user preferably sets the Content of the Expect Header field to 100-continue, which avoids unnecessary data transmission in certain error scenarios.

OPTIONS

Users can pass in the load parameters through the Header part of HTTP.

`label`

A label that is loaded at one time. The data of the same label cannot be loaded multiple times. Users can avoid the problem of repeated data load by specifying the label.

Currently Palo internally retains the most recent successful label within 30 minutes.

`column_separator`

Used to specify the column separator in the load file. The default is `\t`. If it is an invisible character, you need to add `\x` as a prefix and hexadecimal to indicate the separator.

For example, the separator `\x01` of the hive file needs to be specified as `-H "column_separator:\x01"`

`columns`

used to specify the correspondence between the columns in the load file and the columns in the table. If the column in the source file corresponds exactly to the contents of the table, then it is not necessary to specify the contents of this field. If the source file does not correspond to the table schema, then this field is required for some data conversion. There are two forms of column, one is directly corresponding to the field in the load file, directly using the field name to indicate.

One is a derived column with the syntax `column_name` = expression. Give a few examples to help understand.

Example 1: There are three columns "c1, c2, c3" in the table. The three columns in the source file correspond to "c3, c2, c1" at a time; then you need to specify `-H "columns: c3, c2, c1"`

Example 2: There are three columns in the table, "c1, c2, c3". The first three columns in the source file correspond in turn, but there are more than one column; then you need to specify` -H "columns: c1, c2, c3, xxx"`

The last column can optionally specify a name for the placeholder.

Example 3: There are three columns in the table, "year, month, day". There is only one time column in the source file, which is "2018-06-01 01:02:03" format. Then you can specify `-H "columns: col, year = year(col), month=month(col), day=day(col)"` to complete the load.

`where`

Used to extract some data. If the user needs to filter out the unwanted data, it can be achieved by setting this option.

Example 1: load only data larger than k1 column equal to 20180601, then you can specify -H "where: k1 = 20180601" when loading

`max_filter_ratio`

The maximum proportion of data that can be filtered (for reasons such as data irregularity). The default is zero tolerance. Data non-standard does not include rows that are filtered out by the where condition.

`Partitions`

Used to specify the partition designed for this load. If the user is able to determine the partition corresponding to the data, it is recommended to specify the item. Data that does not satisfy these partitions will be filtered out.

For example, specify load to p1, p2 partition, `-H "partitions: p1, p2"`

`Timeout`

Specifies the timeout for the load. Unit seconds. The default is 600 seconds. The range is from 1 second to 259200 seconds.

`strict_mode`

The user specifies whether strict load mode is enabled for this load. The default is enabled. The shutdown mode is `-H "strict_mode: false"`.

`timezone`

Specifies the time zone used for this load. The default is East Eight District. This parameter affects all function results related to the time zone involved in the load.

`exec_mem_limit`

Memory limit. Default is 2GB. Unit is Bytes.

RETURN VALUES

After the load is completed, the related content of this load will be returned in Json format. Current field included

* `Status`: load status.

    * Success: indicates that the load is successful and the data is visible.

    * Publish Timeout: Indicates that the load job has been successfully Commit, but for some reason it is not immediately visible. Users can be considered successful and do not have to retry load

    * Label Already Exists: Indicates that the Label is already occupied by another job, either the load was successful or it is being loaded. The user needs to use the get label state command to determine the subsequent operations.

    * Other: The load failed, the user can specify Label to retry the job.

* Message: A detailed description of the load status. When it fails, it will return the specific reason for failure.

* NumberTotalRows: The total number of rows read from the data stream

* NumberLoadedRows: The number of data rows loaded this time, only valid when Success

* NumberFilteredRows: The number of rows filtered by this load, that is, the number of rows with unqualified data quality.

* NumberUnselectedRows: Number of rows that were filtered by the where condition for this load

* LoadBytes: The amount of source file data loaded this time

* LoadTimeMs: Time spent on this load

* ErrorURL: The specific content of the filtered data, only the first 1000 items are retained

ERRORS

You can view the load error details by the following statement:

    ```SHOW LOAD WARNINGS ON 'url'```

Where url is the url given by ErrorURL.

## example

1. load the data from the local file 'testData' into the table 'testTbl' in the database 'testDb' and use Label for deduplication. Specify a timeout of 100 seconds

    ```Curl --location-trusted -u root -H "label:123" -H "timeout:100" -T testData http://host:port/api/testDb/testTbl/_stream_load```

2. load the data in the local file 'testData' into the table of 'testTbl' in the database 'testDb', use Label for deduplication, and load only data with k1 equal to 20180601
        
    ```Curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData http://host:port/api/testDb/testTbl/_stream_load```

3. load the data from the local file 'testData' into the 'testTbl' table in the database 'testDb', allowing a 20% error rate (user is in defalut_cluster)

    ```Curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData http://host:port/api/testDb/testTbl/_stream_load```

4. load the data from the local file 'testData' into the 'testTbl' table in the database 'testDb', allow a 20% error rate, and specify the column name of the file (user is in defalut_cluster)

    ```Curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -H "columns: k2, k1, v1" -T testData http://host:port/api/testDb/testTbl/_stream_load```

5. load the data from the local file 'testData' into the p1, p2 partition in the 'testTbl' table in the database 'testDb', allowing a 20% error rate.

    ```Curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -H "partitions: p1, p2" -T testData http://host:port/api/testDb/testTbl/stream_load```

6. load using streaming mode (user is in defalut_cluster)

    ```Seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_stream_load```

7. load a table with HLL columns, which can be columns in the table or columns in the data used to generate HLL columns,you can also use hll_empty to supplement columns that are not in the data

    ```Curl --location-trusted -u root -H "columns: k1, k2, v1=hll_hash(k1), v2=hll_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load```

8. load data for strict mode filtering and set the time zone to Africa/Abidjan

    ```Curl --location-trusted -u root -H "strict_mode: true" -H "timezone: Africa/Abidjan" -T testData http://host:port/api/testDb/testTbl/_stream_load```

9. load a table with BITMAP columns, which can be columns in the table or a column in the data used to generate BITMAP columns, you can also use bitmap_empty to supplement columns that are not in the data

    ```Curl --location-trusted -u root -H "columns: k1, k2, v1=to_bitmap(k1), v2=bitmap_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load```


## keyword

    STREAM, LOAD