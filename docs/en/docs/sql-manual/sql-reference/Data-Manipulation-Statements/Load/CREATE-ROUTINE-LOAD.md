---
{
    "title": "CREATE-ROUTINE-LOAD",
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

## CREATE-ROUTINE-LOAD

### Name

CREATE ROUTINE LOAD

### Description

The Routine Load function allows users to submit a resident import task, and import data into Doris by continuously reading data from a specified data source.

Currently, only data in CSV or Json format can be imported from Kakfa through unauthenticated or SSL authentication. [Example of importing data in Json format](../../../../data-operate/import/import-way/routine-load-manual.md#Example_of_importing_data_in_Json_format)

grammar:

```sql
CREATE ROUTINE LOAD [db.]job_name [ON tbl_name]
[merge_type]
[load_properties]
[job_properties]
FROM data_source [data_source_properties]
[COMMENT "comment"]
```

- `[db.]job_name`

  The name of the import job. Within the same database, only one job with the same name can be running.

- `tbl_name`

  Specifies the name of the table to be imported.Optional parameter, If not specified, the dynamic table method will 
  be used, which requires the data in Kafka to contain table name information. Currently, only the table name can be 
  obtained from the Kafka value, and it needs to conform to the format of "table_name|{"col1": "val1", "col2": "val2"}" 
  for JSON data. The "tbl_name" represents the table name, and "|" is used as the delimiter between the table name and 
  the table data. The same format applies to CSV data, such as "table_name|val1,val2,val3". It is important to note that 
  the "table_name" must be consistent with the table name in Doris, otherwise it may cause import failures.

  Tips: The `columns_mapping` parameter is not supported for dynamic tables. If your table structure is consistent with 
  the table structure in Doris and there is a large amount of table information to be imported, this method will be the 
  best choice.

- `merge_type`

  Data merge type. The default is APPEND, which means that the imported data are ordinary append write operations. The MERGE and DELETE types are only available for Unique Key model tables. The MERGE type needs to be used with the [DELETE ON] statement to mark the Delete Flag column. The DELETE type means that all imported data are deleted data.

  Tips: When using dynamic multiple tables, please note that this parameter should be consistent with the type of each dynamic table, otherwise it will result in import failure.

- load_properties

  Used to describe imported data. The composition is as follows:

  ````SQL
  [column_separator],
  [columns_mapping],
  [preceding_filter],
  [where_predicates],
  [partitions],
  [DELETE ON],
  [ORDER BY]
  ````

  - `column_separator`

    Specifies the column separator, defaults to `\t`

    `COLUMNS TERMINATED BY ","`

  - `columns_mapping`

    It is used to specify the mapping relationship between file columns and columns in the table, as well as various column transformations. For a detailed introduction to this part, you can refer to the [Column Mapping, Transformation and Filtering] document.

    `(k1, k2, tmpk1, k3 = tmpk1 + 1)`

    Tips: Dynamic multiple tables are not supported.

  - `preceding_filter`

    Filter raw data. For a detailed introduction to this part, you can refer to the [Column Mapping, Transformation and Filtering] document.

    Tips: Dynamic multiple tables are not supported.

  - `where_predicates`

    Filter imported data based on conditions. For a detailed introduction to this part, you can refer to the [Column Mapping, Transformation and Filtering] document.

    `WHERE k1 > 100 and k2 = 1000`
  
    Tips: When using dynamic multiple tables, please note that this parameter should be consistent with the type of each dynamic table, otherwise it will result in import failure.

  - `partitions`

    Specify in which partitions of the import destination table. If not specified, it will be automatically imported into the corresponding partition.

    `PARTITION(p1, p2, p3)`
    
    Tips: When using dynamic multiple tables, please note that this parameter should conform to each dynamic table, otherwise it may cause import failure.

  - `DELETE ON`

    It needs to be used with the MEREGE import mode, only for the table of the Unique Key model. Used to specify the columns and calculated relationships in the imported data that represent the Delete Flag.

    `DELETE ON v3 >100`

    Tips: When using dynamic multiple tables, please note that this parameter should conform to each dynamic table, otherwise it may cause import failure.

  - `ORDER BY`

    Tables only for the Unique Key model. Used to specify the column in the imported data that represents the Sequence Col. Mainly used to ensure data order when importing.
  
    Tips: When using dynamic multiple tables, please note that this parameter should conform to each dynamic table, otherwise it may cause import failure.

- `job_properties`

  Common parameters for specifying routine import jobs.

  ````text
  PROPERTIES (
      "key1" = "val1",
      "key2" = "val2"
  )
  ````

  Currently we support the following parameters:

  1. `desired_concurrent_number`

     Desired concurrency. A routine import job will be divided into multiple subtasks for execution. This parameter specifies the maximum number of tasks a job can execute concurrently. Must be greater than 0. Default is 3.

     This degree of concurrency is not the actual degree of concurrency. The actual degree of concurrency will be comprehensively considered by the number of nodes in the cluster, the load situation, and the situation of the data source.

     `"desired_concurrent_number" = "3"`

  2. `max_batch_interval/max_batch_rows/max_batch_size`

     These three parameters represent:

     1. The maximum execution time of each subtask, in seconds. The range is 1 to 60. Default is 10.
     2. The maximum number of lines read by each subtask. Must be greater than or equal to 200000. The default is 200000.
     3. The maximum number of bytes read by each subtask. The unit is bytes and the range is 100MB to 1GB. The default is 100MB.

     These three parameters are used to control the execution time and processing volume of a subtask. When either one reaches the threshold, the task ends.

     ````text
     "max_batch_interval" = "20",
     "max_batch_rows" = "300000",
     "max_batch_size" = "209715200"
     ````

  3. `max_error_number`

     The maximum number of error lines allowed within the sampling window. Must be greater than or equal to 0. The default is 0, which means no error lines are allowed.

     The sampling window is `max_batch_rows * 10`. That is, if the number of error lines is greater than `max_error_number` within the sampling window, the routine operation will be suspended, requiring manual intervention to check data quality problems.

     Rows that are filtered out by where conditions are not considered error rows.

  4. `strict_mode`

     Whether to enable strict mode, the default is off. If enabled, the column type conversion of non-null raw data will be filtered if the result is NULL. Specify as:

     `"strict_mode" = "true"`

     The strict mode mode means strict filtering of column type conversions during the load process. The strict filtering strategy is as follows:

     1. For column type conversion, if strict mode is true, the wrong data will be filtered. The error data here refers to the fact that the original data is not null, and the result is a null value after participating in the column type conversion.
     2. When a loaded column is generated by a function transformation, strict mode has no effect on it.
     3. For a column type loaded with a range limit, if the original data can pass the type conversion normally, but cannot pass the range limit, strict mode will not affect it. For example, if the type is decimal(1,0) and the original data is 10, it is eligible for type conversion but not for column declarations. This data strict has no effect on it.

     **strict mode and load relationship of source data**

     Here is an example of a column type of TinyInt.

     > Note: When a column in a table allows a null value to be loaded

     | source data | source data example | string to int | strict_mode   | result                 |
     | ----------- | ------------------- | ------------- | ------------- | ---------------------- |
     | null        | \N                  | N/A           | true or false | NULL                   |
     | not null    | aaa or 2000         | NULL          | true          | invalid data(filtered) |
     | not null    | aaa                 | NULL          | false         | NULL                   |
     | not null    | 1                   | 1             | true or false | correct data           |

     Here the column type is Decimal(1,0)

     > Note: When a column in a table allows a null value to be loaded

     | source data | source data example | string to int | strict_mode   | result                 |
     | ----------- | ------------------- | ------------- | ------------- | ---------------------- |
     | null        | \N                  | N/A           | true or false | NULL                   |
     | not null    | aaa                 | NULL          | true          | invalid data(filtered) |
     | not null    | aaa                 | NULL          | false         | NULL                   |
     | not null    | 1 or 10             | 1             | true or false | correct data           |

     > Note: 10 Although it is a value that is out of range, because its type meets the requirements of decimal, strict mode has no effect on it. 10 will eventually be filtered in other ETL processing flows. But it will not be filtered by strict mode.

  5. `timezone`

     Specifies the time zone used by the import job. The default is to use the Session's timezone parameter. This parameter affects the results of all time zone-related functions involved in the import.

  6. `format`

     Specify the import data format, the default is csv, and the json format is supported.

  7. `jsonpaths`

     When the imported data format is json, the fields in the Json data can be extracted by specifying jsonpaths.

     `-H "jsonpaths: [\"$.k2\", \"$.k1\"]"`

  8. `strip_outer_array`

     When the imported data format is json, strip_outer_array is true, indicating that the Json data is displayed in the form of an array, and each element in the data will be regarded as a row of data. The default value is false.

     `-H "strip_outer_array: true"`

  9. `json_root`

     When the import data format is json, you can specify the root node of the Json data through json_root. Doris will extract the elements of the root node through json_root for parsing. Default is empty.

     `-H "json_root: $.RECORDS"`
  10. `send_batch_parallelism`
     
     Integer, Used to set the default parallelism for sending batch, if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config, then the coordinator BE will use the value of `max_send_batch_parallelism_per_job`.
  
  11. `load_to_single_tablet`
      Boolean type, True means that one task can only load data to one tablet in the corresponding partition at a time. The default value is false. This parameter can only be set when loading data into the OLAP table with random bucketing.

  12. `partial_columns`
      Boolean type, True means that use partial column update, the default value is false, this parameter is only allowed to be set when the table model is Unique and Merge on Write is used. Multi-table does not support this parameter.

  13. `max_filter_ratio`
      The maximum allowed filtering rate within the sampling window. Must be between 0 and 1. The default value is 1.0.

      The sampling window is `max_batch_rows * 10`. That is, if the number of error lines / total lines is greater than `max_filter_ratio` within the sampling window, the routine operation will be suspended, requiring manual intervention to check data quality problems.

      Rows that are filtered out by where conditions are not considered error rows.
  
- `FROM data_source [data_source_properties]`

  The type of data source. Currently supports:

  ````text
  FROM KAFKA
  (
      "key1" = "val1",
      "key2" = "val2"
  )
  ````

  `data_source_properties` supports the following data source properties:

  1. `kafka_broker_list`

     Kafka's broker connection information. The format is ip:host. Separate multiple brokers with commas.

     `"kafka_broker_list" = "broker1:9092,broker2:9092"`

  2. `kafka_topic`

     Specifies the Kafka topic to subscribe to.

     `"kafka_topic" = "my_topic"`

  3. `kafka_partitions/kafka_offsets`

     Specify the kafka partition to be subscribed to, and the corresponding starting offset of each partition. If a time is specified, consumption will start at the nearest offset greater than or equal to the time.

     offset can specify a specific offset from 0 or greater, or:

     - `OFFSET_BEGINNING`: Start subscription from where there is data.
     - `OFFSET_END`: subscribe from the end.
     - Time format, such as: "2021-05-22 11:00:00"

     If not specified, all partitions under topic will be subscribed from `OFFSET_END` by default.

     ````text
     "kafka_partitions" = "0,1,2,3",
     "kafka_offsets" = "101,0,OFFSET_BEGINNING,OFFSET_END"
     ````

     ````text
     "kafka_partitions" = "0,1,2,3",
     "kafka_offsets" = "2021-05-22 11:00:00,2021-05-22 11:00:00,2021-05-22 11:00:00"
     ````

     Note that the time format cannot be mixed with the OFFSET format.

  4. `property`

     Specify custom kafka parameters. The function is equivalent to the "--property" parameter in the kafka shell.

     When the value of the parameter is a file, you need to add the keyword: "FILE:" before the value.

     For how to create a file, please refer to the [CREATE FILE](../../../Data-Definition-Statements/Create/CREATE-FILE) command documentation.

     For more supported custom parameters, please refer to the configuration items on the client side in the official CONFIGURATION document of librdkafka. Such as:

     ````text
     "property.client.id" = "12345",
     "property.ssl.ca.location" = "FILE:ca.pem"
     ````

     1. When connecting to Kafka using SSL, you need to specify the following parameters:

        ````text
        "property.security.protocol" = "ssl",
        "property.ssl.ca.location" = "FILE:ca.pem",
        "property.ssl.certificate.location" = "FILE:client.pem",
        "property.ssl.key.location" = "FILE:client.key",
        "property.ssl.key.password" = "abcdefg"
        ````

        in:

        `property.security.protocol` and `property.ssl.ca.location` are required to indicate the connection method is SSL and the location of the CA certificate.

        If client authentication is enabled on the Kafka server side, thenAlso set:

        ````text
        "property.ssl.certificate.location"
        "property.ssl.key.location"
        "property.ssl.key.password"
        ````

        They are used to specify the client's public key, private key, and password for the private key, respectively.

     2. Specify the default starting offset of the kafka partition

        If `kafka_partitions/kafka_offsets` is not specified, all partitions are consumed by default.

        At this point, you can specify `kafka_default_offsets` to specify the starting offset. Defaults to `OFFSET_END`, i.e. subscribes from the end.

        Example:

        ````text
        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
        ````
- <version since="1.2.3" type="inline"> comment </version>
  Comment for the routine load job.
### Example

1. Create a Kafka routine import task named test1 for example_tbl of example_db. Specify the column separator and group.id and client.id, and automatically consume all partitions by default, and start subscribing from the location where there is data (OFFSET_BEGINNING)

   

   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   COLUMNS TERMINATED BY ",",
   COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100)
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "property.group.id" = "xxx",
       "property.client.id" = "xxx",
       "property.kafka_default_offsets" = "OFFSET_BEGINNING"
   );
   ````

2. Create a Kafka routine dynamic multiple tables import task named "test1" for the "example_db". Specify the column delimiter, group.id, and client.id, and automatically consume all partitions, subscribing from the position with data (OFFSET_BEGINNING).

Assuming that we need to import data from Kafka into tables "test1" and "test2" in the "example_db", we create a routine import task named "test1". At the same time, we write the data in "test1" and "test2" to a Kafka topic named "my_topic" so that data from Kafka can be imported into both tables through a routine import task.

   ```sql
   CREATE ROUTINE LOAD example_db.test1
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "property.group.id" = "xxx",
       "property.client.id" = "xxx",
       "property.kafka_default_offsets" = "OFFSET_BEGINNING"
   );
   ```

3. Create a Kafka routine import task named test1 for example_tbl of example_db. Import tasks are in strict mode.

   

   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
   PRECEDING FILTER k1 = 1,
   WHERE k1 > 100 and k2 like "%doris%"
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2,3",
       "kafka_offsets" = "101,0,0,200"
   );
   ````

4. Import data from the Kafka cluster through SSL authentication. Also set the client.id parameter. The import task is in non-strict mode and the time zone is Africa/Abidjan

   

   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   COLUMNS(k1, k2, k3, v1, v2, v3 = k1 * 100),
   WHERE k1 > 100 and k2 like "%doris%"
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false",
       "timezone" = "Africa/Abidjan"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "property.security.protocol" = "ssl",
       "property.ssl.ca.location" = "FILE:ca.pem",
       "property.ssl.certificate.location" = "FILE:client.pem",
       "property.ssl.key.location" = "FILE:client.key",
       "property.ssl.key.password" = "abcdefg",
       "property.client.id" = "my_client_id"
   );
   ````

5. Import data in Json format. By default, the field name in Json is used as the column name mapping. Specify to import three partitions 0, 1, and 2, and the starting offsets are all 0

   

   ```sql
   CREATE ROUTINE LOAD example_db.test_json_label_1 ON table1
   COLUMNS(category,price,author)
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false",
       "format" = "json"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2",
       "kafka_offsets" = "0,0,0"
   );
   ````

6. Import Json data, extract fields through Jsonpaths, and specify the root node of the Json document

   

   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   COLUMNS(category, author, price, timestamp, dt=from_unixtime(timestamp, '%Y%m%d'))
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false",
       "format" = "json",
       "jsonpaths" = "[\"$.category\",\"$.author\",\"$.price\",\"$.timestamp\"]",
       "json_root" = "$.RECORDS"
       "strip_outer_array" = "true"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2",
       "kafka_offsets" = "0,0,0"
   );
   ````

7. Create a Kafka routine import task named test1 for example_tbl of example_db. And use conditional filtering.

   

   ```sql
   CREATE ROUTINE LOAD example_db.test1 ON example_tbl
   WITH MERGE
   COLUMNS(k1, k2, k3, v1, v2, v3),
   WHERE k1 > 100 and k2 like "%doris%",
   DELETE ON v3 >100
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "20",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200",
       "strict_mode" = "false"
   )
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2,3",
       "kafka_offsets" = "101,0,0,200"
   );
   ````

8. Import data to Unique with sequence column Key model table

   

   ```sql
   CREATE ROUTINE LOAD example_db.test_job ON example_tbl
   COLUMNS TERMINATED BY ",",
   COLUMNS(k1,k2,source_sequence,v1,v2),
   ORDER BY source_sequence
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "30",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200"
   ) FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic",
       "kafka_partitions" = "0,1,2,3",
       "kafka_offsets" = "101,0,0,200"
   );
   ````

9. Consume from a specified point in time

   

   ```sql
   CREATE ROUTINE LOAD example_db.test_job ON example_tbl
   PROPERTIES
   (
       "desired_concurrent_number"="3",
       "max_batch_interval" = "30",
       "max_batch_rows" = "300000",
       "max_batch_size" = "209715200"
   ) FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092",
       "kafka_topic" = "my_topic",
       "kafka_default_offset" = "2021-05-21 10:00:00"
   );
   ````

### Keywords

    CREATE, ROUTINE, LOAD, CREATE LOAD

### Best Practice

Partition and Offset for specified consumption

Doris supports the specified Partition and Offset to start consumption, and also supports the function of consumption at a specified time point. The configuration relationship of the corresponding parameters is described here.

There are three relevant parameters:

- `kafka_partitions`: Specify a list of partitions to be consumed, such as "0, 1, 2, 3".
- `kafka_offsets`: Specify the starting offset of each partition, which must correspond to the number of `kafka_partitions` list. For example: "1000, 1000, 2000, 2000"
- `property.kafka_default_offset`: Specifies the default starting offset of the partition.

When creating an import job, these three parameters can have the following combinations:

| Composition | `kafka_partitions` | `kafka_offsets` | `property.kafka_default_offset` | Behavior                                                     |
| ----------- | ------------------ | --------------- | ------------------------------- | ------------------------------------------------------------ |
| 1           | No                 | No              | No                              | The system will automatically find all partitions corresponding to the topic and start consumption from OFFSET_END |
| 2           | No                 | No              | Yes                             | The system will automatically find all partitions corresponding to the topic and start consumption from the location specified by default offset |
| 3           | Yes                | No              | No                              | The system will start consumption from OFFSET_END of the specified partition |
| 4           | Yes                | Yes             | No                              | The system will start consumption from the specified offset of the specified partition |
| 5           | Yes                | No              | Yes                             | The system will start consumption from the specified partition, the location specified by default offset |
