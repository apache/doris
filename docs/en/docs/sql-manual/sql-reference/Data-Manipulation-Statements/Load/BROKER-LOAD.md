---
{
    "title": "BROKER-LOAD",
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

## BROKER-LOAD

### Name

BROKER LOAD

### Description

This command is mainly used to import data on remote storage (such as S3, HDFS) through the Broker service process.

```sql
LOAD LABEL load_label
(
data_desc1[, data_desc2, ...]
)
WITH BROKER broker_name
[broker_properties]
[load_properties]
[COMMENT "comment"];
```

- `load_label`

  Each import needs to specify a unique Label. You can use this label to view the progress of the job later.

  `[database.]label_name`

- `data_desc1`

  Used to describe a set of files that need to be imported.

  ```sql
  [MERGE|APPEND|DELETE]
  DATA INFILE
  (
  "file_path1"[, file_path2, ...]
  )
  [NEGATIVE]
  INTO TABLE `table_name`
  [PARTITION (p1, p2, ...)]
  [COLUMNS TERMINATED BY "column_separator"]
  [LINES TERMINATED BY "line_delimiter"]
  [FORMAT AS "file_type"]
  [COMPRESS_TYPE AS "compress_type"]
  [(column_list)]
  [COLUMNS FROM PATH AS (c1, c2, ...)]
  [SET (column_mapping)]
  [PRECEDING FILTER predicate]
  [WHERE predicate]
  [DELETE ON expr]
  [ORDER BY source_sequence]
  [PROPERTIES ("key1"="value1", ...)]
  ````

  - `[MERGE|APPEND|DELETE]`

    Data merge type. The default is APPEND, indicating that this import is a normal append write operation. The MERGE and DELETE types are only available for Unique Key model tables. The MERGE type needs to be used with the `[DELETE ON]` statement to mark the Delete Flag column. The DELETE type indicates that all data imported this time are deleted data.

  - `DATA INFILE`

    Specify the file path to be imported. Can be multiple. Wildcards can be used. The path must eventually match to a file, if it only matches a directory the import will fail.

  - `NEGATIVE`

    This keyword is used to indicate that this import is a batch of "negative" imports. This method is only for aggregate data tables with integer SUM aggregate type. This method will reverse the integer value corresponding to the SUM aggregate column in the imported data. Mainly used to offset previously imported wrong data.

  - `PARTITION(p1, p2, ...)`

    You can specify to import only certain partitions of the table. Data that is no longer in the partition range will be ignored.

  - `COLUMNS TERMINATED BY`

    Specifies the column separator. Only valid in CSV format. Only single-byte delimiters can be specified.

  - `LINES TERMINATED BY`

    Specifies the line delimiter. Only valid in CSV format. Only single-byte delimiters can be specified.

  - `FORMAT AS`

    Specifies the file type, CSV, PARQUET and ORC formats are supported. Default is CSV.

  - `COMPRESS_TYPE AS`
    Specifies the file compress type, GZ/LZO/BZ2/LZ4FRAME/DEFLATE/LZOP

  - `column list`

    Used to specify the column order in the original file. For a detailed introduction to this part, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

    `(k1, k2, tmpk1)`

  - `COLUMNS FROM PATH AS`

    Specifies the columns to extract from the import file path.

  - `SET (column_mapping)`

    Specifies the conversion function for the column.
  
  - `PRECEDING FILTER predicate`

    Pre-filter conditions. The data is first concatenated into raw data rows in order according to `column list` and `COLUMNS FROM PATH AS`. Then filter according to the pre-filter conditions. For a detailed introduction to this part, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

  - `WHERE predicate`

    Filter imported data based on conditions. For a detailed introduction to this part, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

  - `DELETE ON expr`

    It needs to be used with the MEREGE import mode, only for the table of the Unique Key model. Used to specify the columns and calculated relationships in the imported data that represent the Delete Flag.

  - `ORDER BY`

    Tables only for the Unique Key model. Used to specify the column in the imported data that represents the Sequence Col. Mainly used to ensure data order when importing.

  - `PROPERTIES ("key1"="value1", ...)`

    Specify some parameters of the imported format. For example, if the imported file is in `json` format, you can specify parameters such as `json_root`, `jsonpaths`, `fuzzy parse`, etc.

- `WITH BROKER broker_name`

  Specify the Broker service name to be used. In the public cloud Doris. Broker service name is `bos`

- `broker_properties`

  Specifies the information required by the broker. This information is usually used by the broker to be able to access remote storage systems. Such as BOS or HDFS. See the [Broker](../../../../advanced/broker.md) documentation for specific information.

  ````text
  (
      "key1" = "val1",
      "key2" = "val2",
      ...
  )
  ````

- `load_properties`

  Specifies import-related parameters. The following parameters are currently supported:

  - `timeout`

    Import timeout. The default is 4 hours. in seconds.

  - `max_filter_ratio`

    The maximum tolerable proportion of data that can be filtered (for reasons such as data irregularity). Zero tolerance by default. The value range is 0 to 1.

  - `exec_mem_limit`

    Import memory limit. Default is 2GB. The unit is bytes.

  - `strict_mode`

    Whether to impose strict restrictions on data. Defaults to false.

  - `partial_columns`

    Boolean type, True means that use partial column update, the default value is false, this parameter is only allowed to be set when the table model is Unique and Merge on Write is used.

  - `timezone`

    Specify the time zone for some functions that are affected by time zones, such as `strftime/alignment_timestamp/from_unixtime`, etc. Please refer to the [timezone](../../../../advanced/time-zone.md) documentation for details. If not specified, the "Asia/Shanghai" timezone is used

  - `load_parallelism`

    It allows the user to set the parallelism of the load execution plan
    on a single node when the broker load is submitted, default value is 1.

  - `send_batch_parallelism`
  
    Used to set the default parallelism for sending batch, if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config, then the coordinator BE will use the value of `max_send_batch_parallelism_per_job`. 
    
  - `load_to_single_tablet`
  
    Boolean type, True means that one task can only load data to one tablet in the corresponding partition at a time. The default value is false. The number of tasks for the job depends on the overall concurrency. This parameter can only be set when loading data into the OLAP table with random bucketing.
    
  - <version since="dev" type="inline"> priority </version>
    
    Set the priority of the load job, there are three options: `HIGH/NORMAL/LOW`, use `NORMAL` priority as default. The pending broker load jobs which have higher priority will be chosen to execute earlier.

  - <version since="dev" type="inline"> enclose </version>
  
      When the csv data field contains row delimiters or column delimiters, to prevent accidental truncation, single-byte characters can be specified as brackets for protection. For example, the column separator is ",", the bracket is "'", and the data is "a,'b,c'", then "b,c" will be parsed as a field.

  - <version since="dev" type="inline"> escape </version>

      Used to escape characters that appear in a csv field identical to the enclosing characters. For example, if the data is "a,'b,'c'", enclose is "'", and you want "b,'c to be parsed as a field, you need to specify a single-byte escape character, such as "\", and then modify the data to "a,' b,\'c'".

-  <version since="1.2.3" type="inline"> comment </version>
    
   Specify the comment for the import job. The comment can be viewed in the `show load` statement.

### Example

1. Import a batch of data from HDFS

   ```sql
   LOAD LABEL example_db.label1
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file.txt")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY ","
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ````

   Import the file `file.txt`, separated by commas, into the table `my_table`.

2. Import data from HDFS, using wildcards to match two batches of files in two batches. into two tables separately.

   ```sql
   LOAD LABEL example_db.label2
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file-10*")
       INTO TABLE `my_table1`
       PARTITION (p1)
       COLUMNS TERMINATED BY ","
       (k1, tmp_k2, tmp_k3)
       SET (
           k2 = tmp_k2 + 1,
           k3 = tmp_k3 + 1
       )
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file-20*")
       INTO TABLE `my_table2`
       COLUMNS TERMINATED BY ","
       (k1, k2, k3)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ````

   Import two batches of files `file-10*` and `file-20*` using wildcard matching. Imported into two tables `my_table1` and `my_table2` respectively. Where `my_table1` specifies to import into partition `p1`, and will import the values of the second and third columns in the source file +1.

3. Import a batch of data from HDFS.

   ```sql
   LOAD LABEL example_db.label3
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/user/doris/data/*/*")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY "\\x01"
   )
   WITH BROKER my_hdfs_broker
   (
       "username" = "",
       "password" = "",
       "fs.defaultFS" = "hdfs://my_ha",
       "dfs.nameservices" = "my_ha",
       "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
       "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
       "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
       "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   );
   ````

   Specify the delimiter as Hive's default delimiter `\\x01`, and use the wildcard * to specify all files in all directories under the `data` directory. Use simple authentication while configuring namenode HA.

4. Import data in Parquet format and specify FORMAT as parquet. The default is to judge by the file suffix

   ```sql
   LOAD LABEL example_db.label4
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/file")
       INTO TABLE `my_table`
       FORMAT AS "parquet"
       (k1, k2, k3)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ````

5. Import the data and extract the partition field in the file path

   ```sql
   LOAD LABEL example_db.label10
   (
       DATA INFILE("hdfs://hdfs_host:hdfs_port/input/city=beijing/*/*")
       INTO TABLE `my_table`
       FORMAT AS "csv"
       (k1, k2, k3)
       COLUMNS FROM PATH AS (city, utc_date)
   )
   WITH BROKER hdfs
   (
       "username"="hdfs_user",
       "password"="hdfs_password"
   );
   ````

   The columns in the `my_table` table are `k1, k2, k3, city, utc_date`.

   The `hdfs://hdfs_host:hdfs_port/user/doris/data/input/dir/city=beijing` directory includes the following files:

   ````text
   hdfs://hdfs_host:hdfs_port/input/city=beijing/utc_date=2020-10-01/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=beijing/utc_date=2020-10-02/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=tianji/utc_date=2020-10-03/0000.csv
   hdfs://hdfs_host:hdfs_port/input/city=tianji/utc_date=2020-10-04/0000.csv
   ````

   The file only contains three columns of `k1, k2, k3`, and the two columns of `city, utc_date` will be extracted from the file path.

6. Filter the data to be imported.

   ```sql
   LOAD LABEL example_db.label6
   (
       DATA INFILE("hdfs://host:port/input/file")
       INTO TABLE `my_table`
       (k1, k2, k3)
       SET (
           k2 = k2 + 1
       )
       PRECEDING FILTER k1 = 1
       WHERE k1 > k2
   )
   WITH BROKER hdfs
   (
       "username"="user",
       "password"="pass"
   );
   ````

   Only in the original data, k1 = 1, and after transformation, rows with k1 > k2 will be imported.

7. Import data, extract the time partition field in the file path, and the time contains %3A (in the hdfs path, ':' is not allowed, all ':' will be replaced by %3A)

   ```sql
   LOAD LABEL example_db.label7
   (
       DATA INFILE("hdfs://host:port/user/data/*/test.txt")
       INTO TABLE `tbl12`
       COLUMNS TERMINATED BY ","
       (k2,k3)
       COLUMNS FROM PATH AS (data_time)
       SET (
           data_time=str_to_date(data_time, '%Y-%m-%d %H%%3A%i%%3A%s')
       )
   )
   WITH BROKER hdfs
   (
       "username"="user",
       "password"="pass"
   );
   ````

   There are the following files in the path:

   ````text
   /user/data/data_time=2020-02-17 00%3A00%3A00/test.txt
   /user/data/data_time=2020-02-18 00%3A00%3A00/test.txt
   ````

   The table structure is:

   ````text
   data_time DATETIME,
   k2 INT,
   k3 INT
   ````

8. Import a batch of data from HDFS, specify the timeout and filter ratio. Broker with clear text my_hdfs_broker. Simple authentication. And delete the columns in the original data that match the columns with v2 greater than 100 in the imported data, and other columns are imported normally

    ```sql
    LOAD LABEL example_db.label8
    (
        MERGE DATA INFILE("HDFS://test:802/input/file")
        INTO TABLE `my_table`
        (k1, k2, k3, v2, v1)
        DELETE ON v2 > 100
    )
    WITH HDFS
    (
        "hadoop.username"="user",
        "password"="pass"
    )
    PROPERTIES
    (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
    );
    ````

   Import using the MERGE method. `my_table` must be a table with Unique Key. When the value of the v2 column in the imported data is greater than 100, the row is considered a delete row.

   The import task timeout is 3600 seconds, and the error rate is allowed to be within 10%.

9. Specify the source_sequence column when importing to ensure the replacement order in the UNIQUE_KEYS table:

    ```sql
    LOAD LABEL example_db.label9
    (
        DATA INFILE("HDFS://test:802/input/file")
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY ","
        (k1,k2,source_sequence,v1,v2)
        ORDER BY source_sequence
    )
    WITH HDFS
    (
        "hadoop.username"="user",
        "password"="pass"
    )
    ````

   `my_table` must be an Unique Key model table with Sequence Col specified. The data will be ordered according to the value of the `source_sequence` column in the source data.

10. Import a batch of data from HDFS, specify the file format as `json`, and specify parameters of `json_root` and `jsonpaths`.

    ```sql
    LOAD LABEL example_db.label10
    (
        DATA INFILE("HDFS://test:port/input/file.json")
        INTO TABLE `my_table`
        FORMAT AS "json"
        PROPERTIES(
          "json_root" = "$.item",
          "jsonpaths" = "[$.id, $.city, $.code]"
        )       
    )
    with HDFS (
    "hadoop.username" = "user"
    "password" = ""
    )
    PROPERTIES
    (
    "timeout"="1200",
    "max_filter_ratio"="0.1"
    );
    ```

    `jsonpaths` can be use with `column list` and `SET(column_mapping)`:

    ```sql
    LOAD LABEL example_db.label10
    (
        DATA INFILE("HDFS://test:port/input/file.json")
        INTO TABLE `my_table`
        FORMAT AS "json"
        (id, code, city)
        SET (id = id * 10)
        PROPERTIES(
          "json_root" = "$.item",
          "jsonpaths" = "[$.id, $.code, $.city]"
        )       
    )
    with HDFS (
    "hadoop.username" = "user"
    "password" = ""
    )
    PROPERTIES
    (
    "timeout"="1200",
    "max_filter_ratio"="0.1"
    );
    ```

11. Load data in csv format from cos(Tencent Cloud Object Storage).

    ```SQL
    LOAD LABEL example_db.label10
    (
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    )
    WITH BROKER "broker_name"
    (
        "fs.cosn.userinfo.secretId" = "xxx",
        "fs.cosn.userinfo.secretKey" = "xxxx",
        "fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxxxxx.myqcloud.com"
    )
    ```

12. Load CSV date and trim double quotes and skip first 5 lines

    ```SQL
    LOAD LABEL example_db.label12
    (
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    PROPERTIES("trim_double_quotes" = "true", "skip_lines" = "5")
    )
    WITH BROKER "broker_name"
    (
        "fs.cosn.userinfo.secretId" = "xxx",
        "fs.cosn.userinfo.secretKey" = "xxxx",
        "fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxxxxx.myqcloud.com"
    )
    ```

### Keywords

    BROKER, LOAD

### Best Practice

1. Check the import task status

   Broker Load is an asynchronous import process. The successful execution of the statement only means that the import task is submitted successfully, and does not mean that the data import is successful. The import status needs to be viewed through the [SHOW LOAD](../../Show-Statements/SHOW-LOAD.md) command.

2. Cancel the import task

   Import tasks that have been submitted but not yet completed can be canceled by the [CANCEL LOAD](./CANCEL-LOAD.md) command. After cancellation, the written data will also be rolled back and will not take effect.

3. Label, import transaction, multi-table atomicity

   All import tasks in Doris are atomic. And the import of multiple tables in the same import task can also guarantee atomicity. At the same time, Doris can also use the Label mechanism to ensure that the data imported is not lost or heavy. For details, see the [Import Transactions and Atomicity](../../../../data-operate/import/import-scenes/load-atomicity.md) documentation.

4. Column mapping, derived columns and filtering

   Doris can support very rich column transformation and filtering operations in import statements. Most built-in functions and UDFs are supported. For how to use this function correctly, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

5. Error data filtering

   Doris' import tasks can tolerate a portion of malformed data. Tolerated via `max_filter_ratio` setting. The default is 0, which means that the entire import task will fail when there is an error data. If the user wants to ignore some problematic data rows, the secondary parameter can be set to a value between 0 and 1, and Doris will automatically skip the rows with incorrect data format.

   For some calculation methods of the tolerance rate, please refer to the [Column Mapping, Conversion and Filtering](../../../../data-operate/import/import-scenes/load-data-convert.md) document.

6. Strict Mode

   The `strict_mode` attribute is used to set whether the import task runs in strict mode. The format affects the results of column mapping, transformation, and filtering. For a detailed description of strict mode, see the [strict mode](../../../../data-operate/import/import-scenes/load-strict-mode.md) documentation.

7. Timeout

   The default timeout for Broker Load is 4 hours. from the time the task is submitted. If it does not complete within the timeout period, the task fails.

8. Limits on data volume and number of tasks

   Broker Load is suitable for importing data within 100GB in one import task. Although theoretically there is no upper limit on the amount of data imported in one import task. But committing an import that is too large results in a longer run time, and the cost of retrying after a failure increases.

   At the same time, limited by the size of the cluster, we limit the maximum amount of imported data to the number of ComputeNode nodes * 3GB. In order to ensure the rational use of system resources. If there is a large amount of data to be imported, it is recommended to divide it into multiple import tasks.

   Doris also limits the number of import tasks running simultaneously in the cluster, usually ranging from 3 to 10. Import jobs submitted after that are queued. The maximum queue length is 100. Subsequent submissions will be rejected outright. Note that the queue time is also calculated into the total job time. If it times out, the job is canceled. Therefore, it is recommended to reasonably control the frequency of job submission by monitoring the running status of the job.

