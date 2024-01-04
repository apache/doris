---
{
    "title": "MYSQL-LOAD",
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

## MYSQL-LOAD

### Name

<version since="dev">
    MYSQL LOAD
</version>

### Description

mysql-load: Import local data using the MySql client

```
LOAD DATA
[LOCAL]
INFILE 'file_name'
INTO TABLE tbl_name
[PARTITION (partition_name [, partition_name] ...)]
[COLUMNS TERMINATED BY 'string']
[LINES TERMINATED BY 'string']
[IGNORE number {LINES | ROWS}]
[(col_name_or_user_var [, col_name_or_user_var] ...)]
[SET (col_name={expr | DEFAULT} [, col_name={expr | DEFAULT}] ...)]
[PROPERTIES (key1 = value1 [, key2=value2]) ]
```

This statement is used to import data to the specified table. Unlike normal Load, this import method is a synchronous import.

This import method can still guarantee the atomicity of a batch of import tasks, either all data imports are successful or all fail.

1. MySQL Load starts with the syntax `LOAD DATA`, without specifying `LABEL`
2. Specify  `LOCAL` to read client side files. Not specified to read FE server side local files. Server side load was disabled by default. It can be enabled by setting a secure path in FE configuration `mysql_load_server_secure_path`
3. The local fill path will be filled after `INFILE`, which can be a relative path or an absolute path. Currently only a single file is supported, and multiple files are not supported
4. The table name after `INTO TABLE` can specify the database name, as shown in the case. It can also be omitted, and the database where the current user is located will be used.
5. `PARTITION` syntax supports specified partition to import
6. `COLUMNS TERMINATED BY` specifies the column separator
7. `LINES TERMINATED BY` specifies the line separator
8. `IGNORE num LINES` The user skips the header of the CSV and can skip any number of lines. This syntax can also be replaced by'IGNORE num ROWS '
9. Column mapping syntax, please refer to the column mapping chapter of [Imported Data Transformation](../../../../data-operate/import/import-way/mysql-load-manual.md)
10. `PROPERTIES` parameter configuration, see below for details

### PROPERTIES

1. max_filter_ratio：The maximum tolerable data ratio that can be filtered (for reasons such as data irregularity). Zero tolerance by default. Data irregularities do not include rows filtered out by where conditions.

2. timeout: Specify the import timeout. in seconds. The default is 600 seconds. The setting range is from 1 second to 259200 seconds.

3. strict_mode: The user specifies whether to enable strict mode for this import. The default is off.

4. timezone: Specify the time zone used for this import. The default is Dongba District. This parameter affects the results of all time zone-related functions involved in the import.

5. exec_mem_limit: Import memory limit. Default is 2GB. The unit is bytes.

6. trim_double_quotes: Boolean type, The default value is false. True means that the outermost double quotes of each field in the load file are trimmed.

7. enclose: When the csv data field contains row delimiters or column delimiters, to prevent accidental truncation, single-byte characters can be specified as brackets for protection. For example, the column separator is ",", the bracket is "'", and the data is "a,'b,c'", then "b,c" will be parsed as a field.

8. escape: Used to escape characters that appear in a csv field identical to the enclosing characters. For example, if the data is "a,'b,'c'", enclose is "'", and you want "b,'c to be parsed as a field, you need to specify a single-byte escape character, such as "\", and then modify the data to "a,' b,\'c'".

### Example

1. Import the data from the client side local file `testData` into the table `testTbl` in the database `testDb`. Specify a timeout of 100 seconds

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("timeout"="100")
    ```

2. Import the data from the server side local file `/root/testData` (set FE config `mysql_load_server_secure_path` to be `root` already) into the table `testTbl` in the database `testDb`. Specify a timeout of 100 seconds

    ```sql
    LOAD DATA
    INFILE '/root/testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("timeout"="100")
    ```

3. Import data from client side local file `testData` into table `testTbl` in database `testDb`, allowing 20% error rate

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("max_filter_ratio"="0.2")
    ```

4. Import the data from the client side local file `testData` into the table `testTbl` in the database `testDb`, allowing a 20% error rate and specifying the column names of the file

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    (k2, k1, v1)
    PROPERTIES ("max_filter_ratio"="0.2")
    ```

5. Import the data in the local file `testData` into the p1, p2 partitions in the table of `testTbl` in the database `testDb`, allowing a 20% error rate.

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PARTITION (p1, p2)
    PROPERTIES ("max_filter_ratio"="0.2")
    ```

6. Import the data in the CSV file `testData` with a local row delimiter of `0102` and a column delimiter of `0304` into the table `testTbl` in the database `testDb`.

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    COLUMNS TERMINATED BY '0304'
    LINES TERMINATED BY '0102'
    ```

7. Import the data from the local file `testData` into the p1, p2 partitions in the table of `testTbl` in the database `testDb` and skip the first 3 lines.

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PARTITION (p1, p2)
    IGNORE 1 LINES
    ```

8. Import data for strict schema filtering and set the time zone to Africa/Abidjan

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("strict_mode"="true", "timezone"="Africa/Abidjan")
    ```

9. Import data is limited to 10GB of import memory and timed out in 10 minutes

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("exec_mem_limit"="10737418240", "timeout"="600")
    ```

### Keywords

    MYSQL, LOAD
