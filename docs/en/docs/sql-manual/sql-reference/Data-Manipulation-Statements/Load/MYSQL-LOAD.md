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
<version since="dev"></version>

## MYSQL-LOAD

### Name

MYSQL LOAD

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

This operation also updates the data of the rollup table associated with this base table.

1. MySQL Load starts with the syntax `LOAD DATA`, without specifying LABEL
2. Specify  `LOCAL` to read client side files. Not specified to read FE server level local files
3. Fill in the local file path in `INFILE`, which can be a relative path or an absolute path. Currently only a single file is supported, and multiple files are not supported
4. The table name of `INTO TABLE` can specify the database name, as shown in the case. It can also be omitted, and the database where the current user is located will be used.
5. `PARTITION` syntax supports specified partition import
6. `COLUMNS TERMINATED BY` specifies the column separator
7. `LINES TERMINATED BY` specifies the line separator
8. `IGNORE num LINES` The user skips the header of the CSV and can skip any number of lines. This syntax can also be replaced by'IGNORE num ROWS '
9. Column mapping syntax, please refer to the column mapping chapter of [Imported Data Transformation] (../../../data-operate/import/import-way/mysql-load-manual.md)
10. `PROPERTIES` parameter configuration, see below for details

### PROPERTIES

1. max_filter_ratio：Maximum tolerance The proportion of data that can be filtered (for reasons such as non-canonical data). Default zero tolerance.

2. timeout: Specifies the timeout for the import. In seconds. The default is 600 seconds. The range can be set from 1 second to 259200 seconds.

3. strict_mode: The user specifies whether this import is in strict mode, which defaults to off.

4. timezone: Specifies the time zone used for this import. Defaults to East Eighth District. This parameter affects the results of all time zone-related functions involved in the import.

5. exec_mem_limit: Import memory limit. Default is 2GB. Units are bytes.

### Example

1. Import the data from the client side local file'testData 'into the table'testTbl' in the database'testDb '. Specify a timeout of 100 seconds

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    PROPERTIES ("timeout"="100")
    ```

2. Import the data from the server level local file '/root/testData' into the table'testTbl 'in the database'testDb'. Specify a timeout of 100 seconds

        ```sql
        LOAD DATA
        INFILE '/root/testData'
        INTO TABLE testDb.testTbl
        PROPERTIES ("timeout"="100")
        ```

3. Import data from client side local file'testData 'into table'testTbl' in database'testDb ', allowing 20% error rate

        ```sql
        LOAD DATA LOCAL
        INFILE 'testData'
        INTO TABLE testDb.testTbl
        PROPERTIES ("max_filter_ratio"="0.2")
        ```

4. Import the data from the client side local file'testData 'into the table'testTbl' in the database'testDb ', allowing a 20% error rate and specifying the column names of the file

        ```sql
        LOAD DATA LOCAL
        INFILE 'testData'
        INTO TABLE testDb.testTbl
        (k2, k1, v1)
        PROPERTIES ("max_filter_ratio"="0.2")
        ```

5. Import the data in the local file'testData 'into the p1, p2 partitions in the table of'testTbl' in the database'testDb ', allowing a 20% error rate.

        ```
        LOAD DATA LOCAL
        INFILE 'testData'
        PARTITION (p1, p2)
        INTO TABLE testDb.testTbl
        PROPERTIES ("max_filter_ratio"="0.2")
        ```

6. Import the data in the CSV file'testData 'with a local row delimiter of' 0102 'and a column delimiter of' 0304 'into the table'testTbl' in the database'testDb '.

    ```sql
    LOAD DATA LOCAL
    INFILE 'testData'
    INTO TABLE testDb.testTbl
    COLUMNS TERMINATED BY '0304'
    LINES TERMINATED BY '0102'
    ```

7. Import the data from the local file'testData 'into the p1, p2 partitions in the table of'testTbl' in the database'testDb 'and skip the first 3 lines.

        ```sql
        LOAD DATA LOCAL
        INFILE 'testData'
        PARTITION (p1, p2)
        INTO TABLE testDb.testTbl
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
