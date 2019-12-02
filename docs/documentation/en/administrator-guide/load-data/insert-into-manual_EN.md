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

# Insert Into

The use of Insert Into statements is similar to that of Insert Into statements in databases such as MySQL. But in Doris, all data writing is a separate import job. So Insert Into is also introduced here as an import method.

The main Insert Into command contains the following two kinds;

* INSERT INTO tbl SELECT ...
* INSERT INTO tbl (col1, col2, ...) VALUES (1, 2, ...), (1,3, ...);

The second command is for Demo only, not in a test or production environment.

## Basic operations

### Create a Load

The Insert Into command needs to be submitted through MySQL protocol. Creating an import request returns the import result synchronously.

Grammar:

```
INSERT INTO table_name [WITH LABEL label] [partition_info] [col_list] [query_stmt] [VALUES];
```

Examples:

```
INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"), ("a");
```

The following is a brief introduction to the parameters used in creating import statements:

+ partition\_info

	Import the target partition of the table. If the target partition is specified, only the data that matches the target partition will be imported. If not specified, the default value is all partitions of the table.

+ col\_list

	The target column of the import table can exist in any order. If no target column is specified, the default value is all columns in this table. If a column in the table does not exist in the target column, the column needs a default value, otherwise Insert Into will fail.

	If the result column type of the query statement is inconsistent with the type of the target column, an implicit type conversion is invoked. If the conversion is not possible, the Insert Into statement will report a parsing error.

+ query\_stmt

	Through a query statement, the results of the query statement are imported into other tables in Doris system. Query statements support any SQL query syntax supported by Doris.

+ VALUES

	Users can insert one or more data through VALUES grammar.

	*Note: VALUES is only suitable for importing several pieces of data as DEMO. It is totally unsuitable for any test and production environment. Doris system itself is not suitable for single data import scenarios. It is recommended to use INSERT INTO SELECT for batch import.*
	
* WITH LABEL

    INSERT as a load job, it can also be with a label. If not with a label, Doris will use a UUID as label.
    
    This feature needs Doris version 0.11+.
    
    *Note: It is recommended that Label be specified rather than automatically allocated by the system. If the system allocates automatically, but during the execution of the Insert Into statement, the connection is disconnected due to network errors, etc., then it is impossible to know whether Insert Into is successful. If you specify Label, you can view the task results again through Label.*

### Load results

Insert Into itself is an SQL command, so the return behavior is the same as the return behavior of the SQL command.

If the load fails, the error will be returned. Examples are as follows:


```
ERROR 1064 (HY000): All partitions have no load data. url: http://ip:port/api/_load_error_log?File=_shard_14/error_log_insert_stmt_f435264d82f342e4-a33764f5f0dfbf00_f4364d82f342e4_a764f344e4_a764f5f5f0df0df0dbf00
```

Where URL can be used to query the wrong data, see the following **view error line** summary.

If the load succeeds, the success will be returned. Examples are as follows:

```
Query OK, 100 row affected, 0 warning (0.22 sec)
```

If the user specifies Label, the label will be returned as well.

```
Query OK, 100 row affected, 0 warning (0.22 sec)
{label':'user_specified_label'}
```

If the load may be partially successful, the Label field is appended. Examples are as follows:

```
Query OK, 100 row affected, 1 warning (0.23 sec)
{label':'7d66c457-658b-4a3e-bdcf-8beee872ef2c'}
```

```
Query OK, 100 row affected, 1 warning (0.23 sec)
{label':'user_specified_label'}
```

Where affected represents the number of rows loaded. Warning denotes the number of rows that failed. Users need to view the wrong line through `SHOW LOAD WHERE LABEL='xxx';` command, and get url to view the errors.

If there is no data, it will return success, and both affected and warning are 0.

Label is the identifier of the Insert Into import job. Each import job has a unique Label inside a single database. Insert Into's Label is generated by the system. Users can use the Label to asynchronously obtain the import status by querying the import command.

## Relevant System Configuration

### FE configuration

+ time out

	The timeout time of the import task (in seconds) will be cancelled by the system if the import task is not completed within the set timeout time, and will become CANCELLED.

	At present, Insert Into does not support custom import timeout time. All Insert Into imports have a uniform timeout time. The default timeout time is 1 hour. If the imported source file cannot complete the import within the specified time, the parameter ``insert_load_default_timeout_second`` of FE needs to be adjusted.

	At the same time, the Insert Into statement receives the restriction of the Session variable `query_timeout`. You can increase the timeout time by `SET query_timeout = xxx;` in seconds.

### Session Variables

+ enable\_insert\_strict

	The Insert Into import itself cannot control the tolerable error rate of the import. Users can only use the Session parameter `enable_insert_strict`. When this parameter is set to false, it indicates that at least one data has been imported correctly, and then it returns successfully. When this parameter is set to true, the import fails if there is a data error. The default is false. It can be set by `SET enable_insert_strict = true;`.

+ query u timeout

	Insert Into itself is also an SQL command, so the Insert Into statement is also restricted by the Session variable `query_timeout`. You can increase the timeout time by `SET query_timeout = xxx;` in seconds.

## Best Practices

### Application scenarios
1. Users want to import only a few false data to verify the functionality of Doris system. The grammar of INSERT INTO VALUS is suitable at this time.
2. Users want to convert the data already in the Doris table into ETL and import it into a new Doris table, which is suitable for using INSERT INTO SELECT grammar.
3. Users can create an external table, such as MySQL external table mapping a table in MySQL system. Or create Broker external tables to map data files on HDFS. Then the data from the external table is imported into the Doris table for storage through the INSERT INTO SELECT grammar.

### Data volume
Insert Into has no limitation on the amount of data, and large data imports can also be supported. However, Insert Into has a default timeout time, and the amount of imported data estimated by users is too large, so it is necessary to modify the system's Insert Into import timeout time.

```
Import data volume = 36G or less than 3600s*10M/s
Among them, 10M/s is the maximum import speed limit. Users need to calculate the average import speed according to the current cluster situation to replace 10M/s in the formula.
```

### Complete examples

Users have a table store sales in the database sales. Users create a table called bj store sales in the database sales. Users want to import the data recorded in the store sales into the new table bj store sales. The amount of data imported is about 10G.

```
large sales scheme
(id, total, user_id, sale_timestamp, region)

Order large sales schedule:
(id, total, user_id, sale_timestamp)

```

Cluster situation: The average import speed of current user cluster is about 5M/s

+ Step1: Determine whether you want to modify the default timeout of Insert Into

	```
	Calculate the approximate time of import
	10G / 5M /s = 2000s
	
	Modify FE configuration
	insert_load_default_timeout_second = 2000
	```

+ Step2: Create Import Tasks

	Since users want to ETL data from a table and import it into the target table, they should use the Insert in query\\stmt mode to import it.

	```
	INSERT INTO bj_store_sales SELECT id, total, user_id, sale_timestamp FROM store_sales where region = "bj";
	```

## Common Questions

* View the wrong line

	Because Insert Into can't control the error rate, it can only tolerate or ignore the error data completely by `enable_insert_strict`. So if `enable_insert_strict` is set to true, Insert Into may fail. If `enable_insert_strict` is set to false, then only some qualified data may be imported. However, in either case, Doris is currently unable to provide the ability to view substandard data rows. Therefore, the user cannot view the specific import error through the Insert Into statement.

	The causes of errors are usually: source data column length exceeds destination data column length, column type mismatch, partition mismatch, column order mismatch, etc. When it's still impossible to check for problems. At present, it is only recommended that the SELECT command in the Insert Into statement be run to export the data to a file, and then import the file through Stream load to see the specific errors.
