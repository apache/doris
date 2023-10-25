---
{
    "title": "Insert Into",
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

# Insert Into

The use of Insert Into statements is similar to that of Insert Into statements in databases such as MySQL. But in Doris, all data writing is a separate import job. So Insert Into is also introduced here as an import method.

The main Insert Into command contains the following two kinds;

- INSERT INTO tbl SELECT ...
- INSERT INTO tbl (col1, col2, ...) VALUES (1, 2, ...), (1,3, ...);

The second command is for Demo only, not in a test or production environment.

## Import operations and load results

The Insert Into command needs to be submitted through MySQL protocol. Creating an import request returns the import result synchronously.

The following are examples of the use of two Insert Intos:

```sql
INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"), ("a");
```

> Note: When you need to use `CTE(Common Table Expressions)` as the query part in an insert operation, you must specify the `WITH LABEL` and column list parts or wrap `CTE`. Example:
>
> ```sql
> INSERT INTO tbl1 WITH LABEL label1
> WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
> SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
> 
> 
> INSERT INTO tbl1 (k1)
> WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
> SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
> 
> INSERT INTO tbl1 (k1)
> select * from (
> WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
> SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1) as ret
> ```

For specific parameter description, you can refer to [INSERT INTO](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) command or execute `HELP INSERT ` to see its help documentation for better use of this import method.


Insert Into itself is a SQL command, and the return result is divided into the following types according to the different execution results:

1. Result set is empty

   If the result set of the insert corresponding SELECT statement is empty, it is returned as follows:

   ```text
   mysql> insert into tbl1 select * from empty_tbl;
   Query OK, 0 rows affected (0.02 sec)
   ```

   `Query OK` indicates successful execution. `0 rows affected` means that no data was loaded.

2. The result set is not empty

   In the case where the result set is not empty. The returned results are divided into the following situations:

   1. Insert is successful and data is visible:

      ```text
      mysql> insert into tbl1 select * from tbl2;
      Query OK, 4 rows affected (0.38 sec)
      {'label': 'insert_8510c568-9eda-4173-9e36-6adc7d35291c', 'status': 'visible', 'txnId': '4005'}
      
      mysql> insert into tbl1 with label my_label1 select * from tbl2;
      Query OK, 4 rows affected (0.38 sec)
      {'label': 'my_label1', 'status': 'visible', 'txnId': '4005'}
      
      mysql> insert into tbl1 select * from tbl2;
      Query OK, 2 rows affected, 2 warnings (0.31 sec)
      {'label': 'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status': 'visible', 'txnId': '4005'}
      
      mysql> insert into tbl1 select * from tbl2;
      Query OK, 2 rows affected, 2 warnings (0.31 sec)
      {'label': 'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status': 'committed', 'txnId': '4005'}
      ```

      `Query OK` indicates successful execution. `4 rows affected` means that a total of 4 rows of data were imported. `2 warnings` indicates the number of lines to be filtered.

      Also returns a json string:

      ```text
      {'label': 'my_label1', 'status': 'visible', 'txnId': '4005'}
      {'label': 'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status': 'committed', 'txnId': '4005'}
      {'label': 'my_label1', 'status': 'visible', 'txnId': '4005', 'err': 'some other error'}
      ```

      `label` is a user-specified label or an automatically generated label. Label is the ID of this Insert Into load job. Each load job has a label that is unique within a single database.

      `status` indicates whether the loaded data is visible. If visible, show `visible`, if not, show`committed`.

      `txnId` is the id of the load transaction corresponding to this insert.

      The `err` field displays some other unexpected errors.

      When user need to view the filtered rows, the user can use the following statement

      ```text
      show load where label = "xxx";
      ```

      The URL in the returned result can be used to query the wrong data. For details, see the following **View Error Lines** Summary.    **"Data is not visible" is a temporary status, this batch of data must be visible eventually**

      You can view the visible status of this batch of data with the following statement:

      ```text
      show transaction where id = 4005;
      ```

      If the `TransactionStatus` column in the returned result is `visible`, the data is visible.

   2. Insert fails

      Execution failure indicates that no data was successfully loaded, and returns as follows:

      ```text
      mysql> insert into tbl1 select * from tbl2 where k1 = "a";
      ERROR 1064 (HY000): all partitions have no load data. Url: http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de850e8de850
      ```

      Where `ERROR 1064 (HY000): all partitions have no load data` shows the reason for the failure. The latter url can be used to query the wrong data. For details, see the following **View Error Lines** Summary.

**In summary, the correct processing logic for the results returned by the insert operation should be:**

1. If the returned result is `ERROR 1064 (HY000)`, it means that the import failed.
2. If the returned result is `Query OK`, it means the execution was successful.
   1. If `rows affected` is 0, the result set is empty and no data is loaded.
   2. If`rows affected` is greater than 0:
      1. If `status` is`committed`, the data is not yet visible. You need to check the status through the `show transaction` statement until `visible`.
      2. If `status` is`visible`, the data is loaded successfully.
   3. If `warnings` is greater than 0, it means that some data is filtered. You can get the url through the `show load` statement to see the filtered rows.

In the previous section, we described how to follow up on the results of insert operations. However, it is difficult to get the json string of the returned result in some mysql libraries. Therefore, Doris also provides the `SHOW LAST INSERT` command to explicitly retrieve the results of the last insert operation.

After executing an insert operation, you can execute `SHOW LAST INSERT` on the same session connection. This command returns the result of the most recent insert operation, e.g.

```sql
mysql> show last insert\G
*************************** 1. row ***************************
    TransactionId: 64067
            Label: insert_ba8f33aea9544866-8ed77e2844d0cc9b
         Database: default_cluster:db1
            Table: t1
TransactionStatus: VISIBLE
       LoadedRows: 2
     FilteredRows: 0
```

This command returns the insert results and the details of the corresponding transaction. Therefore, you can continue to execute the `show last insert` command after each insert operation to get the insert results.

> Note: This command will only return the results of the last insert operation within the same session connection. If the connection is broken or replaced with a new one, the empty set will be returned.

## Relevant System Configuration

### FE configuration

- time out

  The timeout time of the import task (in seconds) will be cancelled by the system if the import task is not completed within the set timeout time, and will become CANCELLED.

  At present, Insert Into does not support custom import timeout time. All Insert Into imports have a uniform timeout time. The default timeout time is 4 hours. If the imported source file cannot complete the import within the specified time, the parameter `insert_load_default_timeout_second` of FE needs to be adjusted.

### Session Variables

- enable_insert_strict

  The Insert Into import itself cannot control the tolerable error rate of the import. Users can only use the Session parameter `enable_insert_strict`. When this parameter is set to false, it indicates that at least one data has been imported correctly, and then it returns successfully. When this parameter is set to true, the import fails if there is a data error. The default is false. It can be set by `SET enable_insert_strict = true;`.

- query_timeout

  Insert Into itself is also an SQL command, and the Insert Into statement is restricted by the Session variable <version since="dev" type="inline">`insert_timeout`</version>. You can increase the timeout time by `SET insert_timeout = xxx;` in seconds.

## Best Practices

### Application scenarios

1. Users want to import only a few false data to verify the functionality of Doris system. The grammar of INSERT INTO VALUES is suitable at this time.
2. Users want to convert the data already in the Doris table into ETL and import it into a new Doris table, which is suitable for using INSERT INTO SELECT grammar.
3. Users can create an external table, such as MySQL external table mapping a table in MySQL system. Or create Broker external tables to map data files on HDFS. Then the data from the external table is imported into the Doris table for storage through the INSERT INTO SELECT grammar.

### Data volume

Insert Into has no limitation on the amount of data, and large data imports can also be supported. However, Insert Into has a default timeout time, and the amount of imported data estimated by users is too large, so it is necessary to modify the system's Insert Into import timeout time.

```text
Import data volume = 36G or less than 3600s*10M/s
Among them, 10M/s is the maximum import speed limit. Users need to calculate the average import speed according to the current cluster situation to replace 10M/s in the formula.
```

### Complete examples

Users have a table store sales in the database sales. Users create a table called bj store sales in the database sales. Users want to import the data recorded in the store sales into the new table bj store sales. The amount of data imported is about 10G.

```text
large sales scheme
(id, total, user_id, sale_timestamp, region)

Order large sales schedule:
(id, total, user_id, sale_timestamp)
```

Cluster situation: The average import speed of current user cluster is about 5M/s

- Step1: Determine whether you want to modify the default timeout of Insert Into

  ```text
  Calculate the approximate time of import
  10G / 5M /s = 2000s
  
  Modify FE configuration
  insert_load_default_timeout_second = 2000
  ```

- Step2: Create Import Tasks

  Since users want to ETL data from a table and import it into the target table, they should use the Insert in query\stmt mode to import it.

  ```text
  INSERT INTO bj_store_sales SELECT id, total, user_id, sale_timestamp FROM store_sales where region = "bj";
  ```

## Common Questions

- View the wrong line

  Because Insert Into can't control the error rate, it can only tolerate or ignore the error data completely by `enable_insert_strict`. So if `enable_insert_strict` is set to true, Insert Into may fail. If `enable_insert_strict` is set to false, then only some qualified data may be imported. However, in either case, Doris is currently unable to provide the ability to view substandard data rows. Therefore, the user cannot view the specific import error through the Insert Into statement.

  The causes of errors are usually: source data column length exceeds destination data column length, column type mismatch, partition mismatch, column order mismatch, etc. When it's still impossible to check for problems. At present, it is only recommended that the SELECT command in the Insert Into statement be run to export the data to a file, and then import the file through Stream load to see the specific errors.

## more help

For more detailed syntax and best practices used by insert into, see [insert](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md) command manual, you can also enter `HELP INSERT` in the MySql client command line for more help information.
