---
{
    "title": "Delete",
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

# Delete

Unlike other import methods, delete is a synchronization process. Similar to insert into, all delete operations are an independent import job in Doris. Generally, delete statements need to specify tables, partitions and delete conditions to tell which data to be deleted, and the data on base index and rollup index will be deleted at the same time.


## Syntax

The delete statement's syntax is as follows：

```
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op value[ AND column_name2 op value ...];
```

example 1：

```
DELETE FROM my_table PARTITION p1 WHERE k1 = 3;
```

example 2:

```
DELETE FROM my_table PARTITION p1 WHERE k1 < 3 AND k2 = "abc";
```

The following describes the parameters used in the delete statement:

* PARTITION
	
	The target partition of the delete statement. If not specified, the table must be a single partition table, otherwise it cannot be deleted

* WHERE
	
	The conditiona of the delete statement. All delete statements must specify a where condition.

说明：

1. The type of `OP` in the WHERE condition can only include `=, >, <, > =, < =,!=`. Currently, where key in (value1, Value2, value3) mode is not supported yet, may be added this support later.
2. The column in the WHERE condition can only be the `key` column.
3. Cannot delete when the `key` column does not exist in any rollup table.
4. Each condition in WHERE condition can only be realated by `and`. If you want `or`, you are suggested to write these conditions into two delete statements.
5. If the specified table is a range partitioned table, `PARTITION` must be specified unless the table is a single partition table,.
6. Unlike the insert into command, delete statement cannot specify `label` manually. You can view the concept of `label` in [Insert Into] (./insert-into-manual.md)

## Delete Result

The delete command is an SQL command, and the returned results are synchronous. It can be divided into the following types:

1. Successful visible

	If delete completes successfully and is visible, the following results will be returned, `query OK` indicates success.
	
	```
	mysql> delete from test_tbl PARTITION p1 where k1 = 1;
    Query OK, 0 rows affected (0.04 sec)
    {'label':'delete_e7830c72-eb14-4cb9-bbb6-eebd4511d251', 'status':'VISIBLE', 'txnId':'4005'}
	```
	
2. Submitted successfully, but not visible


    The transaction submission of Doris is divided into two steps: submission and publish version. Only after the publish version step is completed, the result will be visible to the user. If it has been submitted successfully, then it can be considered that the publish version step will eventually success. Doris will try to wait for publishing for a period of time after submitting. If it has timed out, even if the publishing version has not been completed, it will return to the user in priority and prompt the user that the submission has been completed but not visible. If delete has been committed and executed, but has not been published and visible, the following results will be returned.
    
    ```
	mysql> delete from test_tbl PARTITION p1 where k1 = 1;
    Query OK, 0 rows affected (0.04 sec)
    {'label':'delete_e7830c72-eb14-4cb9-bbb6-eebd4511d251', 'status':'VISIBLE', 'txnId':'4005', 'err':'delete job is committed but may be taking effect later' }
	```
	
     The result will return a JSON string at the same time:
	
    `affected rows`: Indicates the row affected by this deletion. Since the deletion of Doris is currently a logical deletion, the value is always 0.
    
    `label`: The label generated automatically to be the signature of the delete jobs. Each job has a unique label within a single database.
    
    `status`: Indicates whether the data deletion is visible. If it is visible, `visible` will be displayed. If it is not visible, `committed` will be displayed.

    
    `txnId`: The transaction ID corresponding to the delete job
    
    `err`: Field will display some details of this deletion
	
3. Commit failed, transaction cancelled

    If the delete statement is not submitted successfully, it will be automatically aborted by Doris and the following results will be returned

    
    ```
	mysql> delete from test_tbl partition p1 where k1 > 80;
    ERROR 1064 (HY000): errCode = 2, detailMessage = {错误原因}
	```
	
    example：
    
    A timeout deletion will return the timeout and unfinished replicas displayed as ` (tablet = replica)`
    

    ```
	mysql> delete from test_tbl partition p1 where k1 > 80;
    ERROR 1064 (HY000): errCode = 2, detailMessage = failed to delete replicas from job: 4005, Unfinished replicas:10000=60000, 10001=60000, 10002=60000
	```
	
    **The correct processing logic for the returned results of the delete operation is as follows:**
    
    1. If `Error 1064 (HY000)` is returned, deletion fails
    
    2. If the returned result is `Query OK`, the deletion is successful

    	1. If `status` is `committed`, the data deletion is committed and will be eventually invisible. Users can wait for a while and then use the `show delete` command to view the results.
    	2. If `status` is `visible`, the data have been deleted successfully.

## Relevant Configuration

### FE configuration

**TIMEOUT configuration**

In general, Doris's deletion timeout is limited from 30 seconds to 5 minutes. The specific time can be adjusted through the following configuration items

* tablet\_delete\_timeout\_second

    The timeout of delete itself can be elastically changed by the number of tablets in the specified partition. This configuration represents the average timeout contributed by a tablet. The default value is 2.
   
    Assuming that there are 5 tablets under the specified partition for this deletion, the timeout time available for the deletion is 10 seconds. Because the minimum timeout is 30 seconds which is higher than former timeout time, the final timeout is 30 seconds.
   
* load\_straggler\_wait\_second

    If the user estimates a large amount of data, so that the upper limit of 5 minutes is insufficient, the user can adjust the upper limit of timeout through this item, and the default value is 300.
  
    **The specific calculation rule of timeout(seconds)**
  
    `TIMEOUT = MIN(load_straggler_wait_second, MAX(30, tablet_delete_timeout_second * tablet_num))`
  
* query_timeout
  
    Because delete itself is an SQL command, the deletion statement is also limited by the session variables, and the timeout is also affected by the session value `query'timeout`. You can increase the value by `set query'timeout = xxx`.
  
## Show delete history
	
1. The user can view the deletion completed in history through the show delete statement.

	Syntax

	```
	SHOW DELETE [FROM db_name]
	```
	
	example
	
	```
	mysql> show delete from test_db;
	+-----------+---------------+---------------------+-----------------+----------+
	| TableName | PartitionName | CreateTime          | DeleteCondition | State    |
	+-----------+---------------+---------------------+-----------------+----------+
	| empty_tbl | p3            | 2020-04-15 23:09:35 | k1 EQ "1"       | FINISHED |
	| test_tbl  | p4            | 2020-04-15 23:09:53 | k1 GT "80"      | FINISHED |
	+-----------+---------------+---------------------+-----------------+----------+
	2 rows in set (0.00 sec)
	```
	
