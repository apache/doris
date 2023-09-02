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

Delete is different from other import methods. It is a synchronization process, similar to Insert into. All Delete operations are an independent import job in Doris. Generally, the Delete statement needs to specify the table and partition and delete conditions to filter the data to be deleted. , and will delete the data of the base table and the rollup table at the same time.


## Syntax

Please refer to the official website for the [DELETE](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/DELETE.md) syntax of the delete operation.

## Delete Result

The delete command is an SQL command, and the returned results are synchronous. It can be divided into the following types:

1. Successful visible

   If delete completes successfully and is visible, the following results will be returned, `query OK` indicates success.

   ```sql
   mysql> delete from test_tbl PARTITION p1 where k1 = 1;
    Query OK, 0 rows affected (0.04 sec)
    {'label':'delete_e7830c72-eb14-4cb9-bbb6-eebd4511d251', 'status':'VISIBLE', 'txnId':'4005'}
   ```

2. Submitted successfully, but not visible

   The transaction submission of Doris is divided into two steps: submission and publish version. Only after the publish version step is completed, the result will be visible to the user. If it has been submitted successfully, then it can be considered that the publish version step will eventually success. Doris will try to wait for publishing for a period of time after submitting. If it has timed out, even if the publishing version has not been completed, it will return to the user in priority and prompt the user that the submission has been completed but not visible. If delete has been committed and executed, but has not been published and visible, the following results will be returned.

   ```sql
   mysql> delete from test_tbl PARTITION p1 where k1 = 1;
   Query OK, 0 rows affected (0.04 sec)
   {'label':'delete_e7830c72-eb14-4cb9-bbb6-eebd4511d251', 'status':'COMMITTED', 'txnId':'4005', 'err':'delete job is committed but may be taking effect later' }
   ```

    The result will return a JSON string at the same time:

   `affected rows`: Indicates the row affected by this deletion. Since the deletion of Doris is currently a logical deletion, the value is always 0.

   `label`: The label generated automatically to be the signature of the delete jobs. Each job has a unique label within a single database.

   `status`: Indicates whether the data deletion is visible. If it is visible, `visible` will be displayed. If it is not visible, `committed` will be displayed.

   `txnId`: The transaction ID corresponding to the delete job

   `err`: Field will display some details of this deletion

3. Commit failed, transaction cancelled

   If the delete statement is not submitted successfully, it will be automatically aborted by Doris and the following results will be returned


   ```sql
   mysql> delete from test_tbl partition p1 where k1 > 80;
   ERROR 1064 (HY000): errCode = 2, detailMessage = {错误原因}
   ```

   example:

   A timeout deletion will return the timeout and unfinished replicas displayed as ` (tablet = replica)`


   ```sql
   mysql> delete from test_tbl partition p1 where k1 > 80;
   ERROR 1064 (HY000): errCode = 2, detailMessage = failed to delete replicas from job: 4005, Unfinished replicas:10000=60000, 10001=60000, 10002=60000
   ```

   **The correct processing logic for the returned results of the delete operation is as follows:**

   1. If `Error 1064 (HY000)` is returned, deletion fails

   2. If the returned result is `Query OK`, the deletion is successful

      1. If `status` is `committed`, the data deletion is committed and will be eventually invisible. Users can wait for a while and then use the `show delete` command to view the results.
      2. If `status` is `visible`, the data have been deleted successfully.

## Delete Operation Related FE Configuration

**TIMEOUT Configuration**

In general, Doris's deletion timeout is limited from 30 seconds to 5 minutes. The specific time can be adjusted through the following configuration items

* `tablet_delete_timeout_second`

  The timeout of delete itself can be elastically changed by the number of tablets in the specified partition. This configuration represents the average timeout contributed by a tablet. The default value is 2.

  Assuming that there are 5 tablets under the specified partition for this deletion, the timeout time available for the deletion is 10 seconds. Because the minimum timeout is 30 seconds which is higher than former timeout time, the final timeout is 30 seconds.

* `load_straggler_wait_second`

  If the user estimates a large amount of data, so that the upper limit of 5 minutes is insufficient, the user can adjust the upper limit of timeout through this item, and the default value is 300.

  **The specific calculation rule of timeout(seconds)**

  `TIMEOUT = MIN(load_straggler_wait_second, MAX(30, tablet_delete_timeout_second * tablet_num))`

* `query_timeout`

  Because delete itself is an SQL command, the deletion statement is also limited by the session variables, and the timeout is also affected by the session value `query'timeout`. You can increase the value by `set query'timeout = xxx`.

**InPredicate configuration**

* `max_allowed_in_element_num_of_delete`

  If the user needs to take a lot of elements when using the in predicate, the user can adjust the upper limit of the allowed in elements number, and the default value is 1024.

## Show Delete History

The user can view the deletion completed in history through the show delete statement.

Syntax

```sql
SHOW DELETE [FROM db_name]
```

example

```sql
mysql> show delete from test_db;
+-----------+---------------+---------------------+-----------------+----------+
| TableName | PartitionName | CreateTime          | DeleteCondition | State    |
+-----------+---------------+---------------------+-----------------+----------+
| empty_tbl | p3            | 2020-04-15 23:09:35 | k1 EQ "1"       | FINISHED |
| test_tbl  | p4            | 2020-04-15 23:09:53 | k1 GT "80"      | FINISHED |
+-----------+---------------+---------------------+-----------------+----------+
2 rows in set (0.00 sec)
```

### Note

Unlike the Insert into command, delete cannot specify `label` manually. For the concept of label, see the [Insert Into](../import/import-way/insert-into-manual.md) documentation.

## More Help

For more detailed syntax used by **delete**, see the [delete](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/DELETE.md) command manual, You can also enter `HELP DELETE` in the Mysql client command line to get more help information
