---
{
    "title": "SHOW-MTMV-TASK",
    "language": "zh-CN"
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

## SHOW-MTMV-TASK

### Name

SHOW MTMV TASK

### Description

该语句用于展示多表物化视图task列表。

语法：

```sql
SHOW MTMV TASK
SHOW MTMV TASK FOR taskId
SHOW MTMV TASK FROM dbName
SHOW MTMV TASK ON mvName
SHOW MTMV TASK FROM dbName ON mvName
```

说明：

1. 如果指定 FROM 子句，展示DB下所有多表物化视图的TASK。

2. 如果指定 FOR 子句，展示某个具体的TASK。

3. 如果指定 ON 子句，展示指定多表物化视图下的所有TASK

返回结果说明：

```sql
mysql> show mtmv task\G
*************************** 1. row ***************************
    TaskId: 933a49ca-2a28-4193-84a4-12f9f8d134c5
   JobName: mv1_85802d28-c3e3-48dc-8bf6-50c99b2b0eca
    DBName: default_cluster:zd
    MVName: mv1
     Query: SELECT `k1` AS `k1`, `k3` AS `k3` FROM `default_cluster:zd`.`user1`
      User: root
  Priority: 0
RetryTimes: 0
     State: SUCCESS
   Message: {'label':'insert_d14c5317fe8c4511_8466740877f5ed67', 'status':'VISIBLE', 'txnId':'6008'', 'rows':'1'}
 ErrorCode: 0
CreateTime: 2023-06-07T18:01:34
ExpireTime: 2023-06-08T18:01:34
FinishTime: 2023-06-07T18:01:34
1 row in set (0.00 sec)
```

* TaskId：Task唯一ID.
* JobName：所属JOB名称.
* DBName： db名称
* MVName： 物化视图名称
* Query： 用于构建物化视图的查询语句
* User： 创建物化视图的用户
* Priority： 优先级
* RetryTimes： 重试次数
* State： 执行状态
* Message： 执行信息
* ErrorCode： 错误码
* CreateTime： 创建时间
* ExpireTime： 过期时间
* FinishTime： 完成时间


### Example

1. 展示所有TASK

    ```sql
    SHOW MTMV TASK;
    ```

    ```
    *************************** 1. row ***************************
   TaskId: 933a49ca-2a28-4193-84a4-12f9f8d134c5
   JobName: mv1_85802d28-c3e3-48dc-8bf6-50c99b2b0eca
   DBName: default_cluster:zd
   MVName: mv1
   Query: SELECT `k1` AS `k1`, `k3` AS `k3` FROM `default_cluster:zd`.`user1`
   User: root
   Priority: 0
   RetryTimes: 0
   State: SUCCESS
   Message: {'label':'insert_d14c5317fe8c4511_8466740877f5ed67', 'status':'VISIBLE', 'txnId':'6008'', 'rows':'1'}
   ErrorCode: 0
   CreateTime: 2023-06-07T18:01:34
   ExpireTime: 2023-06-08T18:01:34
   FinishTime: 2023-06-07T18:01:34
   1 row in set (0.00 sec)
    ```    

   
### Keywords

    SHOW, MTMV, TASK

### Best Practice

