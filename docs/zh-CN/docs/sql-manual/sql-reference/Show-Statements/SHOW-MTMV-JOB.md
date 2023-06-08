---
{
    "title": "SHOW-MTMV-JOB",
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

## SHOW-MTMV-JOB

### Name

SHOW MTMV JOB

### Description

该语句用于展示多表物化视图job列表。

语法：

```sql
SHOW MTMV JOB
SHOW MTMV JOB FOR jobName
SHOW MTMV JOB FROM dbName
SHOW MTMV JOB ON mvName
SHOW MTMV JOB FROM dbName ON mvName
```

说明：

1. 如果指定 FROM 子句，展示DB下所有多表物化视图的JOB。

2. 如果指定 FOR 子句，展示某个具体的JOB。

3. 如果指定 ON 子句，展示指定多表物化视图下的所有JOB

返回结果说明：

```sql
mysql> show mtmv job\G
*************************** 1. row ***************************
            Id: 15012
          Name: mv1_85802d28-c3e3-48dc-8bf6-50c99b2b0eca
   TriggerMode: PERIODICAL
      Schedule: START 2023-05-16T21:33 EVERY(1 DAYS)
        DBName: default_cluster:zd
        MVName: mv1
         Query: SELECT `k1` AS `k1`, `k3` AS `k3` FROM `default_cluster:zd`.`user1`
          User: root
   RetryPolicy: NEVER
         State: ACTIVE
    CreateTime: 2023-06-05T17:56:23
    ExpireTime: 1970-01-01T07:59:59
LastModifyTime: 2023-06-07T18:01:35
1 row in set (0.00 sec)
```

* Id：JOB唯一ID.
* Name：JOB名称.
* TriggerMode：JOB触发类型，分为PERIODICAL和ONCE.
* Schedule： 调度策略
* DBName： db名称
* MVName： 物化视图名称
* Query： 用于构建物化视图的查询语句
* User： 创建物化视图的用户
* State： JOB状态，ACTIVE代表活跃，COMPLETE代表完成
* CreateTime： 创建时间
* ExpireTime： 过期时间
* LastModifyTime： 最后更新时间


### Example

1. 展示所有JOB

    ```sql
    SHOW MTMV JOB;
    ```

     ```
    *************************** 1. row ***************************
            Id: 15012
          Name: mv1_85802d28-c3e3-48dc-8bf6-50c99b2b0eca
   TriggerMode: PERIODICAL
      Schedule: START 2023-05-16T21:33 EVERY(1 DAYS)
        DBName: default_cluster:zd
        MVName: mv1
         Query: SELECT `k1` AS `k1`, `k3` AS `k3` FROM `default_cluster:zd`.`user1`
          User: root
   RetryPolicy: NEVER
         State: ACTIVE
    CreateTime: 2023-06-05T17:56:23
    ExpireTime: 1970-01-01T07:59:59
    LastModifyTime: 2023-06-07T18:01:35
    1 row in set (0.00 sec)
    ```
   
### Keywords

    SHOW, MTMV, JOB

### Best Practice

