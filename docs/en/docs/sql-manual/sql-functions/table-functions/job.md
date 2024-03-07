---
{
  "title": "JOB",
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

## `job`

### Name

<version since="dev">

job

</version>

### description

Table function, generates a temporary table of jobs, which allows you to view the information of jobs in the current Doris cluster.

This function is used in the FROM clause.

#### syntax

**parameter description**

| parameter | description | type   | required |
|:----------|:------------|:-------|:---------|
| type      | job type    | string | yes      |

the **type** supported types
- insert: insert into type job


##### Insert Job

The table schema of `tasks("type"="insert");` tvf：

```
mysql> desc  function jobs("type"="insert")
+-------------------+------+------+-------+---------+-------+
| Field             | Type | Null | Key   | Default | Extra |
+-------------------+------+------+-------+---------+-------+
| Id                | TEXT | No   | false | NULL    | NONE  |
| Name              | TEXT | No   | false | NULL    | NONE  |
| Definer           | TEXT | No   | false | NULL    | NONE  |
| ExecuteType       | TEXT | No   | false | NULL    | NONE  |
| RecurringStrategy | TEXT | No   | false | NULL    | NONE  |
| Status            | TEXT | No   | false | NULL    | NONE  |
| ExecuteSql        | TEXT | No   | false | NULL    | NONE  |
| CreateTime        | TEXT | No   | false | NULL    | NONE  |
| Comment           | TEXT | No   | false | NULL    | NONE  |
+-------------------+------+------+-------+---------+-------+
```

### example

```
mysql> select * from jobs("type"="insert") where Name='kris'\G
*************************** 1. row ***************************
               Id: 10069
             Name: kris
          Definer: root
      ExecuteType: RECURRING
RecurringStrategy: EVERY 3 SECOND STARTS 2023-12-06 14:44:47
           Status: RUNNING
       ExecuteSql: insert into address select * from mysqluser.orders.address where 'create_time' >=  days_add(now(),-1)
       CreateTime: 2023-12-06 14:44:44
          Comment: load mysql address datas
1 row in set (0.04 sec)
```

### keywords

        job, insert, schedule

