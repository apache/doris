---
{
"title": "CREATE-JOB",
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

## CREATE-JOB

### Name

CREATE JOB

### Description

Doris Job 是根据既定计划运行的任务，用于在特定时间或指定时间间隔触发预定义的操作，从而帮助我们自动执行一些任务。从功能上来讲，它类似于操作系统上的
定时任务（如：Linux 中的 cron、Windows 中的计划任务）。但 Doris 的 Job 调度可以精确到秒级。

Job 有两种类型：`ONE_TIME` 和 `BATCH`。其中 `ONE_TIME` 类型的 Job 会在指定的时间点触发，它主要用于一次性任务，而 `BATCH` 类型的 Job 会在指定的时间间隔内循环触发。
主要用于周期性执行的任务。

目前仅支持 ***ADMIN*** 权限执行此操作。

 语法：

```sql
CREATE
    JOB
    job_name
    ON SCHEDULE schedule
    [COMMENT 'string']
    DO sql_body;

schedule: {
   AT timestamp 
   | EVERY interval
    [STARTS timestamp ]
    [ENDS timestamp ]
}

interval:
    quantity { DAY | HOUR | MINUTE |
              WEEK | SECOND }
```

一条有效的 Job 语句必须包含以下内容

- 关键字 CREATE JOB 加上作业名称，它在一个 db 中标识唯一事件。
- ON SCHEDULE 子句，它指定了 Job 作业的类型和触发时间以及频率。
- DO 子句，它指定了 Job 作业触发时需要执行的操作。

这是一个最简单的例子：

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 MINUTE DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2;
```

该语句表示创建一个名为 my_job 的作业，每分钟执行一次，执行的操作是将 db2.tbl2 中的数据导入到 db1.tbl1 中。

SCHEDULER 语句用于定义作业的执行时间，频率以及持续时间，它可以指定一次性作业或者周期性作业。
- AT timestamp

  用于一次性事件，它指定事件仅在 给定的日期和时间执行一次 timestamp，该日期和时间必须包含日期和时间

- EVERY

   表示定期重复操作，它指定了作业的执行频率，关键字后面要指定一个时间间隔，该时间间隔可以是天、小时、分钟、秒、周。
    
    - interval
  
    用于指定作业执行频率，它可以是天、小时、分钟、秒、周。例如：` 1 DAY` 表示每天执行一次，` 1 HOUR` 表示每小时执行一次，` 1 MINUTE` 表示每分钟执行一次，` 1 WEEK` 表示每周执行一次，` 1 SECOND` 表示每秒执行一次。

    - STARTS timestamp

      用于指定作业的开始时间，如果没有指定，则从当前时间的下一个时间点开始执行。

    - ENDS timestamp

      用于指定作业的结束时间，如果没有指定，则表示永久执行。
- DO
 
   用于指定作业触发时需要执行的操作，目前支持所有的 ***INSERT,UPDATE*** 操作。后续我们会支持更多的操作。

### Example

创建一个一次性的 Job，它会在 2020-01-01 00:00:00 时执行一次，执行的操作是将 db2.tbl2 中的数据导入到 db1.tbl1 中。

```sql

CREATE JOB my_job ON SCHEDULE AT '2020-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2;

```

创建一个周期性的 Job，它会在 2020-01-01 00:00:00 时开始执行，每天执行一次，执行的操作是将 db2.tbl2 中的数据导入到 db1.tbl1 中。

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 DAY STARTS '2020-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 WHERE  create_time >=  days_add(now(),-1);
```

创建一个周期性的 Job，它会在 2020-01-01 00:00:00 时开始执行，每天执行一次，执行的操作是将 db2.tbl2 中的数据导入到 db1.tbl1 中，该 Job 在 2020-01-01 00:10:00 时结束。

```sql
CREATE JOB my_job ON SCHEDULER EVERY 1 DAY STARTS '2020-01-01 00:00:00' ENDS '2020-01-01 00:10:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 create_time >=  days_add(now(),-1);
```

### Keywords

    CREATE, JOB

### Best Practice