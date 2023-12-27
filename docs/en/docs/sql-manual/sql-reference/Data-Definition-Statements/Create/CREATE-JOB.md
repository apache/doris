---
{
"title": "CREATE-JOB",
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
## CREATE-JOB

### Name

CREATE JOB

### Description

Doris Job is a task that runs according to a predetermined plan and is used to trigger predefined actions at a specific time or at a specified time interval, thereby helping us to automate some tasks. Functionally, it is similar to the operating system's
Timing tasks (such as: cron in Linux, scheduled tasks in Windows). But Doris's job scheduling can be accurate to the second level.

There are two types of jobs: `ONE_TIME` and `BATCH`. Among them, the `ONE_TIME` type Job will be triggered at the specified time point, which is mainly used for one-time tasks, while the `BATCH` type Job will be triggered cyclically within the specified time interval.
Mainly used for tasks that are executed periodically.

Currently only ***ADMIN*** permissions are supported for this operation.

grammar:

```sql
CREATE
     job
     job_name
     ON SCHEDULE schedule
     [COMMENT 'string']
     DO sql_body;

schedule: {
    AT timestamp
    | EVERY interval
     [STARTS timestamp]
     [ENDS timestamp ]
}

interval:
     quantity { DAY | HOUR | MINUTE |
               WEEK | SECOND }
```

A valid Job statement must contain the following

- The keyword CREATE JOB plus the job name, which identifies unique events within a db.
- The ON SCHEDULE clause, which specifies the type of Job and when and how often to trigger it.
- The DO clause, which specifies the actions that need to be performed when the Job job is triggered.

Here is a minimal example:

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 MINUTE DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2;
```

This statement means to create a job named my_job to be executed every minute, and the operation performed is to import the data in db2.tbl2 into db1.tbl1.

The SCHEDULE statement is used to define the execution time, frequency and duration of the job, which can specify a one-time job or a periodic job.
- AT timestamp

  For one-time events, it specifies that the event is only executed once at a given date and time timestamp, which must contain the date and time

- EVERY

  Indicates that the operation is repeated periodically, which specifies the execution frequency of the job. After the keyword, a time interval should be specified, which can be days, hours, minutes, seconds, and weeks.

    - interval

  Used to specify the job execution frequency, it can be days, hours, minutes, seconds, weeks. For example: `1 DAY` means execute once a day, `1 HOUR` means execute once an hour, `1 MINUTE` means execute once every minute, `1 WEEK` means execute once a week, `1 SECOND` means execute once every second .

  - STARTS timestamp

       It is used to specify the start time of the job. If not specified, it will be executed from the next time point of the current time.

  - ENDS timestamp

       Used to specify the end time of the job, if not specified, it means permanent execution.
- DO

  It is used to specify the operation that needs to be performed when the job is triggered. Currently, all ***INSERT, UPDATE*** operations are supported. We will support more operations in the future.

### Example

Create a one-time job, which will be executed once at 2020-01-01 00:00:00, and the operation performed is to import the data in db2.tbl2 into db1.tbl1.

```sql

CREATE JOB my_job ON SCHEDULE AT '2020-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2;

```

Create a periodic Job, which will start to execute at 2020-01-01 00:00:00, once a day, and the operation is to import the data in db2.tbl2 into db1.tbl1.

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 DAY STARTS '2020-01-01 00:00:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 WHERE create_time >= days_add(now(),-1);
```

Create a periodic Job, which will start to execute at 2020-01-01 00:00:00, and execute once a day. The operation performed is to import the data in db2.tbl2 into db1.tbl1. This Job will be executed in 2020 Ends at -01-01 00:10:00.

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 DAY STARTS '2020-01-01 00:00:00' ENDS '2020-01-01 00:10:00' DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2 create_time >= days_add (now(),-1);
```

### Keywords

    CREATE, JOB

### Best Practice