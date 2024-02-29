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

Doris Job is a task that runs according to a predefined schedule, triggering predefined actions at specific times or intervals to help automate certain tasks. In terms of functionality, it is similar to scheduled tasks on operating systems (e.g., cron in Linux, scheduled tasks in Windows).↳

There are two types of Jobs: ONE_TIME and RECURRING. The ONE_TIME type of Job triggers at a specified time point and is mainly used for one-time tasks, while the RECURRING type of Job triggers at specified time intervals and is used for periodically recurring tasks. The RECURRING type of Job can specify a start time and an end time using STARTS\ENDS. If the start time is not specified, the first execution time is set to the current time plus one scheduling period. If the end time is specified and the task completes execution by reaching the end time (or exceeds it, or the next execution period exceeds the end time), the Job status is updated to FINISHED, and no more Tasks are generated.

#### Job Status

A Job has four states (RUNNING, STOPPED, PAUSED, FINISHED), with the initial state being RUNNING. A Job in the RUNNING state generates Tasks based on the specified scheduling period. When a Job completes execution and reaches the end time, the status changes to FINISHED.

A Job in the RUNNING state can be paused, which means it will no longer generate Tasks.

A Job in the PAUSED state can be resumed by performing the RESUME operation, changing the state to RUNNING.

A Job in the STOPPED state is triggered by the user, which cancels the running Job and then deletes it.

A Job in the FINISHED state remains in the system for 24 hours and is deleted after that.

#### Task status

A Job only describes the job information, and the execution generates Tasks. The Task status can be PENDING, RUNNING, SUCCESS, FAILED, or CANCELED.

PENDING indicates that the trigger time has been reached but resources are awaited for running. Once resources are allocated, the status changes to RUNNING. When the execution is successful or fails, the status changes to SUCCESS or FAILED, respectively.

CANCELED indicates the cancellation status. The final status of a Task is persisted as SUCCESS or FAILED. Other statuses can be queried while the Task is running, but they become invisible after a restart. Only the latest 100 Task records are retained.

#### Permissions

Currently, only users with the ADMIN role can perform this operation.

#### Related Documentation

[PAUSE-JOB](../Alter/PAUSE-JOB.md),[RESUME-JOB](../Alter/RESUME-JOB.md),[DROP-JOB](../Drop/DROP-JOB.md), [TVF-JOB](../../../sql-functions/table-functions/job.md),
[TVF-TASKS](../../../sql-functions/table-functions/tasks)

### Grammar

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

- The keyword CREATE JOB plus the job name, which uniquely identifies the event within a database. The job name must be globally unique, and if a JOB with the same name already exists, an error will be reported. We reserve the inner_ prefix for internal use, so users cannot create names starting with ***inner_***.
- The ON SCHEDULE clause, which specifies the type of Job and when and how often to trigger it.
- The DO clause, which specifies the actions that need to be performed when the Job is triggered.

Here is a minimal example:

```sql
CREATE JOB my_job ON SCHEDULE EVERY 1 MINUTE DO INSERT INTO db1.tbl1 SELECT * FROM db2.tbl2;
```

This statement means to create a job named my_job to be executed every minute, and the operation performed is to import the data in db2.tbl2 into db1.tbl1.

The SCHEDULE statement is used to define the execution time, frequency and duration of the job, which can specify a one-time job or a periodic job.
- AT timestamp

Format: 'YYYY-MM-DD HH:MM:SS'. Used for one-time events, it specifies that the event should only be executed once at the given date and time. Once the execution is complete, the Job status changes to FINISHED.

- EVERY

  Indicates that the operation is repeated periodically, which specifies the execution frequency of the job. After the keyword, a time interval should be specified, which can be days, hours, minutes, seconds, and weeks.

  - interval

  Used to specify the Job execution frequency, which can be `day`, `hour`, `minute`, or `week`. For example, 1 `DAY` means the Job will run once every day, 1 `HOUR` means once every hour, 1 `MINUTE` means once every minute, and `1 The CREATE JOB statement is used to create a job in a database. A job is a task that can be scheduled to run at specific times or intervals to automate certain actions.

  - STARTS timestamp(optional)

    Format: 'YYYY-MM-DD HH:MM:SS'. It is used to specify the start time of the job. If not specified, the job starts executing from the next occurrence based on the current time. The start time must be greater than the current time.

  - ENDS timestamp(optional)

    Format: 'YYYY-MM-DD HH:MM:SS'. It is used to specify the end time of the job. If not specified, it means the job executes indefinitely. The end date must be greater than the current time. If a start time (↳STARTS) is specified, the end time must be greater than the start time.

- DO

  It is used to specify the operation that needs to be performed when the job is triggered. Currently, all ***INSERT*** operations are supported. We will support more operations in the future.

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

### CONFIG

#### fe.conf

- job_dispatch_timer_job_thread_num: Number of threads used for dispatching scheduled tasks. Default value is 2. If there are a large number of periodically executed tasks, this parameter can be increased.
- job_dispatch_timer_job_queue_size: Size of the queue used for storing scheduled tasks when there is task accumulation. Default value is 1024. If there are a large number of tasks triggered at the same time, this parameter can be increased. Otherwise, the queue may become full and submitting tasks will be blocked, causing subsequent tasks to be unable to submit.
- finished_job_cleanup_threshold_time_hour: Time threshold, in hours, for cleaning up completed tasks. Default value is 24 hours.
- job_insert_task_consumer_thread_num: Number of threads used for executing Insert tasks. The value should be greater than 0, otherwise the default value is 5.

### Best Practice

- Properly manage Jobs to avoid triggering a large number of Jobs simultaneously, which can lead to task accumulation and affect the normal operation of the system.
- Set the execution interval of tasks within a reasonable range, ensuring that it is at least greater than the task execution time.

### Keywords

    CREATE, JOB, SCHEDULE