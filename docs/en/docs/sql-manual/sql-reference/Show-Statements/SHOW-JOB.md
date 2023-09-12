---
{
"title": "SHOW-JOB",
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

## SHOW-JOB

###Name

SHOW JOB

### Description

This statement is used to display the running status of the JOB job

grammar:

```sql
SHOW JOBS|JOB FOR job_name;
```

SHOW JOBS is used to display the running status of all jobs under the current DB, and SHOW JOB FOR job_name is used to display the running status of the specified job.

Result description:

```
                        Id: JobId
                        Db: database name
                      Name: Job name
                   Definer: create user
                  TimeZone: time zone
               ExecuteType: RECURRING means cyclic scheduling, that is, the scheduling time specified by the every statement, ONCE_TIME means a one-time task.
                 ExecuteAT: ONCE_TIME The execution start time of the task
           ExecuteInterval: Interval of periodic scheduling tasks
           ExecuteInterval: The time interval unit for periodic scheduling tasks
                    STARTS: The start time of periodic scheduled task settings
                      ENDS: The end time set by the periodic scheduling task
                    Status: Job status
     LastExecuteFinishTime: The time when the last execution was completed
                  ErrorMsg: error message
                   Comment: Remarks


```

* State

         There are the following 5 states:
         * RUNNING: running
         * PAUSED: Paused
         * STOPPED: end (manually triggered by the user)
         * FINISHED: Finished

### Example

1. Display all JOBs under the current DB.

     ```sql
     SHOW JOBS;
     ```

2. Display the JOB named test1

     ```sql
     SHOW JOB FOR test1;
     ```

###Keywords

     SHOW, JOB

### Best Practice