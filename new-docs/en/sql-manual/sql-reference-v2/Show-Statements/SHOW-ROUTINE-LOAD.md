---
{
    "title": "SHOW-ROUTINE-LOAD",
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

## SHOW-ROUTINE-LOAD

### Name

SHOW ROUTINE LOAD

### Description

This statement is used to display the running status of the Routine Load job

grammar:

```sql
SHOW [ALL] ROUTINE LOAD [FOR jobName];
````

Result description:

````
                  Id: job ID
                Name: job name
          CreateTime: job creation time
           PauseTime: The last job pause time
             EndTime: Job end time
              DbName: corresponding database name
           TableName: corresponding table name
               State: job running state
      DataSourceType: Data source type: KAFKA
      CurrentTaskNum: The current number of subtasks
       JobProperties: Job configuration details
DataSourceProperties: Data source configuration details
    CustomProperties: custom configuration
           Statistic: Job running status statistics
            Progress: job running progress
                 Lag: job delay status
ReasonOfStateChanged: The reason for the job state change
        ErrorLogUrls: The viewing address of the filtered unqualified data
            OtherMsg: other error messages
````

* State

      There are the following 4 states:
      * NEED_SCHEDULE: The job is waiting to be scheduled
      * RUNNING: The job is running
      * PAUSED: The job is paused
      * STOPPED: The job has ended
      * CANCELLED: The job was canceled

* Progress

      For Kafka data sources, displays the currently consumed offset for each partition. For example, {"0":"2"} indicates that the consumption progress of Kafka partition 0 is 2.

*Lag

      For Kafka data sources, shows the consumption latency of each partition. For example, {"0":10} means that the consumption delay of Kafka partition 0 is 10.

### Example

1. Show all routine import jobs named test1 (including stopped or canceled jobs). The result is one or more lines.

   ```sql
   SHOW ALL ROUTINE LOAD FOR test1;
   ````

2. Show the currently running routine import job named test1

   ```sql
   SHOW ROUTINE LOAD FOR test1;
   ````

3. Display all routine import jobs (including stopped or canceled jobs) under example_db. The result is one or more lines.

   ```sql
   use example_db;
   SHOW ALL ROUTINE LOAD;
   ````

4. Display all running routine import jobs under example_db

   ```sql
   use example_db;
   SHOW ROUTINE LOAD;
   ````

5. Display the currently running routine import job named test1 under example_db

   ```sql
   SHOW ROUTINE LOAD FOR example_db.test1;
   ````

6. Displays all routine import jobs named test1 under example_db (including stopped or canceled jobs). The result is one or more lines.

   ```sql
   SHOW ALL ROUTINE LOAD FOR example_db.test1;
   ````

### Keywords

    SHOW, ROUTINE, LOAD

### Best Practice

