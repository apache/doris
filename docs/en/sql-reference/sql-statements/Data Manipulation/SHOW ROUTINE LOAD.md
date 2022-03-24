---
{
    "title": "SHOW ROUTINE LOAD",
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

# SHOW ROUTINE LOAD
## description
    This statement is used to show the running status of the Routine Load job
    grammar:
        SHOW [ALL] ROUTINE LOAD [FOR jobName] [LIKE pattern];

    Result description:

                  Id: Job ID
                Name: job name
          CreateTime: Job creation time
           PauseTime: Last job pause time
             EndTime: The end time of the job
              DbName: corresponding database name
           TableName: Corresponding table name
               State: job running status
      DataSourceType: Data source type: KAFKA
      CurrentTaskNum: current number of subtasks
       JobProperties: Job configuration details
DataSourceProperties: Data source configuration details
    CustomProperties: custom configuration
           Statistic: job running status statistics
            Progress: Job running progress
                 Lag: job delay status
ReasonOfStateChanged: Reason of job status change
        ErrorLogUrls: The viewing address of the filtered data with unqualified quality
            OtherMsg: Other error messages

    * State

        There are the following 4 states:

        * NEED_SCHEDULE: The job is waiting to be scheduled
        * RUNNING: The job is running
        * PAUSED: The job is suspended
        * STOPPED: The job has ended
        * CANCELLED: The job has been cancelled

    * Progress

        For Kafka data sources, the offset currently consumed by each partition is displayed. For example, {"0":"2"} means that the consumption progress of Kafka partition 0 is 2.

    * Lag

        For Kafka data sources, the consumption delay of each partition is displayed. For example, {"0":10} means that the consumption delay of Kafka partition 0 is 10.

## example

1. Show all routine import jobs named test 1 (including stopped or cancelled jobs). The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR test1;

2. Show the current running routine load job named test1

SHOW ROUTINE LOAD FOR test1;

3. Display all routine import jobs (including stopped or cancelled jobs) under example_db. The result is one or more lines.

use example_db;
SHOW ALL ROUTINE LOAD;

4. Display all running routine import jobs under example_db

use example_db;
SHOW ROUTINE LOAD;

5. Display the current running routine import job named test1 under example_db

SHOW ROUTINE LOAD FOR example_db.test1;

6. Display all routine import jobs named test1 (including stopped or cancelled jobs) under example_db. The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR example_db.test1;

7. Show the current running routine load jobs under example_db with name match test1

use example_db;
SHOW ROUTINE LOAD LIKE "%test1%";

## keyword
SHOW,ROUTINE,LOAD
