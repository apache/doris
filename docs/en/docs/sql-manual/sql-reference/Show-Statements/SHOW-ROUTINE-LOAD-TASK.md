---
{
    "title": "SHOW-ROUTINE-LOAD-TASK",
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

## SHOW-ROUTINE-LOAD-TASK

### Name

SHOW ROUTINE LOAD TASK

### Description

View the currently running subtasks of a specified Routine Load job.



```sql
SHOW ROUTINE LOAD TASK
WHERE JobName = "job_name";
````

The returned results are as follows:

````text
              TaskId: d67ce537f1be4b86-abf47530b79ab8e6
               TxnId: 4
           TxnStatus: UNKNOWN
               JobId: 10280
          CreateTime: 2020-12-12 20:29:48
    ExecuteStartTime: 2020-12-12 20:29:48
             Timeout: 20
                BeId: 10002
DataSourceProperties: {"0":19}
````

- `TaskId`: The unique ID of the subtask.
- `TxnId`: The import transaction ID corresponding to the subtask.
- `TxnStatus`: The import transaction status corresponding to the subtask. When TxnStatus is null, it means that the subtask has not yet started scheduling.
- `JobId`: The job ID corresponding to the subtask.
- `CreateTime`: The creation time of the subtask.
- `ExecuteStartTime`: The time when the subtask is scheduled to be executed, usually later than the creation time.
- `Timeout`: Subtask timeout, usually twice the `max_batch_interval` set by the job.
- `BeId`: The ID of the BE node executing this subtask.
- `DataSourceProperties`: The starting offset of the Kafka Partition that the subtask is ready to consume. is a Json format string. Key is Partition Id. Value is the starting offset of consumption.

### Example

1. Display the subtask information of the routine import task named test1.

   ```sql
   SHOW ROUTINE LOAD TASK WHERE JobName = "test1";
   ````

### Keywords

    SHOW, ROUTINE, LOAD, TASK

### Best Practice

With this command, you can view how many subtasks are currently running in a Routine Load job, and which BE node is running on.

