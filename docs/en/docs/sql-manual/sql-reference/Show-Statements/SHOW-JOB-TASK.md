---
{
"title": "SHOW-JOB-TASK",
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
## SHOW-JOB-TASK

### Name

SHOW JOB TASK

### Description

This statement is used to display the list of execution results of JOB subtasks, and the latest 20 records will be kept by default.

grammar:

```sql
SHOW JOB TASKS FOR job_name;
```



Result description:

```
                           JobId: JobId
                           TaskId: TaskId
                        StartTime: start execution time
                          EndTime: end time
                           Status: status
                           Result: execution result
                           ErrMsg: error message
```

* State

         There are the following 2 states:
         * SUCCESS
         * FAIL

### Example

1. Display the task execution list of the JOB named test1

     ```sql
     SHOW JOB TASKS FOR test1;
     ```

###Keywords

     SHOW, JOB, TASK

### Best Practice