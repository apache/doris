---
{
    "title": "RESUME-ROUTINE-LOAD",
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

## RESUME-ROUTINE-LOAD

### Name

RESUME ROUTINE LOAD

### Description

Used to restart a suspended Routine Load job. The restarted job will continue to consume from the previously consumed offset.

```sql
RESUME [ALL] ROUTINE LOAD FOR job_name
````

### Example

1. Restart the routine import job named test1.

    ```sql
    RESUME ROUTINE LOAD FOR test1;
    ````

2. Restart all routine import jobs.

    ```sql
    RESUME ALL ROUTINE LOAD;
    ````

### Keywords

    RESUME, ROUTINE, LOAD

### Best Practice

