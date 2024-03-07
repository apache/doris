---
{
    "title": "RESUME-MATERIALIZED-VIEW",
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

## RESUME-MATERIALIZED-VIEW

### Name

RESUME MATERIALIZED VIEW

### Description

This statement is used to temporarily restore the scheduled scheduling of materialized views

syntax:

```sql
RESUME MATERIALIZED VIEW JOB ON mvName=multipartIdentifier
```

### Example

1. Timed scheduling for restoring materialized view mv1

    ```sql
    RESUME MATERIALIZED VIEW mv1;
    ```
   
### Keywords

    RESUME, MATERIALIZED, VIEW

### Best Practice

