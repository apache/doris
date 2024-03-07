---
{
    "title": "REFRESH-MATERIALIZED-VIEW",
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

## REFRESH-MATERIALIZED-VIEW

### Name

REFRESH MATERIALIZED VIEW

### Description

This statement is used to manually refresh the specified asynchronous materialized view

syntax:

```sql
REFRESH MATERIALIZED VIEW mvName=multipartIdentifier (partitionSpec | COMPLETE)? 
```

Explanation:

Asynchronous refresh of data for a materialized view

### Example

1. Refresh materialized view mv1 (automatically calculate the partition to be refreshed)

    ```sql
    REFRESH MATERIALIZED VIEW mv1;
    ```

2. Refresh partition named p_19950801_19950901和p_19950901_19951001

    ```sql
    REFRESH MATERIALIZED VIEW mv1 partitions(p_19950801_19950901,p_19950901_19951001);
    ```
 
3. Force refresh of all materialized view data

    ```sql
    REFRESH MATERIALIZED VIEW mv1 complete;
    ```
   
### Keywords

    REFRESH, MATERIALIZED, VIEW

### Best Practice

