---
{
    "title": "SYNC",
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

## SYNC

### Name

SYNC

### Description

Used to synchronize metadata for fe non-master nodes. doris only master node can write fe metadata, other fe nodes write metadata operations will be forwarded to master. After master finishes metadata writing operation, there will be a short delay for non-master nodes to replay metadata, you can use this statement to synchronize metadata.

grammar:

```sql
SYNC;
```

### Example

1. Synchronized metadata:

    ```sql
    SYNC;
    ```

### Keywords

    SYNC

### Best Practice

