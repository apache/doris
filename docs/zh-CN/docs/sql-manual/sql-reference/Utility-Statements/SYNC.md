---
{
    "title": "SYNC",
    "language": "zh-CN"
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

用于fe非master节点同步元数据。doris只有master节点才能写fe元数据，其他fe节点写元数据的操作都会转发到master节点。在master完成元数据写入操作后，非master节点replay元数据会有短暂的延迟，可以使用该语句同步元数据。

语法：

```sql
SYNC;
```

### Example

1. 同步元数据

    ```sql
    SYNC;
    ```

### Keywords

    SYNC

### Best Practice

