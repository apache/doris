---
{
    "title": "REFRESH-MATERIALIZED-VIEW",
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

## REFRESH-MATERIALIZED-VIEW

### Name

REFRESH MATERIALIZED VIEW

### Description

该语句用于手动刷新指定多表物化视图

语法：

```sql
REFRESH MATERIALIZED VIEW table_name
REFRESH MATERIALIZED VIEW table_name COMPLETE
```

说明：

1. 异步刷新某个物化视图的数据

2. 目前是否加COMPLETE语义相同，因为默认刷新方式就是COMPLETE，将来会增加其他刷新方式

### Example

1. 刷新物化视图mv1

    ```sql
    REFRESH MATERIALIZED VIEW mv1;
    ```
   
### Keywords

    REFRESH, MATERIALIZED, VIEW

### Best Practice

