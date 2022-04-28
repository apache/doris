---
{
    "title": "DROP-RESOURCE",
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

## DROP-RESOURCE

### Name

DROP RESOURCE

### Description

该语句用于删除一个已有的资源。仅 root 或 admin 用户可以删除资源。
语法：

```sql
DROP RESOURCE 'resource_name'
```

注意：正在使用的 ODBC/S3 资源无法删除。

### Example

1. 删除名为 spark0 的 Spark 资源：
    
    ```sql
    DROP RESOURCE 'spark0';
    ```

### Keywords

    DROP, RESOURCE

### Best Practice

