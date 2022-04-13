---
{
    "title": "SHOW-TABLE-STATUS",
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

## SHOW-TABLE-STATUS

### Name

SHOW TABLE STATUS

### Description

该语句用于查看 Table 的一些信息。

语法：

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
```

说明：

1. 该语句主要用于兼容 MySQL 语法，目前仅显示 Comment 等少量信息

### Example

 1. 查看当前数据库下所有表的信息

     ```sql
SHOW TABLE STATUS;
    ```

 1. 查看指定数据库下，名称包含 example 的表的信息

     ```sql
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```

### Keywords

    SHOW, TABLE, STATUS

### Best Practice

