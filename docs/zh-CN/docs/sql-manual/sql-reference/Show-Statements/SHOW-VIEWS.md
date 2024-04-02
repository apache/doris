---
{ 
    "title": "SHOW-VIEWS",
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

## SHOW-VIEWS

### Name 

SHOW VIEWS

### Description

该语句用于展示当前 db 下所有的 logical view

语法：

```sql
SHOW [FULL] VIEWS [LIKE] | [WHERE where_condition]
```

说明:

1. LIKE：可按照表名进行模糊查询

### Example

 1. 查看DB下所有逻辑视图

    ```sql
    MySQL [test]> show views;
    +----------------+
    | Tables_in_test |
    +----------------+
    | t1_view        |
    | t2_view        |
    +----------------+
    2 rows in set (0.00 sec)
    ```

2. 按照VIEW名进行模糊查询

    ```sql
    MySQL [test]> show views like '%t1%';
    +----------------+
    | Tables_in_test |
    +----------------+
    | t1_view        |
    +----------------+
    1 row in set (0.01 sec)
    ```

### Keywords

    SHOW, VIEWS

### Best Practice
