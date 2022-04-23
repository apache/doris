---
{
    "title": "SHOW-INDEX",
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

## SHOW-INDEX

### Name

SHOW INDEX

### Description

 该语句用于展示一个表中索引的相关信息，目前只支持bitmap 索引

语法：

```SQL
SHOW INDEX[ES] FROM [db_name.]table_name [FROM database];
或者
SHOW KEY[S] FROM [db_name.]table_name [FROM database];
```

### Example

 1. 展示指定 table_name 的下索引
     
     ```SQL
      SHOW INDEX FROM example_db.table_name;
     ```

### Keywords

    SHOW, INDEX

### Best Practice

