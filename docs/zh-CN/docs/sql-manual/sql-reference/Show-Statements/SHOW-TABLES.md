---
{
    "title": "SHOW-TABLES",
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

## SHOW-TABLES

### Name 

SHOW TABLES

### Description

该语句用于展示当前 db 下所有的 table

语法：

```sql
SHOW TABLES [LIKE]
```

说明:

1. LIKE：可按照表名进行模糊查询

### Example

 1. 查看DB下所有表
    
     ```sql
     mysql> show tables;
     +---------------------------------+
     | Tables_in_demo                  |
     +---------------------------------+
     | ads_client_biz_aggr_di_20220419 |
     | cmy1                            |
     | cmy2                            |
     | intern_theme                    |
     | left_table                      |
     +---------------------------------+
     5 rows in set (0.00 sec)
     ```

2. 按照表名进行模糊查询

   ```sql
   mysql> show tables like '%cm%';
   +----------------+
   | Tables_in_demo |
   +----------------+
   | cmy1           |
   | cmy2           |
   +----------------+
   2 rows in set (0.00 sec)
   ```

### Keywords

    SHOW, TABLES

### Best Practice

