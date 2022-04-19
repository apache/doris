---
{
    "title": "DROP-DATABASE",
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

## DROP-DATABASE

### Name 

DOPR DATABASE

### Description

该语句用于删除数据库（database）
语法：    

```sql
DROP DATABASE [IF EXISTS] db_name [FORCE];
```

说明：

- 执行 DROP DATABASE 一段时间内，可以通过 RECOVER 语句恢复被删除的数据库。详见 [RECOVER](../../Data-Definition-Statements/Backup-and-Restore/RECOVER.html) 语句
- 如果执行 DROP DATABASE FORCE，则系统不会检查该数据库是否存在未完成的事务，数据库将直接被删除并且不能被恢复，一般不建议执行此操作

### Example

1. 删除数据库 db_test
    
    ```sql
    DROP DATABASE db_test;
    ```
    

### Keywords

    DROP, DATABASE

### Best Practice

