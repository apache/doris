---
{
    "title": "CANCEL-LOAD",
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

## CANCEL-LOAD

### Name

CANCEL LOAD

### Description

该语句用于撤销指定 label 的导入作业。或者通过模糊匹配批量撤销导入作业

```sql
CANCEL LOAD
[FROM db_name]
WHERE [LABEL = "load_label" | LABEL like "label_pattern" | STATE = "PENDING/ETL/LOADING"]
```

注：1.2.0 版本之后支持根据 State 取消作业。

### Example

1. 撤销数据库 example_db 上， label 为 `example_db_test_load_label` 的导入作业

   ```sql
   CANCEL LOAD
   FROM example_db
   WHERE LABEL = "example_db_test_load_label";
   ```

2. 撤销数据库 example*db 上， 所有包含 example* 的导入作业。

   ```sql
   CANCEL LOAD
   FROM example_db
   WHERE LABEL like "example_";
   ```

<version since="1.2.0">

3. 取消状态为 LOADING 的导入作业。

   ```sql
   CANCEL LOAD
   FROM example_db
   WHERE STATE = "loading";
   ```

</version>

### Keywords

    CANCEL, LOAD

### Best Practice

1. 只能取消处于 PENDING、ETL、LOADING 状态的未完成的导入作业。
2. 当执行批量撤销时，Doris 不会保证所有对应的导入作业原子的撤销。即有可能仅有部分导入作业撤销成功。用户可以通过 SHOW LOAD 语句查看作业状态，并尝试重复执行 CANCEL LOAD 语句。
