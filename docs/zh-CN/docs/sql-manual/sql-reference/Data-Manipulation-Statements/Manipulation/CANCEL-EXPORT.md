---
{
    "title": "CANCEL-EXPORT",
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

## CANCEL-EXPORT

### Name

<version since="dev">

CANCEL EXPORT

</version>

### Description

该语句用于撤销指定 label 的 EXPORT 作业，或者通过模糊匹配批量撤销 EXPORT 作业

```sql
CANCEL EXPORT
[FROM db_name]
WHERE [LABEL = "export_label" | LABEL like "label_pattern" | STATE = "PENDING/EXPORTING"]
```

### Example

1. 撤销数据库 example_db 上， label 为 `example_db_test_export_label` 的 EXPORT 作业

   ```sql
   CANCEL EXPORT
   FROM example_db
   WHERE LABEL = "example_db_test_export_label";
   ```

2. 撤销数据库 example*db 上， 所有包含 example* 的 EXPORT 作业。

   ```sql
   CANCEL EXPORT
   FROM example_db
   WHERE LABEL like "%example%";
   ```

3. 取消状态为 PENDING 的导入作业。

   ```sql
   CANCEL EXPORT
   FROM example_db
   WHERE STATE = "PENDING";
   ```

### Keywords

    CANCEL, EXPORT

### Best Practice

1. 只能取消处于 PENDING、EXPORTING 状态的未完成的导入作业。
2. 当执行批量撤销时，Doris 不会保证所有对应的 EXPORT 作业原子的撤销。即有可能仅有部分 EXPORT 作业撤销成功。用户可以通过 SHOW EXPORT 语句查看作业状态，并尝试重复执行 CANCEL EXPORT 语句。
