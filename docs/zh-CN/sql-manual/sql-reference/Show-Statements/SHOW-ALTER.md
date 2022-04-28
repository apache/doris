---
{
    "title": "SHOW-ALTER",
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

## SHOW-ALTER

### Name

SHOW ALTER

### Description

该语句用于展示当前正在进行的各类修改任务的执行情况

```sql
SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];
```

说明：

1. TABLE COLUMN：展示修改列的 ALTER 任务
2. 支持语法[WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
3.  TABLE ROLLUP：展示创建或删除 ROLLUP index 的任务
4.  如果不指定 db_name，使用当前默认 db
5.  CLUSTER: 展示集群操作相关任务情况（仅管理员使用！待实现...）

### Example

1. 展示默认 db 的所有修改列的任务执行情况

   ```sql
    SHOW ALTER TABLE COLUMN;
   ```

2. 展示某个表最近一次修改列的任务执行情况

   ```sql
   SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
   ```

3. 展示指定 db 的创建或删除 ROLLUP index 的任务执行情况

   ```sql
   SHOW ALTER TABLE ROLLUP FROM example_db;
   ```

4. 展示集群操作相关任务（仅管理员使用！待实现...）

   ```
   SHOW ALTER CLUSTER;
   ```

### Keywords

    SHOW, ALTER

### Best Practice
