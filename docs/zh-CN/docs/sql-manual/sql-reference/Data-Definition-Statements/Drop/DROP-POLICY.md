---
{
    "title": "DROP-POLICY",
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

## DROP-POLICY

### Name

DROP POLICY

### Description

删除安全策略

#### 行安全策略

语法：

1. 删除行安全策略
```sql
DROP ROW POLICY test_row_policy_1 on table1 [FOR user];
```

2. 删除冷热数据存储策略
```sql
DROP STORAGE POLICY policy_name1
```

### Example

1. 删除 table1 的 test_row_policy_1

   ```sql
   DROP ROW POLICY test_row_policy_1 on table1
   ```

2. 删除 table1 作用于 test 的 test_row_policy_1 行安全策略

   ```sql
   DROP ROW POLICY test_row_policy_1 on table1 for test
   ```

3. 删除 policy_name1 对应的冷热数据存储策略
   ```sql
   DROP STORAGE POLICY policy_name1
   ```
### Keywords

    DROP, POLICY

### Best Practice

