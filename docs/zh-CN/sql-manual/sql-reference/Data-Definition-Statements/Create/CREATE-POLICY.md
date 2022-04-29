---
{
    "title": "CREATE-POLICY",
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

## CREATE-POLICY

### Name

CREATE POLICY

### Description

创建安全策略，explain 可以查看改写后的 SQL。

#### 行安全策略
语法：

```sql
CREATE ROW POLICY test_row_policy_1 ON test.table1 
AS {RESTRICTIVE|PERMISSIVE} TO root USING (id in (1, 2));
```

参数说明：

- RESTRICTIVE：将一组策略通过 AND 连接
- PERMISSIVE：将一组策略通过 OR 连接

### Example

1. 创建一组行安全策略

   ```sql
   CREATE ROW POLICY test_row_policy_1 ON test.table1 
   AS RESTRICTIVE TO root USING (c1 = 'a');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_2 ON test.table1 
   AS RESTRICTIVE TO root USING (c2 = 'b');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_3 ON test.table1 
   AS PERMISSIVE TO root USING (c3 = 'c');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_3 ON test.table1 
   AS PERMISSIVE TO root USING (c4 = 'd');
   ```

   当我们执行对 table1 的查询时被改写后的 sql 为

   ```sql
   select * from (select * from table1 where (c1 = 'a' and c2 = 'b') or c3 = 'c' or c4 = 'd' as policy_rewrite_table1_test_row_policy_1)
   ```

### Keywords

```text
CREATE, POLICY
```

### Best Practice

