---
{
    "title": "DROP-MATERIALIZED-VIEW",
    "language": "en"
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

drop policy for row or storage

#### ROW POLICY

Grammar：

1. Drop row policy
```sql
DROP ROW POLICY test_row_policy_1 on table1 [FOR user];
```

2. Drop storage policy
```sql
DROP STORAGE POLICY policy_name1
```

### Example

1. Drop the row policy for table1 named test_row_policy_1

   ```sql
   DROP ROW POLICY test_row_policy_1 on table1
   ```

2. Drop the row policy for table1 using by user test

   ```sql
   DROP ROW POLICY test_row_policy_1 on table1 for test
   ```

3. Drop the storage policy named policy_name1
```sql
DROP STORAGE POLICY policy_name1
```

### Keywords

    DROP, POLICY

### Best Practice

