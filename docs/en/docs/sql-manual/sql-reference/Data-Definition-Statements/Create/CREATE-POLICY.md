---
{
    "title": "CREATE-POLICY",
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

## CREATE-POLICY

### Name

CREATE POLICY

### Description

Create policies,such as:
1. Create security policies(ROW POLICY) and explain to view the rewritten SQL.
2. Create storage migration policy(STORAGE POLICY), used for cold and hot data transform

#### Grammar:

1. ROW POLICY
```sql
CREATE ROW POLICY test_row_policy_1 ON test.table1 
AS {RESTRICTIVE|PERMISSIVE} TO test USING (id in (1, 2));
```

illustrate：

- filterType：It is usual to constrict a set of policies through AND. PERMISSIVE to constrict a set of policies through OR
- Configure multiple policies. First, merge the RESTRICTIVE policy with the PERMISSIVE policy
- It is connected with AND between RESTRICTIVE AND PERMISSIVE
- It cannot be created for users root and admin

2. STORAGE POLICY
```sql
CREATE STORAGE POLICY test_storage_policy_1
PROPERTIES ("key"="value", ...);
```
illustrate：
- PROPERTIES has such keys:
    1. storage_resource：storage resource name for policy
    2. cooldown_datetime：cool down time for tablet, can't be set with cooldown_ttl.
    3. cooldown_ttl：hot data stay time. The time cost between the time of tablet created and
            the time of migrated to cold data, formatted as：
        1d：1 day
        1h：1 hour
        50000: 50000 second

### Example

1. Create a set of row security policies

   ```sql
   CREATE ROW POLICY test_row_policy_1 ON test.table1 
   AS RESTRICTIVE TO test USING (c1 = 'a');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_2 ON test.table1 
   AS RESTRICTIVE TO test USING (c2 = 'b');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_3 ON test.table1 
   AS PERMISSIVE TO test USING (c3 = 'c');
   ```
   ```sql
   CREATE ROW POLICY test_row_policy_3 ON test.table1 
   AS PERMISSIVE TO test USING (c4 = 'd');
   ```

   When we execute the query on Table1, the rewritten SQL is

   ```sql
   select * from (select * from table1 where c1 = 'a' and c2 = 'b' or c3 = 'c' or c4 = 'd')
   ```

2. Create policy for storage
    1. Create policy on cooldown_datetime
    ```sql
    CREATE STORAGE POLICY testPolicy
    PROPERTIES(
      "storage_resource" = "s3",
      "cooldown_datetime" = "2022-06-08 00:00:00"
    );
    ```
    2. Create policy on cooldown_ttl
    ```sql
    CREATE STORAGE POLICY testPolicy
    PROPERTIES(
      "storage_resource" = "s3",
      "cooldown_ttl" = "1d"
    );
    ```

### Keywords

    CREATE, POLICY

### Best Practice

