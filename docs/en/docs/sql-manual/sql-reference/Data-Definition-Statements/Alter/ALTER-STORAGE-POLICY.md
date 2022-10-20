---
{
"title": "ALTER-POLICY",
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

## ALTER-POLICY

### Name

ALTER STORAGE POLICY

### Description

This statement is used to modify an existing cold and hot separation migration strategy. Only root or admin users can modify resources.

```sql
ALTER STORAGE POLICY  'policy_name'
PROPERTIES ("key"="value", ...);
```

### Example

1. Modify the name to coolown_datetime Cold and hot separation data migration time point:
```sql
ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES("cooldown_datetime" = "2023-06-08 00:00:00");
```
2. Modify the name to coolown_countdown of hot and cold separation data migration of ttl
```sql
ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES ("cooldown_ttl" = "10000");
```
### Keywords

```sql
ALTER, STORAGE, POLICY
```

### Best Practice
