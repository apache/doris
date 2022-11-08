---
{
"title": "ALTER-POLICY",
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

## ALTER-POLICY

### Name

ALTER STORAGE POLICY

### Description

该语句用于修改一个已有的冷热分离迁移策略。仅 root 或 admin 用户可以修改资源。
语法：
```sql
ALTER STORAGE POLICY  'policy_name'
PROPERTIES ("key"="value", ...);
```

### Example

1. 修改名为 cooldown_datetime冷热分离数据迁移时间点：
```sql
ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES("cooldown_datetime" = "2023-06-08 00:00:00");
```
2. 修改名为 cooldown_ttl的冷热分离数据迁移倒计时
```sql
ALTER STORAGE POLICY has_test_policy_to_alter PROPERTIES ("cooldown_ttl" = "10000");
```
### Keywords

```sql
ALTER, STORAGE, POLICY
```

### Best Practice
