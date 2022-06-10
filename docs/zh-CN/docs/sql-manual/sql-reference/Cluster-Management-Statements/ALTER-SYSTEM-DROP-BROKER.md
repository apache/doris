---
{
    "title": "ALTER-SYSTEM-DROP-BROKER",
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

## ALTER-SYSTEM-DROP-BROKER

### Name

ALTER SYSTEM DROP BROKER

### Description

该语句是删除 BROKER 节点，（仅限管理员使用）

语法：

```sql
删除所有 Broker
ALTER SYSTEM DROP ALL BROKER broker_name
删除某一个 Broker 节点
ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
```

### Example

1. 删除所有 Broker

   ```sql
   ALTER SYSTEM DROP ALL BROKER broker_name
   ```

2. 删除某一个 Broker 节点

   ```sql
   ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...];
   ```

### Keywords

    ALTER, SYSTEM, DROP, FOLLOWER

### Best Practice

