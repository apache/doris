---
{
    "title": "ALTER-SYSTEM-DROP-FOLLOWER",
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

## ALTER-SYSTEM-DROP-FOLLOWER

### Name

ALTER SYSTEM DROP FOLLOWER

### Description

改语句是增加 FRONTEND 的 FOLLOWER 角色的节点,（仅管理员使用！）

语法：

```sql
ALTER SYSTEM DROP FOLLOWER "follower_host:edit_log_port"
```

说明：

1. host 可以是主机名或者ip地址
2. edit_log_port : edit_log_port 在其配置文件 fe.conf

### Example

1. 添加一个 FOLLOWER节点

   ```sql
   ALTER SYSTEM DROP FOLLOWER "host_ip:9010"
   ```

### Keywords

    ALTER, SYSTEM, DROP, FOLLOWER

### Best Practice

