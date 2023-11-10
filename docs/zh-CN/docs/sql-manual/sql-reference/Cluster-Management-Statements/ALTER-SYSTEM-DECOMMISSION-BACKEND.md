---
{
    "title": "ALTER-SYSTEM-DECOMMISSION-BACKEND",
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

## ALTER-SYSTEM-DECOMMISSION-BACKEND

### Name

ALTER SYSTEM DECOMMISSION BACKEND

### Description

节点下线操作用于安全下线节点。该操作为异步操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线（仅管理员使用！）

语法：

- 通过 host 和 port 查找 backend

```sql
ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
```

- 通过 backend_id 查找 backend

```sql
ALTER SYSTEM DECOMMISSION BACKEND "id1","id2"...;
```

 说明：

1. host 可以是主机名或者ip地址
2.  heartbeat_port 为该节点的心跳端口
3. 节点下线操作用于安全下线节点。该操作为异步操作。如果成功，节点最终会从元数据中删除。如果失败，则不会完成下线。
4. 可以手动取消节点下线操作。详见 CANCEL DECOMMISSION

### Example

1. 下线两个节点

     ```sql
      ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";
     ```
   
    ```sql
      ALTER SYSTEM DECOMMISSION BACKEND "id1", "id2";
    ```

### Keywords

    ALTER, SYSTEM, DECOMMISSION, BACKEND, ALTER SYSTEM

### Best Practice

