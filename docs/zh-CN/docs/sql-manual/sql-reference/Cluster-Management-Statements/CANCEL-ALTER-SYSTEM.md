---
{
    "title": "CANCEL-ALTER-SYSTEM",
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

## CANCEL-ALTER-SYSTEM

### Name

CANCEL DECOMMISSION

### Description

该语句用于撤销一个节点下线操作。（仅管理员使用！）

语法：

- 通过 host 和 port 查找 backend

```sql
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
```

- 通过 backend_id 查找 backend

```sql
CANCEL DECOMMISSION BACKEND "id1","id2","id3...";
```

### Example

 1. 取消两个节点的下线操作：
    
      ```sql
       CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
      ```

 2. 取消 backend_id 为 1 的节点的下线操作：

    ```sql
    CANCEL DECOMMISSION BACKEND "1","2";
    ```

### Keywords

    CANCEL, DECOMMISSION, CANCEL ALTER

### Best Practice

