---
{
    "title": "ALTER-SYSTEM-ADD-BACKEND",
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

## ALTER-SYSTEM-ADD-BACKEND

### Name

ALTER SYSTEM ADD BACKEND

### Description

该语句用于操作一个系统内的节点。（仅管理员使用！）

语法：

```sql
1) 增加节点(不使用多租户功能则按照此方法添加)
   ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
2) 增加空闲节点(即添加不属于任何cluster的BACKEND)
   ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
3) 增加节点到某个cluster
   ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...];
```

 说明：

1. host 可以是主机名或者ip地址
2.  heartbeat_port 为该节点的心跳端口
3. 增加和删除节点为同步操作。这两种操作不考虑节点上已有的数据，节点直接从元数据中删除，请谨慎使用。

### Example

 1. 增加一个节点
    
     ```sql
    ALTER SYSTEM ADD BACKEND "host:port";
    ```

 1. 增加一个空闲节点
    
    ```sql
    ALTER SYSTEM ADD FREE BACKEND "host:port";
    ```

### Keywords

    ALTER, SYSTEM, ADD, BACKEND

### Best Practice

