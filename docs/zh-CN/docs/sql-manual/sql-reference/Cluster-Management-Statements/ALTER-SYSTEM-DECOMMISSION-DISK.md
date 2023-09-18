---
{
    "title": "ALTER-SYSTEM-DECOMMISSION-DISK",
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

## ALTER-SYSTEM-DECOMMISSION-DISK

### Name

ALTER SYSTEM DECOMMISSION DISK

### Description

磁盘下线操作，用于安全下线磁盘，常用于换盘操作。该操作为异步操作。如果成功，所有在磁盘上的数据将会被迁移到其他节点。（仅管理员使用！）

语法：

```sql
ALTER SYSTEM DECOMMISSION DISK "path"[,"path2"...] ON BACKEND "host:heartbeat_port";
```

 说明：

1. host 可以是主机名或者ip地址
2.  heartbeat_port 为该节点的心跳端口
3. path 为节点上的磁盘绝对路径
4. 可以手动取消节点下线操作。详见 CANCEL DECOMMISSION DISK

### Example

1. 下线两个磁盘

     ```sql
      ALTER SYSTEM DECOMMISSION DISK "/home/disk1/palo.HDD", "/home/disk2/palo.HDD" ON BACKEND "host1:port";
     ```

### Keywords

    ALTER, SYSTEM, DECOMMISSION, DISK, ALTER SYSTEM

### Best Practice