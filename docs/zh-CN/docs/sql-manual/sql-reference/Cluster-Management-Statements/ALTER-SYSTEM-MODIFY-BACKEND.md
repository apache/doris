---
{
    "title": "ALTER-SYSTEM-MODIFY-BACKEND",
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

## ALTER-SYSTEM-MODIFY-BACKEND

### Name

ALTER SYSTEM MKDIFY BACKEND

### Description

修改 BE 节点属性（仅管理员使用！）

语法：

- 通过 host 和 port 查找 backend

```sql
ALTER SYSTEM MODIFY BACKEND "host:heartbeat_port" SET ("key" = "value"[, ...]);
```

- 通过 backend_id 查找 backend

```sql
ALTER SYSTEM MODIFY BACKEND "id1" SET ("key" = "value"[, ...]);
````

 说明：

1. host 可以是主机名或者ip地址
2. heartbeat_port 为该节点的心跳端口
3. 修改 BE 节点属性目前支持以下属性：

- tag.xxx：资源标签
- disable_query: 查询禁用属性
- disable_load: 导入禁用属性        

注：
1. 可以给一个 Backend 设置多种资源标签。但必须包含 "tag.location"。

### Example

1. 修改 BE 的资源标签

   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("tag.location" = "group_a");
   ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("tag.location" = "group_a", "tag.compute" = "c1");
   ```

   ```sql
   ALTER SYSTEM MODIFY BACKEND "id1" SET ("tag.location" = "group_a");
   ALTER SYSTEM MODIFY BACKEND "id1" SET ("tag.location" = "group_a", "tag.compute" = "c1");
   ````

2. 修改 BE 的查询禁用属性
   
   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("disable_query" = "true");
   ```

    ```sql
   ALTER SYSTEM MODIFY BACKEND "id1" SET ("disable_query" = "true");
    ````   

3. 修改 BE 的导入禁用属性
   
   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("disable_load" = "true");
   ```

    ```sql
   ALTER SYSTEM MODIFY BACKEND "id1" SET ("disable_load" = "true");
    ````   

### Keywords

    ALTER, SYSTEM, ADD, BACKEND, ALTER SYSTEM

### Best Practice

