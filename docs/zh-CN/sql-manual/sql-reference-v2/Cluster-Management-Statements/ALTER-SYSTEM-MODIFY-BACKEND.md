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

```sql
ALTER SYSTEM MODIFY BACKEND "host:heartbeat_port" SET ("key" = "value"[, ...]);
```

 说明：

1. host 可以是主机名或者ip地址
2. heartbeat_port 为该节点的心跳端口
3. 修改 BE 节点属性目前支持以下属性：

- tag.location：资源标签
- disable_query: 查询禁用属性
- disable_load: 导入禁用属性        

### Example

1. 修改 BE 的资源标签

   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("tag.location" = "group_a");
   ```

2. 修改 BE 的查询禁用属性
   
   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_query" = "true");
   ```
3. 修改 BE 的导入禁用属性
   
   ```sql
   ALTER SYSTEM MODIFY BACKEND "host1:9050" SET ("disable_load" = "true");
   ```
### Keywords

    ALTER, SYSTEM, ADD, BACKEND

### Best Practice

