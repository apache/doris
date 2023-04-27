---
{
    "title": "Backends Action",
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

# Backends Action

## Request

```
GET /api/backends
```

## Description

Backends Action 返回 Backends 列表，包括 Backend 的 IP、PORT 等信息。
    
## Path parameters

无

## Query parameters

* `is_alive`

    可选参数。是否返回存活的 BE 节点。默认为false，即返回所有 BE 节点。

## Request body

无

## Response
    
```
{
    "msg": "success", 
    "code": 0, 
    "data": {
        "backends": [
            {
                "ip": "192.1.1.1",
                "http_port": 8040, 
                "is_alive": true
            }
        ]
    }, 
    "count": 0
}
```
