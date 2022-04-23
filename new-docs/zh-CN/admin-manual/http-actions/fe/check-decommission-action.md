---
{
    "title": "Check Decommission Action",
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

# Check Decommission Action

## Request

`GET /api/check_decommission`

## Description

用于判断指定的BE是否能够被下线。比如判断节点下线后，剩余的节点是否能够满足空间要求和副本数要求等。
    
## Path parameters

无

## Query parameters

* `host_ports`

    指定一个多个BE，由逗号分隔。如：`ip1:port1,ip2:port2,...`。

    其中 port 为 BE 的 heartbeat port。

## Request body

无

## Response

返回可以被下线的节点列表

```
{
	"msg": "OK",
	"code": 0,
	"data": ["192.168.10.11:9050", "192.168.10.11:9050"],
	"count": 0
}
```
    
## Examples

1. 查看指定BE节点是否可以下线

    ```
    GET /api/check_decommission?host_ports=192.168.10.11:9050,192.168.10.11:9050
    
    Response:
    {
    	"msg": "OK",
    	"code": 0,
    	"data": ["192.168.10.11:9050"],
    	"count": 0
    }
    ```




