---
{
    "title": "Check Decommission Action",
    "language": "en"
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

Used to determine whether the specified BE can be decommissioned. For example, after the node being decommissioned, whether the remaining nodes can meet the space requirements and the number of replicas.
    
## Path parameters

None

## Query parameters

* `host_ports`

    Specify one or more BEs, separated by commas. Such as: `ip1:port1,ip2:port2,...`.

    Where port is the heartbeat port of BE.

## Request body

None

## Response

Return a list of nodes that can be decommissioned

```
{
	"msg": "OK",
	"code": 0,
	"data": ["192.168.10.11:9050", "192.168.10.11:9050"],
	"count": 0
}
```
    
## Examples

1. Check whether the specified BE node can be decommissioned

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




