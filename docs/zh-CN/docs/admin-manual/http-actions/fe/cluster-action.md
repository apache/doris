---
{
    "title": "Cluster Action",
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

# Cluster Action

## Request

`GET /rest/v2/manager/cluster/cluster_info/conn_info`

## 集群连接信息

`GET /rest/v2/manager/cluster/cluster_info/conn_info`

### Description

用于获取集群http、mysql连接信息。

### Response

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "http": [
            "fe_host:http_ip"
        ],
        "mysql": [
            "fe_host:query_ip"
        ]
    },
    "count": 0
}
```
    
### Examples
    ```
    GET /rest/v2/manager/cluster/cluster_info/conn_info
    
    Response:
    {
        "msg": "success",
        "code": 0,
        "data": {
            "http": [
                "127.0.0.1:8030"
            ],
            "mysql": [
                "127.0.0.1:9030"
            ]
        },
        "count": 0
    }
    ```