---
{
    "title": "Bootstrap Action",
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

# Bootstrap Action

## Request

`GET /api/bootstrap`

## Description

用于判断FE是否启动完成。当不提供任何参数时，仅返回是否启动成功。如果提供了 `token` 和 `cluster_id`，则返回更多详细信息。
    
## Path parameters

无

## Query parameters

* `cluster_id`

    集群id。可以在 `doris-meta/image/VERSION` 文件中查看。
    
* `token`

    集群token。可以在 `doris-meta/image/VERSION` 文件中查看。

## Request body

无

## Response

* 不提供参数

    ```
    {
    	"msg": "OK",
    	"code": 0,
    	"data": null,
    	"count": 0
    }
    ```
    
    code 为 0 表示FE节点启动成功。非 0 的错误码表示其他错误。
    
* 提供 `token` 和 `cluster_id`

    ```
    {
    	"msg": "OK",
    	"code": 0,
    	"data": {
    		"queryPort": 9030,
    		"rpcPort": 9020,
            "arrowFlightSqlPort": 9040,
    		"maxReplayedJournal": 17287
    	},
    	"count": 0
    }
    ```
    
    * `queryPort` 是 FE 节点的 MySQL 协议端口。
    * `rpcPort` 是 FE 节点的 thrift RPC 端口。
    * `maxReplayedJournal` 表示 FE 节点当前回放的最大元数据日志id。
    * `arrowFlightSqlPort` 是 FE 节点的 Arrow Flight SQL 协议端口。
    
## Examples

1. 不提供参数

    ```
    GET /api/bootstrap

    Response:
    {
    	"msg": "OK",
    	"code": 0,
    	"data": null,
    	"count": 0
    }
    ```
    
2. 提供 `token` 和 `cluster_id`

    ```
    GET /api/bootstrap?cluster_id=935437471&token=ad87f6dd-c93f-4880-bcdb-8ca8c9ab3031

    Response:
    {
    	"msg": "OK",
    	"code": 0,
    	"data": {
    		"queryPort": 9030,
    		"rpcPort": 9020,
            "arrowFlightSqlPort": 9040,
    		"maxReplayedJournal": 17287
    	},
    	"count": 0
    }
    ```




