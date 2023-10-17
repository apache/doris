---
{
    "title": "Bootstrap Action",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Bootstrap Action

## Request

`GET /api/bootstrap`

## Description

It is used to judge whether the FE has started. When no parameters are provided, only whether the startup is successful is returned. If `token` and `cluster_id` are provided, more detailed information is returned.
    
## Path parameters

none

## Query parameters

* `cluster_id`

    The cluster id. It can be viewed in the file `doris-meta/image/VERSION`.
    
* `token`

    Cluster token. It can be viewed in the file `doris-meta/image/VERSION`.

## Request body

none

## Response

* No parameters provided

    ```
    {
    	"msg": "OK",
    	"code": 0,
    	"data": null,
    	"count": 0
    }
    ```
    
    A code of 0 means that the FE node has started successfully. Error codes other than 0 indicate other errors.
    
* Provide `token` and `cluster_id`

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
    
    * `queryPort` is the MySQL protocol port of the FE node.
    * `rpcPort` is the thrift RPC port of the FE node.
    * `maxReplayedJournal` represents the maximum metadata journal id currently played back by the FE node.
    * `arrowFlightSqlPort` is the Arrow Flight SQL port of the FE node.
    
## Examples

1. No parameters

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
    
2. Provide `token` and `cluster_id`

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