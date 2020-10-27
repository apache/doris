---
{
    "title": "Set Config Action",
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

# Set Config Action

## Request

`GET /api/_set_config`

## Description

用于动态设置 FE 的参数。该命令等同于通过 `ADMIN SET FRONTEND CONFIG` 命令。但该命令仅会设置对应 FE 节点的配置。并且不会自动转发 `MasterOnly` 配置项给 Master FE 节点。
    
## Path parameters

无

## Query parameters

* `confkey1=confvalue1`

    指定要设置的配置名称，其值为要修改的配置值。
    
* `persist`

    是否要将修改的配置持久化。默认为 false，即不持久化。如果为 true，这修改后的配置项会写入 `fe_custom.conf` 文件中，并在 FE 重启后仍会生效。

## Request body

无

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"set": {
			"storage_min_left_capacity_bytes": "1024",
			"qe_max_connection": "2048"
		},
		"err": {
		   "replica_ack_policy": "SIMPLE_MAJORITY"
		}
	},
	"count": 0
}
```

`set` 字段表示设置成功的配置。`err` 字段表示设置失败的配置。
    
## Examples

1. 设置 `max_bytes_per_broker_scanner` 和 `max_broker_concurrency` 两个配置的值。

    ```
    GET /api/_set_config?max_bytes_per_broker_scanner=21474836480&max_broker_concurrency=20
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"set": {
    			"max_bytes_per_broker_scanner": "21474836480",
    			"max_broker_concurrency": "20"
    		},
    		"err": {}
    	},
    	"count": 0
    }
    ```

2. 设置 `max_bytes_per_broker_scanner` 并持久化
    ```
    GET /api/_set_config?max_bytes_per_broker_scanner=21474836480&persist=true
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"set": {
    			"max_bytes_per_broker_scanner": "21474836480"
    		},
    		"err": {},
    		"persist": "ok"
    	},
    	"count": 0
    }
    ```