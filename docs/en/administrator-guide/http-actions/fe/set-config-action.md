---
{
    "title": "Set Config Action",
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

# Set Config Action

## Request

`GET /api/_set_config`

## Description

Used to dynamically set the configuration of FE. This command is passed through the `ADMIN SET FRONTEND CONFIG` command. But this command will only set the configuration of the corresponding FE node. And it will not automatically forward the `MasterOnly` configuration item to the Master FE node.
    
## Path parameters

None

## Query parameters

* `confkey1=confvalue1`

    Specify the configuration name to be set, and its value is the configuration value to be modified.
    
* `persist`

     Whether to persist the modified configuration. The default is false, which means it is not persisted. If it is true, the modified configuration item will be written into the `fe_custom.conf` file and will still take effect after FE is restarted.

## Request body

None

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

The `set` field indicates the successfully set configuration. The `err` field indicates the configuration that failed to be set.
    
## Examples

1. Set the two configuration values of `max_bytes_per_broker_scanner` and `max_broker_concurrency`.

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

2. Set `max_bytes_per_broker_scanner` and persist it.

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