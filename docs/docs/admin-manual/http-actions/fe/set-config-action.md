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

* `reset_persist`
    Whether or not to clear the original persist configuration only takes effect when the persist parameter is true. For compatibility with the original version, reset_persist defaults to true.  
	If persist is set to true and reset_persist is not set or reset_persist is true, the configuration in the `fe_custom.conf` file will be cleared before this modified configuration is written to `fe_custom.conf`.  
	If persist is set to true and reset_persist is false, this modified configuration item will be incrementally added to `fe_custom.conf`.


## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"set": {
			"key": "value"
		},
		"err": [
			{
		       "config_name": "",
		       "config_value": "",
		       "err_info": ""
		    }
		],
		"persist":""
	},
	"count": 0
}
```

The `set` field indicates the successfully set configuration. The `err` field indicates the configuration that failed to be set. The `persist` field indicates persistent information.
    
## Examples

1. Set the values of `storage_min_left_capacity_bytes`, `replica_ack_policy` and `agent_task_resend_wait_time_ms`.

    ```
    GET /api/_set_config?storage_min_left_capacity_bytes=1024&replica_ack_policy=SIMPLE_MAJORITY&agent_task_resend_wait_time_ms=true
    
    Response:
    {
    "msg": "success",
    "code": 0,
    "data": {
        "set": {
            "storage_min_left_capacity_bytes": "1024"
        },
        "err": [
            {
                "config_name": "replica_ack_policy",
                "config_value": "SIMPLE_MAJORITY",
                "err_info": "Not support dynamic modification."
            },
            {
                "config_name": "agent_task_resend_wait_time_ms",
                "config_value": "true",
                "err_info": "Unsupported configuration value type."
            }
        ],
        "persist": ""
    },
    "count": 0
    }

	storage_min_left_capacity_bytes  Successfully;    
	replica_ack_policy  Failed, because the configuration item does not support dynamic modification.  
	agent_task_resend_wait_time_ms  Failed, failed to set the boolean type because the configuration item is of type long.
    ```

2. Set `max_bytes_per_broker_scanner` and persist it.

    ```
    GET /api/_set_config?max_bytes_per_broker_scanner=21474836480&persist=true&reset_persist=false
    
    Response:
    {
    "msg": "success",
    "code": 0,
    "data": {
        "set": {
            "max_bytes_per_broker_scanner": "21474836480"
        },
        "err": [],
        "persist": "ok"
    },
    "count": 0
    }
	```

	The fe/conf directory generates the fe_custom.conf file:
	```
	#THIS IS AN AUTO GENERATED CONFIG FILE.
    #You can modify this file manually, and the configurations in this file
    #will overwrite the configurations in fe.conf
    #Wed Jul 28 12:43:14 CST 2021
    max_bytes_per_broker_scanner=21474836480
    ```