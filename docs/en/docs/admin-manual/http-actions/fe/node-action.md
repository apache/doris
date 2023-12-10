---
{
    "title": "Node Action",
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

# Node Action

## Request

`GET /rest/v2/manager/node/frontends`

`GET /rest/v2/manager/node/backends`

`GET /rest/v2/manager/node/brokers`

`GET /rest/v2/manager/node/configuration_name`

`GET /rest/v2/manager/node/node_list`

`POST /rest/v2/manager/node/configuration_info`

`POST /rest/v2/manager/node/set_config/fe`

`POST /rest/v2/manager/node/set_config/be`

<version since="dev">

`POST /rest/v2/manager/node/{action}/be`

`POST /rest/v2/manager/node/{action}/fe`

</version>

## Get information about fe, be, broker nodes

`GET /rest/v2/manager/node/frontends`

`GET /rest/v2/manager/node/backends`

`GET /rest/v2/manager/node/brokers`

### Description

Used to get cluster to get fe, be, broker node information.

### Response

```
frontends:
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "Name",
            "IP",
            "HostName",
            "EditLogPort",
            "HttpPort",
            "QueryPort",
            "RpcPort",
            "ArrowFlightSqlPort",
            "Role",
            "IsMaster",
            "ClusterId",
            "Join",
            "Alive",
            "ReplayedJournalId",
            "LastHeartbeat",
            "IsHelper",
            "ErrMsg",
            "Version"
        ],
        "rows": [
            [
                ...
            ]
        ]
    },
    "count": 0
}
```

```
backends:
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "BackendId",
            "Cluster",
            "IP",
            "HostName",
            "HeartbeatPort",
            "BePort",
            "HttpPort",
            "BrpcPort",
            "LastStartTime",
            "LastHeartbeat",
            "Alive",
            "SystemDecommissioned",
            "ClusterDecommissioned",
            "TabletNum",
            "DataUsedCapacity",
            "AvailCapacity",
            "TotalCapacity",
            "UsedPct",
            "MaxDiskUsedPct",
            "ErrMsg",
            "Version",
            "Status"
        ],
        "rows": [
            [
                ...
            ]
        ]
    },
    "count": 0
}
```

```
brokers:
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "Name",
            "IP",
            "HostName",
            "Port",
            "Alive",
            "LastStartTime",
            "LastUpdateTime",
            "ErrMsg"
        ],
        "rows": [
            [
                ...
            ]
        ]
    },
    "count": 0
}
```

## Get node configuration information

`GET /rest/v2/manager/node/configuration_name`

`GET /rest/v2/manager/node/node_list`

`POST /rest/v2/manager/node/configuration_info`

### Description

configuration_name Used to get the name of the node configuration item.  
node_list Get the list of nodes.  
configuration_info to get the node configuration details.

### Query parameters
`GET /rest/v2/manager/node/configuration_name`   
none

`GET /rest/v2/manager/node/node_list`  
none

`POST /rest/v2/manager/node/configuration_info`

* type 
  The value is fe or be, which specifies to get the configuration information of fe or the configuration information of be.

### Request body

`GET /rest/v2/manager/node/configuration_name`   
none

`GET /rest/v2/manager/node/node_list`  
none

`POST /rest/v2/manager/node/configuration_info`
```
{
	"conf_name": [
		""
	],
	"node": [
		""
	]
}

If no body is included, the parameters in the body use the default values.  
conf_name specifies which configuration items to return, the default is all configuration items.
node is used to specify which node's configuration information is returned, the default is all fe nodes or be nodes configuration information.
```

### Response
`GET /rest/v2/manager/node/configuration_name`  
``` 
{
    "msg": "success",
    "code": 0,
    "data": {
        "backend":[
            ""
        ],
        "frontend":[
            ""
        ]
    },
    "count": 0
}
```

`GET /rest/v2/manager/node/node_list` 
``` 
{
    "msg": "success",
    "code": 0,
    "data": {
        "backend": [
            ""
        ],
        "frontend": [
            ""
        ]
    },
    "count": 0
}
```

`POST /rest/v2/manager/node/configuration_info?type=fe`
```
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "配置项",
            "节点",
            "节点类型",
            "配置值类型",
            "MasterOnly",
            "配置值",
            "可修改"
        ],
        "rows": [
            [
                ""
            ]
        ]
    },
    "count": 0
}
```

`POST /rest/v2/manager/node/configuration_info?type=be`
```
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "配置项",
            "节点",
            "节点类型",
            "配置值类型",
            "配置值",
            "可修改"
        ],
        "rows": [
            [
                ""
            ]
        ]
    },
    "count": 0
}
```
    
### Examples

1. Get the fe agent_task_resend_wait_time_ms configuration information:

    POST /rest/v2/manager/node/configuration_info?type=fe  
    body:
    ```
    {
        "conf_name":[
            "agent_task_resend_wait_time_ms"
        ]
    }
    ```
    
    Response:
    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "column_names": [
                "配置项",
                "节点",
                "节点类型",
                "配置值类型",
                "MasterOnly",
                "配置值",
                "可修改"
            ],
            "rows": [
                [
                    "agent_task_resend_wait_time_ms",
                    "127.0.0.1:8030",
                    "FE",
                    "long",
                    "true",
                    "50000",
                    "true"
                ]
            ]
        },
        "count": 0
    }
    ```

## Modify configuration values

`POST /rest/v2/manager/node/set_config/fe`

`POST /rest/v2/manager/node/set_config/be`

### Description

Used to modify fe or be node configuration values

### Request body
```
{
	"config_name":{
		"node":[
			""
		],
		"value":"",
		"persist":
	}
}

config_name is the corresponding configuration item.  
node is a keyword indicating the list of nodes to be modified;  
value is the value of the configuration.  
persist is true for permanent modification and false for temporary modification. persist means permanent modification, false means temporary modification. permanent modification takes effect after reboot, temporary modification fails after reboot.
```

### Response
`GET /rest/v2/manager/node/configuration_name`  
``` 
{
	"msg": "",
	"code": 0,
	"data": {
		"failed":[
			{
				"config_name":"name",
				"value"="",
				"node":"",
				"err_info":""
			}
		]
	},
	"count": 0
}

failed Indicates a configuration message that failed to be modified.
```
    
### Examples

1. Modify the agent_task_resend_wait_time_ms and alter_table_timeout_second configuration values in the fe 127.0.0.1:8030 node:

    POST /rest/v2/manager/node/set_config/fe
    body:
    ```
    {
        "agent_task_resend_wait_time_ms":{
            "node":[
		    	"127.0.0.1:8030"
		    ],
		    "value":"10000",
		    "persist":"true"
        },
        "alter_table_timeout_second":{
            "node":[
		    	"127.0.0.1:8030"
		    ],
		    "value":"true",
		    "persist":"true"
        }
    }
    ```
    
    Response:
    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "failed": [
                {
                    "config_name": "alter_table_timeout_second",
                    "node": "10.81.85.89:8837",
                    "err_info": "Unsupported configuration value type.",
                    "value": "true"
                }
            ]
        },
        "count": 0
    }

    gent_task_resend_wait_time_ms configuration value modified successfully, alter_table_timeout_second modification failed.
    ```

## Operate be node

`POST /rest/v2/manager/node/{action}/be`

### Description

Used to add/drop/offline be node

action：ADD/DROP/DECOMMISSION

### Request body
```
{
    "hostPorts": ["127.0.0.1:9050"],
    "properties": {
        "tag.location": "test"
    }
}

hostPorts A set of be node addresses to be operated, ip:heartbeat_port
properties The configuration passed in when adding a node is only used to configure the tag. If not, the default tag is used
```

### Response
```
{
    "msg": "Error",
    "code": 1,
    "data": "errCode = 2, detailMessage = Same backend already exists[127.0.0.1:9050]",
    "count": 0
}

msg Success/Error
code 0/1
data ""/Error message
```

### Examples

1. add be node

   post /rest/v2/manager/node/ADD/be
   Request body
    ```
    {
        "hostPorts": ["127.0.0.1:9050"]
    }
    ```

   Response
    ```
    {
        "msg": "success",
        "code": 0,
        "data": null,
        "count": 0
    }
    ```

2. drop be node

   post /rest/v2/manager/node/DROP/be
   Request body
    ```
    {
        "hostPorts": ["127.0.0.1:9050"]
    }
    ```

   Response
    ```
    {
        "msg": "success",
        "code": 0,
        "data": null,
        "count": 0
    }
    ```

3. offline be node

   post /rest/v2/manager/node/DECOMMISSION/be
   Request body
    ```
    {
        "hostPorts": ["127.0.0.1:9050"]
    }
    ```

   Response
    ```
    {
        "msg": "success",
        "code": 0,
        "data": null,
        "count": 0
    }
    ```

## Operate fe node

`POST /rest/v2/manager/node/{action}/fe`

### Description

Used to add/drop fe node

action：ADD/DROP

### Request body
```
{
    "role": "FOLLOWER",
    "hostPort": "127.0.0.1:9030"
}

role FOLLOWER/OBSERVER
hostPort The address of the fe node to be operated, ip:edit_log_port
```

### Response
```
{
    "msg": "Error",
    "code": 1,
    "data": "errCode = 2, detailMessage = frontend already exists name: 127.0.0.1:9030_1670495889415, role: FOLLOWER, 127.0.0.1:9030",
    "count": 0
}

msg Success/Error
code 0/1
data ""/Error message
```

### Examples

1. add FOLLOWER node

   post /rest/v2/manager/node/ADD/fe
   Request body
    ```
    {
        "role": "FOLLOWER",
        "hostPort": "127.0.0.1:9030"
    }
    ```

   Response
    ```
    {
        "msg": "success",
        "code": 0,
        "data": null,
        "count": 0
    }
    ```

2. drop FOLLOWER node

   post /rest/v2/manager/node/DROP/fe
   Request body
    ```
    {
        "role": "FOLLOWER",
        "hostPort": "127.0.0.1:9030"
    }
    ```

   Response
    ```
    {
        "msg": "success",
        "code": 0,
        "data": null,
        "count": 0
    }
    ```