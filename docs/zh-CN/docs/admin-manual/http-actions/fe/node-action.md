---
{
    "title": "Node Action",
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

## 获取fe, be, broker节点信息

`GET /rest/v2/manager/node/frontends`

`GET /rest/v2/manager/node/backends`

`GET /rest/v2/manager/node/brokers`

### Description

用于获取集群获取fe, be, broker节点信息。

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

## 获取节点配置信息

`GET /rest/v2/manager/node/configuration_name`

`GET /rest/v2/manager/node/node_list`

`POST /rest/v2/manager/node/configuration_info`

### Description

configuration_name 用于获取节点配置项名称。  
node_list 用于获取节点列表。  
configuration_info 用于获取节点配置详细信息。

### Query parameters
`GET /rest/v2/manager/node/configuration_name`   
无

`GET /rest/v2/manager/node/node_list`  
无

`POST /rest/v2/manager/node/configuration_info`

* type 
  值为 fe 或 be， 用于指定获取fe的配置信息或be的配置信息。

### Request body

`GET /rest/v2/manager/node/configuration_name`   
无

`GET /rest/v2/manager/node/node_list`  
无

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

若不带body，body中的参数都使用默认值。  
conf_name 用于指定返回哪些配置项的信息， 默认返回所有配置项信息；
node 用于指定返回哪些节点的配置项信息，默认为全部fe节点或be节点配置项信息。
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

1. 获取fe agent_task_resend_wait_time_ms 配置项信息：

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

## 修改配置值

`POST /rest/v2/manager/node/set_config/fe`

`POST /rest/v2/manager/node/set_config/be`

### Description

用于修改fe或be节点配置值

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

config_name为对应的配置项；  
node为关键字，表示要修改的节点列表;  
value为配置的值；  
persist为 true 表示永久修改， false 表示临时修改。永久修改重启后能生效， 临时修改重启后失效。
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

failed 表示修改失败的配置信息。
```
    
### Examples

1. 修改fe 127.0.0.1:8030 节点中 agent_task_resend_wait_time_ms 和alter_table_timeout_second 配置值：

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

    agent_task_resend_wait_time_ms 配置值修改成功，alter_table_timeout_second 修改失败。
    ```
   
## 操作 be 节点

`POST /rest/v2/manager/node/{action}/be`

### Description

用于添加/删除/下线 be 节点

action：ADD/DROP/DECOMMISSION

### Request body
```
{
    "hostPorts": ["127.0.0.1:9050"],
    "properties": {
        "tag.location": "test"
    }
}

hostPorts 需要操作的一组 be 节点地址 ip:heartbeat_port
properties 添加节点时传入的配置，目前只用于配置 tag, 不传使用默认 tag
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
data ""/报错信息
```

### Examples

1. 添加 be 节点

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

2. 删除 be 节点

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

3. 下线 be 节点

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

## 操作 fe 节点

`POST /rest/v2/manager/node/{action}/fe`

### Description

用于添加/删除 fe 节点

action：ADD/DROP

### Request body
```
{
    "role": "FOLLOWER",
    "hostPort": "127.0.0.1:9030"
}

role FOLLOWER/OBSERVER
hostPort 需要操作的 fe 节点地址 ip:edit_log_port
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
data ""/报错信息
```

### Examples

1. 添加 FOLLOWER 节点

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

2. 删除 FOLLOWER 节点

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