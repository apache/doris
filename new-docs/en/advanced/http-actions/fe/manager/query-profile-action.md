---
{
    "title": "Query Profile Action",
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

# Query Profile Action

## Request

`GET /rest/v2/manager/query/query_info`

`GET /rest/v2/manager/query/sql/{query_id}`

`GET /rest/v2/manager/query/profile/text/{query_id}`

`GET /rest/v2/manager/query/profile/fragments/{query_id}`

`GET /rest/v2/manager/query/profile/graph/{query_id}`

## Get the query information

`GET /rest/v2/manager/query/query_info`

### Description

Gets information about select queries for all fe nodes in the cluster.

### Query parameters

* `query_id`

    Optional, specifies the query ID of the query to be returned, default returns information for all queries.
    
* `search`

    Optional, specifies that query information containing strings is returned, currently only string matches are performed.

* `is_all_node`
  
    Optional, if true, returns query information for all fe nodes, if false, returns query information for the current fe node. The default is true.


### Response

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "Query ID",
            "FE节点",
            "查询用户",
            "执行数据库",
            "Sql",
            "查询类型",
            "开始时间",
            "结束时间",
            "执行时长",
            "状态"
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

### Examples
```
GET /rest/v2/manager/query/query_info

{
    "msg": "success",
    "code": 0,
    "data": {
        "column_names": [
            "Query ID",
            "FE节点",
            "查询用户",
            "执行数据库",
            "Sql",
            "查询类型",
            "开始时间",
            "结束时间",
            "执行时长",
            "状态"
        ],
        "rows": [
            [
                "d7c93d9275334c35-9e6ac5f295a7134b",
                "127.0.0.1:8030",
                "root",
                "default_cluster:testdb",
                "select c.id, c.name, p.age, p.phone, c.date, c.cost from cost c join people p on c.id = p.id where p.age > 20 order by c.id",
                "Query",
                "2021-07-29 16:59:12",
                "2021-07-29 16:59:12",
                "109ms",
                "EOF"
            ]
        ]
    },
    "count": 0
}
```

## Get the sql and text profile for the specified query

`GET /rest/v2/manager/query/sql/{query_id}`

`GET /rest/v2/manager/query/profile/text/{query_id}`

### Description

Get the sql and profile text for the specified query id.
    
### Path parameters

* `query_id`

    The query id.

### Query parameters

* `is_all_node`
  
    Optional, if true then query for the specified query id in all fe nodes, if false then query for the specified query id in the currently connected fe nodes. The default is true.

### Response

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "sql": ""
    },
    "count": 0
}
```

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "profile": ""
    },
    "count": 0
}
```
    
### Examples

1. get sql.

    ```
    GET /rest/v2/manager/query/sql/d7c93d9275334c35-9e6ac5f295a7134b
    
    Response:
    {
        "msg": "success",
        "code": 0,
        "data": {
            "sql": "select c.id, c.name, p.age, p.phone, c.date, c.cost from cost c join people p on c.id = p.id where p.age > 20 order by c.id"
        },
        "count": 0
    }
    ```

## Get the specified query fragment and instance information

`GET /rest/v2/manager/query/profile/fragments/{query_id}`

### Description

Get the fragment name, instance id and execution time for the specified query id.
    
### Path parameters

* `query_id`

    The query id.

### Query parameters

* `is_all_node`
  
    Optional, if true then query for the specified query id in all fe nodes, if false then query for the specified query id in the currently connected fe nodes. The default is true.

### Response

```
{
    "msg": "success",
    "code": 0,
    "data": [
        {
            "fragment_id": "",
            "time": "",
            "instance_id": {
                "": ""
            }
        }
    ],
    "count": 0
}
```
    
### Examples

    ```
    GET /rest/v2/manager/query/profile/fragments/d7c93d9275334c35-9e6ac5f295a7134b
    
    Response:
    {
        "msg": "success",
        "code": 0,
        "data": [
            {
                "fragment_id": "0",
                "time": "36.169ms",
                "instance_id": {
                    "d7c93d9275334c35-9e6ac5f295a7134e": "36.169ms"
                }
            },
            {
                "fragment_id": "1",
                "time": "20.710ms",
                "instance_id": {
                    "d7c93d9275334c35-9e6ac5f295a7134c": "20.710ms"
                }
            },
            {
                "fragment_id": "2",
                "time": "7.83ms",
                "instance_id": {
                    "d7c93d9275334c35-9e6ac5f295a7134d": "7.83ms"
                }
            }
        ],
        "count": 0
    }
    ```

## Get the specified query id tree profile information

`GET /rest/v2/manager/query/profile/graph/{query_id}`

### Description

Get the tree profile information of the specified query id, same as `show query profile` command.
    
### Path parameters

* `query_id`

    The query id.

### Query parameters

* `fragment_id` and `instance_id`

    Optional, both parameters must be specified or not.  
    If both are not specified, a simple tree of profiles is returned, equivalent to `show query profile '/query_id'`;  
    If both are specified, a detailed profile tree is returned, which is equivalent to `show query profile '/query_id/fragment_id/instance_id'`.

* `is_all_node`
  
    Optional, if true then query information about the specified query id in all fe nodes, if false then query information about the specified query id in the currently connected fe nodes. The default is true.

### Response

```
{
    "msg": "success",
    "code": 0,
    "data": {
        "graph":""
    },
    "count": 0
}
```