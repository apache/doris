---
{
    "title": "Query Profile Action",
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

# Query Profile Action

## Request

`GET /rest/v2/manager/query/query_info`

`GET /rest/v2/manager/query/sql/{query_id}`

`GET /rest/v2/manager/query/profile/text/{query_id}`

`GET /rest/v2/manager/query/profile/fragments/{query_id}`

`GET /rest/v2/manager/query/profile/graph/{query_id}`

## 获取查询信息

`GET /rest/v2/manager/query/query_info`

### Description

可获取集群所有 fe 节点 select 查询信息。

### Query parameters

* `query_id`

    可选，指定返回查询的queryID， 默认返回所有查询的信息。
    
* `search`

    可选，指定返回包含字符串的查询信息，目前仅进行字符串匹配。

* `is_all_node`
  
    可选，若为 true 则返回所有fe节点的查询信息，若为 false 则返回当前fe节点的查询信息。默认为true。


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

## 获取指定查询的sql和文本profile

`GET /rest/v2/manager/query/sql/{query_id}`

`GET /rest/v2/manager/query/profile/text/{query_id}`

### Description

用于获取指定query id的sql和profile文本。
    
### Path parameters

* `query_id`

    query id。

### Query parameters

* `is_all_node`
  
    可选，若为 true 则在所有fe节点中查询指定query id的信息，若为 false 则在当前连接的fe节点中查询指定query id的信息。默认为true。

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

1. 获取 sql：

    ```
    GET /rest/v2/manager/query/sql/d7c93d9275334c35-9e6ac5f295a7134b
    
    Response:
    {
        "msg": "success",
        "code": 0,
        "data": {
            "sql": "select c.id, c.name, p.age, p.phone, c.date, c.cost from cost c join people p on c.id   = p.id where p.age > 20 order by c.id"
        },
        "count": 0
    }
    ```

## 获取指定查询fragment和instance信息

`GET /rest/v2/manager/query/profile/fragments/{query_id}`

### Description

用于获取指定query id的fragment名称，instance id和执行时长。
    
### Path parameters

* `query_id`

    query id。

### Query parameters

* `is_all_node`
  
    可选，若为 true 则在所有fe节点中查询指定query id的信息，若为 false 则在当前连接的fe节点中查询指定query id的信息。默认为true。

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

## 获取指定query id树状profile信息

`GET /rest/v2/manager/query/profile/graph/{query_id}`

### Description

获取指定query id树状profile信息，同 `show query profile` 指令。
    
### Path parameters

* `query_id`

    query id。

### Query parameters

* `fragment_id` 和 `instance_id`

    可选，这两个参数需同时指定或同时不指定。  
    同时不指定则返回profile 简易树形图，相当于`show query profile '/query_id'`;  
    同时指定则返回指定instance详细profile树形图，相当于`show query profile '/query_id/fragment_id/instance_id'`.

* `is_all_node`
  
    可选，若为 true 则在所有fe节点中查询指定query id的信息，若为 false 则在当前连接的fe节点中查询指定query id的信息。默认为true。

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
