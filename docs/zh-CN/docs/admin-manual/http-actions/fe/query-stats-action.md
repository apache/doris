---
{
    "title": "Query Stats Action",
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

# Query Stats Action

<version since="dev"></version>

## Request

```
查看
get api/query_stats/<catalog_name>
get api/query_stats/<catalog_name>/<db_name>
get api/query_stats/<catalog_name>/<db_name>/<tbl_name>

清空
delete api/query_stats/<catalog_name>/<db_name>
delete api/query_stats/<catalog_name>/<db_name>/<tbl_name>
```

## Description

获取或者删除指定的catalog 数据库或者表中的统计信息， 如果是doris catalog 可以使用default_cluster
    
## Path parameters

* `<catalog_name>`

    指定的catalog 名称
* `<db_name>`

    指定的数据库名称
* `<tbl_name>`

    指定的表名称

## Query parameters
* `summary`
如果为true 则只返回summary信息， 否则返回所有的表的详细统计信息，只在get 时使用

## Request body

```
GET /api/query_stats/default_cluster/test_query_db/baseall?summary=false
{
    "msg": "success",
    "code": 0,
    "data": {
        "summary": {
            "query": 2
        },
        "detail": {
            "baseall": {
                "summary": {
                    "query": 2
                }
            }
        }
    },
    "count": 0
}

```

## Response

* 返回结果集


## Example


2. 使用 curl 命令获取统计信息

    ```
    curl --location -u root: 'http://127.0.0.1:8030/api/query_stats/default_cluster/test_query_db/baseall?summary=false'
    ```
