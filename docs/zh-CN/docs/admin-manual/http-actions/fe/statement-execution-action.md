---
{
    "title": "Statement Execution Action",
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

# Statement Execution Action


## Request

```
POST /api/query/<ns_name>/<db_name>
```

## Description

Statement Execution Action 用于执行语句并返回结果。
    
## Path parameters
* `<ns_name>`

  指定集群的名称，默认值取default_cluster即可

* `<db_name>`

    指定数据库名称。该数据库会被视为当前session的默认数据库，如果在 SQL 中的表名没有限定数据库名称的话，则使用该数据库。

## Query parameters

无

## Request body

```
{
    "stmt" : "select * from tbl1"
}
```

* sql 字段为具体的 SQL

### Response

* 返回结果集

    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "type": "result_set",
            "data": [
                [1],
                [2]
            ],
            "meta": [{
                "name": "k1",
                "type": "INT"
            }],
            "status": {},
            "time": 10
        },
        "count": 0
    }
    ```

    * type 字段为 `result_set` 表示返回结果集。需要根据 meta 和 data 字段获取并展示结果。meta 字段描述返回的列信息。data 字段返回结果行。其中每一行的中的列类型，需要通过 meta 字段内容判断。status 字段返回 MySQL 的一些信息，如告警行数，状态码等。time 字段返回语句执行时间，单位毫秒。

* 返回执行结果

    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "type": "exec_status",
            "status": {},
            "time": 10
        },
        "count": 0
    }
    ```

    * type 字段为 `exec_status` 表示返回执行结果。目前收到该返回结果，则都表示语句执行成功。
