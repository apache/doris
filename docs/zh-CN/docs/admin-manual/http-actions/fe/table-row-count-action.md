---
{
    "title": "Table Row Count Action",
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

# Table Row Count Action

## Request

`GET /api/<db>/<table>/_count`

## Description

用于获取指定表的行数统计信息。该接口目前用于 Spark-Doris-Connector 中，Spark 获取 Doris 的表统计信息。
    
## Path parameters

* `<db>`

    指定数据库

* `<table>`

    指定表

## Query parameters

无

## Request body

无

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"size": 1,
		"status": 200
	},
	"count": 0
}
```

其中 `data.size` 字段表示指定表的行数。
    
## Examples

1. 获取指定表的行数。

    ```
    GET /api/db1/tbl1/_count
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"size": 1,
    		"status": 200
    	},
    	"count": 0
    }
    ```
