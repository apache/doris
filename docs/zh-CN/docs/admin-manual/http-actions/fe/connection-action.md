---
{
    "title": "Connection Action",
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

# Connection Action

## Request

`GET /api/connection`

## Description

给定一个 connection id，返回这个连接当前正在执行的，或最后一次执行完成的 query id。

connection id 可以通过 MySQL 命令 `show processlist;` 中的 id 列查看。
    
## Path parameters

无

## Query parameters

* `connection_id`

    指定的 connection id

## Request body

无

## Response

```
{
	"msg": "OK",
	"code": 0,
	"data": {
		"query_id": "b52513ce3f0841ca-9cb4a96a268f2dba"
	},
	"count": 0
}
```
    
## Examples

1. 获取指定 connection id 的 query id

    ```
    GET /api/connection?connection_id=101
    
    Response:
    {
    	"msg": "OK",
    	"code": 0,
    	"data": {
    		"query_id": "b52513ce3f0841ca-9cb4a96a268f2dba"
    	},
    	"count": 0
    }
    ```
