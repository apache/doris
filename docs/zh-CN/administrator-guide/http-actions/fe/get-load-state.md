---
{
    "title": "Get Load State",
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

# Get Load State

## Request

`GET /api/<db>/get_load_state`

## Description

返回指定label的导入事务的状态
    
## Path parameters

* `<db>`

    指定数据库

## Query parameters

* `label`

    指定导入label

## Request body

无

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": "VISIBLE",
	"count": 0
}
```

返回值：

- `msg`: 错误信息。成功则返回"success"

- `code`: Doris内部错误码。成功则返回0

- `data` 执行成功的场景下取值：
    - UNKNOWN: 没有找到对应的label
    - PREPARE: 对应的事务已经prepare，但尚未提交
    - PRECOMMITTED: 事务预提交
    - COMMITTED: 事务已经提交，不能被cancel
    - VISIBLE: 事务提交，并且数据可见，不能被cancel
    - ABORTED: 事务已经被ROLLBACK，导入已经失败。

- `count`: 该字段当前不使用，默认为0
    
## Examples

1. 获取指定label的导入事务的状态。

    ```
    GET /api/example_db/get_load_state?label=my_label
    
    {
    	"msg": "success",
    	"code": 0,
    	"data": "VISIBLE",
    	"count": 0
    }
    ```

2. 使用curl命令获取事务的状态。

    ```
    curl -u <user[:password]> http://host:port/api/<db>/get_load_state?label=<label>
    ```
