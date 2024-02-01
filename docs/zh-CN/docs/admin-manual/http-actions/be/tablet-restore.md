---
{
    "title": "恢复tablet",
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

# 恢复tablet

## Request

`POST /api/restore_tablet?tablet_id={int}&schema_hash={int}"`

## Description

该功能用于恢复trash目录中被误删的tablet数据。

## Query parameters

* `tablet_id`
    需要恢复的table的id

* `schema_hash`
    schema hash       


## Request body

无

## Response

    ```
    {
        msg: "OK",
        code: 0
    }
    ```
## Examples


    ```
    curl -X POST "http://127.0.0.1:8040/api/restore_tablet?tablet_id=123456&schema_hash=1111111"

    ```

