---
{
    "title": "Checksum",
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

# Checksum

## Request

`GET /api/checksum?tablet_id={int}&version={int}&schema_hash={int}`

## Description

checksum

## Query parameters

* `tablet_id`
    需要校验的tablet的id

* `version`
    需要校验的tablet的version    

* `schema_hash`
    schema hash

## Request body

无

## Response

    ```
    1843743562
    ```
## Examples


    ```
    curl "http://127.0.0.1:8040/api/checksum?tablet_id=1&version=1&schema_hash=-1"
    
    ```

