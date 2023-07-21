---
{
    "title": "Reset Stub Cache",
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

# Reset Stub Cache

## Request

`GET /api/reset_rpc_channel/{endpoints}`

## Description

Reset the connection cache of brpc

## Path parameters

* `endpoints`
    - `all`: clear all caches
    - `host1:port1,host2:port2`: clear cache of the specified target

## Request body

None

## Response

    ```
    {
        "msg":"success",
        "code":0,
        "data": "no cached channel.",
        "count":0
    }
    ```
## Examples


    ```
    curl http://127.0.0.1:8040/api/reset_rpc_channel/all
    ```
    
    ```
    curl http://127.0.0.1:8040/api/reset_rpc_channel/1.1.1.1:8080,2.2.2.2:8080
    ```

