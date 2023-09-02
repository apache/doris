---
{
    "title": "Check Stub Cache",
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

# CHECK Stub Cache

## Request

`GET /api/check_rpc_channel/{host_to_check}/{remot_brpc_port}/{payload_size}`

## Description

Check whether the connection cache is available

## Path parameters

* `host_to_check`

    Host to check

* `remot_brpc_port`

    Remot brpc port

* `payload_size`

    Load size, unit: B, value range 1~1024000.

## Request body

None

## Response

    ```
    {
        "msg":"success",
        "code":0,
        "data": "open brpc connection to {host_to_check}:{remot_brpc_port} succcess.",
        "count":0
    }
    ```
## Examples


    ```
    curl http://127.0.0.1:8040/api/check_rpc_channel/127.0.0.1/8060/1024000
    ```

