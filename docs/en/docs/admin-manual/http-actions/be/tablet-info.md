---
{
    "title": "View Tablet Info",
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

# View Tablet Info

## Request

`GET /tablets_json?limit={int}`

## Description

View Tablet Info, including ID and schema hash.

## Query parameters

* `limit`
    Number of tablets output，Optional with default 1000. Take `all` to output all tablets.

## Request body

None

## Response

    ```
    {
        msg: "OK",
        code: 0,
        data: {
            host: "10.38.157.107",
            tablets: [
                {
                    tablet_id: 11119,
                    schema_hash: 714349777
                },

                    ...

                {
                    tablet_id: 11063,
                    schema_hash: 714349777
                }
            ]
        },
        count: 30
    }
    ```
## Examples


    ```
    curl http://127.0.0.1:8040/api/tablets_json?limit=all

    ```

