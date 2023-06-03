---
{
    "title": "Pad Rowset",
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

# Pad Rowset

## Request

`POST /api/pad_rowset?tablet_id={int}&start_version={int}&end_version={int}`

## Description

Pad one empty rowset as one substitute for error replica.

## Query parameters

* `tablet_id`
    ID of the tablet

* `start_version`
    Start version

* `end_version`
    End version       


## Request body

None

## Response

    ```
    {
        msg: "OK",
        code: 0
    }
    ```
## Examples


    ```
    curl -X POST "http://127.0.0.1:8040/api/pad_rowset?tablet_id=123456&start_version=1111111&end_version=1111112"

    ```

