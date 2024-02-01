---
{
    "title": "Check All Tablet Segment Lost",
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

# Check All Tablet Segment Lost

## Request

`GET /api/check_tablet_segment_lost?repair={bool}`

## Description

There may be some exceptions that cause segment to be lost on BE node. However, the metadata shows that the tablet is normal. This abnormal replica is not detected by FE and cannot be automatically repaired. When query comes, exception information is thrown that `failed to initialize storage reader`. The function of this interface is to check all tablets on the current BE node that have lost segment.

## Query parameters

* `repair`
    - `true`: tablets with lost segment will be set to SHUTDOWN status and treated as bad replica, which can be detected and repaired by FE. 
    - `false`: all tablets with missing segment are returned and nothing is done.

## Request body

None

## Response

    The return is all tablets on the current BE node that have lost segment:

    ```
    {
        status: "Success",
        msg: "Succeed to check all tablet segment",
        num: 3,
        bad_tablets: [
            11190,
            11210,
            11216
        ],
        set_bad: true,
        host: "172.3.0.101"
    }
    ```

## Examples


    ```
    curl http://127.0.0.1:8040/api/check_tablet_segment_lost?repair=false
    ```

