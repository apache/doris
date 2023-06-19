---
{
    "title": "查询tablet分布",
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

# 查询tablet分布

## Request

`GET /api/tablets_distribution?group_by={enum}&partition_id={int}`

## Description

获取BE节点上每一个partition下的tablet在不同磁盘上的分布情况

## Query parameters

* `group_by`
    分组，当前只支持`partition`

* `partition_id`
    指定partition的id，选填，默认返回所有partition。

## Request body

无

## Response

    ```
    {
        msg: "OK",
        code: 0,
        data: {
            host: "***",
            tablets_distribution: [
                {
                    partition_id:***,
                    disks:[
                        {
                            disk_path:"***",
                            tablets_num:***,
                            tablets:[
                                {
                                    tablet_id:***,
                                    schema_hash:***,
                                    tablet_size:***
                                },

                                ...

                            ]
                        },

                        ...

                    ]
                }
            ]
        },
        count: ***
    }
    ```
## Examples


    ```
    curl "http://127.0.0.1:8040/api/tablets_distribution?group_by=partition&partition_id=123"

    ```

