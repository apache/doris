---
{
    "title": "迁移tablet",
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

# 迁移tablet

## Request

`GET /api/tablet_migration?goal={enum}&tablet_id={int}&schema_hash={int}&disk={string}`

## Description

在BE节点上迁移单个tablet到指定磁盘

## Query parameters

* `goal`
    - `run`：提交迁移任务
    - `status`：查询任务的执行状态

* `tablet_id`
    需要迁移的tablet的id

* `schema_hash`
    schema hash

* `disk`
    目标磁盘。    

## Request body

无

## Response

### 提交结果

```
    {
        status: "Success",
        msg: "migration task is successfully submitted."
    }
```
或
```
    {
        status: "Fail",
        msg: "Migration task submission failed"
    }
```

### 执行状态

```
    {
        status: "Success",
        msg: "migration task is running",
        dest_disk: "xxxxxx"
    }
```

或

```
    {
        status: "Success",
        msg: "migration task has finished successfully",
        dest_disk: "xxxxxx"
    }
```

或

```
    {
        status: "Success",
        msg: "migration task failed.",
        dest_disk: "xxxxxx"
    }
```

## Examples


    ```
    curl "http://127.0.0.1:8040/api/tablet_migration?goal=run&tablet_id=123&schema_hash=333&disk=/disk1"

    ```

