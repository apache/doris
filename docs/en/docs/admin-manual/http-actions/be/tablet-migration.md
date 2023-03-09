---
{
    "title": "Migration Tablet",
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

# Migration Tablet

## Request

`GET /api/tablet_migration?goal={enum}&tablet_id={int}&schema_hash={int}&disk={string}`

## Description

Migrate a tablet to the specified disk.

## Query parameters

* `goal`
    - `run`：submit the migration task
    - `status`：show the status of migration task

* `tablet_id`
    ID of the tablet

* `schema_hash`
    Schema hash

* `disk`
    The specified disk.

## Request body

None

## Response

### Submit Task

```
    {
        status: "Success",
        msg: "migration task is successfully submitted."
    }
```
Or
```
    {
        status: "Fail",
        msg: "Migration task submission failed"
    }
```

### Show Status

```
    {
        status: "Success",
        msg: "migration task is running",
        dest_disk: "xxxxxx"
    }
```

Or

```
    {
        status: "Success",
        msg: "migration task has finished successfully",
        dest_disk: "xxxxxx"
    }
```

Or

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

