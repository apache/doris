---
{
    "title": "MIGRATE SINGLE TABLET TO A PARTICULAR DISK",
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

# MIGRATE SINGLE TABLET TO A PARTICULAR DISK
   
Migrate single tablet to a particular disk.

Submit the migration task:

```
curl -X GET http://be_host:webserver_port/api/tablet_migration?goal=run&tablet_id=xxx&schema_hash=xxx&disk=xxx
```

The return is the submission result of the migration task:

```
    {
        status: "Success",
        msg: "migration task is successfully submitted."
    }
```

or

```
    {
        status: "Fail",
        msg: "Migration task submission failed"
    }
```

Show the status of migration task:

```
curl -X GET http://be_host:webserver_port/api/tablet_migration?goal=status&tablet_id=xxx&schema_hash=xxx
```

The return is the execution result of the migration task:

```
    {
        status: "Success",
        msg: "migration task is running.",
        dest_disk: "xxxxxx"
    }
```

or

```
    {
        status: "Success",
        msg: "migration task has finished successfully.",
        dest_disk: "xxxxxx"
    }
```

or

```
    {
        status: "Success",
        msg: "migration task failed.",
        dest_disk: "xxxxxx"
    }
```