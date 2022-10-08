---
{
    "title": "ADMIN-COPY-TABLET",
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

## ADMIN-COPY-TABLET

### Name

ADMIN COPY TABLET

### Description

This statement is used to make a snapshot for the specified tablet, mainly used to load the tablet locally to reproduce the problem.

syntax:

```sql
ADMIN COPY TABLET tablet_id PROPERTIES("xxx");
```

Notes:

This command requires ROOT privileges.

PROPERTIES supports the following properties:

1. backend_id: Specifies the id of the BE node where the replica is located. If not specified, a replica is randomly selected.

2. version: Specifies the version of the snapshot. The version must be less than or equal to the largest version of the replica. If not specified, the largest version is used.

3. expiration_minutes: Snapshot retention time. The default is 1 hour. It will automatically clean up after a timeout. Unit minutes.

The results are shown below:

```
         TabletId: 10020
        BackendId: 10003
               Ip: 192.168.10.1
             Path: /path/to/be/storage/snapshot/20220830101353.2.3600
ExpirationMinutes: 60
  CreateTableStmt: CREATE TABLE `tbl1` (
  `k1` int(11) NULL,
  `k2` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"version_info" = "2"
);
```

* TabletId: tablet id
* BackendId: BE node id
* Ip: BE node ip
* Path: The directory where the snapshot is located
* ExpirationMinutes: snapshot expiration time
* CreateTableStmt: The table creation statement for the table corresponding to the tablet. This statement is not the original table-building statement, but a simplified table-building statement for later loading the tablet locally.

### Example

1. Take a snapshot of the replica on the specified BE node

    ```sql
    ADMIN COPY TABLET 10010 PROPERTIES("backend_id" = "10001");
    ```

2. Take a snapshot of the specified version of the replica on the specified BE node

    ```sql
    ADMIN COPY TABLET 10010 PROPERTIES("backend_id" = "10001", "version" = "10");
    ```

### Keywords

    ADMIN, COPY, TABLET

### Best Practice

