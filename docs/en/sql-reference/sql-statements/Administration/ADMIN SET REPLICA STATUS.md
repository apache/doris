---
{
    "title": "ADMIN SET REPLICA STATUS",
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

# ADMIN SET REPLICA STATUS
## description

    This commend is used to set the status of the specified replica.
    This command is currently only used to manually set the status of some replicas to BAD or OK, allowing the system to automatically repair these replicas.

    Syntax:

        ADMIN SET REPLICA STATUS
        PROPERTIES ("key" = "value", ...);

        The following attributes are currently supported:
        "tablet_id": required. Specify a Tablet Id.
        "backend_id": required. Specify a Backend Id.
        "status": required. Specify the status. Only "bad" and "ok" are currently supported.

        If the specified replica does not exist or the status is already bad or ok, it will be ignored.

    Notice:

        Replica set to Bad status may be dropped immediately, please proceed with caution.

## example

    1. Set the replica status of tablet 10003 on BE 10001 to bad.

        ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");

    2. Set the replica status of tablet 10003 on BE 10001 to ok.

        ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "ok");

## keyword

    ADMIN,SET,REPLICA,STATUS

