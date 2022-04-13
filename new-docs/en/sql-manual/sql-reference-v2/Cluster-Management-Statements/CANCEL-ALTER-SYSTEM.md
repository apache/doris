---
{
    "title": "CANCEL-ALTER-SYSTEM",
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

## CANCEL-ALTER-SYSTEM

### Name

CANCEL DECOMMISSION

### Description

This statement is used to undo a node offline operation. (Administrator only!)

grammar:

```sql
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
````

### Example

  1. Cancel the offline operation of both nodes:

      ```sql
      CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
      ````

### Keywords

    CANCEL, DECOMMISSION

### Best Practice

