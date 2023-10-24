---
{
    "title": "ALTER-SYSTEM-DECOMMISSION-DISK",
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

## ALTER-SYSTEM-DECOMMISSION-DISK

### Name

ALTER SYSTEM DECOMMISSION DISK

### Description

The Disk offline operation is used to safely log off the disks. Commonly used for disk swapping operations. This operation is asynchronous. If successful, all data on the disk will be migrated to other nodes. (For administrator use only!)

grammar:

```sql
ALTER SYSTEM DECOMMISSION DISK "path"[,"path2"...] ON BACKEND "host:heartbeat_port";
```

 illustrate:
 
1. host can be a hostname or an ip address
2. heartbeat_port is the heartbeat port of the node
3. path is the absolute path to the disk on the node.
4. You can manually cancel the node offline operation. See CANCEL DECOMMISSION DISK

### Example

1. Offline two disks

    ```sql
      ALTER SYSTEM DECOMMISSION DISK "/home/disk1/palo.HDD", "/home/disk2/palo.HDD" ON BACKEND "host1:port";
    ```

### Keywords

    ALTER, SYSTEM, DECOMMISSION, DISK, ALTER SYSTEM

### Best Practice