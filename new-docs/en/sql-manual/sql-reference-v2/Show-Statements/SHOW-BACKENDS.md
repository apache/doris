---
{
    "title": "SHOW-BACKENDS",
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

## SHOW-BACKENDS

### Name

SHOW BACKENDS

### Description

This statement is used to view the BE nodes in the cluster

```sql
 SHOW BACKENDS;
````

illustrate:

        1. LastStartTime indicates the last BE start time.
        2. LastHeartbeat indicates the last heartbeat.
        3. Alive indicates whether the node is alive or not.
        4. If SystemDecommissioned is true, it means that the node is being safely decommissioned.
        5. If ClusterDecommissioned is true, it means that the node is going offline in the current cluster.
        6. TabletNum represents the number of shards on the node.
        7. DataUsedCapacity Indicates the space occupied by the actual user data.
        8. AvailCapacity Indicates the available space on the disk.
        9. TotalCapacity represents the total disk space. TotalCapacity = AvailCapacity + DataUsedCapacity + other non-user data files occupy space.
       10. UsedPct Indicates the percentage of disk used.
       11. ErrMsg is used to display the error message when the heartbeat fails.
       12. Status is used to display some status information of BE in JSON format, including the time information of the last time BE reported its tablet.

### Example

### Keywords

    SHOW, BACKENDS

### Best Practice

