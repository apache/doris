---
{
    "title": "SHOW BACKENDS",
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

# SHOW BACKENDS
## Description
This statement is used to view BE nodes in the cluster
Grammar:
SHOW BACKENDS;

Explain:
1. LastStartTime indicates the last BE start-up time.
2. LastHeartbeat represents the latest heartbeat.
3. Alive indicates whether the node survives.
4. System Decommissioned is true to indicate that the node is safely offline.
5. Cluster Decommissioned is true to indicate that the node is rushing downline in the current cluster.
6. TabletNum represents the number of fragments on the node.
7. Data Used Capacity represents the space occupied by the actual user data.
8. Avail Capacity represents the available space on the disk.
9. Total Capacity represents total disk space. Total Capacity = AvailCapacity + DataUsedCapacity + other non-user data files take up space.
10. UsedPct represents the percentage of disk usage.
11. ErrMsg is used to display error messages when a heartbeat fails.
12. Status is used to display some Status information about BE in JSON format, including the last time that BE reported it's tablet.

## keyword
SHOW, BACKENDS
