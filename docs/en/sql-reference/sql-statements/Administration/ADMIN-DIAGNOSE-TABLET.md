---
{
    "title": "ADMIN DIAGNOSE TABLET",
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

# ADMIN DIAGNOSE TABLET
## description

    This statement is used to diagnose the specified tablet. The results will show information about the tablet and some potential problems.

    grammar:

        ADMIN DIAGNOSE TABLET tblet_id

    illustrate:

        The lines of information in the result are as follows:
        1. TabletExist:                         Whether the Tablet exists
        2. TabletId:                            Tablet ID
        3. Database:                            The DB to which the Tablet belongs and its ID
        4. Table:                               The Table to which Tablet belongs and its ID
        5. Partition:                           The Partition to which the Tablet belongs and its ID
        6. MaterializedIndex:                   The materialized view to which the Tablet belongs and its ID
        7. Replicas(ReplicaId -> BackendId):    Tablet replicas and their BE.
        8. ReplicasNum:                         Whether the number of replicas is correct.
        9. ReplicaBackendStatus:                Whether the BE node where the replica is located is normal.
        10.ReplicaVersionStatus:                Whether the version number of the replica is normal.
        11.ReplicaStatus:                       Whether the replica status is normal.
        12.ReplicaCompactionStatus:             Whether the replica Compaction status is normal.
        
## example

    1. Diagnose tablet 10001

        ADMIN DIAGNOSE TABLET 10001;
        
## keyword
    ADMIN,DIAGNOSE,TABLET
