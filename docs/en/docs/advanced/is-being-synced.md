---
{
    "title": "property is_being_synced",
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

# Background

<version since="2.0">

"is_being_synced" = "true"

</version>

CCR will create replica tables (referred to as target tables, located in the dest cluster) of the tables (referred to as source tables, located in the source cluster) in the synchronization scope of the source cluster in the dest cluster when establishing synchronization, but some functions and properties need to be invalidated or erased when creating replica tables to ensure the correctness of the synchronization process.  

such as：  
- The source table contains information that may not have been synchronized to the dest cluster, such as `storage_policy`, etc., which may cause the target table creation to fail or behave abnormally.
- The source table may contain some dynamic functions, such as dynamic partitioning, etc., which may cause the behavior of the target table to be out of syncer control and cause partition inconsistency.

The properties that need to be erased due to invalidation when being replicated are:
- `storage_policy`
- `colocate_with`

The functions that need to be invalidated when being synchronized are:
- auto bucket
- dynamic partition

# Implementation
When creating the target table, this properties will be added or deleted by syncer control. In CCR, there are two ways to create a target table:
1. When synchronizing tables, syncer uses backup/restore method to perform full replication of the source table to obtain the target table.
2. When synchronizing databases, for existing tables, syncer also uses backup/restore method to obtain the target table, for incremental tables, syncer will create the target table through binlog with CreateTableRecord.  

In summary, there are two entry points for inserting `is_being_synced` attribute: the restore process in full synchronization and the getDdlStmt in incremental synchronization.  

In the restore process of full synchronization, syncer will initiate the restore of the snapshot in the original cluster through rpc. In this process, it will add `is_being_synced` attribute to RestoreStmt and take effect in the final restoreJob, executing the relevant logic of `isBeingSynced`.  
In the getDdlStmt of incremental synchronization, add the parameter `boolean getDdlForSync` to the getDdlStmt method to distinguish whether it is an operation to convert to the target table ddl under control, and execute the relevant logic of `isBeingSynced` when creating the target table.
  
There is no need to say more about the erasure of invalid properties. The invalidation of the above functions needs to be explained:
1. auto bucket  
    Auto bucket takes effect when creating a table, calculating the current appropriate number of buckets, which may cause the number of buckets in the source table and the destination table to be inconsistent. Therefore, when synchronizing, you need to obtain the number of buckets in the source table, and also need to obtain the information whether the source table is an automatic bucketing table in order to restore the function after ending the synchronization. The current approach is to default autobucket to false when getting distribution information, and when restoring the table, check the `_auto_bucket` attribute to determine whether the source table is an automatic bucketing table. If so, set the autobucket field of the target table to true, so as to achieve the purpose of skipping calculating the number of buckets and directly applying the number of buckets in the source table.
2. dynamic partition  
    Dynamic partition is achieved by adding `olapTable.isBeingSynced()` to the judgment of whether to execute add/drop partition, so that the target table will not periodically execute add/drop partition operation during the synchronization process.
# Note
When no exception occurs, the `is_being_synced` attribute should be completely controlled by syncer to turn on or off, and users should not modify the attribute by themselves.