---
{
    "title": "Disk Capacity Management",
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

# Disk Capacity Management

This document mainly introduces system parameters and processing strategies related to disk storage capacity. 

If Doris' data disk capacity is not controlled, the process will hang because the disk is full. Therefore, we monitor the disk usage and remaining capacity, and control various operations in the Doris system by setting different warning levels, and try to avoid the situation where the disk is full. 

## Glossary

* Data Dir：Data directory, each data directory specified in the `storage_root_path` of the BE configuration file `be.conf`. Usually a data directory corresponds to a disk, so the following **disk** also refers to a data directory. 

## Basic Principles

BE will report disk usage to FE on a regular basis (every minute). FE records these statistical values and restricts various operation requests based on these statistical values. 

Two thresholds, **High Watermark** and **Flood Stage**, are set in FE. Flood Stage is higher than High Watermark. When the disk usage is higher than High Watermark, Doris will restrict the execution of certain operations (such as replica balancing, etc.). If it is higher than Flood Stage, certain operations (such as load data) will be prohibited. 

At the same time, a **Flood Stage** is also set on the BE. Taking into account that FE cannot fully detect the disk usage on BE in a timely manner, and cannot control certain BE operations (such as Compaction). Therefore, Flood Stage on the BE is used for the BE to actively refuse and stop certain operations to achieve the purpose of self-protection. 

## FE Parameter

**High Watermark:**

```
storage_high_watermark_usage_percent: default value is 85 (85%).
storage_min_left_capacity_bytes: default value is 2GB.
```

When disk capacity **more than** `storage_high_watermark_usage_percent`, **or** disk free capacity **less than** `storage_min_left_capacity_bytes`, the disk will no longer be used as the destination path for the following operations:

* Tablet Balance
* Colocation Relocation
* Decommission

**Flood Stage:**

```
storage_flood_stage_usage_percent: default value is 95 (95%).
storage_flood_stage_left_capacity_bytes: default value is 1GB.
```

When disk capacity **more than** `storage_flood_stage_usage_percent`, **or** disk free capacity **less than** `storage_flood_stage_left_capacity_bytes`, the disk will no longer be used as the destination path for the following operations:
    
* Tablet Balance
* Colocation Relocation
* Replica make up
* Restore
* Load/Insert

## BE Parameter

**Flood Stage:**

```
storage_flood_stage_usage_percent: default value is 90 (90%).
storage_flood_stage_left_capacity_bytes: default value is 1GB.
```

When disk capacity **more than** `storage_flood_stage_usage_percent`, **and** disk free capacity **less than** `storage_flood_stage_left_capacity_bytes`, the following operations on this disk will be prohibited:

* Base/Cumulative Compaction
* Data load
* Clone Task (Usually occurs when the replica is repaired or balanced.)
* Push Task (Occurs during the Loading phase of Hadoop import, and the file is downloaded. )
* Alter Task (Schema Change or Rollup Task.)
* Download Task (The Downloading phase of the recovery operation.)
    
## Disk Capacity Release

When the disk capacity is higher than High Watermark or even Flood Stage, many operations will be prohibited. At this time, you can try to reduce the disk usage and restore the system in the following ways. 

* Delete table or partition 

    By deleting tables or partitions, you can quickly reduce the disk space usage and restore the cluster. 
    **Note: Only the `DROP` operation can achieve the purpose of quickly reducing the disk space usage, the `DELETE` operation cannot.**

    ```
    DROP TABLE tbl;
    ALTER TABLE tbl DROP PARTITION p1;
    ```
    
* BE expansion

    After backend expansion, data tablets will be automatically balanced to BE nodes with lower disk usage. The expansion operation will make the cluster reach a balanced state in a few hours or days depending on the amount of data and the number of nodes. 
    
* Modify replica of a table or partition 

    You can reduce the number of replica of a table or partition. For example, the default 3 replica can be reduced to 2 replica. Although this method reduces the reliability of the data, it can quickly reduce the disk usage rate and restore the cluster to normal.
    This method is usually used in emergency recovery systems. Please restore the number of copies to 3 after reducing the disk usage rate by expanding or deleting data after recovery.  
    Modifying the replica operation takes effect instantly, and the backends will automatically and asynchronously delete the redundant replica. 
    
    ```
    ALTER TABLE tbl MODIFY PARTITION p1 SET("replication_num" = "2");
    ```
    
* Delete unnecessary files 

    When the BE has crashed because the disk is full and cannot be started (this phenomenon may occur due to untimely detection of FE or BE), you need to delete some temporary files in the data directory to ensure that the BE process can start.
    Files in the following directories can be deleted directly: 

    * log/：Log files in the log directory. 
    * snapshot/: Snapshot files in the snapshot directory. 
    * trash/ Trash files in the trash directory. 

    **This operation will affect [Restore data from BE Recycle Bin](../data-admin/delete-recover.md).**

    If the BE can still be started, you can use `ADMIN CLEAN TRASH ON(BackendHost:BackendHeartBeatPort);` to actively clean up temporary files. **all trash files** and expired snapshot files will be cleaned up, **This will affect the operation of restoring data from the trash bin**.


    If you do not manually execute `ADMIN CLEAN TRASH`, the system will still automatically execute the cleanup within a few minutes to tens of minutes.There are two situations as follows: 
    * If the disk usage does not reach 90% of the **Flood Stage**, expired trash files and expired snapshot files will be cleaned up. At this time, some recent files will be retained without affecting the recovery of data. 
    * If the disk usage has reached 90% of the **Flood Stage**, **all trash files** and expired snapshot files will be cleaned up, **This will affect the operation of restoring data from the trash bin**.

    The time interval for automatic execution can be changed by `max_garbage_sweep_interval` and `min_garbage_sweep_interval` in the configuration items. 

    When the recovery fails due to lack of trash files, the following results may be returned: 

    ```
    {"status": "Fail","msg": "can find tablet path in trash"}
    ```

* Delete data file (dangerous!!!)

    When none of the above operations can free up capacity, you need to delete data files to free up space. The data file is in the `data/` directory of the specified data directory. To delete a tablet, you must first ensure that at least one replica of the tablet is normal, otherwise **deleting the only replica will result in data loss**. 
    
    Suppose we want to delete the tablet with id 12345: 
    
    * Find the directory corresponding to Tablet, usually under `data/shard_id/tablet_id/`. like: 

        ```data/0/12345/```
        
    * Record the tablet id and schema hash. The schema hash is the name of the next-level directory of the previous step. The following is 352781111: 

        ```data/0/12345/352781111```

    * Delete the data directory: 

        ```rm -rf data/0/12345/```

    * Delete tablet metadata (refer to [Tablet metadata management tool](./tablet-meta-tool.md)）

        ```./lib/meta_tool --operation=delete_header --root_path=/path/to/root_path --tablet_id=12345 --schema_hash= 352781111```
