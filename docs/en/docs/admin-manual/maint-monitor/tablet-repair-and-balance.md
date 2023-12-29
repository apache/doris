---
{
    "title": "Data replica management",
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

# Data replica management

Beginning with version 0.9.0, Doris introduced an optimized replica management strategy and supported a richer replica status viewing tool. This document focuses on Doris data replica balancing, repair scheduling strategies, and replica management operations and maintenance methods. Help users to more easily master and manage the replica status in the cluster.

> Repairing and balancing copies of tables with Colocation attributes can be referred to [HERE](../../query-acceleration/join-optimization/colocation-join.md)

## Noun Interpretation

1. Tablet: The logical fragmentation of a Doris table, where a table has multiple fragments.
2. Replica: A sliced copy, defaulting to three copies of a slice.
3. Healthy Replica: A healthy copy that survives at Backend and has a complete version.
4. Tablet Checker (TC): A resident background thread that scans all Tablets regularly, checks the status of these Tablets, and decides whether to send them to Tablet Scheduler based on the results.
5. Tablet Scheduler (TS): A resident background thread that handles Tablets sent by Tablet Checker that need to be repaired. At the same time, cluster replica balancing will be carried out.
6. Tablet SchedCtx (TSC): is a tablet encapsulation. When TC chooses a tablet, it encapsulates it as a TSC and sends it to TS.
7. Storage Medium: Storage medium. Doris supports specifying different storage media for partition granularity, including SSD and HDD. The replica scheduling strategy is also scheduled for different storage media.

```

              +--------+              +-----------+
              |  Meta  |              |  Backends |
              +---^----+              +------^----+
                  | |                        | 3. Send clone tasks
 1. Check tablets | |                        |
           +--------v------+        +-----------------+
           | TabletChecker +--------> TabletScheduler |
           +---------------+        +-----------------+
                   2. Waiting to be scheduled


```
The figure above is a simplified workflow.


## Duplicate status

Multiple copies of a Tablet may cause state inconsistencies due to certain circumstances. Doris will attempt to automatically fix the inconsistent copies of these states so that the cluster can recover from the wrong state as soon as possible.

**The health status of a Replica is as follows:**

1. BAD

	That is, the copy is damaged. Includes, but is not limited to, the irrecoverable damaged status of copies caused by disk failures, BUGs, etc.

2. VERSION\_MISSING

	Version missing. Each batch of imports in Doris corresponds to a data version. A copy of the data consists of several consecutive versions. However, due to import errors, delays and other reasons, the data version of some copies may be incomplete.

3. HEALTHY

	Health copy. That is, a copy of the normal data, and the BE node where the copy is located is in a normal state (heartbeat is normal and not in the offline process).

**The health status of a Tablet is determined by the status of all its copies. There are the following categories:**

1. REPLICA\_MISSING

	The copy is missing. That is, the number of surviving copies is less than the expected number of copies.

2. VERSION\_INCOMPLETE

	The number of surviving copies is greater than or equal to the number of expected copies, but the number of healthy copies is less than the number of expected copies.

3. REPLICA\_RELOCATING

	Have a full number of live copies of the replication num version, but the BE nodes where some copies are located are in unavailable state (such as decommission)

4. REPLICA\_MISSING\_IN\_CLUSTER

	When using multi-cluster, the number of healthy replicas is greater than or equal to the expected number of replicas, but the number of replicas in the corresponding cluster is less than the expected number of replicas.

5. REDUNDANT

	Duplicate redundancy. Healthy replicas are in the corresponding cluster, but the number of replicas is larger than the expected number. Or there's a spare copy of unavailable.

6. FORCE\_REDUNDANT

	This is a special state. It only occurs when the number of existed replicas is greater than or equal to the number of available nodes, and the number of available nodes is greater than or equal to the number of expected replicas, and when the number of alive replicas is less than the number of expected replicas. In this case, you need to delete a copy first to ensure that there are available nodes for creating a new copy.

7. COLOCATE\_MISMATCH

	Fragmentation status of tables for Collocation attributes. Represents that the distribution of fragmented copies is inconsistent with the specified distribution of Colocation Group.

8. COLOCATE\_REDUNDANT

	Fragmentation status of tables for Collocation attributes. Represents the fragmented copy redundancy of the Colocation table.

8. HEALTHY

	Healthy fragmentation, that is, conditions [1-5] are not satisfied.

## Replica Repair

As a resident background process, Tablet Checker regularly checks the status of all fragments. For unhealthy fragmentation, it will be sent to Tablet Scheduler for scheduling and repair. The actual operation of repair is accomplished by clone task on BE. FE is only responsible for generating these clone tasks.

> Note 1: The main idea of replica repair is to make the number of fragmented replicas reach the desired value by creating or completing them first. Then delete the redundant copy.
>
> Note 2: A clone task is to complete the process of copying specified data from a specified remote end to a specified destination.

For different states, we adopt different repair methods:

1. REPLICA\_MISSING/REPLICA\_RELOCATING

	Select a low-load, available BE node as the destination. Choose a healthy copy as the source. Clone tasks copy a complete copy from the source to the destination. For replica completion, we will directly select an available BE node, regardless of the storage medium.

2. VERSION\_INCOMPLETE

	Select a relatively complete copy as the destination. Choose a healthy copy as the source. The clone task attempts to copy the missing version from the source to the destination.

3. REPLICA\_MISSING\_IN\_CLUSTER

	This state processing method is the same as REPLICA\_MISSING.

4. REDUNDANT

	Usually, after repair, there will be redundant copies in fragmentation. We select a redundant copy to delete it. The selection of redundant copies follows the following priorities:
	1. The BE where the copy is located has been offline.
	2. The copy is damaged
	3. The copy is lost in BE or offline
	4. The replica is in the CLONE state (which is an intermediate state during clone task execution)
	5. The copy has version missing
	6. The cluster where the copy is located is incorrect
	7. The BE node where the replica is located has a high load

5. FORCE\_REDUNDANT

	Unlike REDUNDANT, because at this point Tablet has a copy missing, because there are no additional available nodes for creating new copies. So at this point, a copy must be deleted to free up a available node for creating a new copy.
	The order of deleting copies is the same as REDUNDANT.

6. COLOCATE\_MISMATCH

	Select one of the replica distribution BE nodes specified in Colocation Group as the destination node for replica completion.

7. COLOCATE\_REDUNDANT

	Delete a copy on a BE node that is distributed by a copy specified in a non-Colocation Group.

	Doris does not deploy a copy of the same Tablet on a different BE of the same host when selecting a replica node. It ensures that even if all BEs on the same host are deactivated, all copies will not be lost.

### Scheduling priority

Waiting for the scheduled fragments in Tablet Scheduler gives different priorities depending on the status. High priority fragments will be scheduled first. There are currently several priorities.

1. VERY\_HIGH

	* REDUNDANT. For slices with duplicate redundancy, we give priority to them. Logically, duplicate redundancy is the least urgent, but because it is the fastest to handle and can quickly release resources (such as disk space, etc.), we give priority to it.
	* FORCE\_REDUNDANT. Ditto.

2. HIGH

	* REPLICA\_MISSING and most copies are missing (for example, 2 copies are missing in 3 copies)
	* VERSION\_INCOMPLETE and most copies are missing
	* COLOCATE\_MISMATCH We hope that the fragmentation related to the Collocation table can be repaired as soon as possible.
	* COLOCATE\_REDUNDANT

3. NORMAL

	* REPLICA\_MISSING, but most survive (for example, three copies lost one)
	* VERSION\_INCOMPLETE, but most copies are complete
	* REPLICA\_RELOCATING and relocate is required for most replicas (e.g. 3 replicas with 2 replicas)

4. LOW

	* REPLICA\_MISSING\_IN\_CLUSTER
	* REPLICA\_RELOCATING most copies stable

### Manual priority

The system will automatically determine the scheduling priority. Sometimes, however, users want the fragmentation of some tables or partitions to be repaired faster. So we provide a command that the user can specify that a slice of a table or partition is repaired first:

`ADMIN REPAIR TABLE tbl [PARTITION (p1, p2, ...)];`

This command tells TC to give VERY HIGH priority to the problematic tables or partitions that need to be repaired first when scanning Tablets.

> Note: This command is only a hint, which does not guarantee that the repair will be successful, and the priority will change with the scheduling of TS. And when Master FE switches or restarts, this information will be lost.

Priority can be cancelled by the following commands:

`ADMIN CANCEL REPAIR TABLE tbl [PARTITION (p1, p2, ...)];`

### Priority scheduling

Priority ensures that severely damaged fragments can be repaired first, and improves system availability. But if the high priority repair task fails all the time, the low priority task will never be scheduled. Therefore, we will dynamically adjust the priority of tasks according to the running status of tasks, so as to ensure that all tasks have the opportunity to be scheduled.

* If the scheduling fails for five consecutive times (e.g., no resources can be obtained, no suitable source or destination can be found, etc.), the priority will be lowered.
* If not scheduled for 30 minutes, priority will be raised.
* The priority of the same tablet task is adjusted at least five minutes apart.

At the same time, in order to ensure the weight of the initial priority, we stipulate that the initial priority is VERY HIGH, and the lowest is lowered to NORMAL. When the initial priority is LOW, it is raised to HIGH at most. The priority adjustment here also adjusts the priority set manually by the user.

## Replicas Balance

Doris automatically balances replicas within the cluster. Currently supports two rebalance strategies, BeLoad and Partition. BeLoad rebalance will consider about the disk usage and replica count for each BE. Partition rebalance just aim at replica count for each partition, this helps to avoid hot spots. If you want high read/write performance, you may need this. Note that Partition rebalance do not consider about the disk usage, pay more attention to it when you are using Partition rebalance. The strategy selection config is not mutable at runtime. 

### BeLoad

The main idea of balancing is to create a replica of some fragments on low-load nodes, and then delete the replicas of these fragments on high-load nodes. At the same time, because of the existence of different storage media, there may or may not exist one or two storage media on different BE nodes in the same cluster. We require that fragments of storage medium A be stored in storage medium A as far as possible after equalization. So we divide the BE nodes of the cluster according to the storage medium. Then load balancing scheduling is carried out for different BE node sets of storage media.

Similarly, replica balancing ensures that a copy of the same table will not be deployed on the BE of the same host.

### BE Node Load

We use Cluster LoadStatistics (CLS) to represent the load balancing of each backend in a cluster. Tablet Scheduler triggers cluster equilibrium based on this statistic. We currently calculate a load Score for each BE as the BE load score by using **disk usage** and **number of copies**. The higher the score, the heavier the load on the BE.

Disk usage and number of copies have a weight factor, which is **capacityCoefficient** and **replicaNumCoefficient**, respectively. The sum of them is **constant to 1**. Among them, capacityCoefficient will dynamically adjust according to actual disk utilization. When the overall disk utilization of a BE is below 50%, the capacityCoefficient value is 0.5, and if the disk utilization is above 75% (configurable through the FE configuration item `capacity_used_percent_high_water`), the value is 1. If the utilization rate is between 50% and 75%, the weight coefficient increases smoothly. The formula is as follows:

`capacityCoefficient = 2 * Disk Utilization - 0.5`

The weight coefficient ensures that when disk utilization is too high, the backend load score will be higher to ensure that the BE load is reduced as soon as possible.

Tablet Scheduler updates CLS every 20 seconds.

### Partition

The main idea of `partition rebalancing` is to decrease the skew of partitions. The skew of the partition is defined as the difference between the maximum replica count of the partition over all bes and the minimum replica count over all bes. 

So we only consider about the replica count, do not consider replica size(disk usage).
To fewer moves, we use TwoDimensionalGreedyAlgo which two dims are cluster & partition. It prefers a move that reduce the skew of the cluster when we want to rebalance a max skew partition. 

#### Skew Info

The skew info is represented by `ClusterBalanceInfo`. `partitionInfoBySkew` is a multimap which key is the partition's skew, so we can get max skew partitions simply. `beByTotalReplicaCount` is a multimap which key is the total replica count of the backend.

`ClusterBalanceInfo` is in CLS, updated every 20 seconds.

When get more than one max skew partitions, we random select one partition to calculate the move.

### Equilibrium strategy

Tablet Scheduler uses Load Balancer to select a certain number of healthy fragments as candidate fragments for balance in each round of scheduling. In the next scheduling, balanced scheduling will be attempted based on these candidate fragments.

## Resource control

Both replica repair and balancing are accomplished by replica copies between BEs. If the same BE performs too many tasks at the same time, it will bring a lot of IO pressure. Therefore, Doris controls the number of tasks that can be performed on each node during scheduling. The smallest resource control unit is the disk (that is, a data path specified in be.conf). By default, we configure two slots per disk for replica repair. A clone task occupies one slot at the source and one slot at the destination. If the number of slots is zero, no more tasks will be assigned to this disk. The number of slots can be configured by FE's `schedule_slot_num_per_hdd_path` or `schedule_slot_num_per_ssd_path` parameter.

In addition, by default, we provide two separate slots per disk for balancing tasks. The purpose is to prevent high-load nodes from losing space by balancing because slots are occupied by repair tasks.

## Tablet State View

Tablet state view mainly looks at the state of the tablet, as well as the state of the tablet repair and balancing tasks. Most of these states **exist only in** Master FE nodes. Therefore, the following commands need to be executed directly to Master FE.

### Tablet state

1. Global state checking

	Through `SHOW PROC'/cluster_health/tablet_health'; `commands can view the replica status of the entire cluster.

     ```
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    | DbId  | DbName                         | TabletNum | HealthyNum | ReplicaMissingNum | VersionIncompleteNum | ReplicaRelocatingNum | RedundantNum | ReplicaMissingInClusterNum | ReplicaMissingForTagNum | ForceRedundantNum | ColocateMismatchNum | ColocateRedundantNum | NeedFurtherRepairNum | UnrecoverableNum | ReplicaCompactionTooSlowNum | InconsistentNum | OversizeNum | CloningNum |
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    | 10005 | default_cluster:doris_audit_db | 84        | 84         | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | 13402 | default_cluster:ssb1           | 709       | 708        | 1                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | 10108 | default_cluster:tpch1          | 278       | 278        | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | Total | 3                              | 1071      | 1070       | 1                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    ```

	The `HealthyNum` column shows how many Tablets are in a healthy state in the corresponding database. `ReplicaCompactionTooSlowNum` column shows how many Tablets are in a too many versions state in the corresponding database, `InconsistentNum` column shows how many Tablets are in an inconsistent replica state in the corresponding database. The last `Total` line counts the entire cluster. Normally `TabletNum` and `HealthyNum` should be equal. If it's not equal, you can further see which Tablets are there. As shown in the figure above, one table in the ssb1 database is not healthy, you can use the following command to see which one is.

    `SHOW PROC '/cluster_health/tablet_health/13402';`

	Among them `13402` is the corresponding DbId.

   ```
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   | ReplicaMissingTablets | VersionIncompleteTablets | ReplicaRelocatingTablets | RedundantTablets | ReplicaMissingInClusterTablets | ReplicaMissingForTagTablets | ForceRedundantTablets | ColocateMismatchTablets | ColocateRedundantTablets | NeedFurtherRepairTablets | UnrecoverableTablets | ReplicaCompactionTooSlowTablets | InconsistentTablets | OversizeTablets |
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   | 14679                 |                          |                          |                  |                                |                             |                       |                         |                          |                          |                      |                                 |                     |                 |
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   ```

	The figure above shows the specific unhealthy Tablet ID (14679). Later we'll show you how to view the status of each copy of a specific Tablet.

2. Table (partition) level status checking

	Users can view the status of a copy of a specified table or partition through the following commands and filter the status through a WHERE statement. If you look at table tbl1, the state on partitions P1 and P2 is a copy of OK:

	`ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2) WHERE STATUS = "OK";`

	```
	+----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
	| TabletId | ReplicaId | BackendId | Version | LastFailedVersion | LastSuccessVersion | CommittedVersion | SchemaHash | VersionNum | IsBad | State  | Status |
	+----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
	| 29502429 | 29502432  | 10006     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
	| 29502429 | 36885996  | 10002     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
	| 29502429 | 48100551  | 10007     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
	| 29502433 | 29502434  | 10001     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
	| 29502433 | 44900737  | 10004     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
	| 29502433 | 48369135  | 10006     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
	+----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
   ```

	The status of all copies is shown here. Where `IsBad` is listed as `true`, the copy is damaged. The `Status` column displays other states. Specific status description, you can see help through `HELP ADMIN SHOW REPLICA STATUS`.

	` The ADMIN SHOW REPLICA STATUS `command is mainly used to view the health status of copies. Users can also view additional information about copies of a specified table by using the following commands:

	`SHOW TABLETS FROM tbl1;`

    ```
	+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
	| TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion | 	CheckVersionHash | VersionCount | PathHash             | MetaUrl              | CompactionStatus     |
	+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
	| 29502429 | 29502432  | 10006     | 1421156361 | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           | 	-1               | 2            | -5822326203532286804 | url                  | url                  |
	| 29502429 | 36885996  | 10002     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           | 	-1               | 2            | -1441285706148429853 | url                  | url                  |
	| 29502429 | 48100551  | 10007     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           | 	-1               | 2            | -4784691547051455525 | url                  | url                  |
	+----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+  
    ```
   
	The figure above shows some additional information, including copy size, number of rows, number of versions, where the data path is located.

	> Note: The contents of the `State` column shown here do not represent the health status of the replica, but the status of the replica under certain tasks, such as CLONE, SCHEMA CHANGE, ROLLUP, etc.

	In addition, users can check the distribution of replicas in a specified table or partition by following commands.

	`ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;`

    ```
	+-----------+------------+-------+---------+
	| BackendId | ReplicaNum | Graph | Percent |
	+-----------+------------+-------+---------+
	| 10000     | 7          |       | 7.29 %  |
	| 10001     | 9          |       | 9.38 %  |
	| 10002     | 7          |       | 7.29 %  |
	| 10003     | 7          |       | 7.29 %  |
	| 10004     | 9          |       | 9.38 %  |
	| 10005     | 11         | >     | 11.46 % |
	| 10006     | 18         | >     | 18.75 % |
	| 10007     | 15         | >     | 15.62 % |
	| 10008     | 13         | >     | 13.54 % |
	+-----------+------------+-------+---------+
    ```

	Here we show the number and percentage of replicas of table tbl1 on each BE node, as well as a simple graphical display.

4. Tablet level status checking

	When we want to locate a specific Tablet, we can use the following command to view the status of a specific Tablet. For example, check the tablet with ID 2950253:

	`SHOW TABLET 29502553;`

    ```
	+------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
	| DbName                 | TableName | PartitionName | IndexName | DbId     | TableId  | PartitionId | IndexId  | IsSync | DetailCmd                                                                 |
	+------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
	| default_cluster:test   | test      | test          | test      | 29502391 | 29502428 | 29502427    | 29502428 | true   | SHOW PROC '/dbs/29502391/29502428/partitions/29502427/29502428/29502553'; |
	+------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    ```

	The figure above shows the database, tables, partitions, roll-up tables and other information corresponding to this tablet. The user can copy the command in the `DetailCmd` command to continue executing:

	`Show Proc'/DBS/29502391/29502428/Partitions/29502427/29502428/29502553;`

    ```
	+-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+
	| ReplicaId | BackendId | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | SchemaHash | DataSize | RowCount | State  | IsBad | VersionCount | PathHash             |
	+-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+
	| 43734060  | 10004     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | -8566523878520798656 |
	| 29502555  | 10002     | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1885826196444191611  |
	| 39279319  | 10007     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1656508631294397870  |
	+-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+
    ```
   
	The figure above shows all replicas of the corresponding Tablet. The content shown here is the same as `SHOW TABLETS FROM tbl1;`. But here you can clearly see the status of all copies of a specific Tablet.

### Duplicate Scheduling Task

1. View tasks waiting to be scheduled

	`SHOW PROC '/cluster_balance/pending_tablets';`

    ```
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | TabletId | Type   | Status          | State   | OrigPrio | DynmPrio | SrcBe | SrcPath | DestBe | DestPath | Timeout | Create              | LstSched            | LstVisit            | Finished | Rate | FailedSched | FailedRunning | LstAdjPrio          | VisibleVer | VisibleVerHash      | CmtVer | CmtVerHash          | ErrMsg                        |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | 4203036  | REPAIR | REPLICA_MISSING | PENDING | HIGH     | LOW      | -1    | -1      | -1     | -1       | 0       | 2019-02-21 15:00:20 | 2019-02-24 11:18:41 | 2019-02-24 11:18:41 | N/A      | N/A  | 2           | 0             | 2019-02-21 15:00:43 | 1          | 0                   | 2      | 0                   | unable to find source replica |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    ```
   
	The specific meanings of each column are as follows:

	* TabletId: The ID of the Tablet waiting to be scheduled. A scheduling task is for only one Tablet
	* Type: Task type, which can be REPAIR (repair) or BALANCE (balance)
	* Status: The current status of the Tablet, such as REPLICA\_MISSING (copy missing)
	* State: The status of the scheduling task may be PENDING/RUNNING/FINISHED/CANCELLED/TIMEOUT/UNEXPECTED
	* OrigPrio: Initial Priority
	* DynmPrio: Current dynamically adjusted priority
	* SrcBe: ID of the BE node at the source end
	* SrcPath: hash value of the path of the BE node at the source end
	* DestBe: ID of destination BE node
	* DestPath: hash value of the path of the destination BE node
	* Timeout: When the task is scheduled successfully, the timeout time of the task is displayed here in units of seconds.
	* Create: The time when the task was created
	* LstSched: The last time a task was scheduled
	* LstVisit: The last time a task was accessed. Here "accessed" refers to the processing time points associated with the task, including scheduling, task execution reporting, and so on.
	* Finished: Task End Time
	* Rate: Clone Task Data Copy Rate
	* Failed Sched: Number of Task Scheduling Failures
	* Failed Running: Number of task execution failures
	* LstAdjPrio: Time of last priority adjustment
	* CmtVer/CmtVerHash/VisibleVer/VisibleVerHash: version information for clone tasks
	* ErrMsg: Error messages that occur when tasks are scheduled and run

2. View running tasks

	`SHOW PROC '/cluster_balance/running_tablets';`

	The columns in the result have the same meaning as `pending_tablets`.

3. View completed tasks

	`SHOW PROC '/cluster_balance/history_tablets';`

	By default, we reserve only the last 1,000 completed tasks. The columns in the result have the same meaning as `pending_tablets`. If `State` is listed as `FINISHED`, the task is normally completed. For others, you can see the specific reason based on the error information in the `ErrMsg` column.

## Viewing Cluster Load and Scheduling Resources

1. Cluster load

	You can view the current load of the cluster by following commands:

	`SHOW PROC '/cluster_balance/cluster_load_stat/location_default';`

	First of all, we can see the division of different storage media:

    ```
    +---------------+
    | StorageMedium |
    +---------------+
    | HDD           |
    | SSD           |
    +---------------+
    ```

	Click on a storage medium to see the equilibrium state of the BE node that contains the storage medium:

	`SHOW PROC '/cluster_balance/cluster_load_stat/location_default/HDD';`

	```
	+----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
	| BeId     | Cluster         | Available | UsedCapacity  | Capacity       | UsedPercent | ReplicaNum | CapCoeff | ReplCoeff | Score              | Class |
	+----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
	| 10003    | default_cluster | true      | 3477875259079 | 19377459077121 | 17.948      | 493477     | 0.5      | 0.5       | 0.9284678149967587 | MID   |
	| 10002    | default_cluster | true      | 3607326225443 | 19377459077121 | 18.616      | 496928     | 0.5      | 0.5       | 0.948660871419998  | MID   |
	| 10005    | default_cluster | true      | 3523518578241 | 19377459077121 | 18.184      | 545331     | 0.5      | 0.5       | 0.9843539990641831 | MID   |
	| 10001    | default_cluster | true      | 3535547090016 | 19377459077121 | 18.246      | 558067     | 0.5      | 0.5       | 0.9981869446537612 | MID   |
	| 10006    | default_cluster | true      | 3636050364835 | 19377459077121 | 18.764      | 547543     | 0.5      | 0.5       | 1.0011489897614072 | MID   |
	| 10004    | default_cluster | true      | 3506558163744 | 15501967261697 | 22.620      | 468957     | 0.5      | 0.5       | 1.0228319835582569 | MID   |
	| 10007    | default_cluster | true      | 4036460478905 | 19377459077121 | 20.831      | 551645     | 0.5      | 0.5       | 1.057279369420761  | MID   |
	| 10000    | default_cluster | true      | 4369719923760 | 19377459077121 | 22.551      | 547175     | 0.5      | 0.5       | 1.0964036415787461 | MID   |
	+----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
	```

	Some of these columns have the following meanings:

	* Available: True means that BE heartbeat is normal and not offline.
	* UsedCapacity: Bytes, the size of disk space used on BE
	* Capacity: Bytes, the total disk space size on BE
	* UsedPercent: Percentage, disk space utilization on BE
	* ReplicaNum: Number of copies on BE
	* CapCoeff/ReplCoeff: Weight Coefficient of Disk Space and Copy Number
	* Score: Load score. The higher the score, the heavier the load.
	* Class: Classified by load, LOW/MID/HIGH. Balanced scheduling moves copies from high-load nodes to low-load nodes

	Users can further view the utilization of each path on a BE, such as the BE with ID 10001:

	`SHOW PROC '/cluster_balance/cluster_load_stat/location_default/HDD/10001';`

    ```
	+------------------+------------------+---------------+---------------+---------+--------+----------------------+
	| RootPath         | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | State  | PathHash             |
	+------------------+------------------+---------------+---------------+---------+--------+----------------------+
	| /home/disk4/palo | 498.757 GB       | 3.033 TB      | 3.525 TB      | 13.94 % | ONLINE | 4883406271918338267  |
	| /home/disk3/palo | 704.200 GB       | 2.832 TB      | 3.525 TB      | 19.65 % | ONLINE | -5467083960906519443 |
	| /home/disk1/palo | 512.833 GB       | 3.007 TB      | 3.525 TB      | 14.69 % | ONLINE | -7733211489989964053 |
	| /home/disk2/palo | 881.955 GB       | 2.656 TB      | 3.525 TB      | 24.65 % | ONLINE | 4870995507205544622  |
	| /home/disk5/palo | 694.992 GB       | 2.842 TB      | 3.525 TB      | 19.36 % | ONLINE | 1916696897889786739  |
	+------------------+------------------+---------------+---------------+---------+--------+----------------------+
	 ```

	The disk usage of each data path on the specified BE is shown here.

2. Scheduling resources

	Users can view the current slot usage of each node through the following commands:

	`SHOW PROC '/cluster_balance/working_slots';`

    ```
    +----------+----------------------+------------+------------+-------------+----------------------+
    | BeId     | PathHash             | AvailSlots | TotalSlots | BalanceSlot | AvgRate              |
    +----------+----------------------+------------+------------+-------------+----------------------+
    | 10000    | 8110346074333016794  | 2          | 2          | 2           | 2.459007474009069E7  |
    | 10000    | -5617618290584731137 | 2          | 2          | 2           | 2.4730105014001578E7 |
    | 10001    | 4883406271918338267  | 2          | 2          | 2           | 1.6711402709780257E7 |
    | 10001    | -5467083960906519443 | 2          | 2          | 2           | 2.7540126380326536E7 |
    | 10002    | 9137404661108133814  | 2          | 2          | 2           | 2.417217089806745E7  |
    | 10002    | 1885826196444191611  | 2          | 2          | 2           | 1.6327378456676323E7 |
    +----------+----------------------+------------+------------+-------------+----------------------+
    ```

	In this paper, data path is used as granularity to show the current use of slot. Among them, `AvgRate'is the copy rate of clone task in bytes/seconds on the path of historical statistics.

3. Priority repair view

	The following command allows you to view the priority repaired tables or partitions set by the `ADMIN REPAIR TABLE'command.

	`SHOW PROC '/cluster_balance/priority_repair';`

	Among them, `Remaining TimeMs'indicates that these priority fixes will be automatically removed from the priority fix queue after this time. In order to prevent resources from being occupied due to the failure of priority repair.

### Scheduler Statistical Status View

We have collected some statistics of Tablet Checker and Tablet Scheduler during their operation, which can be viewed through the following commands:

`SHOW PROC '/cluster_balance/sched_stat';`

```
+---------------------------------------------------+-------------+
| Item                                              | Value       |
+---------------------------------------------------+-------------+
| num of tablet check round                         | 12041       |
| cost of tablet check(ms)                          | 7162342     |
| num of tablet checked in tablet checker           | 18793506362 |
| num of unhealthy tablet checked in tablet checker | 7043900     |
| num of tablet being added to tablet scheduler     | 1153        |
| num of tablet schedule round                      | 49538       |
| cost of tablet schedule(ms)                       | 49822       |
| num of tablet being scheduled                     | 4356200     |
| num of tablet being scheduled succeeded           | 320         |
| num of tablet being scheduled failed              | 4355594     |
| num of tablet being scheduled discard             | 286         |
| num of tablet priority upgraded                   | 0           |
| num of tablet priority downgraded                 | 1096        |
| num of clone task                                 | 230         |
| num of clone task succeeded                       | 228         |
| num of clone task failed                          | 2           |
| num of clone task timeout                         | 2           |
| num of replica missing error                      | 4354857     |
| num of replica version missing error              | 967         |
| num of replica relocating                         | 0           |
| num of replica redundant error                    | 90          |
| num of replica missing in cluster error           | 0           |
| num of balance scheduled                          | 0           |
+---------------------------------------------------+-------------+
```

The meanings of each line are as follows:

* num of tablet check round: Tablet Checker 检查次数
* cost of tablet check(ms): Tablet Checker 检查总耗时
* num of tablet checked in tablet checker: Tablet Checker 检查过的 tablet 数量
* num of unhealthy tablet checked in tablet checker: Tablet Checker 检查过的不健康的 tablet 数量
* num of tablet being added to tablet scheduler: 被提交到 Tablet Scheduler 中的 tablet 数量
* num of tablet schedule round: Tablet Scheduler 运行次数
* cost of tablet schedule(ms): Tablet Scheduler 运行总耗时
* num of tablet being scheduled: 被调度的 Tablet 总数量
* num of tablet being scheduled succeeded: 被成功调度的 Tablet 总数量
* num of tablet being scheduled failed: 调度失败的 Tablet 总数量
* num of tablet being scheduled discard: 调度失败且被抛弃的 Tablet 总数量
* num of tablet priority upgraded: 优先级上调次数
* num of tablet priority downgraded: 优先级下调次数
* num of clone task: number of clone tasks generated
* num of clone task succeeded: clone 任务成功的数量
* num of clone task failed: clone 任务失败的数量
* num of clone task timeout: clone 任务超时的数量
* num of replica missing error: the number of tablets whose status is checked is the missing copy
* num of replica version missing error: 检查的状态为版本缺失的 tablet 的数量（该统计值包括了 num of replica relocating 和 num of replica missing in cluster error）
*num of replica relocation *29366;* 24577;*replica relocation tablet *
* num of replica redundant error: Number of tablets whose checked status is replica redundant
* num of replica missing in cluster error: 检查的状态为不在对应 cluster 的 tablet 的数量
* num of balance scheduled: 均衡调度的次数

> Note: The above states are only historical accumulative values. We also print these statistics regularly in the FE logs, where the values in parentheses represent the number of changes in each statistical value since the last printing dependence of the statistical information.

## Relevant configuration instructions

### Adjustable parameters

The following adjustable parameters are all configurable parameters in fe.conf.

* use\_new\_tablet\_scheduler

	* Description: Whether to enable the new replica scheduling mode. The new replica scheduling method is the replica scheduling method introduced in this document. If turned on, `disable_colocate_join` must be `true`. Because the new scheduling strategy does not support data fragmentation scheduling of co-location tables for the time being.
	* Default value:true
	* Importance: High

* tablet\_repair\_delay\_factor\_second

	* Note: For different scheduling priorities, we will delay different time to start repairing. In order to prevent a large number of unnecessary replica repair tasks from occurring in the process of routine restart and upgrade. This parameter is a reference coefficient. For HIGH priority, the delay is the reference coefficient * 1; for NORMAL priority, the delay is the reference coefficient * 2; for LOW priority, the delay is the reference coefficient * 3. That is, the lower the priority, the longer the delay waiting time. If the user wants to repair the copy as soon as possible, this parameter can be reduced appropriately.
	* Default value: 60 seconds
	* Importance: High

* schedule\_slot\_num\_per\_path

	* Note: The default number of slots allocated to each disk for replica repair. This number represents the number of replica repair tasks that a disk can run simultaneously. If you want to repair the copy faster, you can adjust this parameter appropriately. The higher the single value, the greater the impact on IO.
	* Default value: 2
	* Importance: High

* balance\_load\_score\_threshold

	* Description: Threshold of Cluster Equilibrium. The default is 0.1, or 10%. When the load core of a BE node is not higher than or less than 10% of the average load core, we think that the node is balanced. If you want to make the cluster load more even, you can adjust this parameter appropriately.
	* Default value: 0.1
	* Importance:

* storage\_high\_watermark\_usage\_percent 和 storage\_min\_left\_capacity\_bytes

	* Description: These two parameters represent the upper limit of the maximum space utilization of a disk and the lower limit of the minimum space remaining, respectively. When the space utilization of a disk is greater than the upper limit or the remaining space is less than the lower limit, the disk will no longer be used as the destination address for balanced scheduling.
	* Default values: 0.85 and 2097152000 (2GB)
	* Importance:

* disable\_balance

	* Description: Control whether to turn off the balancing function. When replicas are in equilibrium, some functions, such as ALTER TABLE, will be banned. Equilibrium can last for a long time. Therefore, if the user wants to do the prohibited operation as soon as possible. This parameter can be set to true to turn off balanced scheduling.
	* Default value: false
	* Importance:

The following adjustable parameters are all configurable parameters in be.conf.

* clone\_worker\_count

     * Description: Affects the speed of copy equalization. In the case of low disk pressure, you can speed up replica balancing by adjusting this parameter.
     * Default: 3
     * Importance: Medium
### Unadjustable parameters

The following parameters do not support modification for the time being, just for illustration.

* Tablet Checker scheduling interval

	Tablet Checker schedules checks every 20 seconds.

* Tablet Scheduler scheduling interval

	Tablet Scheduler schedules every five seconds

* Number of Tablet Scheduler Schedules per Batch

	Tablet Scheduler schedules up to 50 tablets at a time.

* Tablet Scheduler Maximum Waiting Schedule and Number of Tasks in Operation

	The maximum number of waiting tasks and running tasks is 2000. When over 2000, Tablet Checker will no longer generate new scheduling tasks to Tablet Scheduler.

* Tablet Scheduler Maximum Balanced Task Number

	The maximum number of balanced tasks is 500. When more than 500, there will be no new balancing tasks.

* Number of slots per disk for balancing tasks

	The number of slots per disk for balancing tasks is 2. This slot is independent of the slot used for replica repair.

* Update interval of cluster equilibrium

	Tablet Scheduler recalculates the load score of the cluster every 20 seconds.

* Minimum and Maximum Timeout for Clone Tasks

	A clone task timeout time range is 3 minutes to 2 hours. The specific timeout is calculated by the size of the tablet. The formula is (tablet size)/ (5MB/s). When a clone task fails three times, the task terminates.

* Dynamic Priority Adjustment Strategy

	The minimum priority adjustment interval is 5 minutes. When a tablet schedule fails five times, priority is lowered. When a tablet is not scheduled for 30 minutes, priority is raised.

## Relevant issues

* In some cases, the default replica repair and balancing strategy may cause the network to be full (mostly in the case of gigabit network cards and a large number of disks per BE). At this point, some parameters need to be adjusted to reduce the number of simultaneous balancing and repair tasks.

* Current balancing strategies for copies of Colocate Table do not guarantee that copies of the same Tablet will not be distributed on the BE of the same host. However, the repair strategy of the copy of Colocate Table detects this distribution error and corrects it. However, it may occur that after correction, the balancing strategy regards the replicas as unbalanced and rebalances them. As a result, the Colocate Group cannot achieve stability because of the continuous alternation between the two states. In view of this situation, we suggest that when using Colocate attribute, we try to ensure that the cluster is isomorphic, so as to reduce the probability that replicas are distributed on the same host.

## Best Practices

### Control and manage the progress of replica repair and balancing of clusters

In most cases, Doris can automatically perform replica repair and cluster balancing by default parameter configuration. However, in some cases, we need to manually intervene to adjust the parameters to achieve some special purposes. Such as prioritizing the repair of a table or partition, disabling cluster balancing to reduce cluster load, prioritizing the repair of non-colocation table data, and so on.

This section describes how to control and manage the progress of replica repair and balancing of the cluster by modifying the parameters.

1. Deleting Corrupt Replicas

    In some cases, Doris may not be able to automatically detect some corrupt replicas, resulting in frequent query or import errors on the corrupt replicas. In this case, we need to delete the corrupted copies manually. This method can be used to: delete a copy with a high version number resulting in a -235 error, delete a corrupted copy of a file, etc.
    
    First, find the tablet id of the corresponding copy, let's say 10001, and use `show tablet 10001;` and execute the `show proc` statement to see the details of each copy of the corresponding tablet.
    
    Assuming that the backend id of the copy to be deleted is 20001, the following statement is executed to mark the copy as `bad`.
    
    ```
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10001", "backend_id" = "20001", "status" = "bad");
    ```
    
    At this point, the `show proc` statement again shows that the `IsBad` column of the corresponding copy has a value of `true`.
    
    The replica marked as `bad` will no longer participate in imports and queries. The replica repair logic will automatically replenish a new replica at the same time. 2.
    
2. prioritize repairing a table or partition

    `help admin repair table;` View help. This command attempts to repair the tablet of the specified table or partition as a priority.
    
3. Stop the balancing task

    The balancing task will take up some network bandwidth and IO resources. If you wish to stop the generation of new balancing tasks, you can do so with the following command.
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```

4. Stop all replica scheduling tasks

    Copy scheduling tasks include balancing and repair tasks. These tasks take up some network bandwidth and IO resources. All replica scheduling tasks (excluding those already running, including colocation tables and common tables) can be stopped with the following command.
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "true");
    ```

5. Stop the copy scheduling task for all colocation tables.

    The colocation table copy scheduling is run separately and independently from the regular table. In some cases, users may wish to stop the balancing and repair of colocation tables first and use the cluster resources for normal table repair with the following command.
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_colocate_balance" = "true");
    ```

6. Repair replicas using a more conservative strategy

    Doris automatically repairs replicas when it detects missing replicas, BE downtime, etc. However, in order to reduce some errors caused by jitter (e.g., BE being down briefly), Doris delays triggering these tasks.
    
    * The `tablet_repair_delay_factor_second` parameter. Default 60 seconds. Depending on the priority of the repair task, it will delay triggering the repair task for 60 seconds, 120 seconds, or 180 seconds. This time can be extended so that longer exceptions can be tolerated to avoid triggering unnecessary repair tasks by using the following command.

    ```
    ADMIN SET FRONTEND CONFIG ("tablet_repair_delay_factor_second" = "120");
    ```

7. use a more conservative strategy to trigger redistribution of colocation groups

    Redistribution of colocation groups may be accompanied by a large number of tablet migrations. `colocate_group_relocate_delay_second` is used to control the redistribution trigger delay. The default is 1800 seconds. If a BE node is likely to be offline for a long time, you can try to increase this parameter to avoid unnecessary redistribution by.
    
    ```
    ADMIN SET FRONTEND CONFIG ("colocate_group_relocate_delay_second" = "3600");
    ```

8. Faster Replica Balancing

    Doris' replica balancing logic adds a normal replica first and then deletes the old one for the purpose of replica migration. When deleting the old replica, Doris waits for the completion of the import task that has already started on this replica to avoid the balancing task from affecting the import task. However, this will slow down the execution speed of the balancing logic. In this case, you can make Doris ignore this wait and delete the old replica directly by modifying the following parameters.
    
    ```
    ADMIN SET FRONTEND CONFIG ("enable_force_drop_redundant_replica" = "true");
    ```

    This operation may cause some import tasks to fail during balancing (requiring a retry), but it will speed up balancing significantly.
    
Overall, when we need to bring the cluster back to a normal state quickly, consider handling it along the following lines.

1. find the tablet that is causing the highly optimal task to report an error and set the problematic copy to bad.
2. repair some tables with the `admin repair` statement.
3. Stop the replica balancing logic to avoid taking up cluster resources, and then turn it on again after the cluster is restored.
4. Use a more conservative strategy to trigger repair tasks to deal with the avalanche effect caused by frequent BE downtime.
5. Turn off scheduling tasks for colocation tables on-demand and focus cluster resources on repairing other high-optimality data.
