---
{
"title": "Cold hot separation",
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

# [Experimental] Cold hot separation

## Demand scenario

A big usage scenario in the future is similar to the es log storage. In the log scenario, the data will be cut by date. Many data are cold data, with few queries. Therefore, the storage cost of such data needs to be reduced. From the perspective of saving storage costs
1. The price of ordinary cloud disks of cloud manufacturers is higher than that of object storage
2. In the actual online use of the doris cluster, the utilization rate of ordinary cloud disks cannot reach 100%
3. Cloud disk is not paid on demand, but object storage can be paid on demand
4. High availability based on ordinary cloud disks requires multiple replicas, and a replica migration is required for a replica exception. This problem does not exist when data is placed on the object store, because the object store is shared。

## Solution
Set the freeze time on the partition level to indicate how long the partition will be frozen, and define the location of remote storage stored after the freeze. On the be, the daemon thread will periodically determine whether the table needs to be frozen. If it does, it will upload the data to s3.

The cold and hot separation supports all doris functions, but only places some data on object storage to save costs without sacrificing functions. Therefore, it has the following characteristics:

- When cold data is stored on object storage, users need not worry about data consistency and data security
- Flexible freeze policy, cooling remote storage property can be applied to table and partition levels
- Users query data without paying attention to the data distribution location. If the data is not local, they will pull the data on the object and cache it to be local
- Optimization of replica clone. If the stored data is on the object, the replica clone does not need to pull the stored data locally
- Remote object space recycling recycler. If the table and partition are deleted, or the space is wasted due to abnormal conditions in the cold and hot separation process, the recycler thread will periodically recycle, saving storage resources
- Cache optimization, which caches the accessed cold data to be local, achieving the query performance of non cold and hot separation
- Be thread pool optimization, distinguish whether the data source is local or object storage, and prevent the delay of reading objects from affecting query performance
- newly created materialized view would inherit storage policy from it's base table's correspoding partition

## Storage policy

The storage policy is the entry to use the cold and hot separation function. Users only need to associate a storage policy with a table or partition during table creation or doris use. that is, they can use the cold and hot separation function.

<version since="dev"></version> When creating an S3 RESOURCE, the S3 remote link verification will be performed to ensure that the RESOURCE is created correctly.

In addition, fe configuration needs to be added: `enable_storage_policy=true`  

Note: This property will not be synchronized by CCR. If this table is copied by CCR, that is, PROPERTIES contains `is_being_synced = true`, this property will be erased in this table.

For example:

```
CREATE RESOURCE "remote_s3"
PROPERTIES
(
    "type" = "s3",
    "s3.endpoint" = "bj.s3.com",
    "s3.region" = "bj",
    "s3.bucket" = "test-bucket",
    "s3.root.path" = "path/to/root",
    "s3.access_key" = "bbb",
    "s3.secret_key" = "aaaa",
    "s3.connection.maximum" = "50",
    "s3.connection.request.timeout" = "3000",
    "s3.connection.timeout" = "1000"
);

CREATE STORAGE POLICY test_policy
PROPERTIES(
    "storage_resource" = "remote_s3",
    "cooldown_ttl" = "1d"
);

CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048)
)
UNIQUE KEY(k1)
DISTRIBUTED BY HASH (k1) BUCKETS 3
PROPERTIES(
    "storage_policy" = "test_policy"
);
```
Or for an existing table, associate the storage policy
```
ALTER TABLE create_table_not_have_policy set ("storage_policy" = "test_policy");
```
Or associate a storage policy with an existing partition
```
ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="test_policy");
```
**Note**: If the user specifies different storage policies for the entire table and certain partitions during table creation, the storage policy set for the partitions will be ignored, and all partitions of the table will use the table's policy. If you need a specific partition to have a different policy than the others, you can modify it by associating the partition with the desired storage policy, as mentioned earlier in the context of modifying an existing partition.
For details, please refer to the [resource](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md), [policy](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-POLICY.md), create table, alter and other documents in the docs directory

### Some restrictions

- A single table or a single partition can only be associated with one storage policy. After association, the storage policy cannot be dropped，need to solve the relationship between the two.
- The object information associated with the storage policy does not support modifying the data storage path information, such as bucket, endpoint, and root_ Path and other information
- Currently, the storage policy only supports creation,modification and deletion. before deleting, you need to ensure that no table uses this storage policy.

## Show size of objects occupied by cold data
1. Through show proc '/backends', you can view the size of each object being uploaded to, and the RemoteUsedCapacity item.There is a slight delay in this method

2. Through show tables from tableName, you can view the object size occupied by each table, and the RemoteDataSize item.

## cold data cache

As above, cold data introduces the cache in order to optimize query performance. After the first hit after cooling, Doris will reload the cooled data to be's local disk. The cache has the following characteristics:
- The cache is actually stored on the be local disk and does not occupy memory.
- The cache can limit expansion and clean up data through LRU
- The implementation of the cache is the same as the cache of the federated query catalog. The documentation is [here](../lakehouse/filecache.md)

## cold data compaction
The time when cold data is imported is from the moment when the data rowset file is written to the local disk, plus the cooling time. Since the data is not written and cooled at one time, to avoid the problem of small files in the object storage, doris will also perform compaction of cold data.
However, the frequency of cold data compaction and the priority of resource occupation are not very high, let the local hot data be compacted as much as possible before performing cooling. Specifically, it can be adjusted by the following be parameters:
- The be parameter `cold_data_compaction_thread_num` can set the concurrency of executing cold data compaction, the default is 2.
- The be parameter `cold_data_compaction_interval_sec` can set the time interval for executing cold data compaction, the default is 1800, unit: second, that is, half an hour.


## cold data schema change
The supported schema change types after data cooling are as follows:
* Add and delete columns
* Modify column type
* Adjust column order
* Add and modify Bloom Filter
* Add and delete bitmap index

## cold data Garbage collection
The garbage data of cold data refers to the data that is not used by any Replica. Object storage may have garbage data generated by the following situations:
1. Failed to upload rowset but uploaded some segments successfully.
2. After the FE re-selects the CooldownReplica, the rowset versions of the old and new CooldownReplicas are inconsistent, and the FollowerReplicas synchronize the CooldownMeta of the new CooldownReplica. The rowsets with inconsistent versions in the old CooldownReplica are not used by the Replica and become garbage data.
3. After the cold data compaction, the rowset before the merge cannot be deleted immediately because it may be used by other Replicas, but in the end FollowerReplicas all use the latest merged rowset, and the rowset before the merge becomes garbage data.

In addition, the garbage data on the object will not be cleaned up immediately.
The be parameter `remove_unused_remote_files_interval_sec` can set the garbage collection interval of cold data, the default is 21600, unit: second, that is, 6 hours.


## Unfinished Matters

- Currently, there is no way to query the tables associated with a specific storage policy.
- The acquisition of some remote occupancy indicators is not perfect enough.