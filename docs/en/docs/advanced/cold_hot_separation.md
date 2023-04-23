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

## Storage policy

The storage policy is the entry to use the cold and hot separation function. Users only need to associate a storage policy with a table or partition during table creation or doris use. that is, they can use the cold and hot separation function.

<version since="dev"></version> When creating an S3 RESOURCE, the S3 remote link verification will be performed to ensure that the RESOURCE is created correctly.

In addition, fe configuration needs to be added: `enable_storage_policy=true`

For example:

```
CREATE RESOURCE "remote_s3"
PROPERTIES
(
    "type" = "s3",
    "AWS_ENDPOINT" = "bj.s3.com",
    "AWS_REGION" = "bj",
    "AWS_BUCKET" = "test-bucket",
    "AWS_ROOT_PATH" = "path/to/root",
    "AWS_ACCESS_KEY" = "bbb",
    "AWS_SECRET_KEY" = "aaaa",
    "AWS_MAX_CONNECTIONS" = "50",
    "AWS_REQUEST_TIMEOUT_MS" = "3000",
    "AWS_CONNECTION_TIMEOUT_MS" = "1000"
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
For details, please refer to the [resource](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md), [policy](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-POLICY.md), create table, alter and other documents in the docs directory

### Some restrictions

- A single table or a single partition can only be associated with one storage policy. After association, the storage policy cannot be dropped
- The object information associated with the storage policy does not support modifying the data storage path information, such as bucket, endpoint, and root_ Path and other information
- Currently, the storage policy only supports creation and modification, not deletion

## Show size of objects occupied by cold data
1. Through show proc '/backends', you can view the size of each object being uploaded to, and the RemoteUsedCapacity item.

2. Through show tables from tableName, you can view the object size occupied by each table, and the RemoteDataSize item.

## cold data cache

As above, cold data introduces the cache in order to optimize query performance. After the first hit after cooling, Doris will reload the cooled data to be's local disk. The cache has the following characteristics:
- The cache is actually stored on the be local disk and does not occupy memory.
- the cache can limit expansion and clean up data through LRU
- The be parameter `file_cache_alive_time_sec` can set the maximum storage time of the cache data after it has not been accessed. The default is 604800, which is one week.
- The be parameter `file_cache_max_size_per_disk` can set the disk size occupied by the cache. Once this setting is exceeded, the cache that has not been accessed for the longest time will be deleted. The default is 0, means no limit to the size, unit: byte.
- The be parameter `file_cache_type` is optional `sub_file_cache` (segment the remote file for local caching) and `whole_file_cache` (the entire remote file for local caching), the default is "", means no file is cached, please set it when caching is required this parameter.

## Unfinished Matters

- After the data is frozen, there are new data updates or imports, etc. The compression has not been processed at present.
- The schema change operation after the data is frozen is not supported at present.
