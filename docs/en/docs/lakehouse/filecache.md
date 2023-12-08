---
{
    "title": "File Cache",
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

# File Cache

File Cache accelerates queries that read the same data by caching the data files of recently accessed from remote storage system (HDFS or Object Storage). In Ad Hoc scenarios where the same data is frequently accessed, File Cache can avoid repeated remote data access costs and improve the query analysis performance and stability of hot data.

## How it works

File Cache caches the accessed remote data in the local BE node. The original data file will be divided into blocks according to the read IO size, and the block will be stored in file `cache_path/hash(filepath).substr(0, 3)/hash(filepath)/offset`, and save the block meta information in the BE node. When accessing the same remote file, doris will check whether the cached data of the file exists in the local cache, and according to the offset and size of the block, confirm which data is read from the local block, which data is pulled from the remote, and cache the new data pulled from the remote. When the BE node restarts, scan `cache_path` directory, recover the meta information of the block. When the cache size reaches the upper threshold, the blocks that have not been accessed for a long time shall be cleaned according to the LRU principle.

## Usage

File Cache is disabled by default. You need to set the relevant configuration in FE and BE to enable it.

### Configurations for FE

Enable File Cache for a given session:

```
SET enable_file_cache = true;
```

Enable File Cache globally:

```
SET GLOBAL enable_file_cache = true;
```

> The File Cache is only applicable to external queries for files (such as Hive, Hudi). It has no effect on internal table queries, or non-file external queries (such as JDBC, Elasticsearch), etc.

### Configurations for BE
Add settings to the BE node's configuration file `conf/be.conf`, and restart the BE node for the configuration to take effect.

|  Parameter   | Description  |
|  ---  | ---  |
| `enable_file_cache`  | Whether to enable File Cache, default false |
| `file_cache_path` | Parameters about cache path, json format, for exmaple: `[{"path": "/path/to/file_cache1", "total_size":53687091200,"query_limit": 10737418240},{"path": "/path/to/file_cache2", "total_size":53687091200,"query_limit": 10737418240},{"path": "/path/to/file_cache3", "total_size":53687091200,"query_limit": 10737418240}]`. `path` is the path to save cached data; `total_size` is the max size of cached data; `query_limit` is the max size of cached data for a single query. |
| `file_cache_min_file_segment_size` | Min size of a single cached block, default 1MB, should greater than 4096 |
| `file_cache_max_file_segment_size` | Max size of a single cached block, default 4MB, should greater than 4096 |
| `enable_file_cache_query_limit` | Whether to limit the cache size used by a single query, default false |
| `clear_file_cache` | Whether to delete the previous cache data when the BE restarts, default false |

## Check whether a query hits cache

Execute `set enable_profile = true` to enable the session variable, and you can view the query profile in the Queris tab of FE's web page. The metrics related to File Cache are as follows:

```
-  FileCache:
  -  IOHitCacheNum:  552
  -  IOTotalNum:  835
  -  ReadFromFileCacheBytes:  19.98  MB
  -  ReadFromWriteCacheBytes:  0.00  
  -  ReadTotalBytes:  29.52  MB
  -  WriteInFileCacheBytes:  915.77  MB
  -  WriteInFileCacheNum:  283 
```

- `IOTotalNum`:  Number of remote access
- `IOHitCacheNum`: Number of cache hits
- `ReadFromFileCacheBytes`: Amount of data read from cache file
- `ReadTotalBytes`: Total amount of data read
- `SkipCacheBytes`: Failed to create the cache file, or the cache file was deleted. The amount of data that needs to be read from the remote again
- `WriteInFileCacheBytes`: Amount of data saved to cache file
- `WriteInFileCacheNum`: The number of blocks saved, so 'WriteInFileCacheBytes'/' WriteInFileCacheBytes' is the average size of blocks

`IOHitCacheNum` / `IOTotalNum` Equal to 1, indicating that read data only from file cache

`ReadFromFileCacheBytes` / `ReadTotalBytes` Equal to 1, indicating that read data only from file cache

`ReadFromFileCacheBytes` The smaller the better, the smaller the amount of data read from remote
