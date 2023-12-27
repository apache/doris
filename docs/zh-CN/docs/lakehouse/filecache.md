---
{
    "title": "文件缓存",
    "language": "zh-CN"
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


# 文件缓存

文件缓存(File Cache)通过缓存最近访问的远端存储系统(HDFS 或对象存储)的数据文件，加速后续访问相同数据的查询。在频繁访问相同数据的查询场景中，File Cache 可以避免重复的远端数据访问开销，提升热点数据的查询分析性能和稳定性。

## 原理

File Cache 将访问的远程数据缓存到本地的 BE 节点。原始的数据文件会根据访问的 IO 大小切分为 Block，Block 被存储到本地文件 `cache_path/hash(filepath).substr(0, 3)/hash(filepath)/offset` 中，并在 BE 节点中保存 Block 的元信息。当访问相同的远程文件时，doris 会检查本地缓存中是否存在该文件的缓存数据，并根据 Block 的 offset 和 size，确认哪些数据从本地 Block 读取，哪些数据从远程拉起，并缓存远程拉取的新数据。BE 节点重启的时候，扫描 `cache_path` 目录，恢复 Block 的元信息。当缓存大小达到阈值上限的时候，按照 LRU 原则清理长久未访问的 Block。

## 使用方式

File Cache 默认关闭，需要在 FE 和 BE 中设置相关参数进行开启。

### FE 配置

单个会话中开启 File Cache:

```
SET enable_file_cache = true;
```

全局开启 File Cache:

```
SET GLOBAL enable_file_cache = true;
```

> File Cache 功能仅作用于针对文件的外表查询（如 Hive、Hudi ）。对内表查询，或非文件的外表查询（如 JDBC、Elasticsearch）等无影响。

### BE 配置

添加参数到 BE 节点的配置文件 conf/be.conf 中，并重启 BE 节点让配置生效。

|  参数   | 必选项 | 说明  |
|  ---  | ---  | --- |
| `enable_file_cache`  | 是 | 是否启用 File Cache，默认 false |
| `file_cache_path` | 是 | 缓存目录的相关配置，json格式，例子: `[{"path": "/path/to/file_cache1", "total_size":53687091200,"query_limit": 10737418240},{"path": "/path/to/file_cache2", "total_size":53687091200,"query_limit": 10737418240},{"path": "/path/to/file_cache3", "total_size":53687091200,"query_limit": 10737418240}]`。`path` 是缓存的保存路径，`total_size` 是缓存的大小上限，`query_limit` 是单个查询能够使用的最大缓存大小。 |
| `file_cache_min_file_segment_size` | 否 | 单个 Block 的大小下限，默认 1MB，需要大于 4096 |
| `file_cache_max_file_segment_size` | 否 | 单个 Block 的大小上限，默认 4MB，需要大于 4096 |
| `enable_file_cache_query_limit` | 否 | 是否限制单个 query 使用的缓存大小，默认 false |
| `clear_file_cache` | 否 | BE 重启时是否删除之前的缓存数据，默认 false |

### 查看 File Cache 命中情况

执行 `set enable_profile=true` 打开会话变量，可以在 FE 的 web 页面的 Queris 标签中查看到作业的 Profile。File Cache 相关的指标如下:

```
-  FileCache:  0ns
    -  BytesScannedFromCache:  2.02  GB
    -  BytesScannedFromRemote:  0.00  
    -  BytesWriteIntoCache:  0.00  
    -  LocalIOUseTimer:  2s723ms
    -  NumLocalIOTotal:  444
    -  NumRemoteIOTotal:  0
    -  NumSkipCacheIOTotal:  0
    -  RemoteIOUseTimer:  0ns
    -  WriteCacheIOUseTimer:  0ns
```

- `BytesScannedFromCache`：从本地缓存中读取的数据量。
- `BytesScannedFromRemote`：从远端读取的数据量。
- `BytesWriteIntoCache`：写入缓存的数据量。
- `LocalIOUseTimer`：本地缓存的 IO 时间。
- `RemoteIOUseTimer`：远端读取的 IO 时间。
- `NumLocalIOTotal`：本地缓存的 IO 次数。
- `NumRemoteIOTotal`：远端 IO 次数。
- `WriteCacheIOUseTimer`：写入缓存的 IO 时间。

如果 `BytesScannedFromRemote` 为 0，表示全部命中缓存。

