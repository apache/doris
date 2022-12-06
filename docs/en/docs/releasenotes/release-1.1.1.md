---
{
    "title": "Release 1.1.1",
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

## Features

### Support ODBC Sink in Vectorized Engine.

This feature is enabled in non-vectorized engine but it is missed in vectorized engine in 1.1. So that we add back this feature in 1.1.1.

### Simple Memtracker for Vectorized Engine.

There is no memtracker in BE for vectorized engine in 1.1, so that the memory is out of control and cause OOM. In 1.1.1, a simple memtracker is added to BE and could control the memory and cancel the query when memory exceeded.

## Improvements

### Cache decompressed data in page cache.

Some data is compressed using bitshuffle and it costs a lot of time to decompress it during query. In 1.1.1, doris will decompress the data that encoded by bitshuffle to accelerate query and we find it could reduce 30% latency for some query in ssb-flat.

## Bug Fix

### Fix the problem that could not do rolling upgrade from 1.0.(Serious)

This issue was introduced in version 1.1 and may cause BE core when upgrade BE but not upgrade FE.

If you encounter this problem, you can try to fix it with [#10833](https://github.com/apache/doris/pull/10833).

### Fix the problem that some query not fall back to non-vectorized engine, and BE will core.

Currently, vectorized engine could not deal with all sql queries and some queries (like left outer join) will use non-vectorized engine to run. But there are some cases not covered in 1.1. And it will cause be crash.

### Compaction not work correctly and cause -235 Error.

One rowset multi segments in uniq key compaction, segments rows will be merged in generic_iterator but merged_rows not increased. Compaction will failed in check_correctness, and make a tablet with too much versions which lead to -235 load error.

### Some segment fault cases during query.

[#10961](https://github.com/apache/doris/pull/10961) 
[#10954](https://github.com/apache/doris/pull/10954) 
[#10962](https://github.com/apache/doris/pull/10962)

# Thanks

Thanks to everyone who has contributed to this release:

```
@jacktengg
@mrhhsg
@xinyiZzz
@yixiutt
@starocean999
@morrySnow
@morningman
@HappenLee
```