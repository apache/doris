// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp doris
namespace java org.apache.doris.thrift

struct TQueryCacheParam {
  1: optional i32 node_id

  2: optional binary digest

  // the query slots order can different to the query cache slots order,
  // so we should mapping current slot id in planNode to normalized slot id
  // say:
  //   SQL1: select id, count(*) cnt, sum(value) s from tbl group by id
  //   SQL2: select sum(value) s, count(*) cnt, id from tbl group by id
  //   the id always has normalized slot id 0,
  //   the cnt always has normalized slot id 1
  //   the s always has normalized slot id 2
  //   but in SQL1, id, cnt, s can has slot id 5, 6, 7
  //       in SQL2, s, cnt, id can has slot id 10, 11, 12
  //   if generate plan cache in SQL1, we will make output_slot_mapping: {5: 0, 6: 1, 7: 2},
  //   the SQL2 read plan cache and make output_slot_mapping: {10: 2, 11: 1, 12: 0},
  //   even the select order is different, the normalized slot id is always equals:
  //   the id always is 0, the cnt always is 1, the s always is 2.
  //   then backend can mapping the current slots in the tuple to the query cached slots
  3: optional map<i32, i32> output_slot_mapping

  // mapping tablet to filter range,
  // BE will use <digest, tablet id, filter range> as the key to search query cache.
  // note that, BE not care what the filter range content is, just use as the part of the key.
  4: optional map<i64, string> tablet_to_range

  5: optional bool force_refresh_query_cache

  6: optional i64 entry_max_bytes

  7: optional i64 entry_max_rows

  // Whether BE is allowed to serve a stale cache entry by incremental merge:
  // when the cached version is behind the current version, BE may scan only the
  // delta rowsets in (cached_version, current_version], produce the partial
  // aggregation of the delta, emit it together with the cached partial blocks
  // (the upstream merge aggregation combines both), and write the merged entry
  // back with the new version.
  //
  // FE only sets this to true when all of the following hold, otherwise the
  // "cached + delta" union would not equal the new snapshot or could not be
  // merged safely:
  //  - the scanned index is append-only: DUP_KEYS, or merge-on-write
  //    UNIQUE_KEYS (BE verifies per tablet, through the delete bitmap of the
  //    delta window, that no pre-existing key was rewritten); merge-on-read
  //    UNIQUE resolves duplicates while reading and AGG tables merge rows in
  //    the storage layer, so those always fall back
  //  - the cache point aggregation does not finalize (its output is a partial
  //    state that is always merged again by an upstream aggregation, so the
  //    cached blocks and the delta blocks can be emitted side by side)
  //  - the cache point aggregates the raw detail rows directly (its child is
  //    the olap scan node); with a nested aggregation the inner finalized agg
  //    would see only the delta rows, whose output is not a mergeable
  //    complement of the cached snapshot
  // BE additionally falls back to a full recompute when the delta version path
  // cannot be captured (e.g. merged away by compaction), when the delta
  // contains delete predicates, or when the delta rewrites history rows.
  8: optional bool allow_incremental
}
