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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/string_ref.h"

namespace doris::segment_v2 {

// VariantDocPathStats holds per-bucket path → total-non-null-count for one
// variant column across all source segments of a compaction.
//
// Lifecycle:
//   1. Aggregation code (variant_util.cpp::aggregate_variant_extended_info)
//      scans source segment footers and collects raw {path, count} pairs.
//   2. get_extended_compaction_schema converts the flat map into a
//      bucket-partitioned VariantDocPathStats via `build_from_path_counts`.
//   3. The shared_ptr is attached to RowsetWriterContext.
//   4. VariantDocCompactWriter looks up its own bucket_idx in _flush_batch
//      and uses the resulting bucket-local map to pre-reserve sparse-subcolumn
//      rowid vectors (avoids PaddedPODArray 2x growth waste).
//
// Partitioning by bucket up front means each writer only sees the subset of
// paths that hash into its bucket — no per-writer filter pass over the full
// path set.
class VariantDocPathStats {
public:
    using BucketCounts = std::unordered_map<std::string, uint64_t>;

    // Build from a flat {path → total_count} map by hashing each path into its
    // bucket slot. `total_buckets` must match the variant column's
    // variant_doc_hash_shard_count at write time.
    static std::shared_ptr<VariantDocPathStats> build_from_path_counts(
            const std::unordered_map<std::string, uint64_t>& path_counts, int total_buckets);

    // Returns the bucket-local {path → count} map. Safe to call with any
    // bucket_idx in [0, total_buckets); returns an empty map if out of range.
    const BucketCounts& bucket_counts(uint32_t bucket_idx) const;

    int total_buckets() const { return _total_buckets; }

private:
    explicit VariantDocPathStats(int total_buckets);

    int _total_buckets = 1;
    // Pre-partitioned: index by bucket_idx.
    std::vector<BucketCounts> _bucket_counts;
    // Stable reference returned from bucket_counts() when bucket_idx is out of range.
    static const BucketCounts kEmpty;
};

} // namespace doris::segment_v2
