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

#include "storage/segment/variant/variant_doc_path_stats.h"

#include <algorithm>

#include "exec/common/variant_util.h"

namespace doris::segment_v2 {

const VariantDocPathStats::BucketCounts VariantDocPathStats::kEmpty {};

VariantDocPathStats::VariantDocPathStats(int total_buckets)
        : _total_buckets(std::max(1, total_buckets)), _bucket_counts(_total_buckets) {}

std::shared_ptr<VariantDocPathStats> VariantDocPathStats::build_from_path_counts(
        const std::unordered_map<std::string, uint64_t>& path_counts, int total_buckets) {
    auto stats = std::shared_ptr<VariantDocPathStats>(new VariantDocPathStats(total_buckets));
    for (const auto& [path, cnt] : path_counts) {
        if (cnt == 0) {
            continue;
        }
        StringRef path_ref {path.data(), path.size()};
        const uint32_t bucket =
                variant_util::variant_binary_shard_of(path_ref, stats->_total_buckets);
        stats->_bucket_counts[bucket].emplace(path, cnt);
    }
    return stats;
}

const VariantDocPathStats::BucketCounts& VariantDocPathStats::bucket_counts(
        uint32_t bucket_idx) const {
    if (bucket_idx >= _bucket_counts.size()) {
        return kEmpty;
    }
    return _bucket_counts[bucket_idx];
}

} // namespace doris::segment_v2
