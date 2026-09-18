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
#include <shared_mutex>
#include <vector>

#include "common/status.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/vexpr_fwd.h"

namespace doris {

class TPaloScanRange;
struct TRuntimeFilterDesc;

// Per-scan-instance state for single-column HASH bucket pruning. Runtime filters
// are conjunctive, so each exact IN filter can monotonically shrink the selected
// bucket set for each bucket count. The retained state is bounded by bucket
// counts rather than the number of tablets across all partitions.
// Both pruning updates and is_bucket_pruned() are safe to call concurrently.
class RuntimeFilterBucketPruner {
public:
    Status prune_by_runtime_filters(const std::vector<std::unique_ptr<TPaloScanRange>>& ranges,
                                    const VExprContextSPtrs& conjuncts,
                                    const std::vector<TRuntimeFilterDesc>& rf_descs,
                                    int scan_node_id, int max_in_num, int64_t* newly_pruned_count);

    bool is_bucket_pruned(int32_t bucket_seq, int32_t bucket_num) const;
    int64_t pruned_tablet_count() const;

private:
    phmap::flat_hash_map<int32_t, phmap::flat_hash_set<int32_t>> _selected_buckets_by_num;
    int64_t _pruned_tablet_count = 0;
    mutable std::shared_mutex _prune_mutex;
};

} // namespace doris
