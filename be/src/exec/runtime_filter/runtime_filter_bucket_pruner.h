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
#include <shared_mutex>
#include <vector>

#include "common/status.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/vexpr_fwd.h"

namespace doris {

struct TRuntimeFilterDesc;

struct RuntimeFilterBucketPruneRange {
    int64_t tablet_id = 0;
    int32_t bucket_seq = 0;
    int32_t bucket_num = 0;
};

// Per-scan-instance state for single-column HASH bucket pruning. Runtime filters
// are conjunctive, so each exact IN filter can monotonically add tablet ids to
// the pruned set without retaining or combining its original value set.
// is_tablet_pruned() is safe to call concurrently with the serialized pruning
// updates performed by ScanLocalStateBase.
class RuntimeFilterBucketPruner {
public:
    Status prune_by_runtime_filters(const std::vector<RuntimeFilterBucketPruneRange>& ranges,
                                    const VExprContextSPtrs& conjuncts,
                                    const std::vector<TRuntimeFilterDesc>& rf_descs,
                                    int scan_node_id, int max_in_num, int64_t* newly_pruned_count);

    bool is_tablet_pruned(int64_t tablet_id) const;
    int64_t pruned_tablet_count() const;

private:
    phmap::flat_hash_set<int64_t> _pruned_tablet_ids;
    mutable std::shared_mutex _prune_mutex;
};

} // namespace doris
