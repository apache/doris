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

#include "exec/runtime_filter/runtime_filter_bucket_pruner.h"

#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <memory>
#include <mutex>

#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"

namespace doris {
namespace {

using SelectedBuckets = phmap::flat_hash_set<int32_t>;
using SelectedBucketsByNum = phmap::flat_hash_map<int32_t, SelectedBuckets>;

const SelectedBuckets& get_selected_buckets(int32_t bucket_num, const DorisVector<uint32_t>& hashes,
                                            SelectedBucketsByNum* selected_buckets_by_num) {
    auto [selected_it, inserted] = selected_buckets_by_num->try_emplace(bucket_num);
    if (inserted) {
        auto& selected_buckets = selected_it->second;
        selected_buckets.reserve(std::min(hashes.size(), static_cast<size_t>(bucket_num)));
        for (uint32_t hash : hashes) {
            selected_buckets.insert(static_cast<int32_t>(hash % static_cast<uint32_t>(bucket_num)));
        }
    }
    return selected_it->second;
}

void collect_pruned_tablets(const std::vector<std::unique_ptr<TPaloScanRange>>& ranges,
                            const DorisVector<uint32_t>& hashes,
                            phmap::flat_hash_set<int64_t>* newly_pruned) {
    SelectedBucketsByNum selected_buckets_by_num;
    for (const auto& range_ptr : ranges) {
        DCHECK(range_ptr != nullptr);
        const auto& range = *range_ptr;
        if (newly_pruned->contains(range.tablet_id)) {
            continue;
        }
        DCHECK(range.__isset.bucket_seq);
        DCHECK(range.__isset.bucket_num);
        DCHECK_GT(range.bucket_num, 0);
        DCHECK_GE(range.bucket_seq, 0);
        DCHECK_LT(range.bucket_seq, range.bucket_num);

        const auto& selected_buckets =
                get_selected_buckets(range.bucket_num, hashes, &selected_buckets_by_num);
        if (!selected_buckets.contains(range.bucket_seq)) {
            newly_pruned->insert(range.tablet_id);
        }
    }
}

} // namespace

Status RuntimeFilterBucketPruner::prune_by_runtime_filters(
        const std::vector<std::unique_ptr<TPaloScanRange>>& ranges,
        const VExprContextSPtrs& conjuncts, const std::vector<TRuntimeFilterDesc>& rf_descs,
        int scan_node_id, int max_in_num, int64_t* newly_pruned_count) {
    *newly_pruned_count = 0;
    if (ranges.empty()) {
        return Status::OK();
    }

    phmap::flat_hash_set<int> eligible_filter_ids;
    for (const auto& desc : rf_descs) {
        if (desc.__isset.bucket_pruning_target_ids &&
            desc.bucket_pruning_target_ids.contains(scan_node_id)) {
            eligible_filter_ids.insert(desc.filter_id);
        }
    }
    if (eligible_filter_ids.empty()) {
        return Status::OK();
    }

    phmap::flat_hash_set<int64_t> newly_pruned;
    for (const auto& conjunct_ctx : conjuncts) {
        VExprSPtr root = conjunct_ctx->root();
        if (!root->is_rf_wrapper()) {
            continue;
        }
        auto* rf_expr = assert_cast<RuntimeFilterExpr*>(root.get());
        if (!eligible_filter_ids.contains(rf_expr->filter_id())) {
            continue;
        }

        VExprSPtr impl = root->get_impl();
        DORIS_CHECK(impl != nullptr);
        std::shared_ptr<HybridSetBase> hybrid_set = impl->get_set_func();
        if (hybrid_set == nullptr) {
            // IN_OR_BLOOM may become a Bloom filter at runtime. A Bloom filter
            // cannot be inverted to a safe finite bucket set.
            continue;
        }
        if (hybrid_set->size() > max_in_num) {
            continue;
        }

        DORIS_CHECK_EQ(impl->children().size(), 1);
        VExprSPtr target_expr = impl->children()[0];
        DORIS_CHECK_EQ(target_expr->node_type(), TExprNodeType::SLOT_REF);

        auto hashes = rf_expr->get_bucket_prune_hashes(target_expr->data_type());
        collect_pruned_tablets(ranges, *hashes, &newly_pruned);
    }

    if (!newly_pruned.empty()) {
        std::unique_lock lock(_prune_mutex);
        for (int64_t tablet_id : newly_pruned) {
            if (_pruned_tablet_ids.insert(tablet_id).second) {
                ++*newly_pruned_count;
            }
        }
    }
    return Status::OK();
}

bool RuntimeFilterBucketPruner::is_tablet_pruned(int64_t tablet_id) const {
    std::shared_lock lock(_prune_mutex);
    return _pruned_tablet_ids.contains(tablet_id);
}

int64_t RuntimeFilterBucketPruner::pruned_tablet_count() const {
    std::shared_lock lock(_prune_mutex);
    return static_cast<int64_t>(_pruned_tablet_ids.size());
}

} // namespace doris
