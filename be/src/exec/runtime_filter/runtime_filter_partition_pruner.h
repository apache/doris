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

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exprs/vexpr_fwd.h"
#include "storage/olap_scan_common.h"

namespace doris {

class SlotDescriptor;
class VExprContext;
struct TPartitionBoundary;
struct TRuntimeFilterDesc;
struct TTargetExprMonotonicity;

// Parsed representation of one partition boundary for one slot column.
struct ParsedBoundary {
    int64_t partition_id = 0;
    SlotId slot_id = 0;
    bool is_nullable = false;
    // True only when the original/projection boundary is a finite LIST value set.
    // Bloom RF pruning relies on complete value enumeration and must not use RANGE
    // boundaries, even when a RANGE projection degenerates to a single point.
    bool is_list_boundary = false;
    ColumnValueRangeType boundary_cvr;
    // True if the partition's value set is exactly {NULL} (e.g. LIST
    // partition whose only key is NULL). The CVR alone cannot encode
    // "only NULL" -- it stays as the whole range with contain_null=true
    // -- so we track it explicitly to enable accurate pruning.
    bool only_null = false;
    // True if the partition's value set includes NULL (covers both
    // only-NULL and mixed LIST partitions). Tracked separately because
    // ColumnValueRange::set_contain_null(true) destructively clears the
    // fixed-value set, so we cannot stash the NULL flag inside the CVR
    // alongside the concrete values.
    bool contains_null = false;
};

// Immutable, fragment-shared parse result of TPartitionBoundary list.
//
// Parsing is expensive (constructs VLiteral per literal, materializes a
// ColumnPtr, builds ColumnValueRange) and depends only on plan-time data.
// All pipeline instances of the same fragment share one parse, performed
// in OperatorX::prepare() which is single-threaded fragment setup.
class ParsedPartitionBoundaries {
public:
    ParsedPartitionBoundaries() = default;

    // Build the parse result from the thrift `boundaries` list. Caller must
    // ensure this is invoked at most once per instance (OperatorX::prepare()
    // is the natural call site).
    Status parse(const std::vector<TPartitionBoundary>& boundaries,
                 const phmap::flat_hash_map<int, SlotDescriptor*>& slot_descs);

    bool empty() const { return _slot_to_boundaries.empty(); }
    int64_t total_partitions() const { return _total_partition_count; }

    const std::unordered_map<SlotId, std::vector<ParsedBoundary>>& slot_to_boundaries() const {
        return _slot_to_boundaries;
    }

    // Lazily compute target-domain boundaries for a monotonic RF target.
    // `target_expr` is `impl->children()[0]` of the RF wrapper (a sub-tree of
    // the conjunct). `leaf_slot_id` is the unique VSlotRef leaf inside it
    // (FE asserted target_expr has exactly one input slot). `leaf_column_id`
    // is that slot ref's `column_id()` -- the position in the runtime block.
    // Only partitions present in `partition_directions` are projected and each
    // partition uses its own FE-proven local direction.
    // `ctx` is the conjunct's VExprContext (used to execute the sub-expression).
    //
    // Direct SlotRef targets reuse the parsed partition boundaries. Expression
    // targets project finite RANGE endpoints by executing `target_expr`.
    //
    // Returns an empty vector when no selected boundary can be projected (e.g.
    // every candidate contains NULL). Unexpected FE/BE metadata mismatches
    // return an error instead of disabling pruning silently.
    // The shared_ptr keeps the computed vector alive even if another pipeline
    // instance inserts into the shared map and triggers unordered_map rehash.
    //
    // Direction:
    //   MONOTONIC_INCREASING: projected lo and hi keep their roles
    //   MONOTONIC_DECREASING: swap (projected lo, hi) -> (hi, lo)
    //
    // Open endpoints (MINVALUE/MAXVALUE) stay open after projection and swap
    // sides for monotonic decreasing targets. Boundaries containing NULL
    // partition values, or finite endpoints that project to NULL, are omitted
    // from the result so this RF conservatively leaves them unpruned.
    Status get_or_compute_projected_boundaries(
            int filter_id, const VExprSPtr& target_expr, SlotId leaf_slot_id, int leaf_column_id,
            const std::unordered_map<int64_t, TTargetExprMonotonicity::type>& partition_directions,
            VExprContext* ctx, std::shared_ptr<const std::vector<ParsedBoundary>>* output) const;

private:
    std::unordered_map<SlotId, std::vector<ParsedBoundary>> _slot_to_boundaries;
    int64_t _total_partition_count = 0;
    std::unordered_map<SlotId, DataTypePtr> _slot_data_types;

    mutable std::mutex _projected_boundaries_mutex;
    mutable std::unordered_map<int /*filter_id*/,
                               std::shared_ptr<const std::vector<ParsedBoundary>>>
            _projected_boundaries_by_filter_id;
};

// Per-instance pruning state for runtime-filter partition pruning.
//
// Holds the set of partition IDs already pruned for this scan instance and
// nothing else: the parsed boundaries are shared per-fragment and reached via
// `OperatorXBase::parsed_partition_boundaries()`. The owner (ScanLocalStateBase)
// passes the parsed object into `prune_by_runtime_filters` on each call.
//
// Thread safety: `is_partition_pruned()` is safe to call concurrently with
// `prune_by_runtime_filters()` via an internal shared_mutex.
class RuntimeFilterPartitionPruner {
public:
    RuntimeFilterPartitionPruner() = default;

    // Evaluate RF conjuncts against the given parsed boundaries and mark
    // pruned partitions on this per-instance state. Returns the number of
    // *newly* pruned partitions in this call.
    Status prune_by_runtime_filters(const ParsedPartitionBoundaries& parsed,
                                    const VExprContextSPtrs& conjuncts,
                                    const std::vector<TRuntimeFilterDesc>& rf_descs,
                                    int scan_node_id, int64_t* newly_pruned_count);

    // Thread-safe query: is the given partition_id pruned?
    bool is_partition_pruned(int64_t partition_id) const;

    // Number of partitions currently marked as pruned.
    int64_t pruned_partition_count() const;

private:
    phmap::flat_hash_set<int64_t> _pruned_partition_ids;
    mutable std::shared_mutex _prune_mutex;

    // Try to prune partitions using a single RF's impl expression on the given boundaries.
    // Adds newly pruned partition IDs to `newly_pruned`.
    void _try_prune_by_single_rf(const std::vector<ParsedBoundary>& boundaries,
                                 const VExprSPtr& impl,
                                 phmap::flat_hash_set<int64_t>& newly_pruned);
};

} // namespace doris
