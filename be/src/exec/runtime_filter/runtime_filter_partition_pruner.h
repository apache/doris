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

#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/global_types.h"
#include "exprs/vexpr_fwd.h"
#include "storage/olap_scan_common.h"
#include "core/column/column.h"

namespace doris {

class SlotDescriptor;
struct TPartitionBoundary;

// Reusable runtime-filter partition pruner.
//
// Given a set of partition boundaries (parsed from thrift TPartitionBoundary)
// and a set of RF conjuncts, determines which partitions can be pruned because
// their value ranges don't intersect the RF filter values.
//
// This class is scan-type agnostic: OlapScan maps pruned partition IDs to
// tablets; future external scans may map them to splits or other units.
//
// Thread safety: `is_partition_pruned()` is safe to call concurrently with
// `prune_by_runtime_filters()` via an internal shared_mutex.
class RuntimeFilterPartitionPruner {
public:
    RuntimeFilterPartitionPruner() = default;

    // Parse partition boundaries from thrift into typed ColumnValueRange objects.
    // slot_descs: maps slot_id → SlotDescriptor* for type lookup.
    void parse_boundaries(const std::vector<TPartitionBoundary>& boundaries,
                          const phmap::flat_hash_map<int, SlotDescriptor*>& slot_descs);

    // Evaluate RF conjuncts against parsed boundaries and mark pruned partitions.
    // Returns the number of *newly* pruned partitions in this call.
    int64_t prune_by_runtime_filters(const VExprContextSPtrs& conjuncts);

    // Thread-safe query: is the given partition_id pruned?
    bool is_partition_pruned(int64_t partition_id) const;

    // True if no boundaries were parsed (pruning is impossible).
    bool empty() const { return _partition_column_slot_ids.empty(); }

    // Number of distinct partitions that have parsed boundaries.
    int64_t total_partitions() const { return _total_partition_count; }

    // Number of partitions currently marked as pruned.
    int64_t pruned_partition_count() const;

private:
    // Parsed representation of one partition boundary for one slot column.
    struct ParsedBoundary {
        int64_t partition_id = 0;
        SlotId slot_id = 0;
        bool is_nullable = false;
        ColumnValueRangeType boundary_cvr;
        // Keep VLiteral column data alive so StringRef values remain valid.
        std::vector<ColumnPtr> literal_columns;
    };

    std::unordered_map<SlotId, std::vector<ParsedBoundary>> _slot_to_boundaries;
    std::unordered_set<SlotId> _partition_column_slot_ids;

    phmap::flat_hash_set<int64_t> _pruned_partition_ids;
    mutable std::shared_mutex _prune_mutex;

    int64_t _total_partition_count = 0;
};

} // namespace doris
