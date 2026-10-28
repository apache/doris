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
#include <vector>

#include "core/arena.h"
#include "exprs/aggregate/aggregate_function.h"
#include "load/memtable/memtable.h"

namespace doris {

// Shared implementation of the models that merge rows with equal keys. AGG_KEYS and UNIQUE_KEYS
// both keep one RowInBlock per input row and aggregate the value columns into per-row states, so
// that mechanism lives here exactly once. The leaves only provide the model rules: which
// aggregate function a value column uses, and which extra columns partial update and sequence
// handling need.
class AggregatingMemTable : public MemTable {
public:
    ~AggregatingMemTable() override;

    void shrink_memtable_by_agg() override;

protected:
    AggregatingMemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                        const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                        PartialUpdateInfo* partial_update_info,
                        const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn);

    // Share one tracked allocation across a batch. Allocating each row separately also retains an
    // address record and a stack trace per row when memory diagnostics are enabled.
    template <typename RowFactory>
    static std::shared_ptr<RowInBlock[]> make_row_batch(size_t num_rows, RowFactory&& make_row) {
        auto rows = std::allocate_shared<RowInBlock[]>(CustomStdAllocator<RowInBlock>(), num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            rows[i] = make_row(i);
        }
        return rows;
    }

    Status _on_first_insertion() override;
    void _reserve_rows(size_t num_rows, size_t cursor) override;
    void _append_row_metadata(size_t num_rows, size_t cursor,
                              const DorisVector<int64_t>& allocated_lsns) override;
    size_t _sort() override;
    Status _put_into_output(Block& in_block) override;
    Status _to_block(std::unique_ptr<Block>* res) override;

    // Resolve the per-block column positions aggregation needs. Called before _init_agg_functions
    // because the aggregate function of a column depends on them.
    virtual void _init_aggregation_columns();
    // Aggregate function used for one value column.
    virtual AggregateFunctionPtr _make_agg_function(int cid);
    // Flexible partial update is only reachable for UNIQUE_KEYS, which owns the delete sign and
    // skip bitmap rules. Pure virtual so a model that sets _skip_bitmap_col_idx without providing
    // the rules fails to compile instead of aborting the BE at run time.
    virtual void _aggregate_flexible_partial_update(
            bool is_final, MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) = 0;
    // Ordering applied after aggregation; UNIQUE merge-on-write sorts by cluster keys.
    virtual Status _after_aggregate() { return Status::OK(); }

    // Merge allocated LSN sidecar only when MemTable merges two RowInBlock objects.
    // Table models that require complex merge semantics, such as AGG tables and unique key
    // merge-on-read tables, do not support allocated LSN now; MemTableWriter::init() rejects such
    // a configuration before the first row is inserted.
    void _merge_allocated_lsn(RowInBlock* src_row, RowInBlock* dst_row);

    void _append_output_allocated_lsn(RowInBlock* row);

    template <bool has_skip_bitmap_col>
    void _aggregate_two_row_in_block(MutableBlock& mutable_block, RowInBlock* src_row,
                                     RowInBlock* dst_row);

    void _aggregate_two_row_with_sequence_map(MutableBlock& mutable_block, RowInBlock* src_row,
                                              RowInBlock* dst_row);

    template <bool is_final>
    void _finalize_one_row(RowInBlock* row, MutableBlock& mutable_block, int row_pos);
    void _init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block);
    void _clear_row_agg(RowInBlock* row);

    template <bool is_final, bool has_skip_bitmap_col = false>
    void _aggregate();

    void _init_agg_functions();

    // Holds only the tablet schema, so it is a value member: no heap allocation and no shared
    // ownership to reason about.
    RowInBlockComparator _vec_row_comparator;

    // Only the rows will be inserted into block can allocate memory from _arena.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    Arena _arena;

    // One RowInBlock per input row. Those models still need per-row aggregation state and, when
    // enabled, the allocated LSN stored in RowInBlock.
    std::unique_ptr<DorisVector<std::shared_ptr<RowInBlock>>> _row_in_blocks;

    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states = 0;
    size_t _last_agg_pos = 0;

    int32_t _seq_col_idx_in_block {-1};
    int32_t _skip_bitmap_col_idx {-1};
};

// Defined in memtable_aggregating.cpp. The skip-bitmap variant is only reached from the UNIQUE_KEYS
// flexible partial update path, which lives in another translation unit.
extern template void AggregatingMemTable::_aggregate_two_row_in_block<true>(
        MutableBlock& mutable_block, RowInBlock* src_row, RowInBlock* dst_row);

// Definitions are provided in memtable_aggregating.cpp.
extern template void AggregatingMemTable::_finalize_one_row<true>(RowInBlock* row,
                                                                  MutableBlock& mutable_block,
                                                                  int row_pos);
extern template void AggregatingMemTable::_finalize_one_row<false>(RowInBlock* row,
                                                                   MutableBlock& mutable_block,
                                                                   int row_pos);

} // namespace doris
