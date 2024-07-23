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
#include <gen_cpp/Metrics_types.h>
#include <stddef.h>
#include <stdint.h>

#include <deque>
#include <memory>
#include <queue>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/sort_cursor.h"
#include "vec/core/sort_description.h"
#include "vec/runtime/vsorted_run_merger.h"
#include "vec/utils/util.hpp"

namespace doris {
class ObjectPool;
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

// TODO: now we only use merge sort
class MergeSorterState {
    ENABLE_FACTORY_CREATOR(MergeSorterState);

public:
    MergeSorterState(const RowDescriptor& row_desc, int64_t offset, int64_t limit,
                     RuntimeState* state, RuntimeProfile* profile)
            // create_empty_block should ignore invalid slots, unsorted_block
            // should be same structure with arrival block from child node
            // since block from child node may ignored these slots
            : unsorted_block_(Block::create_unique(
                      VectorizedUtils::create_empty_block(row_desc, true /*ignore invalid slot*/))),
              offset_(offset) {}

    ~MergeSorterState() = default;

    Status add_sorted_block(Block& block);

    Status build_merge_tree(const SortDescription& sort_description);

    Status merge_sort_read(doris::vectorized::Block* block, int batch_size, bool* eos);

    size_t data_size() const {
        size_t size = unsorted_block_->bytes();
        return size + in_mem_sorted_bocks_size_;
    }

    uint64_t num_rows() const { return num_rows_; }

    Block& last_sorted_block() { return sorted_blocks_.back(); }

    std::vector<Block>& get_sorted_block() { return sorted_blocks_; }
    std::priority_queue<MergeSortCursor>& get_priority_queue() { return priority_queue_; }
    std::vector<MergeSortCursorImpl>& get_cursors() { return cursors_; }
    void reset();

    std::unique_ptr<Block> unsorted_block_;

private:
    int _calc_spill_blocks_to_merge() const;

    Status _merge_sort_read_impl(int batch_size, doris::vectorized::Block* block, bool* eos);

    std::priority_queue<MergeSortCursor> priority_queue_;
    std::vector<MergeSortCursorImpl> cursors_;
    std::vector<Block> sorted_blocks_;
    size_t in_mem_sorted_bocks_size_ = 0;
    uint64_t num_rows_ = 0;

    int64_t offset_;

    Block merge_sorted_block_;
    std::unique_ptr<VSortedRunMerger> merger_;
};

class Sorter {
public:
    Sorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset, ObjectPool* pool,
           std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first)
            : _vsort_exec_exprs(vsort_exec_exprs),
              _limit(limit),
              _offset(offset),
              _pool(pool),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _materialize_sort_exprs(vsort_exec_exprs.need_materialize_tuple()) {}

    virtual ~Sorter() = default;

    virtual void init_profile(RuntimeProfile* runtime_profile) {
        _partial_sort_timer = ADD_TIMER(runtime_profile, "PartialSortTime");
        _merge_block_timer = ADD_TIMER(runtime_profile, "MergeBlockTime");
    }

    virtual Status append_block(Block* block) = 0;

    virtual Status prepare_for_read() = 0;

    virtual Status get_next(RuntimeState* state, Block* block, bool* eos) = 0;

    virtual size_t data_size() const = 0;

    // for topn runtime predicate
    const SortDescription& get_sort_description() const { return _sort_description; }
    virtual Field get_top_value() { return Field {Field::Types::Null}; }

    virtual Status merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                             int batch_size, bool* eos);
    virtual void reset() {}

    int64_t limit() const { return _limit; }
    int64_t offset() const { return _offset; }

    void set_enable_spill() { _enable_spill = true; }

protected:
    Status partial_sort(Block& src_block, Block& dest_block);

    bool _enable_spill = false;
    SortDescription _sort_description;
    VSortExecExprs& _vsort_exec_exprs;
    int _limit;
    int64_t _offset;
    ObjectPool* _pool = nullptr;
    std::vector<bool>& _is_asc_order;
    std::vector<bool>& _nulls_first;

    RuntimeProfile::Counter* _partial_sort_timer = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;

    std::priority_queue<MergeSortBlockCursor> _block_priority_queue;
    bool _materialize_sort_exprs;
};

class FullSorter final : public Sorter {
    ENABLE_FACTORY_CREATOR(FullSorter);

public:
    FullSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset, ObjectPool* pool,
               std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first,
               const RowDescriptor& row_desc, RuntimeState* state, RuntimeProfile* profile);

    ~FullSorter() override = default;

    Status append_block(Block* block) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    size_t data_size() const override;

    Status merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                     int batch_size, bool* eos) override;
    void reset() override;

private:
    bool _reach_limit() {
        return _state->unsorted_block_->rows() > buffered_block_size_ ||
               _state->unsorted_block_->bytes() > buffered_block_bytes_;
    }

    Status _do_sort();

    std::unique_ptr<MergeSorterState> _state;

    static constexpr size_t INITIAL_BUFFERED_BLOCK_SIZE = 1024 * 1024;
    static constexpr size_t INITIAL_BUFFERED_BLOCK_BYTES = 64 << 20;

    static constexpr size_t SPILL_BUFFERED_BLOCK_SIZE = 4 * 1024 * 1024;
    static constexpr size_t SPILL_BUFFERED_BLOCK_BYTES = 256 << 20;

    size_t buffered_block_size_ = INITIAL_BUFFERED_BLOCK_SIZE;
    size_t buffered_block_bytes_ = INITIAL_BUFFERED_BLOCK_BYTES;
};

} // namespace doris::vectorized
