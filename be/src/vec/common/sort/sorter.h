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

#include <cstddef>
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
#include "common/compile_check_begin.h"
class ObjectPool;
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

using MergeSorterQueue = SortingQueueBatch<MergeSortCursor>;

// TODO: now we only use merge sort
class MergeSorterState {
    ENABLE_FACTORY_CREATOR(MergeSorterState);

public:
    MergeSorterState(const RowDescriptor& row_desc, int64_t offset)
            // create_empty_block should ignore invalid slots, unsorted_block
            // should be same structure with arrival block from child node
            // since block from child node may ignored these slots
            : _unsorted_block(Block::create_unique(
                      VectorizedUtils::create_empty_block(row_desc, true /*ignore invalid slot*/))),
              _offset(offset) {}

    ~MergeSorterState() = default;

    void add_sorted_block(std::shared_ptr<Block> block);

    Status build_merge_tree(const SortDescription& sort_description);

    Status merge_sort_read(doris::vectorized::Block* block, int batch_size, bool* eos);

    size_t data_size() const {
        size_t size = _unsorted_block->bytes();
        return size + _in_mem_sorted_bocks_size;
    }

    uint64_t num_rows() const { return _num_rows; }

    std::shared_ptr<Block> last_sorted_block() { return _sorted_blocks.back(); }

    std::vector<std::shared_ptr<Block>>& get_sorted_block() { return _sorted_blocks; }
    MergeSorterQueue& get_queue() { return _queue; }
    void reset();

    std::unique_ptr<Block>& unsorted_block() { return _unsorted_block; }

    void ignore_offset() { _offset = 0; }

private:
    void _merge_sort_read_impl(int batch_size, doris::vectorized::Block* block, bool* eos);

    std::unique_ptr<Block> _unsorted_block;
    MergeSorterQueue _queue;
    std::vector<std::shared_ptr<Block>> _sorted_blocks;
    size_t _in_mem_sorted_bocks_size = 0;
    uint64_t _num_rows = 0;

    size_t _offset;

    Block _merge_sorted_block;
    std::unique_ptr<VSortedRunMerger> _merger;
};

class Sorter {
public:
    Sorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset, ObjectPool* pool,
           std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first)
            : _vsort_exec_exprs(vsort_exec_exprs),
              _limit(limit),
              _offset(offset),
              _pool(pool),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first),
              _materialize_sort_exprs(vsort_exec_exprs.need_materialize_tuple()) {}
#ifdef BE_TEST
    VSortExecExprs mock_vsort_exec_exprs;
    std::vector<bool> mock_is_asc_order;
    std::vector<bool> mock_nulls_first;
    Sorter()
            : _vsort_exec_exprs(mock_vsort_exec_exprs),
              _is_asc_order(mock_is_asc_order),
              _nulls_first(mock_nulls_first) {}
#endif

    virtual ~Sorter() = default;

    virtual void init_profile(RuntimeProfile* runtime_profile) {
        _partial_sort_timer = ADD_TIMER(runtime_profile, "PartialSortTime");
        _merge_block_timer = ADD_TIMER(runtime_profile, "MergeBlockTime");
        _partial_sort_counter = ADD_COUNTER(runtime_profile, "PartialSortCounter", TUnit::UNIT);
    }

    virtual Status append_block(Block* block) = 0;

    virtual Status prepare_for_read(bool is_spill) = 0;

    virtual Status get_next(RuntimeState* state, Block* block, bool* eos) = 0;

    virtual size_t data_size() const = 0;

    virtual size_t get_reserve_mem_size(RuntimeState* state, bool eos) const { return 0; }

    // for topn runtime predicate
    const SortDescription& get_sort_description() const { return _sort_description; }
    virtual Field get_top_value() { return Field {PrimitiveType::TYPE_NULL}; }

    virtual Status merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                             int batch_size, bool* eos);
    virtual void reset() {}

    int64_t limit() const { return _limit; }
    int64_t offset() const { return _offset; }

    void set_enable_spill() { _enable_spill = true; }

protected:
    Status partial_sort(Block& src_block, Block& dest_block, bool reversed = false);

    bool _enable_spill = false;
    SortDescription _sort_description;
    VSortExecExprs& _vsort_exec_exprs;
    int64_t _limit;
    int64_t _offset;
    ObjectPool* _pool = nullptr;
    std::vector<bool>& _is_asc_order;
    std::vector<bool>& _nulls_first;

    RuntimeProfile::Counter* _partial_sort_timer = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;
    RuntimeProfile::Counter* _partial_sort_counter = nullptr;

    std::priority_queue<MergeSortBlockCursor> _block_priority_queue;
    bool _materialize_sort_exprs;
};

class FullSorter final : public Sorter {
    ENABLE_FACTORY_CREATOR(FullSorter);

public:
    FullSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset, ObjectPool* pool,
               std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first,
               const RowDescriptor& row_desc, RuntimeState* state, RuntimeProfile* profile);

    ~FullSorter() override = default;

    Status append_block(Block* block) override;

    Status prepare_for_read(bool is_spill) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    size_t data_size() const override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) const override;

    Status merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                     int batch_size, bool* eos) override;
    void reset() override;

    void set_max_buffered_block_bytes(size_t max_buffered_block_bytes) {
        _max_buffered_block_bytes = max_buffered_block_bytes;
    }

private:
    bool _reach_limit() {
        return _state->unsorted_block()->allocated_bytes() >= _max_buffered_block_bytes;
    }

    bool has_enough_capacity(Block* input_block, Block* unsorted_block) const;

    Status _do_sort();

    std::unique_ptr<MergeSorterState> _state;

    static constexpr size_t INITIAL_BUFFERED_BLOCK_BYTES = 64 * 1024 * 1024;

    static constexpr size_t SPILL_BUFFERED_BLOCK_SIZE = 4 * 1024 * 1024;
    static constexpr size_t SPILL_BUFFERED_BLOCK_BYTES = 256 << 20;

    size_t _buffered_block_size = SPILL_BUFFERED_BLOCK_SIZE;
    size_t _buffered_block_bytes = SPILL_BUFFERED_BLOCK_BYTES;

    size_t _max_buffered_block_bytes = INITIAL_BUFFERED_BLOCK_BYTES;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
