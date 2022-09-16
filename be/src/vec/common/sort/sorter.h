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
#include <queue>

#include "common/status.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_cursor.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// TODO: now we only use merge sort
class MergeSorterState {
public:
    MergeSorterState(const RowDescriptor& row_desc, int64_t offset)
            : unsorted_block(new MutableBlock(
                      VectorizedUtils::create_empty_columnswithtypename(row_desc))),
              _offset(offset),
              _row_desc(row_desc) {}

    ~MergeSorterState() = default;

    void reset_block() {
        unsorted_block.reset(
                new MutableBlock(VectorizedUtils::create_empty_columnswithtypename(_row_desc)));
    }

    void build_merge_tree(SortDescription& sort_description);

    Status merge_sort_read(doris::RuntimeState* state, doris::vectorized::Block* block, bool* eos);

    std::priority_queue<SortCursor> priority_queue;
    std::vector<SortCursorImpl> cursors;
    std::unique_ptr<MutableBlock> unsorted_block;
    std::vector<Block> sorted_blocks;
    uint64_t num_rows = 0;

private:
    int64_t _offset;
    const RowDescriptor& _row_desc;
};

class Sorter {
public:
    Sorter(SortDescription& sort_description, VSortExecExprs& vsort_exec_exprs, int limit,
           int64_t offset, ObjectPool* pool, std::vector<bool>& is_asc_order,
           std::vector<bool>& nulls_first)
            : _sort_description(sort_description),
              _vsort_exec_exprs(vsort_exec_exprs),
              _limit(limit),
              _offset(offset),
              _pool(pool),
              _is_asc_order(is_asc_order),
              _nulls_first(nulls_first) {}

    virtual ~Sorter() = default;

    void init_profile(RuntimeProfile* runtime_profile) {
        _partial_sort_timer = ADD_TIMER(runtime_profile, "PartialSortTime");
        _merge_block_timer = ADD_TIMER(runtime_profile, "MergeBlockTime");
    }

    virtual Status append_block(Block* block, bool* mem_reuse) = 0;

    virtual Status prepare_for_read() = 0;

    virtual Status get_next(RuntimeState* state, Block* block, bool* eos) = 0;

protected:
    Status partial_sort(Block& block);

    SortDescription& _sort_description;
    VSortExecExprs& _vsort_exec_exprs;
    int _limit;
    int64_t _offset;
    ObjectPool* _pool;
    std::vector<bool>& _is_asc_order;
    std::vector<bool>& _nulls_first;

    std::priority_queue<SortBlockCursor> _block_priority_queue;
    RuntimeProfile::Counter* _partial_sort_timer = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;
};

class FullSorter final : public Sorter {
public:
    FullSorter(SortDescription& sort_description, VSortExecExprs& vsort_exec_exprs, int limit,
               int64_t offset, ObjectPool* pool, std::vector<bool>& is_asc_order,
               std::vector<bool>& nulls_first, const RowDescriptor& row_desc);

    ~FullSorter() override = default;

    Status append_block(Block* block, bool* mem_reuse) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

private:
    bool _reach_limit() {
        return _state->unsorted_block->rows() > BUFFERED_BLOCK_SIZE ||
               _state->unsorted_block->allocated_bytes() > BUFFERED_BLOCK_BYTES;
    }

    Status _do_sort();

    std::unique_ptr<MergeSorterState> _state;

    static constexpr size_t BUFFERED_BLOCK_SIZE = 1024 * 1024;
    static constexpr size_t BUFFERED_BLOCK_BYTES = 16 << 20;
};

class TopNSorter final : public Sorter {
public:
    TopNSorter(SortDescription& sort_description, VSortExecExprs& vsort_exec_exprs, int limit,
               int64_t offset, ObjectPool* pool, std::vector<bool>& is_asc_order,
               std::vector<bool>& nulls_first, const RowDescriptor& row_desc);

    ~TopNSorter() override = default;

    Status append_block(Block* block, bool* mem_reuse) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

private:
    Status _do_sort(Block* block, bool* mem_reuse);

    std::unique_ptr<MergeSorterState> _state;
};

} // namespace doris::vectorized
