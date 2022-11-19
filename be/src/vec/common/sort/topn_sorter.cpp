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

#include "vec/common/sort/topn_sorter.h"

namespace doris::vectorized {

TopNSorter::TopNSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(std::unique_ptr<MergeSorterState>(new MergeSorterState(row_desc, offset))),
          _row_desc(row_desc) {}

Status TopNSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    RETURN_IF_ERROR(_do_sort(block));
    return Status::OK();
}

Status TopNSorter::prepare_for_read() {
    _state->build_merge_tree(_sort_description);
    return Status::OK();
}

Status TopNSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (_state->sorted_blocks.empty()) {
        *eos = true;
    } else if (_state->sorted_blocks.size() == 1) {
        if (_offset != 0) {
            _state->sorted_blocks[0].skip_num_rows(_offset);
        }
        block->swap(_state->sorted_blocks[0]);
        *eos = true;
    } else {
        RETURN_IF_ERROR(_state->merge_sort_read(state, block, eos));
    }
    return Status::OK();
}

Status TopNSorter::_do_sort(Block* block) {
    Block sorted_block = VectorizedUtils::create_empty_columnswithtypename(_row_desc);
    RETURN_IF_ERROR(partial_sort(*block, sorted_block));

    // dispose TOP-N logic
    if (_limit != -1) {
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows < _limit) {
            _state->num_rows += sorted_block.rows();
            _state->sorted_blocks.emplace_back(std::move(sorted_block));
            _block_priority_queue.emplace(_pool->add(
                    new MergeSortCursorImpl(_state->sorted_blocks.back(), _sort_description)));
        } else {
            MergeSortBlockCursor block_cursor(
                    _pool->add(new MergeSortCursorImpl(sorted_block, _sort_description)));
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                _state->sorted_blocks.emplace_back(std::move(sorted_block));
                _block_priority_queue.push(block_cursor);
            }
        }
    } else {
        return Status::InternalError("Should not reach TopN sorter for full sort query");
    }
    return Status::OK();
}

} // namespace doris::vectorized
