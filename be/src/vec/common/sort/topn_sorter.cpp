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

#include <glog/logging.h>

#include <algorithm>
#include <queue>

#include "common/object_pool.h"
#include "vec/core/block.h"
#include "vec/core/sort_cursor.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class VSortExecExprs;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

TopNSorter::TopNSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset, limit, state, profile)),
          _row_desc(row_desc) {}

Status TopNSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    RETURN_IF_ERROR(_do_sort(block));
    return Status::OK();
}

Status TopNSorter::prepare_for_read() {
    return _state->build_merge_tree(_sort_description);
}

Status TopNSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(state, block, eos);
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
        if (_state->num_rows() < _offset + _limit) {
            static_cast<void>(_state->add_sorted_block(sorted_block));
            // if it's spilled, sorted_block is not added into sorted block vector,
            // so it's should not be added to _block_priority_queue, since
            // sorted_block will be destroyed when _do_sort is finished
            if (!_state->is_spilled()) {
                _block_priority_queue.emplace(_pool->add(
                        new MergeSortCursorImpl(_state->last_sorted_block(), _sort_description)));
            }
        } else {
            if (!_state->is_spilled()) {
                auto tmp_cursor_impl =
                        std::make_unique<MergeSortCursorImpl>(sorted_block, _sort_description);
                MergeSortBlockCursor block_cursor(tmp_cursor_impl.get());
                if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                    static_cast<void>(_state->add_sorted_block(sorted_block));
                    if (!_state->is_spilled()) {
                        _block_priority_queue.emplace(_pool->add(new MergeSortCursorImpl(
                                _state->last_sorted_block(), _sort_description)));
                    }
                }
            } else {
                static_cast<void>(_state->add_sorted_block(sorted_block));
            }
        }
    } else {
        return Status::InternalError("Should not reach TopN sorter for full sort query");
    }
    return Status::OK();
}

size_t TopNSorter::data_size() const {
    return _state->data_size();
}

} // namespace doris::vectorized
