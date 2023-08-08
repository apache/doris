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

#include "vec/runtime/vsorted_run_merger.h"

#include <utility>
#include <vector>

#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/utils/util.hpp"

namespace doris {
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

using std::vector;

namespace doris::vectorized {

VSortedRunMerger::VSortedRunMerger(const VExprContextSPtrs& ordering_expr,
                                   const std::vector<bool>& is_asc_order,
                                   const std::vector<bool>& nulls_first, const size_t batch_size,
                                   int64_t limit, size_t offset, RuntimeProfile* profile)
        : _ordering_expr(ordering_expr),
          _is_asc_order(is_asc_order),
          _nulls_first(nulls_first),
          _batch_size(batch_size),
          _limit(limit),
          _offset(offset) {
    init_timers(profile);
}

VSortedRunMerger::VSortedRunMerger(const SortDescription& desc, const size_t batch_size,
                                   int64_t limit, size_t offset, RuntimeProfile* profile)
        : _desc(desc),
          _batch_size(batch_size),
          _use_sort_desc(true),
          _limit(limit),
          _offset(offset),
          _get_next_timer(nullptr),
          _get_next_block_timer(nullptr) {
    init_timers(profile);
}

void VSortedRunMerger::init_timers(RuntimeProfile* profile) {
    _get_next_timer = ADD_TIMER(profile, "MergeGetNext");
    _get_next_block_timer = ADD_TIMER(profile, "MergeGetNextBlock");
}

Status VSortedRunMerger::prepare(const vector<BlockSupplier>& input_runs) {
    for (const auto& supplier : input_runs) {
        if (_use_sort_desc) {
            _cursors.emplace_back(supplier, _desc);
        } else {
            _cursors.emplace_back(supplier, _ordering_expr, _is_asc_order, _nulls_first);
        }
    }

    for (auto& _cursor : _cursors) {
        if (!_cursor._is_eof) {
            _priority_queue.push(MergeSortCursor(&_cursor));
        }
    }

    for (const auto& cursor : _cursors) {
        if (!cursor._is_eof) {
            _empty_block = cursor.create_empty_blocks();
            break;
        }
    }

    return Status::OK();
}

Status VSortedRunMerger::get_next(Block* output_block, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_get_next_timer);
    // Only have one receive data queue of data, no need to do merge and
    // copy the data of block.
    // return the data in receive data directly

    if (_pending_cursor != nullptr) {
        MergeSortCursor cursor(_pending_cursor);
        if (has_next_block(cursor)) {
            _priority_queue.push(cursor);
        }
        _pending_cursor = nullptr;
    }

    if (_priority_queue.empty()) {
        *eos = true;
        return Status::OK();
    } else if (_priority_queue.size() == 1) {
        auto current = _priority_queue.top();
        while (_offset != 0 && current->block_ptr() != nullptr) {
            if (_offset >= current->rows - current->pos) {
                _offset -= (current->rows - current->pos);
                if (_pipeline_engine_enabled) {
                    _pending_cursor = current.impl;
                    _priority_queue.pop();
                    return Status::OK();
                }
                has_next_block(current);
            } else {
                current->pos += _offset;
                _offset = 0;
            }
        }

        if (current->isFirst()) {
            if (current->block_ptr() != nullptr) {
                current->block_ptr()->swap(*output_block);
                if (_pipeline_engine_enabled) {
                    _pending_cursor = current.impl;
                    _priority_queue.pop();
                    return Status::OK();
                }
                *eos = !has_next_block(current);
            } else {
                *eos = true;
            }
        } else {
            if (current->block_ptr() != nullptr) {
                for (int i = 0; i < current->all_columns.size(); i++) {
                    auto& column_with_type = current->block_ptr()->get_by_position(i);
                    column_with_type.column = column_with_type.column->cut(
                            current->pos, current->rows - current->pos);
                }
                current->block_ptr()->swap(*output_block);
                if (_pipeline_engine_enabled) {
                    _pending_cursor = current.impl;
                    _priority_queue.pop();
                    return Status::OK();
                }
                *eos = !has_next_block(current);
            } else {
                *eos = true;
            }
        }
    } else {
        size_t num_columns = _empty_block.columns();
        MutableBlock m_block =
                VectorizedUtils::build_mutable_mem_reuse_block(output_block, _empty_block);
        MutableColumns& merged_columns = m_block.mutable_columns();

        /// Take rows from queue in right order and push to 'merged'.
        size_t merged_rows = 0;
        while (!_priority_queue.empty()) {
            auto current = _priority_queue.top();
            _priority_queue.pop();

            if (_offset > 0) {
                _offset--;
            } else {
                for (size_t i = 0; i < num_columns; ++i)
                    merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
                ++merged_rows;
            }

            // In pipeline engine, needs to check if the sender is readable before the next reading.
            if (!next_heap(current)) {
                return Status::OK();
            }

            if (merged_rows == _batch_size) {
                break;
            }
        }

        if (merged_rows == 0) {
            *eos = true;
            return Status::OK();
        }
    }

    _num_rows_returned += output_block->rows();
    if (_limit != -1 && _num_rows_returned >= _limit) {
        output_block->set_num_rows(output_block->rows() - (_num_rows_returned - _limit));
        *eos = true;
    }
    return Status::OK();
}

bool VSortedRunMerger::next_heap(MergeSortCursor& current) {
    if (!current->isLast()) {
        current->next();
        _priority_queue.push(current);
    } else if (_pipeline_engine_enabled) {
        // need to check sender is readable again before the next reading.
        _pending_cursor = current.impl;
        return false;
    } else if (has_next_block(current)) {
        _priority_queue.push(current);
    }
    return true;
}

inline bool VSortedRunMerger::has_next_block(doris::vectorized::MergeSortCursor& current) {
    ScopedTimer<MonotonicStopWatch> timer(_get_next_block_timer);
    return current->has_next_block();
}

} // namespace doris::vectorized