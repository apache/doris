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

#include "common/exception.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/utils/util.hpp"

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

Status VSortedRunMerger::prepare(const std::vector<BlockSupplier>& input_runs) {
    try {
        for (const auto& supplier : input_runs) {
            if (_use_sort_desc) {
                _cursors.emplace_back(BlockSupplierSortCursorImpl::create_shared(supplier, _desc));
            } else {
                _cursors.emplace_back(BlockSupplierSortCursorImpl::create_shared(
                        supplier, _ordering_expr, _is_asc_order, _nulls_first));
            }
        }
    } catch (const std::exception& e) {
        return Status::Cancelled(e.what());
    }

    for (auto& _cursor : _cursors) {
        if (!_cursor->_is_eof) {
            _priority_queue.push(MergeSortCursor(_cursor));
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
                _pending_cursor = current.impl;
                _priority_queue.pop();
                return Status::OK();
            } else {
                current->pos += _offset;
                _offset = 0;
            }
        }

        if (current->is_first()) {
            if (current->block_ptr() != nullptr) {
                current->block_ptr()->swap(*output_block);
                _pending_cursor = current.impl;
                _priority_queue.pop();
                return Status::OK();
            } else {
                *eos = true;
            }
        } else {
            if (current->block_ptr() != nullptr) {
                for (int i = 0; i < current->block->columns(); i++) {
                    auto& column_with_type = current->block_ptr()->get_by_position(i);
                    column_with_type.column = column_with_type.column->cut(
                            current->pos, current->rows - current->pos);
                }
                current->block_ptr()->swap(*output_block);
                _pending_cursor = current.impl;
                _priority_queue.pop();
                return Status::OK();
            } else {
                *eos = true;
            }
        }
    } else {
        size_t num_columns = _priority_queue.top().impl->block->columns();
        MutableBlock m_block = VectorizedUtils::build_mutable_mem_reuse_block(
                output_block, *_priority_queue.top().impl->block);
        MutableColumns& merged_columns = m_block.mutable_columns();

        if (num_columns != merged_columns.size()) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "num_columns!=merged_columns.size(), num_columns={}, merged_columns.size()={}",
                    num_columns, merged_columns.size());
        }

        _indexs.reserve(_batch_size);
        _block_addrs.reserve(_batch_size);

        auto do_insert = [&]() {
            _column_addrs.resize(_indexs.size());
            for (size_t i = 0; i < num_columns; ++i) {
                for (size_t j = 0; j < _indexs.size(); j++) {
                    _column_addrs[j] = _block_addrs[j]->get_by_position(i).column.get();
                }
                merged_columns[i]->insert_from_multi_column(_column_addrs, _indexs);
            }
            _indexs.clear();
            _block_addrs.clear();
            _column_addrs.clear();
        };

        /// Take rows from queue in right order and push to 'merged'.
        size_t merged_rows = 0;
        while (merged_rows != _batch_size && !_priority_queue.empty()) {
            auto current = _priority_queue.top();
            _priority_queue.pop();

            if (_offset > 0) {
                _offset--;
            } else {
                _indexs.emplace_back(current->pos);
                _block_addrs.emplace_back(current->block_ptr());
                ++merged_rows;
            }

            if (!next_heap(current)) {
                do_insert();
                return Status::OK();
            }
        }
        do_insert();
        output_block->set_columns(std::move(merged_columns));

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
    if (!current->is_last()) {
        current->next();
        _priority_queue.push(current);
        return true;
    }

    _pending_cursor = current.impl;
    return false;
}

inline bool VSortedRunMerger::has_next_block(doris::vectorized::MergeSortCursor& current) {
    ScopedTimer<MonotonicStopWatch> timer(_get_next_block_timer);
    return current->has_next_block();
}

} // namespace doris::vectorized