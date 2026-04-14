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

#include "exec/sort/vsorted_run_merger.h"

#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "exec/common/util.hpp"
#include "runtime/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {

VSortedRunMerger::VSortedRunMerger(const VExprContextSPtrs& ordering_expr,
                                   const std::vector<bool>& is_asc_order,
                                   const std::vector<bool>& nulls_first, const size_t batch_size,
                                   int64_t limit, size_t offset, RuntimeProfile* profile,
                                   size_t block_max_bytes)
        : _ordering_expr(ordering_expr),
          _is_asc_order(is_asc_order),
          _nulls_first(nulls_first),
          _batch_size(batch_size),
          _block_max_bytes(block_max_bytes),
          _limit(limit),
          _offset(offset) {
    init_timers(profile);
}

VSortedRunMerger::VSortedRunMerger(const SortDescription& desc, const size_t batch_size,
                                   int64_t limit, size_t offset, RuntimeProfile* profile,
                                   size_t block_max_bytes)
        : _desc(desc),
          _batch_size(batch_size),
          _block_max_bytes(block_max_bytes),
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

    for (auto& cursor : _cursors) {
        if (!cursor->eof()) {
            _priority_queue.push(MergeSortCursor(cursor));
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
        {
            ScopedTimer<MonotonicStopWatch> timer1(_get_next_block_timer);
            cursor->process_next();
        }
        if (!cursor->eof()) {
            _priority_queue.push(cursor);
        }
        _pending_cursor = nullptr;
    }

    Defer set_limit([&]() {
        _num_rows_returned += output_block->rows();
        if (_limit != -1 && _num_rows_returned >= _limit) {
            output_block->set_num_rows(output_block->rows() - (_num_rows_returned - _limit));
            *eos = true;
        }
    });

    if (_priority_queue.empty()) {
        *eos = true;
        return Status::OK();
    } else if (_priority_queue.size() == 1) {
        // Single-run fast path: cut/swap the supplier block into output.
        // Uses column->cut() which is O(columns) instead of row-by-row merge O(rows*columns).
        auto current = _priority_queue.top();
        DCHECK(!current->eof());
        DCHECK(current->block_ptr() != nullptr);
        while (_offset != 0) {
            auto process_rows = std::min(current->rows - current->pos, (int)_offset);
            current->next(process_rows);
            _offset -= process_rows;
            if (current->is_last(0)) {
                _priority_queue.pop();
                if (current->eof()) {
                    *eos = true;
                } else {
                    _pending_cursor = current.impl;
                }
                return Status::OK();
            }
        }

        size_t remaining = current->rows - current->pos;
        size_t output_rows = std::min(remaining, _batch_size);

        // Apply byte budget: estimate rows that fit within _block_max_bytes.
        // _block_max_bytes == 0 means no byte budget (e.g., iceberg sort writer).
        if (_block_max_bytes > 0 && remaining > 1 && current->block_ptr()->bytes() > 0) {
            size_t bytes_per_row = current->block_ptr()->bytes() / current->rows;
            if (bytes_per_row > 0) {
                size_t byte_limited = _block_max_bytes / bytes_per_row;
                output_rows = std::min(output_rows, std::max(size_t(1), byte_limited));
            }
        }

        if (current->is_first() && output_rows == remaining) {
            // Entire block fits — zero-copy swap.
            current->block_ptr()->swap(*output_block);
        } else {
            // Build output block from cut columns without modifying the supplier block.
            // This preserves remaining rows for subsequent get_next calls.
            auto* src_block = current->block_ptr();
            output_block->clear();
            for (int i = 0; i < src_block->columns(); i++) {
                auto col_with_type = src_block->get_by_position(i);
                col_with_type.column = col_with_type.column->cut(current->pos, output_rows);
                output_block->insert(std::move(col_with_type));
            }
        }
        current->next(output_rows);
        if (output_rows >= remaining) {
            _priority_queue.pop();
            if (current->eof()) {
                *eos = true;
            } else {
                _pending_cursor = current.impl;
            }
        }
        return Status::OK();
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

            current->next();
            if (_need_more_data(current)) {
                do_insert();
                return Status::OK();
            }

            if (merged_rows > 0 && (merged_rows & 255) == 0 && !_indexs.empty()) {
                do_insert();
                // _block_max_bytes == 0 means no byte budget.
                if (_block_max_bytes > 0 && m_block.bytes() >= _block_max_bytes) {
                    break;
                }
            }
        }
        do_insert();
        output_block->set_columns(std::move(merged_columns));

        if (merged_rows == 0) {
            *eos = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

bool VSortedRunMerger::_need_more_data(MergeSortCursor& current) {
    if (!current->is_last(0)) {
        _priority_queue.push(current);
        return false;
    } else if (current->eof()) {
        return false;
    } else {
        _pending_cursor = current.impl;
        return true;
    }
}

} // namespace doris