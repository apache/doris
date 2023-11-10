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

#include "vec/common/sort/sorter.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/object_pool.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

// When doing spillable sorting, each sorted block is spilled into a single file.
//
// In order to decrease memory pressure when merging
// multiple spilled blocks into one bigger sorted block, only part
// of each spilled blocks are read back into memory at a time.
//
// Currently the spilled blocks are splitted into small sub blocks,
// each sub block is serialized in PBlock format and appended
// to the spill file.
//
// This number specifies the maximum size of sub blocks
static constexpr int BLOCK_SPILL_BATCH_BYTES = 8 * 1024 * 1024;

Status MergeSorterState::add_sorted_block(Block& block) {
    auto rows = block.rows();
    if (0 == rows) {
        return Status::OK();
    }
    if (0 == avg_row_bytes_) {
        avg_row_bytes_ = std::max((std::size_t)1, block.bytes() / rows);
        spill_block_batch_size_ = (BLOCK_SPILL_BATCH_BYTES + avg_row_bytes_ - 1) / avg_row_bytes_;
    }

    auto bytes_used = data_size();
    auto total_bytes_used = bytes_used + block.bytes();
    if (is_spilled_ || (external_sort_bytes_threshold_ > 0 &&
                        total_bytes_used >= external_sort_bytes_threshold_)) {
        is_spilled_ = true;
        BlockSpillWriterUPtr spill_block_writer;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_writer(
                spill_block_batch_size_, spill_block_writer, block_spill_profile_));

        RETURN_IF_ERROR(spill_block_writer->write(block));
        spilled_sorted_block_streams_.emplace_back(spill_block_writer->get_id());

        COUNTER_UPDATE(spilled_block_count_, 1);
        COUNTER_UPDATE(spilled_original_block_size_, spill_block_writer->get_written_bytes());
        RETURN_IF_ERROR(spill_block_writer->close());

        if (init_merge_sorted_block_) {
            init_merge_sorted_block_ = false;
            merge_sorted_block_ = block.clone_empty();
        }
    } else {
        sorted_blocks_.emplace_back(std::move(block));
    }
    num_rows_ += rows;
    return Status::OK();
}

void MergeSorterState::_build_merge_tree_not_spilled(const SortDescription& sort_description) {
    for (const auto& block : sorted_blocks_) {
        cursors_.emplace_back(block, sort_description);
    }

    if (sorted_blocks_.size() > 1) {
        for (auto& cursor : cursors_) priority_queue_.push(MergeSortCursor(&cursor));
    }
}

Status MergeSorterState::build_merge_tree(const SortDescription& sort_description) {
    _build_merge_tree_not_spilled(sort_description);

    if (spilled_sorted_block_streams_.size() > 0) {
        if (sorted_blocks_.size() > 0) {
            BlockSpillWriterUPtr spill_block_writer;
            RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_writer(
                    spill_block_batch_size_, spill_block_writer, block_spill_profile_));

            if (sorted_blocks_.size() == 1) {
                RETURN_IF_ERROR(spill_block_writer->write(sorted_blocks_[0]));
            } else {
                bool eos = false;

                // merge blocks in memory and write merge result to disk
                while (!eos) {
                    merge_sorted_block_.clear_column_data();
                    RETURN_IF_ERROR(_merge_sort_read_not_spilled(spill_block_batch_size_,
                                                                 &merge_sorted_block_, &eos));
                    RETURN_IF_ERROR(spill_block_writer->write(merge_sorted_block_));
                }
            }
            spilled_sorted_block_streams_.emplace_back(spill_block_writer->get_id());
            RETURN_IF_ERROR(spill_block_writer->close());
        }
        RETURN_IF_ERROR(_merge_spilled_blocks(sort_description));
    }
    return Status::OK();
}

Status MergeSorterState::merge_sort_read(doris::RuntimeState* state,
                                         doris::vectorized::Block* block, bool* eos) {
    if (is_spilled_) {
        RETURN_IF_ERROR(merger_->get_next(block, eos));
    } else {
        if (sorted_blocks_.empty()) {
            *eos = true;
        } else if (sorted_blocks_.size() == 1) {
            if (offset_ != 0) {
                sorted_blocks_[0].skip_num_rows(offset_);
            }
            block->swap(sorted_blocks_[0]);
            *eos = true;
        } else {
            RETURN_IF_ERROR(_merge_sort_read_not_spilled(state->batch_size(), block, eos));
        }
    }
    return Status::OK();
}

Status MergeSorterState::_merge_sort_read_not_spilled(int batch_size,
                                                      doris::vectorized::Block* block, bool* eos) {
    size_t num_columns = sorted_blocks_[0].columns();

    MutableBlock m_block = VectorizedUtils::build_mutable_mem_reuse_block(block, sorted_blocks_[0]);
    MutableColumns& merged_columns = m_block.mutable_columns();

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!priority_queue_.empty()) {
        auto current = priority_queue_.top();
        priority_queue_.pop();

        if (offset_ == 0) {
            for (size_t i = 0; i < num_columns; ++i)
                merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
            ++merged_rows;
        } else {
            offset_--;
        }

        if (!current->isLast()) {
            current->next();
            priority_queue_.push(current);
        }

        if (merged_rows == batch_size) break;
    }

    if (merged_rows == 0) {
        *eos = true;
        return Status::OK();
    }

    return Status::OK();
}

int MergeSorterState::_calc_spill_blocks_to_merge() const {
    int count = external_sort_bytes_threshold_ / BLOCK_SPILL_BATCH_BYTES;
    return std::max(2, count);
}

// merge all the intermediate spilled blocks
Status MergeSorterState::_merge_spilled_blocks(const SortDescription& sort_description) {
    int num_of_blocks_to_merge = _calc_spill_blocks_to_merge();
    while (true) {
        // pick some spilled blocks to merge, and spill the merged result
        // to disk, until all splled blocks can be merged in a run.
        RETURN_IF_ERROR(_create_intermediate_merger(num_of_blocks_to_merge, sort_description));
        if (spilled_sorted_block_streams_.empty()) {
            break;
        }

        bool eos = false;

        BlockSpillWriterUPtr spill_block_writer;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_writer(
                spill_block_batch_size_, spill_block_writer, block_spill_profile_));

        while (!eos) {
            merge_sorted_block_.clear_column_data();
            RETURN_IF_ERROR(merger_->get_next(&merge_sorted_block_, &eos));
            RETURN_IF_ERROR(spill_block_writer->write(merge_sorted_block_));
        }
        spilled_sorted_block_streams_.emplace_back(spill_block_writer->get_id());
        RETURN_IF_ERROR(spill_block_writer->close());
    }
    return Status::OK();
}

Status MergeSorterState::_create_intermediate_merger(int num_blocks,
                                                     const SortDescription& sort_description) {
    spilled_block_readers_.clear();

    std::vector<BlockSupplier> child_block_suppliers;
    merger_.reset(new VSortedRunMerger(sort_description, spill_block_batch_size_, limit_, offset_,
                                       profile_));

    for (int i = 0; i < num_blocks && !spilled_sorted_block_streams_.empty(); ++i) {
        auto stream_id = spilled_sorted_block_streams_.front();
        BlockSpillReaderUPtr spilled_block_reader;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_reader(
                stream_id, spilled_block_reader, block_spill_profile_));
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&BlockSpillReader::read),
                                                     spilled_block_reader.get(),
                                                     std::placeholders::_1, std::placeholders::_2));
        spilled_block_readers_.emplace_back(std::move(spilled_block_reader));

        spilled_sorted_block_streams_.pop_front();
    }
    RETURN_IF_ERROR(merger_->prepare(child_block_suppliers));
    return Status::OK();
}

Status Sorter::partial_sort(Block& src_block, Block& dest_block) {
    size_t num_cols = src_block.columns();
    if (_materialize_sort_exprs) {
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&src_block, &valid_column_ids[i]));
        }

        Block new_block;
        int i = 0;
        const auto& convert_nullable_flags = _vsort_exec_exprs.get_convert_nullable_flags();
        for (auto column_id : valid_column_ids) {
            if (column_id < 0) {
                continue;
            }
            if (i < convert_nullable_flags.size() && convert_nullable_flags[i]) {
                auto column_ptr = make_nullable(src_block.get_by_position(column_id).column);
                new_block.insert(
                        {column_ptr, make_nullable(src_block.get_by_position(column_id).type), ""});
            } else {
                new_block.insert(src_block.get_by_position(column_id));
            }
            i++;
        }
        dest_block.swap(new_block);
    }

    _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    Block* result_block = _materialize_sort_exprs ? &dest_block : &src_block;
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(result_block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
    }

    {
        SCOPED_TIMER(_partial_sort_timer);
        if (_materialize_sort_exprs) {
            sort_block(dest_block, dest_block, _sort_description, _offset + _limit);
        } else {
            sort_block(src_block, dest_block, _sort_description, _offset + _limit);
        }
        src_block.clear_column_data(num_cols);
    }

    return Status::OK();
}

FullSorter::FullSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset, limit, state, profile)) {}

Status FullSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    {
        SCOPED_TIMER(_merge_block_timer);
        auto& data = _state->unsorted_block_->get_columns_with_type_and_name();
        const auto& arrival_data = block->get_columns_with_type_and_name();
        auto sz = block->rows();
        for (int i = 0; i < data.size(); ++i) {
            DCHECK(data[i].type->equals(*(arrival_data[i].type)))
                    << " type1: " << data[i].type->get_name()
                    << " type2: " << arrival_data[i].type->get_name() << " i: " << i;
            //TODO: to eliminate unnecessary expansion, we need a `insert_range_from_const` for every column type.
            data[i].column->assume_mutable()->insert_range_from(
                    *arrival_data[i].column->convert_to_full_column_if_const(), 0, sz);
        }
        block->clear_column_data();
    }
    if (_reach_limit()) {
        RETURN_IF_ERROR(_do_sort());
    }
    return Status::OK();
}

Status FullSorter::prepare_for_read() {
    if (_state->unsorted_block_->rows() > 0) {
        RETURN_IF_ERROR(_do_sort());
    }
    return _state->build_merge_tree(_sort_description);
}

Status FullSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(state, block, eos);
}

Status FullSorter::_do_sort() {
    Block* src_block = _state->unsorted_block_.get();
    Block desc_block = src_block->clone_without_columns();
    RETURN_IF_ERROR(partial_sort(*src_block, desc_block));

    // dispose TOP-N logic
    if (_limit != -1 && !_state->is_spilled()) {
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows() < _offset + _limit) {
            static_cast<void>(_state->add_sorted_block(desc_block));
            // if it's spilled, sorted_block is not added into sorted block vector,
            // so it's should not be added to _block_priority_queue, since
            // sorted_block will be destroyed when _do_sort is finished
            if (!_state->is_spilled()) {
                _block_priority_queue.emplace(_pool->add(
                        new MergeSortCursorImpl(_state->last_sorted_block(), _sort_description)));
            }
        } else {
            auto tmp_cursor_impl =
                    std::make_unique<MergeSortCursorImpl>(desc_block, _sort_description);
            MergeSortBlockCursor block_cursor(tmp_cursor_impl.get());
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                static_cast<void>(_state->add_sorted_block(desc_block));
                if (!_state->is_spilled()) {
                    _block_priority_queue.emplace(_pool->add(new MergeSortCursorImpl(
                            _state->last_sorted_block(), _sort_description)));
                }
            }
        }
    } else {
        // dispose normal sort logic
        static_cast<void>(_state->add_sorted_block(desc_block));
    }
    if (_state->is_spilled()) {
        std::priority_queue<MergeSortBlockCursor> tmp;
        _block_priority_queue.swap(tmp);

        buffered_block_size_ = SPILL_BUFFERED_BLOCK_SIZE;
        buffered_block_bytes_ = SPILL_BUFFERED_BLOCK_BYTES;
    }
    return Status::OK();
}
size_t FullSorter::data_size() const {
    return _state->data_size();
}

} // namespace doris::vectorized
