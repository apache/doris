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

#include "exec/operator/table_function_operator.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "exec/operator/operator.h"
#include "exprs/table_function/table_function_factory.h"
#include "util/simd/bits.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
} // namespace doris

namespace doris {

TableFunctionLocalState::TableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<>(state, parent), _child_block(Block::create_unique()) {}

Status TableFunctionLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _init_function_timer = ADD_TIMER(custom_profile(), "InitTableFunctionTime");
    _process_rows_timer = ADD_TIMER(custom_profile(), "ProcessRowsTime");
    _filter_timer = ADD_TIMER(custom_profile(), "FilterTime");
    return Status::OK();
}

Status TableFunctionLocalState::_clone_table_function(RuntimeState* state) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    _vfn_ctxs.resize(p._vfn_ctxs.size());
    for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));

        TableFunction* fn = nullptr;
        RETURN_IF_ERROR(TableFunctionFactory::get_fn(_vfn_ctxs[i]->root()->fn(), state->obj_pool(),
                                                     &fn, state->be_exec_version()));
        fn->set_expr_context(_vfn_ctxs[i]);
        _fns.push_back(fn);
    }
    _expand_conjuncts_ctxs.resize(p._expand_conjuncts_ctxs.size());
    for (size_t i = 0; i < _expand_conjuncts_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._expand_conjuncts_ctxs[i]->clone(state, _expand_conjuncts_ctxs[i]));
    }
    _need_to_handle_outer_conjuncts = (!_expand_conjuncts_ctxs.empty() && _fns[0]->is_outer());
    return Status::OK();
}

Status TableFunctionLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(PipelineXLocalState<>::exec_time_counter());
    SCOPED_TIMER(PipelineXLocalState<>::_open_timer);
    RETURN_IF_ERROR(PipelineXLocalState<>::open(state));
    RETURN_IF_ERROR(_clone_table_function(state));
    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->open());
    }
    _cur_child_offset = -1;
    _reset_block_fast_path_state();
    return Status::OK();
}

void TableFunctionLocalState::_copy_output_slots(std::vector<MutableColumnPtr>& columns,
                                                 const TableFunctionOperatorX& p) {
    if (!_current_row_insert_times) {
        return;
    }
    for (auto index : p._output_slot_indexs) {
        auto src_column = _child_block->get_by_position(index).column;
        columns[index]->insert_many_from(*src_column, _cur_child_offset, _current_row_insert_times);
    }
    _current_row_insert_times = 0;
}

// Returns the index of fn of the last eos counted from back to front
// eg: there are 3 functions in `_fns`
//      eos:    false, true, true
//      return: 1
//
//      eos:    false, false, true
//      return: 2
//
//      eos:    false, false, false
//      return: -1
//
//      eos:    true, true, true
//      return: 0
//
// return:
//  0: all fns are eos
// -1: all fns are not eos
// >0: some of fns are eos
int TableFunctionLocalState::_find_last_fn_eos_idx() const {
    for (int i = _parent->cast<TableFunctionOperatorX>()._fn_num - 1; i >= 0; --i) {
        if (!_fns[i]->eos()) {
            if (i == _parent->cast<TableFunctionOperatorX>()._fn_num - 1) {
                return -1;
            } else {
                return i + 1;
            }
        }
    }
    // all eos
    return 0;
}

// Roll to reset the table function.
// Eg:
//  There are 3 functions f1, f2 and f3 in `_fns`.
//  If `last_eos_idx` is 1, which means f2 and f3 are eos.
//  So we need to forward f1, and reset f2 and f3.
bool TableFunctionLocalState::_roll_table_functions(int last_eos_idx) {
    int i = last_eos_idx - 1;
    for (; i >= 0; --i) {
        _fns[i]->forward();
        if (!_fns[i]->eos()) {
            break;
        }
    }
    if (i == -1) {
        // after forward, all functions are eos.
        // we should process next child row to get more table function results.
        return false;
    }

    for (int j = i + 1; j < _parent->cast<TableFunctionOperatorX>()._fn_num; ++j) {
        _fns[j]->reset();
    }

    return true;
}

bool TableFunctionLocalState::_is_inner_and_empty() {
    for (int i = 0; i < _parent->cast<TableFunctionOperatorX>()._fn_num; i++) {
        // if any table function is not outer and has empty result, go to next child row
        // if it's outer function, will be insert into one row NULL
        if (!_fns[i]->is_outer() && _fns[i]->current_empty()) {
            return true;
        }
    }
    return false;
}

bool TableFunctionLocalState::_can_use_block_fast_path() const {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    // Fast path is only valid when:
    // - only one table function exists
    // - there is an active child row to expand
    // - the child block is non-empty
    // - the table function can expose nested/offsets via prepare_block_fast_path()
    return p._fn_num == 1 && _cur_child_offset != -1 && _child_block->rows() > 0 &&
           _fns[0]->support_block_fast_path();
}

void TableFunctionLocalState::_reset_block_fast_path_state() {
    _block_fast_path_prepared = false;
    _block_fast_path_enabled = false;
    _block_fast_path_ctx = {};
    _block_fast_path_row = 0;
    _block_fast_path_in_row_offset = 0;
}

Status TableFunctionLocalState::_prepare_block_fast_path(RuntimeState* state) {
    if (_block_fast_path_prepared) {
        return Status::OK();
    }

    RETURN_IF_ERROR(
            _fns[0]->prepare_block_fast_path(_child_block.get(), state, &_block_fast_path_ctx));
    if (_block_fast_path_ctx.offsets_ptr == nullptr ||
        _block_fast_path_ctx.nested_col.get() == nullptr) {
        return Status::InternalError("block fast path context is invalid");
    }

    const auto child_rows = cast_set<int64_t>(_block_fast_path_ctx.offsets_ptr->size());
    if (child_rows != cast_set<int64_t>(_child_block->rows())) {
        return Status::InternalError("block fast path offsets size mismatch");
    }

    _block_fast_path_row = _cur_child_offset;
    _block_fast_path_in_row_offset = 0;
    _block_fast_path_enabled = _has_contiguous_block_fast_path_suffix();
    _block_fast_path_prepared = true;
    return Status::OK();
}

bool TableFunctionLocalState::_has_contiguous_block_fast_path_suffix() const {
    const auto& offsets = *_block_fast_path_ctx.offsets_ptr;
    const auto child_rows = cast_set<int64_t>(offsets.size());
    int64_t child_row = _block_fast_path_row;
    uint64_t in_row_offset = _block_fast_path_in_row_offset;
    uint64_t expected_next_nested_idx = 0;
    bool found_nested_range = false;

    while (child_row < child_rows) {
        if (_block_fast_path_ctx.array_nullmap_data &&
            _block_fast_path_ctx.array_nullmap_data[child_row]) {
            child_row++;
            in_row_offset = 0;
            continue;
        }

        const uint64_t prev_off = child_row == 0 ? 0 : offsets[child_row - 1];
        const uint64_t cur_off = offsets[child_row];
        const uint64_t nested_len = cur_off - prev_off;
        if (in_row_offset >= nested_len) {
            child_row++;
            in_row_offset = 0;
            continue;
        }

        const uint64_t nested_start = prev_off + in_row_offset;
        if (!found_nested_range) {
            found_nested_range = true;
        } else if (nested_start != expected_next_nested_idx) {
            return false;
        }
        expected_next_nested_idx = cur_off;
        child_row++;
        in_row_offset = 0;
    }

    return true;
}

Status TableFunctionLocalState::_get_expanded_block_block_fast_path(
        RuntimeState* state, std::vector<MutableColumnPtr>& columns) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    DCHECK(_block_fast_path_prepared);
    DCHECK(_block_fast_path_enabled);

    const auto remaining_capacity =
            state->batch_size() - cast_set<int>(columns[p._child_slots.size()]->size());
    if (remaining_capacity <= 0) {
        return Status::OK();
    }

    const auto& offsets = *_block_fast_path_ctx.offsets_ptr;
    const auto child_rows = cast_set<int64_t>(offsets.size());

    int64_t child_row = _block_fast_path_row;
    uint64_t in_row_offset = _block_fast_path_in_row_offset;
    int produced_rows = 0;

    const bool is_outer = _fns[0]->is_outer();
    const bool is_posexplode = _block_fast_path_ctx.generate_row_index;
    auto& out_col = columns[p._child_slots.size()];

    // Decompose posexplode struct output column if needed
    ColumnStruct* struct_col_ptr = nullptr;
    ColumnUInt8* outer_struct_nullmap_ptr = nullptr;
    IColumn* value_col_ptr = nullptr;
    ColumnInt32* pos_col_ptr = nullptr;
    if (is_posexplode) {
        if (out_col->is_nullable()) {
            auto* nullable = assert_cast<ColumnNullable*>(out_col.get());
            struct_col_ptr = assert_cast<ColumnStruct*>(nullable->get_nested_column_ptr().get());
            outer_struct_nullmap_ptr =
                    assert_cast<ColumnUInt8*>(nullable->get_null_map_column_ptr().get());
        } else {
            struct_col_ptr = assert_cast<ColumnStruct*>(out_col.get());
        }
        pos_col_ptr = assert_cast<ColumnInt32*>(&struct_col_ptr->get_column(0));
        value_col_ptr = &struct_col_ptr->get_column(1);
    }
    // Segment tracking: accumulate contiguous nested ranges, flush on boundaries.
    // Array column offsets are monotonically non-decreasing, so nested data across child rows
    // is always contiguous (even with NULL/empty rows that contribute zero elements).
    struct ExpandSegmentContext {
        std::vector<uint32_t>
                seg_row_ids; // row ids of non table-function columns to replicate for this segment
        std::vector<int32_t>
                seg_positions; // for posexplode, the position values to write for this segment
        int64_t seg_nested_start = -1; // start offset in the nested column of this segment
        int seg_nested_count =
                0; // number of nested rows in this segment (can be > child row count due to multiple elements per row)
    };
    ExpandSegmentContext segment_ctx;
    segment_ctx.seg_row_ids.reserve(remaining_capacity);
    if (is_posexplode) {
        segment_ctx.seg_positions.reserve(remaining_capacity);
    }

    auto reset_expand_segment_ctx = [&segment_ctx, is_posexplode]() {
        segment_ctx.seg_nested_start = -1;
        segment_ctx.seg_nested_count = 0;
        segment_ctx.seg_row_ids.clear();
        if (is_posexplode) {
            segment_ctx.seg_positions.clear();
        }
    };

    // Flush accumulated contiguous segment to output columns
    auto flush_segment = [&]() {
        if (segment_ctx.seg_nested_count == 0) {
            return;
        }

        // Non-TF columns: replicate each child row for every output element
        for (auto index : p._output_slot_indexs) {
            auto src_column = _child_block->get_by_position(index).column;
            columns[index]->insert_indices_from(
                    *src_column, segment_ctx.seg_row_ids.data(),
                    segment_ctx.seg_row_ids.data() + segment_ctx.seg_row_ids.size());
        }

        if (is_posexplode) {
            // Write positions
            pos_col_ptr->insert_many_raw_data(
                    reinterpret_cast<const char*>(segment_ctx.seg_positions.data()),
                    segment_ctx.seg_positions.size());
            // Write nested values to the struct's value sub-column
            DCHECK(value_col_ptr->is_nullable())
                    << "posexplode fast path requires nullable value column";
            auto* val_nullable = assert_cast<ColumnNullable*>(value_col_ptr);
            val_nullable->get_nested_column_ptr()->insert_range_from(
                    *_block_fast_path_ctx.nested_col, segment_ctx.seg_nested_start,
                    segment_ctx.seg_nested_count);
            auto* val_nullmap =
                    assert_cast<ColumnUInt8*>(val_nullable->get_null_map_column_ptr().get());
            auto& val_nullmap_data = val_nullmap->get_data();
            const size_t old_size = val_nullmap_data.size();
            val_nullmap_data.resize(old_size + segment_ctx.seg_nested_count);
            if (_block_fast_path_ctx.nested_nullmap_data != nullptr) {
                memcpy(val_nullmap_data.data() + old_size,
                       _block_fast_path_ctx.nested_nullmap_data + segment_ctx.seg_nested_start,
                       segment_ctx.seg_nested_count * sizeof(UInt8));
            } else {
                memset(val_nullmap_data.data() + old_size, 0,
                       segment_ctx.seg_nested_count * sizeof(UInt8));
            }
            // Struct-level null map: these rows are not null
            if (outer_struct_nullmap_ptr) {
                outer_struct_nullmap_ptr->insert_many_defaults(segment_ctx.seg_nested_count);
            }
        } else if (out_col->is_nullable()) {
            auto* out_nullable = assert_cast<ColumnNullable*>(out_col.get());
            out_nullable->get_nested_column_ptr()->insert_range_from(
                    *_block_fast_path_ctx.nested_col, segment_ctx.seg_nested_start,
                    segment_ctx.seg_nested_count);
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(out_nullable->get_null_map_column_ptr().get());
            auto& nullmap_data = nullmap_column->get_data();
            const size_t old_size = nullmap_data.size();
            nullmap_data.resize(old_size + segment_ctx.seg_nested_count);
            if (_block_fast_path_ctx.nested_nullmap_data != nullptr) {
                memcpy(nullmap_data.data() + old_size,
                       _block_fast_path_ctx.nested_nullmap_data + segment_ctx.seg_nested_start,
                       segment_ctx.seg_nested_count * sizeof(UInt8));
            } else {
                memset(nullmap_data.data() + old_size, 0,
                       segment_ctx.seg_nested_count * sizeof(UInt8));
            }
        } else {
            out_col->insert_range_from(*_block_fast_path_ctx.nested_col,
                                       segment_ctx.seg_nested_start, segment_ctx.seg_nested_count);
        }
        reset_expand_segment_ctx();
    };

    // Emit one NULL output row for an outer-null/empty child row
    auto emit_outer_null = [&](int64_t cr) {
        for (auto index : p._output_slot_indexs) {
            auto src_column = _child_block->get_by_position(index).column;
            columns[index]->insert_from(*src_column, cr);
        }
        out_col->insert_default();
    };
    // Walk through child rows, accumulating contiguous segments into the output,
    // then when hitting a null/empty row or reaching the end,
    // flush the segment using bulk operations.
    // For outer-null rows, insert a NULL and copy the non-table-function columns directly.
    // This naturally handles both outer and non-outer modes since non-outer mode
    // just won't produce any null outputs.
    // For posexplode, generate position indices alongside this.
    while (produced_rows < remaining_capacity && child_row < child_rows) {
        const bool is_null_row = _block_fast_path_ctx.array_nullmap_data &&
                                 _block_fast_path_ctx.array_nullmap_data[child_row];

        const uint64_t prev_off = child_row == 0 ? 0 : offsets[child_row - 1];
        const uint64_t cur_off = is_null_row ? prev_off : offsets[child_row];
        const uint64_t nested_len = cur_off - prev_off;

        if (is_null_row || in_row_offset >= nested_len) {
            // for outer functions, emit null row for NULL or empty array rows
            if (is_outer && in_row_offset == 0 && (is_null_row || nested_len == 0)) {
                flush_segment();
                emit_outer_null(child_row);
                produced_rows++;
            }
            child_row++;
            in_row_offset = 0;
            continue;
        }

        const uint64_t remaining_in_row = nested_len - in_row_offset;
        const int take_count =
                std::min<int>(remaining_capacity - produced_rows, cast_set<int>(remaining_in_row));
        const uint64_t nested_start = prev_off + in_row_offset;

        DCHECK_LE(nested_start + take_count, cur_off);
        DCHECK_LE(nested_start + take_count, _block_fast_path_ctx.nested_col->size());

        if (segment_ctx.seg_nested_count == 0) {
            segment_ctx.seg_nested_start = nested_start;
        } else {
            // Nested data from an array column is always contiguous: offsets are monotonically
            // non-decreasing, so skipping NULL/empty rows doesn't create gaps.
            DCHECK_EQ(static_cast<uint64_t>(segment_ctx.seg_nested_start +
                                            segment_ctx.seg_nested_count),
                      nested_start)
                    << "nested data must be contiguous across child rows";
        }

        // Map each produced output row back to its source child row for copying non-table-function
        // columns via insert_indices_from().
        for (int j = 0; j < take_count; ++j) {
            segment_ctx.seg_row_ids.push_back(cast_set<uint32_t>(child_row));
            if (is_posexplode) {
                segment_ctx.seg_positions.push_back(cast_set<int32_t>(in_row_offset + j));
            }
        }

        segment_ctx.seg_nested_count += take_count;
        produced_rows += take_count;
        in_row_offset += take_count;
        if (in_row_offset >= nested_len) {
            child_row++;
            in_row_offset = 0;
        }
    }

    // Flush any remaining segment
    flush_segment();

    _block_fast_path_row = child_row;
    _block_fast_path_in_row_offset = in_row_offset;
    _cur_child_offset = child_row >= child_rows ? -1 : child_row;

    if (child_row >= child_rows) {
        for (TableFunction* fn : _fns) {
            fn->process_close();
        }
        _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                ._child->row_desc()
                                                .num_materialized_slots());
        _reset_block_fast_path_state();
    }

    return Status::OK();
}

Status TableFunctionLocalState::get_expanded_block(RuntimeState* state, Block* output_block,
                                                   bool* eos) {
    SCOPED_TIMER(_process_rows_timer);
    if (_need_to_handle_outer_conjuncts) {
        return _get_expanded_block_for_outer_conjuncts(state, output_block, eos);
    }

    auto& p = _parent->cast<TableFunctionOperatorX>();
    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, p._output_slots);
    MutableColumns& columns = m_block.mutable_columns();

    for (int i = 0; i < p._fn_num; i++) {
        if (columns[i + p._child_slots.size()]->is_nullable()) {
            _fns[i]->set_nullable();
        }
    }

    bool use_slow_path = true;
    if (_can_use_block_fast_path()) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_prepare_block_fast_path(state));
        if (_block_fast_path_enabled) {
            // Only use fast path when the remaining nested suffix stays contiguous for the rest of
            // the child block. This keeps fast-path progress entirely inside local state and avoids
            // re-synchronizing the table function cursor when a later batch would otherwise fall
            // back to the row-wise path.
            RETURN_IF_ERROR(_get_expanded_block_block_fast_path(state, columns));
            use_slow_path = false;
        }
    }

    if (use_slow_path) {
        bool skip_child_row = false;
        while (columns[p._child_slots.size()]->size() < state->batch_size()) {
            RETURN_IF_CANCELLED(state);

            if (_child_block->rows() == 0) {
                break;
            }
            int idx = _find_last_fn_eos_idx();
            if (idx == 0 || skip_child_row) {
                _copy_output_slots(columns, p);
                // all table functions' results are exhausted, process next child row.
                process_next_child_row();
                if (_cur_child_offset == -1) {
                    break;
                }
            } else if (idx < p._fn_num && idx != -1) {
                // some of table functions' results are exhausted.
                if (!_roll_table_functions(idx)) {
                    // continue to process next child row.
                    continue;
                }
            }

            // if any table function is not outer and has empty result, go to next child row
            if (skip_child_row = _is_inner_and_empty(); skip_child_row) {
                continue;
            }

            DCHECK_LE(1, p._fn_num);
            // It may take multiple iterations of this while loop to process a child row if
            // any table function produces a large number of rows.
            auto repeat_times = _fns[p._fn_num - 1]->get_value(
                    columns[p._child_slots.size() + p._fn_num - 1],
                    //// It has already been checked that
                    // columns[p._child_slots.size()]->size() < state->batch_size(),
                    // so columns[p._child_slots.size()]->size() will not exceed the range of int.
                    state->batch_size() - (int)columns[p._child_slots.size()]->size());
            _current_row_insert_times += repeat_times;
            for (int i = 0; i < p._fn_num - 1; i++) {
                _fns[i]->get_same_many_values(columns[i + p._child_slots.size()], repeat_times);
            }
        }
    }

    _copy_output_slots(columns, p);

    size_t row_size = columns[p._child_slots.size()]->size();
    for (auto index : p._useless_slot_indexs) {
        columns[index]->insert_many_defaults(row_size - columns[index]->size());
    }

    {
        SCOPED_TIMER(_filter_timer); // 3. eval conjuncts
        RETURN_IF_ERROR(VExprContext::filter_block(_expand_conjuncts_ctxs, output_block,
                                                   output_block->columns()));
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    }

    *eos = _child_eos && _cur_child_offset == -1;
    return Status::OK();
}

Status TableFunctionLocalState::_get_expanded_block_for_outer_conjuncts(RuntimeState* state,
                                                                        Block* output_block,
                                                                        bool* eos) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, p._output_slots);
    MutableColumns& columns = m_block.mutable_columns();
    auto child_slot_count = p._child_slots.size();
    for (int i = 0; i < p._fn_num; i++) {
        if (columns[i + child_slot_count]->is_nullable()) {
            _fns[i]->set_nullable();
        }
    }

    DorisVector<int64_t> child_row_to_output_rows_indices;
    DorisVector<int64_t> handled_row_indices;
    bool child_block_empty = _child_block->empty();
    if (!child_block_empty) {
        child_row_to_output_rows_indices.push_back(0);
    }

    auto batch_size = state->batch_size();
    auto output_row_count = columns[child_slot_count]->size();
    while (output_row_count < batch_size) {
        RETURN_IF_CANCELLED(state);

        // finished handling current child block
        if (_cur_child_offset == -1) {
            break;
        }

        bool skip_child_row = false;
        while (output_row_count < batch_size) {
            // if table function is not outer and has empty result, go to next child row
            if (_fns[0]->eos() || skip_child_row) {
                _copy_output_slots(columns, p);
                if (!skip_child_row) {
                    handled_row_indices.push_back(_cur_child_offset);
                    child_row_to_output_rows_indices.push_back(output_row_count);
                }
                process_next_child_row();
                if (_cur_child_offset == -1) {
                    break;
                }
            }
            if (skip_child_row = _is_inner_and_empty(); skip_child_row) {
                _child_rows_has_output[_cur_child_offset] = true;
                continue;
            }

            // It may take multiple iterations of this while loop to process a child row if
            // the table function produces a large number of rows.
            auto repeat_times = _fns[0]->get_value(columns[child_slot_count],
                                                   batch_size - (int)output_row_count);
            _current_row_insert_times += repeat_times;
            output_row_count = columns[child_slot_count]->size();
        }
    }
    // Two scenarios the loop above will exit:
    // 1. current child block is finished processing
    //    _cur_child_offset == -1
    // 2. output_block reaches batch size
    //    fn maybe or maybe not eos
    if (output_row_count >= batch_size) {
        _copy_output_slots(columns, p);
        handled_row_indices.push_back(_cur_child_offset);
        child_row_to_output_rows_indices.push_back(output_row_count);
        if (_fns[0]->eos()) {
            process_next_child_row();
        }
    }
    for (auto index : p._useless_slot_indexs) {
        columns[index]->insert_many_defaults(output_row_count - columns[index]->size());
    }
    output_block->set_columns(std::move(columns));

    /**
    Handle the outer conjuncts after unnest. Currently, only left outer is supported.
    e.g., for the following example data,
    select id, name, tags from items_dict_unnest_t order by id;
    +------+---------------------+-------------------------------------------------+
    | id   | name                | tags                                            |
    +------+---------------------+-------------------------------------------------+
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 |
    +------+---------------------+-------------------------------------------------+
    
    for this query: ``` SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;```
    
    after unnest, before evaluating the outer conjuncts, the result is:
    +------+---------------------+-------------------------------------------------+--------------+
    | id   | name                | tags                                            | unnest(tags) |
    +------+---------------------+-------------------------------------------------+--------------+
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Electronics  |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Office       |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | High-End     |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Laptop       |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  | Electronics  |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  | Accessories  |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           | Sports       |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           | Outdoor      |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         | Sports       |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         | Equipment    |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Clothing     |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Office       |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Shirt        |
    +------+---------------------+-------------------------------------------------+--------------+
    13 rows in set (0.47 sec)
    
    the vector child_row_to_output_rows_indices is used to record the mapping relationship,
    between child row and output rows, for example:
    child row 0 -> output rows [0, 4)
    child row 1 -> output rows [4, 6)
    child row 2 -> output rows [6, 8)
    child row 3 -> output rows [8, 10)
    child row 4 -> output rows [10, 13)
    it's contents are: [0, 4, 6, 8, 10, 13].
    
    After evaluating the left join conjuncts `t.tag = name`,
    the content of filter is: [0, 0, 0, 1, // child row 0
                               0, 0,       // child row 1
                               0, 0,       // child row 2
                               0, 0,       // child row 3
                               0, 0, 1     // child row 4
                               ]
    child rows 1, 2, 3 are all filtered out, so we need to insert one row with NULL tag value for each of them.
    */
    if (!child_block_empty) {
        IColumn::Filter filter;
        auto column_count = output_block->columns();
        ColumnNumbers columns_to_filter(column_count);
        std::iota(columns_to_filter.begin(), columns_to_filter.end(), 0);
        RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                _expand_conjuncts_ctxs, output_block, columns_to_filter, column_count, filter));
        size_t remain_row_count = output_block->rows();
        // for outer table function, need to handle those child rows which all expanded rows are filtered out
        auto handled_child_row_count = handled_row_indices.size();
        if (remain_row_count < output_row_count) {
            for (size_t i = 0; i < handled_child_row_count; ++i) {
                auto start_row_idx = child_row_to_output_rows_indices[i];
                auto end_row_idx = child_row_to_output_rows_indices[i + 1];
                if (simd::contain_one((uint8_t*)filter.data() + start_row_idx,
                                      end_row_idx - start_row_idx)) {
                    _child_rows_has_output[handled_row_indices[i]] = true;
                }
            }
        } else {
            for (auto row_idx : handled_row_indices) {
                _child_rows_has_output[row_idx] = true;
            }
        }

        if (-1 == _cur_child_offset) {
            // Finished handling current child block,
            auto child_block_row_count = _child_block->rows();
            DorisVector<uint32_t> null_row_indices;
            for (uint32_t i = 0; i != child_block_row_count; i++) {
                if (!_child_rows_has_output[i]) {
                    null_row_indices.push_back(i);
                }
            }
            if (!null_row_indices.empty()) {
                MutableBlock m_block2 = VectorizedUtils::build_mutable_mem_reuse_block(
                        output_block, p._output_slots);
                MutableColumns& columns2 = m_block2.mutable_columns();
                for (auto index : p._output_slot_indexs) {
                    auto src_column = _child_block->get_by_position(index).column;
                    columns2[index]->insert_indices_from(
                            *src_column, null_row_indices.data(),
                            null_row_indices.data() + null_row_indices.size());
                }
                for (auto index : p._useless_slot_indexs) {
                    columns2[index]->insert_many_defaults(null_row_indices.size());
                }
                columns2[child_slot_count]->insert_many_defaults(null_row_indices.size());
                output_block->set_columns(std::move(columns2));
            }
            _child_rows_has_output.clear();
            _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                    ._child->row_desc()
                                                    .num_materialized_slots());
        }
    }

    {
        SCOPED_TIMER(_filter_timer); // 3. eval conjuncts
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    }

    *eos = _child_eos && _cur_child_offset == -1;
    return Status::OK();
}

void TableFunctionLocalState::process_next_child_row() {
    _cur_child_offset++;

    if (_cur_child_offset >= _child_block->rows()) {
        // release block use count.
        for (TableFunction* fn : _fns) {
            fn->process_close();
        }

        // if there are any _expand_conjuncts_ctxs and it's outer, don't clear child block here,
        // because we still need _child_block to output NULL rows for outer table function
        if (!_need_to_handle_outer_conjuncts) {
            _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                    ._child->row_desc()
                                                    .num_materialized_slots());
        }
        _cur_child_offset = -1;
        _reset_block_fast_path_state();
        return;
    }

    for (TableFunction* fn : _fns) {
        fn->process_row(_cur_child_offset);
    }
}

TableFunctionOperatorX::TableFunctionOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {}

Status TableFunctionOperatorX::_prepare_output_slot_ids(const TPlanNode& tnode) {
    // Prepare output slot ids
    SlotId max_id = -1;
    for (auto slot_id : tnode.table_function_node.outputSlotIds) {
        if (slot_id > max_id) {
            max_id = slot_id;
        }
    }
    _output_slot_ids = std::vector<bool>(max_id + 1, false);
    for (auto slot_id : tnode.table_function_node.outputSlotIds) {
        _output_slot_ids[slot_id] = true;
    }

    return Status::OK();
}

Status TableFunctionOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));

    for (const TExpr& texpr : tnode.table_function_node.fnCallExprList) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(texpr, ctx));
        _vfn_ctxs.push_back(ctx);

        auto root = ctx->root();
        TableFunction* fn = nullptr;
        RETURN_IF_ERROR(
                TableFunctionFactory::get_fn(root->fn(), _pool, &fn, state->be_exec_version()));
        fn->set_expr_context(ctx);
        _fns.push_back(fn);
    }
    _fn_num = cast_set<int>(_fns.size());

    for (const TExpr& texpr : tnode.table_function_node.expand_conjuncts) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(texpr, ctx));
        _expand_conjuncts_ctxs.push_back(ctx);
    }
    if (!_expand_conjuncts_ctxs.empty()) {
        DCHECK(1 == _fn_num) << "Only support one table function when there are expand conjuncts.";
    }

    // Prepare output slot ids
    RETURN_IF_ERROR(_prepare_output_slot_ids(tnode));
    return Status::OK();
}

Status TableFunctionOperatorX::prepare(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->prepare());
    }
    RETURN_IF_ERROR(VExpr::prepare(_vfn_ctxs, state, row_descriptor()));

    RETURN_IF_ERROR(VExpr::prepare(_expand_conjuncts_ctxs, state, row_descriptor()));

    // get current all output slots
    for (const auto& tuple_desc : row_descriptor().tuple_descriptors()) {
        for (const auto& slot_desc : tuple_desc->slots()) {
            _output_slots.push_back(slot_desc);
        }
    }

    // get all input slots
    for (const auto& child_tuple_desc : _child->row_desc().tuple_descriptors()) {
        for (const auto& child_slot_desc : child_tuple_desc->slots()) {
            _child_slots.push_back(child_slot_desc);
        }
    }

    for (int i = 0; i < _child_slots.size(); i++) {
        if (_slot_need_copy(i)) {
            _output_slot_indexs.push_back(i);
        } else {
            _useless_slot_indexs.push_back(i);
        }
    }

    RETURN_IF_ERROR(VExpr::open(_expand_conjuncts_ctxs, state));
    return VExpr::open(_vfn_ctxs, state);
}

#include "common/compile_check_end.h"
} // namespace doris
