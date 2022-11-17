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

#include "vec/exec/join/vnested_loop_join_node.h"

#include <sstream>

#include "common/status.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vec/common/typeid_cast.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

VNestedLoopJoinNode::VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : VJoinNodeBase(pool, tnode, descs),
          _cur_probe_row_visited_flags(false),
          _matched_rows_done(false),
          _left_block_pos(0),
          _left_side_eos(false),
          _old_version_flag(!tnode.__isset.nested_loop_join_node) {}

Status VNestedLoopJoinNode::prepare(RuntimeState* state) {
    DCHECK(_join_op == TJoinOp::CROSS_JOIN || _join_op == TJoinOp::INNER_JOIN ||
           _join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
           _join_op == TJoinOp::FULL_OUTER_JOIN);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _build_rows_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _probe_rows_counter = ADD_COUNTER(runtime_profile(), "ProbeRows", TUnit::UNIT);
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");

    // pre-compute the tuple index of build tuples in the output row
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }

    _num_probe_side_columns = child(0)->row_desc().num_materialized_slots();
    _num_build_side_columns = child(1)->row_desc().num_materialized_slots();
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    _construct_mutable_join_block();
    return Status::OK();
}

Status VNestedLoopJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::close");
    return VJoinNodeBase::close(state);
}

Status VNestedLoopJoinNode::_materialize_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));

    bool eos = false;
    while (true) {
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);

        Block block;
        RETURN_IF_ERROR_AND_CHECK_SPAN(child(1)->get_next_after_projects(state, &block, &eos),
                                       child(1)->get_next_span(), eos);
        auto rows = block.rows();
        auto mem_usage = block.allocated_bytes();

        if (rows != 0) {
            _build_rows += rows;
            _total_mem_usage += mem_usage;
            _build_blocks.emplace_back(std::move(block));
            if (_match_all_build) {
                _build_side_visited_flags.emplace_back(ColumnUInt8::create(rows, 0));
            }
        }

        if (eos) {
            break;
        }
    }

    COUNTER_UPDATE(_build_rows_counter, _build_rows);
    return Status::OK();
}

Status VNestedLoopJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span,
                                 "VNestedLoopJoinNode::get_next");
    SCOPED_TIMER(_probe_timer);
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    if ((_match_all_build && _matched_rows_done &&
         _output_null_idx_build_side == _build_blocks.size()) ||
        _matched_rows_done) {
        *eos = true;
        return Status::OK();
    }

    _join_block.clear_column_data();
    MutableBlock mutable_join_block(&_join_block);
    auto& dst_columns = mutable_join_block.mutable_columns();

    std::stack<uint16_t> offset_stack;
    RETURN_IF_ERROR(std::visit(
            [&](auto&& join_op_variants, auto set_build_side_flag, auto set_probe_side_flag) {
                while (mutable_join_block.rows() < state->batch_size() && !_matched_rows_done) {
                    // If this left block is exhausted or empty, we need to pull data from left child.
                    if (_left_block_pos == _left_block.rows()) {
                        _left_block_pos = 0;

                        if (_left_side_eos) {
                            _matched_rows_done = true;
                        } else {
                            do {
                                release_block_memory(_left_block);
                                RETURN_IF_ERROR_AND_CHECK_SPAN(
                                        child(0)->get_next_after_projects(state, &_left_block,
                                                                          &_left_side_eos),
                                        child(0)->get_next_span(), _left_side_eos);
                            } while (_left_block.rows() == 0 && !_left_side_eos);
                            COUNTER_UPDATE(_probe_rows_counter, _left_block.rows());
                            if (_left_block.rows() == 0) {
                                _matched_rows_done = _left_side_eos;
                            }
                        }
                    }

                    // We should try to join rows if there still are some rows from probe side.
                    if (!_matched_rows_done && _current_build_pos < _build_blocks.size()) {
                        do {
                            const auto& now_process_build_block =
                                    _build_blocks[_current_build_pos++];
                            if constexpr (set_build_side_flag) {
                                offset_stack.push(mutable_join_block.rows());
                            }
                            _process_left_child_block(dst_columns, now_process_build_block);
                        } while (mutable_join_block.rows() < state->batch_size() &&
                                 _current_build_pos < _build_blocks.size());
                    }

                    if constexpr (set_probe_side_flag) {
                        Block tmp_block = mutable_join_block.to_block(0);
                        Status status = _do_filtering_and_update_visited_flags<set_build_side_flag,
                                                                               set_probe_side_flag>(
                                &tmp_block, offset_stack);
                        if (!status.OK()) {
                            return status;
                        }
                        mutable_join_block = MutableBlock(std::move(tmp_block));
                        // If this join operation is left outer join or full outer join, when
                        // `_current_build_pos == _build_blocks.size()`, means all rows from build
                        // side have been joined with the current probe row, we should output current
                        // probe row with null from build side.
                        if (_current_build_pos == _build_blocks.size()) {
                            if (!_matched_rows_done) {
                                _output_null_data<false>(dst_columns, state->batch_size());
                                _reset_with_next_probe_row(dst_columns);
                            }
                            break;
                        }
                    }

                    if (!_matched_rows_done && _current_build_pos == _build_blocks.size()) {
                        _reset_with_next_probe_row(dst_columns);
                    }
                }
                if constexpr (!set_probe_side_flag) {
                    Block tmp_block = mutable_join_block.to_block(0);
                    Status status = _do_filtering_and_update_visited_flags<set_build_side_flag,
                                                                           set_probe_side_flag>(
                            &tmp_block, offset_stack);
                    mutable_join_block = MutableBlock(std::move(tmp_block));
                    if (!status.OK()) {
                        return status;
                    }
                }

                if constexpr (set_build_side_flag) {
                    if (_matched_rows_done && _output_null_idx_build_side < _build_blocks.size()) {
                        auto& cols = mutable_join_block.mutable_columns();
                        _output_null_data<true>(cols, state->batch_size());
                    }
                }
                return Status::OK();
            },
            _join_op_variants, make_bool_variant(_match_all_build),
            make_bool_variant(_match_all_probe)));
    *eos = _match_all_build
                   ? _output_null_idx_build_side == _build_blocks.size() && _matched_rows_done
                   : _matched_rows_done;

    Block tmp_block = mutable_join_block.to_block(0);
    RETURN_IF_ERROR(_build_output_block(&tmp_block, block));
    reached_limit(block, eos);
    return Status::OK();
}

void VNestedLoopJoinNode::_process_left_child_block(MutableColumns& dst_columns,
                                                    const Block& now_process_build_block) const {
    const int max_added_rows = now_process_build_block.rows();
    for (size_t i = 0; i < _num_probe_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = _left_block.get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_many_from(*src_column.column, _left_block_pos, max_added_rows);
            assert_cast<ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[i]->insert_many_from(*src_column.column, _left_block_pos, max_added_rows);
        }
    }
    for (size_t i = 0; i < _num_build_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = now_process_build_block.get_by_position(i);
        if (!src_column.column->is_nullable() &&
            dst_columns[_num_probe_side_columns + i]->is_nullable()) {
            auto origin_sz = dst_columns[_num_probe_side_columns + i]->size();
            DCHECK(_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column.get(), 0, max_added_rows);
            assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + max_added_rows, 0);
        } else {
            dst_columns[_num_probe_side_columns + i]->insert_range_from(*src_column.column.get(), 0,
                                                                        max_added_rows);
        }
    }
}

template <bool BuildSide>
void VNestedLoopJoinNode::_output_null_data(MutableColumns& dst_columns, size_t batch_size) {
    if constexpr (BuildSide) {
        auto build_block_sz = _build_blocks.size();
        size_t i = _output_null_idx_build_side;
        for (; i < build_block_sz; i++) {
            const auto& cur_block = _build_blocks[i];
            const auto* __restrict cur_visited_flags =
                    assert_cast<ColumnUInt8*>(_build_side_visited_flags[i].get())
                            ->get_data()
                            .data();
            const auto num_rows = cur_block.rows();

            std::vector<int> selector(num_rows);
            size_t selector_idx = 0;
            for (size_t j = 0; j < num_rows; j++) {
                if (!cur_visited_flags[j]) {
                    selector[selector_idx++] = j;
                }
            }
            for (size_t j = 0; j < _num_probe_side_columns; ++j) {
                DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                       _join_op == TJoinOp::FULL_OUTER_JOIN);
                dst_columns[j]->insert_many_defaults(selector_idx);
            }

            for (size_t j = 0; j < _num_build_side_columns; ++j) {
                auto src_column = cur_block.get_by_position(j);
                if (!src_column.column->is_nullable() &&
                    dst_columns[_num_probe_side_columns + j]->is_nullable()) {
                    auto origin_sz = dst_columns[_num_probe_side_columns + j]->size();
                    DCHECK(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN);
                    assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + j].get())
                            ->get_nested_column_ptr()
                            ->insert_indices_from(*src_column.column, selector.data(),
                                                  selector.data() + selector_idx);
                    assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + j].get())
                            ->get_null_map_column()
                            .get_data()
                            .resize_fill(origin_sz + selector_idx, 0);
                } else {
                    dst_columns[_num_probe_side_columns + j]->insert_indices_from(
                            *src_column.column.get(), selector.data(),
                            selector.data() + selector_idx);
                }
            }
            if (dst_columns[0]->size() > batch_size) {
                i++;
                break;
            }
        }
        _output_null_idx_build_side = i;
    } else {
        if (_cur_probe_row_visited_flags) {
            return;
        }

        DCHECK_LT(_left_block_pos, _left_block.rows());
        for (size_t i = 0; i < _num_probe_side_columns; ++i) {
            const ColumnWithTypeAndName src_column = _left_block.get_by_position(i);
            if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
                auto origin_sz = dst_columns[i]->size();
                DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                       _join_op == TJoinOp::FULL_OUTER_JOIN);
                assert_cast<ColumnNullable*>(dst_columns[i].get())
                        ->get_nested_column_ptr()
                        ->insert_many_from(*src_column.column, _left_block_pos, 1);
                assert_cast<ColumnNullable*>(dst_columns[i].get())
                        ->get_null_map_column()
                        .get_data()
                        .resize_fill(origin_sz + 1, 0);
            } else {
                dst_columns[i]->insert_many_from(*src_column.column, _left_block_pos, 1);
            }
        }
        for (size_t i = 0; i < _num_build_side_columns; ++i) {
            dst_columns[_num_probe_side_columns + i]->insert_default();
        }
    }
}

void VNestedLoopJoinNode::_reset_with_next_probe_row(MutableColumns& dst_columns) {
    _cur_probe_row_visited_flags = false;
    _current_build_pos = 0;
    _left_block_pos++;
}

template <bool SetBuildSideFlag, bool SetProbeSideFlag>
Status VNestedLoopJoinNode::_do_filtering_and_update_visited_flags(
        Block* block, std::stack<uint16_t>& offset_stack) {
    auto column_to_keep = block->columns();
    // If we need to set visited flags for build side,
    // 1. Execute conjuncts and get a column with bool type to do filtering.
    // 2. Use bool column to update build-side visited flags.
    // 3. Use bool column to do filtering.
    size_t build_block_idx =
            _current_build_pos == 0 ? _build_blocks.size() - 1 : _current_build_pos - 1;
    size_t processed_blocks_num = offset_stack.size();
    if (LIKELY(_vconjunct_ctx_ptr != nullptr && block->rows() > 0)) {
        DCHECK((*_vconjunct_ctx_ptr) != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR((*_vconjunct_ctx_ptr)->execute(block, &result_column_id));
        ColumnPtr filter_column = block->get_by_position(result_column_id).column;
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
            ColumnPtr nested_column = nullable_column->get_nested_column_ptr();

            MutableColumnPtr mutable_holder =
                    nested_column->use_count() == 1
                            ? nested_column->assume_mutable()
                            : nested_column->clone_resized(nested_column->size());

            ColumnUInt8* concrete_column = assert_cast<ColumnUInt8*>(mutable_holder.get());
            auto* __restrict null_map = nullable_column->get_null_map_data().data();
            IColumn::Filter& filter = concrete_column->get_data();
            auto* __restrict filter_data = filter.data();

            const size_t size = filter.size();
            for (size_t i = 0; i < size; ++i) {
                filter_data[i] &= !null_map[i];
            }
            if constexpr (SetBuildSideFlag) {
                for (size_t i = 0; i < processed_blocks_num; i++) {
                    auto& build_side_flag =
                            assert_cast<ColumnUInt8*>(
                                    _build_side_visited_flags[build_block_idx].get())
                                    ->get_data();
                    auto* __restrict build_side_flag_data = build_side_flag.data();
                    auto cur_sz = build_side_flag.size();
                    const size_t offset = offset_stack.top();
                    offset_stack.pop();
                    for (size_t j = 0; j < cur_sz; j++) {
                        build_side_flag_data[j] |= filter_data[offset + j];
                    }
                    build_block_idx =
                            build_block_idx == 0 ? _build_blocks.size() - 1 : build_block_idx - 1;
                }
            }
            if constexpr (SetProbeSideFlag) {
                _cur_probe_row_visited_flags |= simd::contain_byte<uint8>(filter_data, size, 1);
            }
            Block::filter_block_internal(block, filter, column_to_keep);
        } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
            bool ret = const_column->get_bool(0);
            if (!ret) {
                for (size_t i = 0; i < column_to_keep; ++i) {
                    std::move(*block->get_by_position(i).column).assume_mutable()->clear();
                }
            } else {
                if constexpr (SetBuildSideFlag) {
                    for (size_t i = 0; i < processed_blocks_num; i++) {
                        auto& build_side_flag =
                                assert_cast<ColumnUInt8*>(
                                        _build_side_visited_flags[build_block_idx].get())
                                        ->get_data();
                        auto* __restrict build_side_flag_data = build_side_flag.data();
                        auto cur_sz = build_side_flag.size();
                        offset_stack.pop();
                        memset(reinterpret_cast<void*>(build_side_flag_data), 1, cur_sz);
                        build_block_idx = build_block_idx == 0 ? _build_blocks.size() - 1
                                                               : build_block_idx - 1;
                    }
                }
                if constexpr (SetProbeSideFlag) {
                    _cur_probe_row_visited_flags |= ret;
                }
            }
        } else {
            const IColumn::Filter& filter =
                    assert_cast<const doris::vectorized::ColumnVector<UInt8>&>(*filter_column)
                            .get_data();
            if constexpr (SetBuildSideFlag) {
                for (size_t i = 0; i < processed_blocks_num; i++) {
                    auto& build_side_flag =
                            assert_cast<ColumnUInt8*>(
                                    _build_side_visited_flags[build_block_idx].get())
                                    ->get_data();
                    auto* __restrict build_side_flag_data = build_side_flag.data();
                    auto cur_sz = build_side_flag.size();
                    const size_t offset = offset_stack.top();
                    offset_stack.pop();
                    for (size_t j = 0; j < cur_sz; j++) {
                        build_side_flag_data[j] |= filter[offset + j];
                    }
                    build_block_idx =
                            build_block_idx == 0 ? _build_blocks.size() - 1 : build_block_idx - 1;
                }
            }
            if constexpr (SetProbeSideFlag) {
                _cur_probe_row_visited_flags |=
                        simd::contain_byte<uint8>(filter.data(), filter.size(), 1);
            }
            Block::filter_block_internal(block, filter, column_to_keep);
        }
        Block::erase_useless_column(block, column_to_keep);
    }
    return Status::OK();
}

Status VNestedLoopJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::open")
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    // We can close the right child to release its resources because its input has been
    // fully consumed.
    child(1)->close(state);
    return Status::OK();
}

void VNestedLoopJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "VNestedLoopJoinNode";
    *out << "(eos=" << (_matched_rows_done ? "true" : "false")
         << " left_block_pos=" << _left_block_pos;
    VJoinNodeBase::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris::vectorized
