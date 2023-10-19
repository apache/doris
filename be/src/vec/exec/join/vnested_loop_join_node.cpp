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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <string.h>

#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "pipeline/exec/nested_loop_join_build_operator.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/exec_node.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_slots_cross.h"
#include "gutil/integral_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

template <typename Parent>
Status RuntimeFilterBuild<Parent>::operator()(RuntimeState* state) {
    if (_parent->runtime_filter_descs().empty()) {
        return Status::OK();
    }
    VRuntimeFilterSlotsCross runtime_filter_slots(_parent->runtime_filter_descs(),
                                                  _parent->filter_src_expr_ctxs());

    RETURN_IF_ERROR(runtime_filter_slots.init(state));

    if (!runtime_filter_slots.empty() && !_parent->build_blocks().empty()) {
        SCOPED_TIMER(_parent->push_compute_timer());
        for (auto& build_block : _parent->build_blocks()) {
            static_cast<void>(runtime_filter_slots.insert(&build_block));
        }
    }
    {
        SCOPED_TIMER(_parent->push_down_timer());
        RETURN_IF_ERROR(runtime_filter_slots.publish());
    }

    return Status::OK();
}

template struct RuntimeFilterBuild<doris::pipeline::NestedLoopJoinBuildSinkLocalState>;
template struct RuntimeFilterBuild<VNestedLoopJoinNode>;

VNestedLoopJoinNode::VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : VJoinNodeBase(pool, tnode, descs),
          _matched_rows_done(false),
          _left_block_pos(0),
          _left_side_eos(false),
          _old_version_flag(!tnode.__isset.nested_loop_join_node),
          _runtime_filter_descs(tnode.runtime_filters) {}

Status VNestedLoopJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::init(tnode, state));

    if (tnode.nested_loop_join_node.__isset.is_output_left_side_only) {
        _is_output_left_side_only = tnode.nested_loop_join_node.is_output_left_side_only;
    }

    if (tnode.nested_loop_join_node.__isset.join_conjuncts &&
        !tnode.nested_loop_join_node.join_conjuncts.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.nested_loop_join_node.join_conjuncts,
                                                 _join_conjuncts));
    } else if (tnode.nested_loop_join_node.__isset.vjoin_conjunct) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(
                VExpr::create_expr_tree(tnode.nested_loop_join_node.vjoin_conjunct, context));
        _join_conjuncts.emplace_back(context);
    }

    std::vector<TExpr> filter_src_exprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        filter_src_exprs.push_back(_runtime_filter_descs[i].src_expr);
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_producer_filter(
                _runtime_filter_descs[i], state->query_options()));
    }
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(filter_src_exprs, _filter_src_expr_ctxs));
    return Status::OK();
}

Status VNestedLoopJoinNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::prepare(state));
    SCOPED_TIMER(_exec_timer);
    _build_get_next_timer = ADD_TIMER(_build_phase_profile, "BuildGetNextTime");
    _build_timer = ADD_TIMER(_build_phase_profile, "BuildTime");
    _build_rows_counter = ADD_COUNTER(_build_phase_profile, "BuildRows", TUnit::UNIT);
    // pre-compute the tuple index of build tuples in the output row
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }

    for (auto& conjunct : _join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
    }
    _num_probe_side_columns = child(0)->row_desc().num_materialized_slots();
    _num_build_side_columns = child(1)->row_desc().num_materialized_slots();
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    RETURN_IF_ERROR(VExpr::prepare(_filter_src_expr_ctxs, state, child(1)->row_desc()));
    _loop_join_timer = ADD_CHILD_TIMER(_probe_phase_profile, "LoopGenerateJoin", "ProbeTime");
    _construct_mutable_join_block();
    return Status::OK();
}

Status VNestedLoopJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    _release_mem();

    return VJoinNodeBase::close(state);
}

// TODO: This method should be implemented by the parent class
Status VNestedLoopJoinNode::_materialize_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    {
        SCOPED_TIMER(_build_get_next_timer);
        RETURN_IF_ERROR(child(1)->open(state));
    }

    bool eos = false;
    Block block;
    while (true) {
        RETURN_IF_CANCELLED(state);
        {
            SCOPED_TIMER(_build_get_next_timer);
            RETURN_IF_ERROR(child(1)->get_next_after_projects(
                    state, &block, &eos,
                    std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                      ExecNode::get_next,
                              _children[1], std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3)));
        }

        static_cast<void>(sink(state, &block, eos));

        if (eos) {
            break;
        }
    }

    return Status::OK();
}

Status VNestedLoopJoinNode::sink(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_build_timer);
    auto rows = block->rows();
    auto mem_usage = block->allocated_bytes();

    if (rows != 0) {
        _build_rows += rows;
        _total_mem_usage += mem_usage;
        _build_blocks.emplace_back(std::move(*block));
        if (_match_all_build || _is_right_semi_anti) {
            _build_side_visited_flags.emplace_back(ColumnUInt8::create(rows, 0));
        }
    }

    if (eos) {
        COUNTER_UPDATE(_build_rows_counter, _build_rows);
        static_cast<void>(RuntimeFilterBuild<VNestedLoopJoinNode>(this)(state));

        // optimize `in bitmap`, see https://github.com/apache/doris/issues/14338
        if (_is_output_left_side_only &&
            ((_join_op == TJoinOp::type::LEFT_SEMI_JOIN && _build_blocks.empty()) ||
             (_join_op == TJoinOp::type::LEFT_ANTI_JOIN && !_build_blocks.empty()))) {
            _left_side_eos = true;
        }
    }

    return Status::OK();
}

Status VNestedLoopJoinNode::push(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    SCOPED_TIMER(_exec_timer);
    COUNTER_UPDATE(_probe_rows_counter, block->rows());
    _cur_probe_row_visited_flags.resize(block->rows());
    std::fill(_cur_probe_row_visited_flags.begin(), _cur_probe_row_visited_flags.end(), 0);
    _left_block_pos = 0;
    _need_more_input_data = false;
    _left_side_eos = eos;

    if (!_is_output_left_side_only) {
        auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                        auto set_probe_side_flag) {
            return _generate_join_block_data<std::decay_t<decltype(join_op_variants)>,
                                             set_build_side_flag, set_probe_side_flag>(
                    state, join_op_variants);
        };
        RETURN_IF_ERROR(std::visit(func, _join_op_variants,
                                   make_bool_variant(_match_all_build || _is_right_semi_anti),
                                   make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    }
    return Status::OK();
}

Status VNestedLoopJoinNode::_fresh_left_block(doris::RuntimeState* state) {
    do {
        release_block_memory(_left_block);
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, &_left_block, &_left_side_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));

    } while (_left_block.rows() == 0 && !_left_side_eos);

    return Status::OK();
}

Status VNestedLoopJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    while (need_more_input_data()) {
        RETURN_IF_ERROR(_fresh_left_block(state));
        static_cast<void>(push(state, &_left_block, _left_side_eos));
    }

    return pull(state, block, eos);
}

void VNestedLoopJoinNode::_append_left_data_with_null(MutableBlock& mutable_block) const {
    auto& dst_columns = mutable_block.mutable_columns();
    DCHECK(_is_mark_join);
    for (size_t i = 0; i < _num_probe_side_columns; ++i) {
        const ColumnWithTypeAndName& src_column = _left_block.get_by_position(i);
        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
            auto origin_sz = dst_columns[i]->size();
            DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN);
            assert_cast<ColumnNullable*>(dst_columns[i].get())
                    ->get_nested_column_ptr()
                    ->insert_range_from(*src_column.column, _left_block_start_pos,
                                        _left_side_process_count);
            assert_cast<ColumnNullable*>(dst_columns[i].get())
                    ->get_null_map_column()
                    .get_data()
                    .resize_fill(origin_sz + 1, 0);
        } else {
            dst_columns[i]->insert_range_from(*src_column.column, _left_block_start_pos,
                                              _left_side_process_count);
        }
    }
    for (size_t i = 0; i < _num_build_side_columns; ++i) {
        dst_columns[_num_probe_side_columns + i]->insert_many_defaults(_left_side_process_count);
    }

    auto& mark_column = *dst_columns[dst_columns.size() - 1];
    ColumnFilterHelper(mark_column).resize_fill(mark_column.size() + _left_side_process_count, 0);
}

void VNestedLoopJoinNode::_process_left_child_block(MutableBlock& mutable_block,
                                                    const Block& now_process_build_block) const {
    auto& dst_columns = mutable_block.mutable_columns();
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

void VNestedLoopJoinNode::_update_additional_flags(Block* block) {
    if (_is_outer_join) {
        auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
        auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
        auto& left_null_map = reinterpret_cast<ColumnUInt8&>(*p0);
        auto& right_null_map = reinterpret_cast<ColumnUInt8&>(*p1);
        auto left_size = left_null_map.size();
        auto right_size = right_null_map.size();

        if (left_size < block->rows()) {
            left_null_map.get_data().resize_fill(block->rows(), 0);
        }
        if (right_size < block->rows()) {
            right_null_map.get_data().resize_fill(block->rows(), 0);
        }
    }
    if (_is_mark_join) {
        auto mark_column = block->get_by_position(block->columns() - 1).column->assume_mutable();
        if (mark_column->size() < block->rows()) {
            ColumnFilterHelper(*mark_column).resize_fill(block->rows(), 1);
        }
    }
}

void VNestedLoopJoinNode::_resize_fill_tuple_is_null_column(size_t new_size, int left_flag,
                                                            int right_flag) {
    if (_is_outer_join) {
        reinterpret_cast<ColumnUInt8*>(_tuple_is_null_left_flag_column.get())
                ->get_data()
                .resize_fill(new_size, left_flag);
        reinterpret_cast<ColumnUInt8*>(_tuple_is_null_right_flag_column.get())
                ->get_data()
                .resize_fill(new_size, right_flag);
    }
}

void VNestedLoopJoinNode::_add_tuple_is_null_column(Block* block) {
    if (_is_outer_join) {
        auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
        auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
        block->insert({std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(),
                       "left_tuples_is_null"});
        block->insert({std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(),
                       "right_tuples_is_null"});
    }
}

template <bool BuildSide, bool IsSemi>
void VNestedLoopJoinNode::_finalize_current_phase(MutableBlock& mutable_block, size_t batch_size) {
    auto& dst_columns = mutable_block.mutable_columns();
    DCHECK_GT(dst_columns.size(), 0);
    auto column_size = dst_columns[0]->size();
    if constexpr (BuildSide) {
        DCHECK(!_is_mark_join);
        auto build_block_sz = _build_blocks.size();
        size_t i = _output_null_idx_build_side;
        for (; i < build_block_sz && column_size < batch_size; i++) {
            const auto& cur_block = _build_blocks[i];
            const auto* __restrict cur_visited_flags =
                    assert_cast<ColumnUInt8*>(_build_side_visited_flags[i].get())
                            ->get_data()
                            .data();
            const auto num_rows = cur_block.rows();

            std::vector<int> selector(num_rows);
            size_t selector_idx = 0;
            for (size_t j = 0; j < num_rows; j++) {
                if constexpr (IsSemi) {
                    if (cur_visited_flags[j]) {
                        selector[selector_idx++] = j;
                    }
                } else {
                    if (!cur_visited_flags[j]) {
                        selector[selector_idx++] = j;
                    }
                }
            }

            column_size += selector_idx;
            for (size_t j = 0; j < _num_probe_side_columns; ++j) {
                DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                       _join_op == TJoinOp::FULL_OUTER_JOIN ||
                       _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                       _join_op == TJoinOp::RIGHT_SEMI_JOIN);
                dst_columns[j]->insert_many_defaults(selector_idx);
            }
            for (size_t j = 0; j < _num_build_side_columns; ++j) {
                auto src_column = cur_block.get_by_position(j);
                if (!src_column.column->is_nullable() &&
                    dst_columns[_num_probe_side_columns + j]->is_nullable()) {
                    DCHECK(_join_op == TJoinOp::FULL_OUTER_JOIN);
                    assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + j].get())
                            ->get_nested_column_ptr()
                            ->insert_indices_from(*src_column.column, selector.data(),
                                                  selector.data() + selector_idx);
                    assert_cast<ColumnNullable*>(dst_columns[_num_probe_side_columns + j].get())
                            ->get_null_map_column()
                            .get_data()
                            .resize_fill(column_size, 0);
                } else {
                    dst_columns[_num_probe_side_columns + j]->insert_indices_from(
                            *src_column.column.get(), selector.data(),
                            selector.data() + selector_idx);
                }
            }
            _resize_fill_tuple_is_null_column(column_size, 1, 0);
        }
        _output_null_idx_build_side = i;
    } else {
        if (!_is_mark_join) {
            auto new_size = column_size;
            DCHECK_LE(_left_block_start_pos + _left_side_process_count, _left_block.rows());
            for (int j = _left_block_start_pos;
                 j < _left_block_start_pos + _left_side_process_count; ++j) {
                if (_cur_probe_row_visited_flags[j] == IsSemi) {
                    new_size++;
                    for (size_t i = 0; i < _num_probe_side_columns; ++i) {
                        const ColumnWithTypeAndName src_column = _left_block.get_by_position(i);
                        if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
                            DCHECK(_join_op == TJoinOp::FULL_OUTER_JOIN);
                            assert_cast<ColumnNullable*>(dst_columns[i].get())
                                    ->get_nested_column_ptr()
                                    ->insert_many_from(*src_column.column, j, 1);
                            assert_cast<ColumnNullable*>(dst_columns[i].get())
                                    ->get_null_map_column()
                                    .get_data()
                                    .resize_fill(new_size, 0);
                        } else {
                            dst_columns[i]->insert_many_from(*src_column.column, j, 1);
                        }
                    }
                }
            }
            if (new_size > column_size) {
                for (size_t i = 0; i < _num_build_side_columns; ++i) {
                    dst_columns[_num_probe_side_columns + i]->insert_many_defaults(new_size -
                                                                                   column_size);
                }
                _resize_fill_tuple_is_null_column(new_size, 0, 1);
            }
        } else {
            ColumnFilterHelper mark_column(*dst_columns[dst_columns.size() - 1]);
            mark_column.reserve(mark_column.size() + _left_side_process_count);
            DCHECK_LE(_left_block_start_pos + _left_side_process_count, _left_block.rows());
            for (int j = _left_block_start_pos;
                 j < _left_block_start_pos + _left_side_process_count; ++j) {
                mark_column.insert_value(IsSemi == _cur_probe_row_visited_flags[j]);
            }
            for (size_t i = 0; i < _num_probe_side_columns; ++i) {
                const ColumnWithTypeAndName src_column = _left_block.get_by_position(i);
                DCHECK(_join_op != TJoinOp::FULL_OUTER_JOIN);
                dst_columns[i]->insert_range_from(*src_column.column, _left_block_start_pos,
                                                  _left_side_process_count);
            }
            for (size_t i = 0; i < _num_build_side_columns; ++i) {
                dst_columns[_num_probe_side_columns + i]->insert_many_defaults(
                        _left_side_process_count);
            }
            _resize_fill_tuple_is_null_column(_left_side_process_count, 0, 1);
        }
    }
}

void VNestedLoopJoinNode::_reset_with_next_probe_row() {
    // TODO: need a vector of left block to register the _probe_row_visited_flags
    _current_build_pos = 0;
    _left_block_pos++;
}

#define CLEAR_BLOCK                                                  \
    for (size_t i = 0; i < column_to_keep; ++i) {                    \
        block->get_by_position(i).column->assume_mutable()->clear(); \
    }

// need exception safety
template <typename Filter, bool SetBuildSideFlag, bool SetProbeSideFlag>
void VNestedLoopJoinNode::_do_filtering_and_update_visited_flags_impl(
        Block* block, int column_to_keep, int build_block_idx, int processed_blocks_num,
        bool materialize, Filter& filter) {
    if constexpr (SetBuildSideFlag) {
        for (size_t i = 0; i < processed_blocks_num; i++) {
            auto& build_side_flag =
                    assert_cast<ColumnUInt8*>(_build_side_visited_flags[build_block_idx].get())
                            ->get_data();
            auto* __restrict build_side_flag_data = build_side_flag.data();
            auto cur_sz = build_side_flag.size();
            const size_t offset = _build_offset_stack.top();
            _build_offset_stack.pop();
            for (size_t j = 0; j < cur_sz; j++) {
                build_side_flag_data[j] |= filter[offset + j];
            }
            build_block_idx = build_block_idx == 0 ? _build_blocks.size() - 1 : build_block_idx - 1;
        }
    }
    if constexpr (SetProbeSideFlag) {
        int end = filter.size();
        for (int i = _left_block_pos == _left_block.rows() ? _left_block_pos - 1 : _left_block_pos;
             i >= _left_block_start_pos; i--) {
            int offset = 0;
            if (!_probe_offset_stack.empty()) {
                offset = _probe_offset_stack.top();
                _probe_offset_stack.pop();
            }
            if (!_cur_probe_row_visited_flags[i]) {
                _cur_probe_row_visited_flags[i] =
                        simd::contain_byte<uint8>(filter.data() + offset, end - offset, 1) ? 1 : 0;
            }
            end = offset;
        }
    }
    if (materialize) {
        Block::filter_block_internal(block, filter, column_to_keep);
    } else {
        CLEAR_BLOCK
    }
}

// need exception safety
template <bool SetBuildSideFlag, bool SetProbeSideFlag, bool IgnoreNull>
Status VNestedLoopJoinNode::_do_filtering_and_update_visited_flags(Block* block, bool materialize) {
    auto column_to_keep = block->columns();
    // If we need to set visited flags for build side,
    // 1. Execute conjuncts and get a column with bool type to do filtering.
    // 2. Use bool column to update build-side visited flags.
    // 3. Use bool column to do filtering.
    size_t build_block_idx =
            _current_build_pos == 0 ? _build_blocks.size() - 1 : _current_build_pos - 1;
    size_t processed_blocks_num = _build_offset_stack.size();
    if (LIKELY(!_join_conjuncts.empty() && block->rows() > 0)) {
        IColumn::Filter filter(block->rows(), 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(_join_conjuncts, nullptr, IgnoreNull, block,
                                                        &filter, &can_filter_all));

        if (can_filter_all) {
            CLEAR_BLOCK
            std::stack<uint16_t> empty1;
            _probe_offset_stack.swap(empty1);

            std::stack<uint16_t> empty2;
            _build_offset_stack.swap(empty2);
        } else {
            _do_filtering_and_update_visited_flags_impl<decltype(filter), SetBuildSideFlag,
                                                        SetProbeSideFlag>(
                    block, column_to_keep, build_block_idx, processed_blocks_num, materialize,
                    filter);
        }
    } else if (block->rows() > 0) {
        if constexpr (SetBuildSideFlag) {
            for (size_t i = 0; i < processed_blocks_num; i++) {
                auto& build_side_flag =
                        assert_cast<ColumnUInt8*>(_build_side_visited_flags[build_block_idx].get())
                                ->get_data();
                auto* __restrict build_side_flag_data = build_side_flag.data();
                auto cur_sz = build_side_flag.size();
                _build_offset_stack.pop();
                memset(reinterpret_cast<void*>(build_side_flag_data), 1, cur_sz);
                build_block_idx =
                        build_block_idx == 0 ? _build_blocks.size() - 1 : build_block_idx - 1;
            }
        }
        if constexpr (SetProbeSideFlag) {
            std::stack<uint16_t> empty;
            _probe_offset_stack.swap(empty);
            std::fill(_cur_probe_row_visited_flags.begin(), _cur_probe_row_visited_flags.end(), 1);
        }
        if (!materialize) {
            CLEAR_BLOCK
        }
    }
    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status VNestedLoopJoinNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::alloc_resource(state));
    for (auto& conjunct : _join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    return VExpr::open(_filter_src_expr_ctxs, state);
}

Status VNestedLoopJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::open(state));
    RETURN_IF_CANCELLED(state);
    // We can close the right child to release its resources because its input has been
    // fully consumed.
    static_cast<void>(child(1)->close(state));
    return Status::OK();
}

void VNestedLoopJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "VNestedLoopJoinNode";
    *out << "(need_more_input_data=" << (need_more_input_data() ? "true" : "false")
         << " left_block_pos=" << _left_block_pos;
    *out << ")\n children=";
    VJoinNodeBase::debug_string(indentation_level, out);
    *out << ")";
}

void VNestedLoopJoinNode::_release_mem() {
    _left_block.clear();

    Blocks tmp_build_blocks;
    _build_blocks.swap(tmp_build_blocks);

    MutableColumns tmp_build_side_visited_flags;
    _build_side_visited_flags.swap(tmp_build_side_visited_flags);

    _tuple_is_null_left_flag_column = nullptr;
    _tuple_is_null_right_flag_column = nullptr;
}

Status VNestedLoopJoinNode::pull(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_probe_timer);
    if (_is_output_left_side_only) {
        RETURN_IF_ERROR(_build_output_block(&_left_block, block));
        *eos = _left_side_eos;
        _need_more_input_data = !_left_side_eos;
    } else {
        *eos = (_match_all_build || _is_right_semi_anti)
                       ? _output_null_idx_build_side == _build_blocks.size() && _matched_rows_done
                       : _matched_rows_done;

        {
            Block tmp_block = _join_block;

            // Here make _join_block release the columns' ptr
            _join_block.set_columns(_join_block.clone_empty_columns());

            _add_tuple_is_null_column(&tmp_block);
            {
                SCOPED_TIMER(_join_filter_timer);
                RETURN_IF_ERROR(
                        VExprContext::filter_block(_conjuncts, &tmp_block, tmp_block.columns()));
            }
            RETURN_IF_ERROR(_build_output_block(&tmp_block, block, false));
            _reset_tuple_is_null_column();
        }
        _join_block.clear_column_data();

        if (!(*eos) and !_need_more_input_data) {
            auto func = [&](auto&& join_op_variants, auto set_build_side_flag,
                            auto set_probe_side_flag) {
                return _generate_join_block_data<std::decay_t<decltype(join_op_variants)>,
                                                 set_build_side_flag, set_probe_side_flag>(
                        state, join_op_variants);
            };
            SCOPED_TIMER(_loop_join_timer);
            RETURN_IF_ERROR(std::visit(func, _join_op_variants,
                                       make_bool_variant(_match_all_build || _is_right_semi_anti),
                                       make_bool_variant(_match_all_probe || _is_left_semi_anti)));
        }
    }

    reached_limit(block, eos);
    return Status::OK();
}

bool VNestedLoopJoinNode::need_more_input_data() const {
    return _need_more_input_data and !_left_side_eos and _join_block.rows() == 0;
}

void VNestedLoopJoinNode::release_resource(doris::RuntimeState* state) {
    VJoinNodeBase::release_resource(state);
}

} // namespace doris::vectorized
