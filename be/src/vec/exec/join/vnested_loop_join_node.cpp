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

#include <glog/logging.h>

#include <sstream>

#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/runtime_filter_slots_cross.h"
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

struct RuntimeFilterBuild {
    RuntimeFilterBuild(VNestedLoopJoinNode* join_node) : _join_node(join_node) {}

    Status operator()(RuntimeState* state) {
        if (_join_node->_runtime_filter_descs.empty()) {
            return Status::OK();
        }
        VRuntimeFilterSlotsCross runtime_filter_slots(_join_node->_runtime_filter_descs,
                                                      _join_node->_filter_src_expr_ctxs);

        RETURN_IF_ERROR(runtime_filter_slots.init(state));

        if (!runtime_filter_slots.empty() && !_join_node->_build_blocks.empty()) {
            SCOPED_TIMER(_join_node->_push_compute_timer);
            for (auto& build_block : _join_node->_build_blocks) {
                runtime_filter_slots.insert(&build_block);
            }
        }
        {
            SCOPED_TIMER(_join_node->_push_down_timer);
            runtime_filter_slots.publish();
        }

        return Status::OK();
    }

private:
    VNestedLoopJoinNode* _join_node;
};

VNestedLoopJoinNode::VNestedLoopJoinNode(ObjectPool* pool, const TPlanNode& tnode,
                                         const DescriptorTbl& descs)
        : VJoinNodeBase(pool, tnode, descs),
          _cur_probe_row_visited_flags(false),
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

    if (tnode.nested_loop_join_node.__isset.vjoin_conjunct) {
        _vjoin_conjunct_ptr.reset(new VExprContext*);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, tnode.nested_loop_join_node.vjoin_conjunct,
                                                _vjoin_conjunct_ptr.get()));
    }

    std::vector<TExpr> filter_src_exprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        filter_src_exprs.push_back(_runtime_filter_descs[i].src_expr);
        RETURN_IF_ERROR(state->runtime_filter_mgr()->register_filter(
                RuntimeFilterRole::PRODUCER, _runtime_filter_descs[i], state->query_options()));
    }
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(_pool, filter_src_exprs, &_filter_src_expr_ctxs));
    DCHECK(!filter_src_exprs.empty() == _is_output_left_side_only);
    return Status::OK();
}

Status VNestedLoopJoinNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _build_rows_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _probe_rows_counter = ADD_COUNTER(runtime_profile(), "ProbeRows", TUnit::UNIT);
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _join_filter_timer = ADD_TIMER(runtime_profile(), "JoinFilterTimer");

    // pre-compute the tuple index of build tuples in the output row
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();

    for (int i = 0; i < num_build_tuples; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
    }

    if (_vjoin_conjunct_ptr) {
        RETURN_IF_ERROR((*_vjoin_conjunct_ptr)->prepare(state, *_intermediate_row_desc));
    }
    _num_probe_side_columns = child(0)->row_desc().num_materialized_slots();
    _num_build_side_columns = child(1)->row_desc().num_materialized_slots();
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    RETURN_IF_ERROR(VExpr::prepare(_filter_src_expr_ctxs, state, child(1)->row_desc()));

    _construct_mutable_join_block();
    return Status::OK();
}

Status VNestedLoopJoinNode::close(RuntimeState* state) {
    // avoid double close
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::close");
    _release_mem();

    return VJoinNodeBase::close(state);
}

Status VNestedLoopJoinNode::_materialize_build_side(RuntimeState* state) {
    // Do a full scan of child(1) and store all build row batches.
    RETURN_IF_ERROR(child(1)->open(state));

    bool eos = false;
    while (true) {
        RETURN_IF_CANCELLED(state);

        Block block;
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(1)->get_next_after_projects(
                        state, &block, &eos,
                        std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                          ExecNode::get_next,
                                  _children[1], std::placeholders::_1, std::placeholders::_2,
                                  std::placeholders::_3)),
                child(1)->get_next_span(), eos);

        sink(state, &block, eos);

        if (eos) {
            break;
        }
    }

    return Status::OK();
}

Status VNestedLoopJoinNode::sink(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
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
        RuntimeFilterBuild(this)(state);
    }

    return Status::OK();
}

Status VNestedLoopJoinNode::push(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    COUNTER_UPDATE(_probe_rows_counter, block->rows());
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
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(0)->get_next_after_projects(
                        state, &_left_block, &_left_side_eos,
                        std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                          ExecNode::get_next,
                                  _children[0], std::placeholders::_1, std::placeholders::_2,
                                  std::placeholders::_3)),
                child(0)->get_next_span(), _left_side_eos);

    } while (_left_block.rows() == 0 && !_left_side_eos);

    return Status::OK();
}

Status VNestedLoopJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span,
                                 "VNestedLoopJoinNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);
    RETURN_IF_CANCELLED(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    while (need_more_input_data()) {
        RETURN_IF_ERROR(_fresh_left_block(state));
        push(state, &_left_block, _left_side_eos);
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
    IColumn::Filter& mark_data = assert_cast<doris::vectorized::ColumnVector<UInt8>&>(
                                         *dst_columns[dst_columns.size() - 1])
                                         .get_data();
    mark_data.resize_fill(mark_data.size() + 1, 0);
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
        IColumn::Filter& mark_data =
                assert_cast<doris::vectorized::ColumnVector<UInt8>&>(
                        *block->get_by_position(block->columns() - 1).column->assume_mutable())
                        .get_data();
        if (mark_data.size() < block->rows()) {
            mark_data.resize_fill(block->rows(), 1);
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
        for (; i < build_block_sz and column_size < batch_size; i++) {
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
        if constexpr (IsSemi) {
            if (!_cur_probe_row_visited_flags && !_is_mark_join) {
                return;
            }
        } else {
            if (_cur_probe_row_visited_flags && !_is_mark_join) {
                return;
            }
        }

        auto new_size = column_size + 1;
        if (_is_mark_join) {
            IColumn::Filter& mark_data = assert_cast<doris::vectorized::ColumnVector<UInt8>&>(
                                                 *dst_columns[dst_columns.size() - 1])
                                                 .get_data();
            mark_data.resize_fill(mark_data.size() + 1,
                                  (IsSemi && !_cur_probe_row_visited_flags) ||
                                                  (!IsSemi && _cur_probe_row_visited_flags)
                                          ? 0
                                          : 1);
        }

        DCHECK_LT(_left_block_pos, _left_block.rows());
        for (size_t i = 0; i < _num_probe_side_columns; ++i) {
            const ColumnWithTypeAndName src_column = _left_block.get_by_position(i);
            if (!src_column.column->is_nullable() && dst_columns[i]->is_nullable()) {
                DCHECK(_join_op == TJoinOp::FULL_OUTER_JOIN);
                assert_cast<ColumnNullable*>(dst_columns[i].get())
                        ->get_nested_column_ptr()
                        ->insert_many_from(*src_column.column, _left_block_pos, 1);
                assert_cast<ColumnNullable*>(dst_columns[i].get())
                        ->get_null_map_column()
                        .get_data()
                        .resize_fill(new_size, 0);
            } else {
                dst_columns[i]->insert_many_from(*src_column.column, _left_block_pos, 1);
            }
        }
        for (size_t i = 0; i < _num_build_side_columns; ++i) {
            dst_columns[_num_probe_side_columns + i]->insert_default();
        }
        _resize_fill_tuple_is_null_column(new_size, 0, 1);
    }
}

void VNestedLoopJoinNode::_reset_with_next_probe_row() {
    // TODO: need a vector of left block to register the _probe_row_visited_flags
    _cur_probe_row_visited_flags = false;
    _current_build_pos = 0;
    _left_block_pos++;
}

#define CLEAR_BLOCK                                                  \
    for (size_t i = 0; i < column_to_keep; ++i) {                    \
        block->get_by_position(i).column->assume_mutable()->clear(); \
    }

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
            const size_t offset = _offset_stack.top();
            _offset_stack.pop();
            for (size_t j = 0; j < cur_sz; j++) {
                build_side_flag_data[j] |= filter[offset + j];
            }
            build_block_idx = build_block_idx == 0 ? _build_blocks.size() - 1 : build_block_idx - 1;
        }
    }
    if constexpr (SetProbeSideFlag) {
        _cur_probe_row_visited_flags |= simd::contain_byte<uint8>(filter.data(), filter.size(), 1);
    }
    if (materialize) {
        Block::filter_block_internal(block, filter, column_to_keep);
    } else {
        CLEAR_BLOCK
    }
}

template <bool SetBuildSideFlag, bool SetProbeSideFlag, bool IgnoreNull>
Status VNestedLoopJoinNode::_do_filtering_and_update_visited_flags(Block* block, bool materialize) {
    auto column_to_keep = block->columns();
    // If we need to set visited flags for build side,
    // 1. Execute conjuncts and get a column with bool type to do filtering.
    // 2. Use bool column to update build-side visited flags.
    // 3. Use bool column to do filtering.
    size_t build_block_idx =
            _current_build_pos == 0 ? _build_blocks.size() - 1 : _current_build_pos - 1;
    size_t processed_blocks_num = _offset_stack.size();
    if (LIKELY(_vjoin_conjunct_ptr != nullptr && block->rows() > 0)) {
        DCHECK((*_vjoin_conjunct_ptr) != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR((*_vjoin_conjunct_ptr)->execute(block, &result_column_id));
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
            if constexpr (IgnoreNull) {
                for (size_t i = 0; i < size; ++i) {
                    filter_data[i] |= null_map[i];
                }
            } else {
                for (size_t i = 0; i < size; ++i) {
                    filter_data[i] &= !null_map[i];
                }
            }
            _do_filtering_and_update_visited_flags_impl<decltype(filter), SetBuildSideFlag,
                                                        SetProbeSideFlag>(
                    block, column_to_keep, build_block_idx, processed_blocks_num, materialize,
                    filter);
        } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
            bool ret = const_column->get_bool(0);
            if (ret) {
                if constexpr (SetBuildSideFlag) {
                    for (size_t i = 0; i < processed_blocks_num; i++) {
                        auto& build_side_flag =
                                assert_cast<ColumnUInt8*>(
                                        _build_side_visited_flags[build_block_idx].get())
                                        ->get_data();
                        auto* __restrict build_side_flag_data = build_side_flag.data();
                        auto cur_sz = build_side_flag.size();
                        _offset_stack.pop();
                        memset(reinterpret_cast<void*>(build_side_flag_data), 1, cur_sz);
                        build_block_idx = build_block_idx == 0 ? _build_blocks.size() - 1
                                                               : build_block_idx - 1;
                    }
                }
                if constexpr (SetProbeSideFlag) {
                    _cur_probe_row_visited_flags |= ret;
                }
            }
            if (!materialize || !ret) {
                CLEAR_BLOCK
            }
        } else {
            const IColumn::Filter& filter =
                    assert_cast<const doris::vectorized::ColumnVector<UInt8>&>(*filter_column)
                            .get_data();
            _do_filtering_and_update_visited_flags_impl<decltype(filter), SetBuildSideFlag,
                                                        SetProbeSideFlag>(
                    block, column_to_keep, build_block_idx, processed_blocks_num, materialize,
                    filter);
        }
    }
    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status VNestedLoopJoinNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(VJoinNodeBase::alloc_resource(state));
    if (_vjoin_conjunct_ptr) {
        RETURN_IF_ERROR((*_vjoin_conjunct_ptr)->open(state));
    }
    return VExpr::open(_filter_src_expr_ctxs, state);
}

Status VNestedLoopJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::open")
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VJoinNodeBase::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());
    RETURN_IF_CANCELLED(state);
    // We can close the right child to release its resources because its input has been
    // fully consumed.
    child(1)->close(state);
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
    if (_is_output_left_side_only) {
        RETURN_IF_ERROR(_build_output_block(&_left_block, block));
        *eos = _left_side_eos;
        _need_more_input_data = !_left_side_eos;
    } else {
        *eos = _match_all_build
                       ? _output_null_idx_build_side == _build_blocks.size() && _matched_rows_done
                       : _matched_rows_done;

        {
            Block tmp_block = _join_block;
            _add_tuple_is_null_column(&tmp_block);
            {
                SCOPED_TIMER(_join_filter_timer);
                RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, &tmp_block,
                                                           tmp_block.columns()));
            }
            RETURN_IF_ERROR(_build_output_block(&tmp_block, block));
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
            RETURN_IF_ERROR(std::visit(func, _join_op_variants,
                                       make_bool_variant(_match_all_build || _is_right_semi_anti),
                                       make_bool_variant(_match_all_probe || _is_left_semi_anti)));
        }
    }

    reached_limit(block, eos);
    return Status::OK();
}

bool VNestedLoopJoinNode::need_more_input_data() const {
    return _need_more_input_data and !_left_side_eos;
}

void VNestedLoopJoinNode::release_resource(doris::RuntimeState* state) {
    VJoinNodeBase::release_resource(state);
    VExpr::close(_filter_src_expr_ctxs, state);
    if (_vjoin_conjunct_ptr) (*_vjoin_conjunct_ptr)->close(state);
}

} // namespace doris::vectorized
