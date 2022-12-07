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
    VExpr::close(_filter_src_expr_ctxs, state);
    if (_vjoin_conjunct_ptr) (*_vjoin_conjunct_ptr)->close(state);
    _release_mem();

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
            if (_match_all_build || _is_right_semi_anti) {
                _build_side_visited_flags.emplace_back(ColumnUInt8::create(rows, 0));
            }
        }

        if (eos) {
            break;
        }
    }

    COUNTER_UPDATE(_build_rows_counter, _build_rows);

    RuntimeFilterBuild processRuntimeFilterBuild {this};
    processRuntimeFilterBuild(state);

    return Status::OK();
}

Status VNestedLoopJoinNode::get_left_side(RuntimeState* state, Block* block) {
    do {
        release_block_memory(*block);
        RETURN_IF_ERROR_AND_CHECK_SPAN(
                child(0)->get_next_after_projects(state, block, &_left_side_eos),
                child(0)->get_next_span(), _left_side_eos);

    } while (block->rows() == 0 && !_left_side_eos);
    COUNTER_UPDATE(_probe_rows_counter, block->rows());
    if (block->rows() == 0) {
        _matched_rows_done = _left_side_eos;
    }
    return Status::OK();
}

Status VNestedLoopJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span,
                                 "VNestedLoopJoinNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);
    RETURN_IF_CANCELLED(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_growh());

    if (_is_output_left_side_only) {
        RETURN_IF_ERROR(get_left_side(state, &_left_block));
        RETURN_IF_ERROR(_build_output_block(&_left_block, block));
        *eos = _left_side_eos;
        reached_limit(block, eos);
        return Status::OK();
    }

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
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                while (mutable_join_block.rows() < state->batch_size() && !_matched_rows_done) {
                    // If this left block is exhausted or empty, we need to pull data from left child.
                    if (_left_block_pos == _left_block.rows()) {
                        _left_block_pos = 0;

                        if (_left_side_eos) {
                            _matched_rows_done = true;
                        } else {
                            RETURN_IF_ERROR(get_left_side(state, &_left_block));
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
                                &tmp_block, offset_stack, !_is_left_semi_anti);
                        _update_tuple_is_null_column(&tmp_block);
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
                                _finalize_current_phase<false, JoinOpType::value ==
                                                                       TJoinOp::LEFT_SEMI_JOIN>(
                                        dst_columns, state->batch_size());
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
                            &tmp_block, offset_stack, !_is_right_semi_anti);
                    _update_tuple_is_null_column(&tmp_block);
                    mutable_join_block = MutableBlock(std::move(tmp_block));
                    if (!status.OK()) {
                        return status;
                    }
                }

                if constexpr (set_build_side_flag) {
                    if (_matched_rows_done && _output_null_idx_build_side < _build_blocks.size()) {
                        auto& cols = mutable_join_block.mutable_columns();
                        _finalize_current_phase<true,
                                                JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN>(
                                cols, state->batch_size());
                    }
                }
                return Status::OK();
            },
            _join_op_variants, make_bool_variant(_match_all_build || _is_right_semi_anti),
            make_bool_variant(_match_all_probe || _is_left_semi_anti)));
    *eos = _match_all_build
                   ? _output_null_idx_build_side == _build_blocks.size() && _matched_rows_done
                   : _matched_rows_done;

    Block tmp_block = mutable_join_block.to_block(0);
    if (_is_outer_join) {
        _add_tuple_is_null_column(&tmp_block);
    }
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(
                VExprContext::filter_block(_vconjunct_ctx_ptr, &tmp_block, tmp_block.columns()));
    }
    RETURN_IF_ERROR(_build_output_block(&tmp_block, block));
    _reset_tuple_is_null_column();
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

void VNestedLoopJoinNode::_update_tuple_is_null_column(Block* block) {
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
}

void VNestedLoopJoinNode::_add_tuple_is_null_column(Block* block) {
    DCHECK(_is_outer_join);
    auto p0 = _tuple_is_null_left_flag_column->assume_mutable();
    auto p1 = _tuple_is_null_right_flag_column->assume_mutable();
    block->insert(
            {std::move(p0), std::make_shared<vectorized::DataTypeUInt8>(), "left_tuples_is_null"});
    block->insert(
            {std::move(p1), std::make_shared<vectorized::DataTypeUInt8>(), "right_tuples_is_null"});
}

template <bool BuildSide, bool IsSemi>
void VNestedLoopJoinNode::_finalize_current_phase(MutableColumns& dst_columns, size_t batch_size) {
    DCHECK_GT(dst_columns.size(), 0);
    auto pre_size = dst_columns[0]->size();
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
            for (size_t j = 0; j < _num_probe_side_columns; ++j) {
                DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                       _join_op == TJoinOp::FULL_OUTER_JOIN ||
                       _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                       _join_op == TJoinOp::RIGHT_SEMI_JOIN);
                dst_columns[j]->insert_many_defaults(selector_idx);
            }

            if (_is_outer_join) {
                reinterpret_cast<ColumnUInt8*>(_tuple_is_null_left_flag_column.get())
                        ->get_data()
                        .resize_fill(pre_size + selector_idx, 1);
                reinterpret_cast<ColumnUInt8*>(_tuple_is_null_right_flag_column.get())
                        ->get_data()
                        .resize_fill(pre_size + selector_idx, 0);
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
            pre_size = dst_columns[0]->size();
        }
        _output_null_idx_build_side = i;
    } else {
        if constexpr (IsSemi) {
            if (!_cur_probe_row_visited_flags) {
                return;
            }
        } else {
            if (_cur_probe_row_visited_flags) {
                return;
            }
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
        if (_is_outer_join) {
            reinterpret_cast<ColumnUInt8*>(_tuple_is_null_left_flag_column.get())
                    ->get_data()
                    .resize_fill(pre_size + 1, 0);
            reinterpret_cast<ColumnUInt8*>(_tuple_is_null_right_flag_column.get())
                    ->get_data()
                    .resize_fill(pre_size + 1, 1);
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
        Block* block, std::stack<uint16_t>& offset_stack, bool materialize) {
    auto column_to_keep = block->columns();
    // If we need to set visited flags for build side,
    // 1. Execute conjuncts and get a column with bool type to do filtering.
    // 2. Use bool column to update build-side visited flags.
    // 3. Use bool column to do filtering.
    size_t build_block_idx =
            _current_build_pos == 0 ? _build_blocks.size() - 1 : _current_build_pos - 1;
    size_t processed_blocks_num = offset_stack.size();
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
#define CLEAR_BLOCK                                                  \
    for (size_t i = 0; i < column_to_keep; ++i) {                    \
        block->get_by_position(i).column->assume_mutable()->clear(); \
    }
            if (materialize) {
                Block::filter_block_internal(block, filter, column_to_keep);
            } else {
                CLEAR_BLOCK
            }
        } else if (auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
            bool ret = const_column->get_bool(0);
            if (!ret) {
                for (size_t i = 0; i < column_to_keep; ++i) {
                    block->get_by_position(i).column->assume_mutable()->clear();
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
                if (!materialize) {
                    CLEAR_BLOCK
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
            if (materialize) {
                Block::filter_block_internal(block, filter, column_to_keep);
            } else {
                CLEAR_BLOCK
            }
        }
    }
#undef CLEAR_BLOCK
    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status VNestedLoopJoinNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VNestedLoopJoinNode::open")
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(VExpr::open(_filter_src_expr_ctxs, state));
    if (_vjoin_conjunct_ptr) {
        RETURN_IF_ERROR((*_vjoin_conjunct_ptr)->open(state));
    }
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
    *out << "(eos=" << (_matched_rows_done ? "true" : "false")
         << " left_block_pos=" << _left_block_pos;
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

} // namespace doris::vectorized
