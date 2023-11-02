
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

#include "analytic_sink_operator.h"

#include <string>

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(AnalyticSinkOperator, StreamingOperator)

Status AnalyticSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticDependency>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<AnalyticSinkOperatorX>();
    _shared_state->partition_by_column_idxs.resize(p._partition_by_eq_expr_ctxs.size());
    _shared_state->ordey_by_column_idxs.resize(p._order_by_eq_expr_ctxs.size());

    _memory_usage_counter = ADD_LABEL_COUNTER(profile(), "MemoryUsage");
    _blocks_memory_usage = _profile->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage");
    _evaluation_timer = ADD_TIMER(profile(), "EvaluationTime");

    size_t agg_size = p._agg_expr_ctxs.size();
    _agg_expr_ctxs.resize(agg_size);
    _shared_state->agg_input_columns.resize(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        _shared_state->agg_input_columns[i].resize(p._num_agg_input[i]);
        _agg_expr_ctxs[i].resize(p._agg_expr_ctxs[i].size());
        for (int j = 0; j < p._agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(p._agg_expr_ctxs[i][j]->clone(state, _agg_expr_ctxs[i][j]));
        }

        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            _shared_state->agg_input_columns[i][j] =
                    _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
    }
    _shared_state->partition_by_eq_expr_ctxs.resize(p._partition_by_eq_expr_ctxs.size());
    for (size_t i = 0; i < _shared_state->partition_by_eq_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._partition_by_eq_expr_ctxs[i]->clone(
                state, _shared_state->partition_by_eq_expr_ctxs[i]));
    }
    _shared_state->order_by_eq_expr_ctxs.resize(p._order_by_eq_expr_ctxs.size());
    for (size_t i = 0; i < _shared_state->order_by_eq_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                p._order_by_eq_expr_ctxs[i]->clone(state, _shared_state->order_by_eq_expr_ctxs[i]));
    }
    return Status::OK();
}

AnalyticSinkOperatorX::AnalyticSinkOperatorX(ObjectPool* pool, int operator_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _buffered_tuple_id(tnode.analytic_node.__isset.buffered_tuple_id
                                     ? tnode.analytic_node.buffered_tuple_id
                                     : 0) {}

Status AnalyticSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    size_t agg_size = analytic_node.analytic_functions.size();
    _agg_expr_ctxs.resize(agg_size);
    _num_agg_input.resize(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        _num_agg_input[i] = desc.nodes[0].num_children;
        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            vectorized::VExprSPtr expr;
            vectorized::VExprContextSPtr ctx;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_tree_from_thrift(desc.nodes, &node_idx, expr, ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }
    }

    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(analytic_node.partition_exprs,
                                                         _partition_by_eq_expr_ctxs));
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(analytic_node.order_by_exprs,
                                                         _order_by_eq_expr_ctxs));
    _agg_functions_size = agg_size;
    return Status::OK();
}

Status AnalyticSinkOperatorX::prepare(RuntimeState* state) {
    for (const auto& ctx : _agg_expr_ctxs) {
        static_cast<void>(vectorized::VExpr::prepare(ctx, state, _child_x->row_desc()));
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(_child_x->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc));
        }
    }
    return Status::OK();
}

Status AnalyticSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(vectorized::VExpr::open(_agg_expr_ctxs[i], state));
    }
    return Status::OK();
}

Status AnalyticSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                   SourceState source_state) {
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
    local_state._shared_state->input_eos = source_state == SourceState::FINISHED;
    if (local_state._shared_state->input_eos && input_block->rows() == 0) {
        local_state._dependency->set_ready_for_read();
        local_state._dependency->block_writing();
        return Status::OK();
    }

    local_state._shared_state->input_block_first_row_positions.emplace_back(
            local_state._shared_state->input_total_rows);
    size_t block_rows = input_block->rows();
    local_state._shared_state->input_total_rows += block_rows;
    local_state._shared_state->all_block_end.block_num =
            local_state._shared_state->input_blocks.size();
    local_state._shared_state->all_block_end.row_num = block_rows;
    local_state._shared_state->all_block_end.pos = local_state._shared_state->input_total_rows;

    if (local_state._shared_state->origin_cols
                .empty()) { //record origin columns, maybe be after this, could cast some column but no need to save
        for (int c = 0; c < input_block->columns(); ++c) {
            local_state._shared_state->origin_cols.emplace_back(c);
        }
    }

    for (size_t i = 0; i < _agg_functions_size;
         ++i) { //insert _agg_input_columns, execute calculate for its
        for (size_t j = 0; j < local_state._agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(_insert_range_column(
                    input_block, local_state._agg_expr_ctxs[i][j],
                    local_state._shared_state->agg_input_columns[i][j].get(), block_rows));
        }
    }
    //record column idx in block
    for (size_t i = 0; i < local_state._shared_state->partition_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(local_state._shared_state->partition_by_eq_expr_ctxs[i]->execute(
                input_block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        local_state._shared_state->partition_by_column_idxs[i] = result_col_id;
    }

    for (size_t i = 0; i < local_state._shared_state->order_by_eq_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(local_state._shared_state->order_by_eq_expr_ctxs[i]->execute(
                input_block, &result_col_id));
        DCHECK_GE(result_col_id, 0);
        local_state._shared_state->ordey_by_column_idxs[i] = result_col_id;
    }

    local_state.mem_tracker()->consume(input_block->allocated_bytes());
    local_state._blocks_memory_usage->add(input_block->allocated_bytes());

    //TODO: if need improvement, the is a tips to maintain a free queue,
    //so the memory could reuse, no need to new/delete again;
    local_state._shared_state->input_blocks.emplace_back(std::move(*input_block));
    {
        SCOPED_TIMER(local_state._evaluation_timer);
        local_state._shared_state->found_partition_end =
                local_state._dependency->get_partition_by_end();
    }
    local_state._dependency->refresh_need_more_input();
    return Status::OK();
}

Status AnalyticSinkOperatorX::_insert_range_column(vectorized::Block* block,
                                                   const vectorized::VExprContextSPtr& expr,
                                                   vectorized::IColumn* dst_column, size_t length) {
    int result_col_id = -1;
    RETURN_IF_ERROR(expr->execute(block, &result_col_id));
    DCHECK_GE(result_col_id, 0);
    auto column = block->get_by_position(result_col_id).column->convert_to_full_column_if_const();
    dst_column->insert_range_from(*column, 0, length);
    return Status::OK();
}

template class DataSinkOperatorX<AnalyticSinkLocalState>;

} // namespace doris::pipeline
