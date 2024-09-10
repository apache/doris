
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
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

Status AnalyticSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _blocks_memory_usage =
            _profile->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage", 1);
    _evaluation_timer = ADD_TIMER(profile(), "EvaluationTime");
    return Status::OK();
}

Status AnalyticSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<AnalyticSharedState>::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<AnalyticSinkOperatorX>();
    _shared_state->partition_by_column_idxs.resize(p._partition_by_eq_expr_ctxs.size());
    _shared_state->ordey_by_column_idxs.resize(p._order_by_eq_expr_ctxs.size());

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

bool AnalyticSinkLocalState::_whether_need_next_partition(BlockRowPos& found_partition_end) {
    auto& shared_state = *_shared_state;
    if (shared_state.input_eos ||
        (shared_state.current_row_position <
         shared_state.partition_by_end.pos)) { //now still have partition data
        return false;
    }
    if ((shared_state.partition_by_eq_expr_ctxs.empty() && !shared_state.input_eos) ||
        (found_partition_end.pos == 0)) { //no partition, get until fetch to EOS
        return true;
    }
    if (!shared_state.partition_by_eq_expr_ctxs.empty() &&
        found_partition_end.pos == shared_state.all_block_end.pos &&
        !shared_state.input_eos) { //current partition data calculate done
        return true;
    }
    return false;
}

//_partition_by_columns,_order_by_columns save in blocks, so if need to calculate the boundary, may find in which blocks firstly
BlockRowPos AnalyticSinkLocalState::_compare_row_to_find_end(int idx, BlockRowPos start,
                                                             BlockRowPos end,
                                                             bool need_check_first) {
    auto& shared_state = *_shared_state;
    int64_t start_init_row_num = start.row_num;
    vectorized::ColumnPtr start_column =
            shared_state.input_blocks[start.block_num].get_by_position(idx).column;
    vectorized::ColumnPtr start_next_block_column = start_column;

    DCHECK_LE(start.block_num, end.block_num);
    DCHECK_LE(start.block_num, shared_state.input_blocks.size() - 1);
    int64_t start_block_num = start.block_num;
    int64_t end_block_num = end.block_num;
    int64_t mid_blcok_num = end.block_num;
    // To fix this problem: https://github.com/apache/doris/issues/15951
    // in this case, the partition by column is last row of block, so it's pointed to a new block at row = 0, range is: [left, right)
    // From the perspective of order by column, the two values are exactly equal.
    // so the range will be get wrong because it's compare_at == 0 with next block at row = 0
    if (need_check_first && end.block_num > 0 && end.row_num == 0) {
        end.block_num--;
        end_block_num--;
        end.row_num = shared_state.input_blocks[end_block_num].rows();
    }
    //binary search find in which block
    while (start_block_num < end_block_num) {
        mid_blcok_num = (start_block_num + end_block_num + 1) >> 1;
        start_next_block_column =
                shared_state.input_blocks[mid_blcok_num].get_by_position(idx).column;
        //Compares (*this)[n] and rhs[m], this: start[init_row]  rhs: mid[0]
        if (start_column->compare_at(start_init_row_num, 0, *start_next_block_column, 1) == 0) {
            start_block_num = mid_blcok_num;
        } else {
            end_block_num = mid_blcok_num - 1;
        }
    }

    // have check the start.block_num:  start_column[start_init_row_num] with mid_blcok_num start_next_block_column[0]
    // now next block must not be result, so need check with end_block_num: start_next_block_column[last_row]
    if (end_block_num == mid_blcok_num - 1) {
        start_next_block_column =
                shared_state.input_blocks[end_block_num].get_by_position(idx).column;
        int64_t block_size = shared_state.input_blocks[end_block_num].rows();
        if ((start_column->compare_at(start_init_row_num, block_size - 1, *start_next_block_column,
                                      1) == 0)) {
            start.block_num = end_block_num + 1;
            start.row_num = 0;
            return start;
        }
    }

    //check whether need get column again, maybe same as first init
    // if the start_block_num have move to forword, so need update start block num and compare it from row_num=0
    if (start_block_num != start.block_num) {
        start_init_row_num = 0;
        start.block_num = start_block_num;
        start_column = shared_state.input_blocks[start.block_num].get_by_position(idx).column;
    }
    //binary search, set start and end pos
    int64_t start_pos = start_init_row_num;
    int64_t end_pos = shared_state.input_blocks[start.block_num].rows();
    //if end_block_num haven't moved, only start_block_num go to the end block
    //so could use the end.row_num for binary search
    if (start.block_num == end.block_num) {
        end_pos = end.row_num;
    }
    while (start_pos < end_pos) {
        int64_t mid_pos = (start_pos + end_pos) >> 1;
        if (start_column->compare_at(start_init_row_num, mid_pos, *start_column, 1)) {
            end_pos = mid_pos;
        } else {
            start_pos = mid_pos + 1;
        }
    }
    start.row_num = start_pos; //update row num, return the find end
    return start;
}

BlockRowPos AnalyticSinkLocalState::_get_partition_by_end() {
    auto& shared_state = *_shared_state;
    if (shared_state.current_row_position <
        shared_state.partition_by_end.pos) { //still have data, return partition_by_end directly
        return shared_state.partition_by_end;
    }

    if (shared_state.partition_by_eq_expr_ctxs.empty() ||
        (shared_state.input_total_rows == 0)) { //no partition_by, the all block is end
        return shared_state.all_block_end;
    }

    BlockRowPos cal_end = shared_state.all_block_end;
    for (size_t i = 0; i < shared_state.partition_by_eq_expr_ctxs.size();
         ++i) { //have partition_by, binary search the partiton end
        cal_end = _compare_row_to_find_end(shared_state.partition_by_column_idxs[i],
                                           shared_state.partition_by_end, cal_end);
    }
    cal_end.pos = shared_state.input_block_first_row_positions[cal_end.block_num] + cal_end.row_num;
    return cal_end;
}

AnalyticSinkOperatorX::AnalyticSinkOperatorX(ObjectPool* pool, int operator_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs,
                                             bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _buffered_tuple_id(tnode.analytic_node.__isset.buffered_tuple_id
                                     ? tnode.analytic_node.buffered_tuple_id
                                     : 0),
          _is_colocate(tnode.analytic_node.__isset.is_colocate && tnode.analytic_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution),
          _partition_exprs(tnode.__isset.distribute_expr_lists && require_bucket_distribution
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.analytic_node.partition_exprs) {}

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

Status AnalyticSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AnalyticSinkLocalState>::open(state));
    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(ctx, state, _child->row_desc()));
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(_child->row_desc().tuple_descriptors()[0]->id());
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
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(vectorized::VExpr::open(_agg_expr_ctxs[i], state));
    }
    return Status::OK();
}

Status AnalyticSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                   bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
    local_state._shared_state->input_eos = eos;
    if (local_state._shared_state->input_eos && input_block->rows() == 0) {
        local_state._dependency->set_ready_to_read();
        local_state._dependency->block();
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
        local_state._shared_state->found_partition_end = local_state._get_partition_by_end();
    }
    local_state._refresh_need_more_input();
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
