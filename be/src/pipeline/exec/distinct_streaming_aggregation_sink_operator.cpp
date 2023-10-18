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

#include "distinct_streaming_aggregation_sink_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

DistinctStreamingAggSinkOperator::DistinctStreamingAggSinkOperator(
        OperatorBuilderBase* operator_builder, ExecNode* agg_node, std::shared_ptr<DataQueue> queue)
        : StreamingOperator(operator_builder, agg_node), _data_queue(std::move(queue)) {}

bool DistinctStreamingAggSinkOperator::can_write() {
    // sink and source in diff threads
    return _data_queue->has_enough_space_to_push();
}

Status DistinctStreamingAggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                                              SourceState source_state) {
    if (in_block && in_block->rows() > 0) {
        if (_output_block == nullptr) {
            _output_block = _data_queue->get_free_block();
        }
        RETURN_IF_ERROR(
                _node->_distinct_pre_agg_with_serialized_key(in_block, _output_block.get()));

        // get enough data or reached limit rows, need push block to queue
        if (_node->limit() != -1 &&
            (_output_block->rows() + _output_distinct_rows) >= _node->limit()) {
            auto limit_rows = _node->limit() - _output_distinct_rows;
            _output_block->set_num_rows(limit_rows);
            _output_distinct_rows += limit_rows;
            _data_queue->push_block(std::move(_output_block));
        } else if (_output_block->rows() >= state->batch_size()) {
            _output_distinct_rows += _output_block->rows();
            _data_queue->push_block(std::move(_output_block));
        }
    }

    // reach limit or source finish
    if ((UNLIKELY(source_state == SourceState::FINISHED)) || reached_limited_rows()) {
        if (_output_block != nullptr) { //maybe the last block with eos
            _output_distinct_rows += _output_block->rows();
            _data_queue->push_block(std::move(_output_block));
        }
        _data_queue->set_finish();
        return Status::Error<ErrorCode::END_OF_FILE>("");
    }
    return Status::OK();
}

Status DistinctStreamingAggSinkOperator::close(RuntimeState* state) {
    if (_data_queue && !_data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _data_queue->set_canceled();
    }
    return StreamingOperator::close(state);
}

DistinctStreamingAggSinkOperatorBuilder::DistinctStreamingAggSinkOperatorBuilder(
        int32_t id, ExecNode* exec_node, std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "DistinctStreamingAggSinkOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr DistinctStreamingAggSinkOperatorBuilder::build_operator() {
    return std::make_shared<DistinctStreamingAggSinkOperator>(this, _node, _data_queue);
}

DistinctStreamingAggSinkLocalState::DistinctStreamingAggSinkLocalState(
        DataSinkOperatorXBase* parent, RuntimeState* state)
        : AggSinkLocalState<AggDependency, DistinctStreamingAggSinkLocalState>(parent, state),
          dummy_mapped_data(std::make_shared<char>('A')) {}

Status DistinctStreamingAggSinkLocalState::_distinct_pre_agg_with_serialized_key(
        doris::vectorized::Block* in_block, doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_shared_state->probe_expr_ctxs.empty());

    size_t key_size = _shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(
                    _shared_state->probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = in_block->rows();
    _distinct_row.clear();
    _distinct_row.reserve(rows);

    RETURN_IF_CATCH_EXCEPTION(
            _emplace_into_hash_table_to_distinct(_distinct_row, key_columns, rows));

    bool mem_reuse = _dependency->make_nullable_keys().empty() && out_block->mem_reuse();
    if (mem_reuse) {
        for (int i = 0; i < key_size; ++i) {
            auto dst = out_block->get_by_position(i).column->assume_mutable();
            key_columns[i]->append_data_by_selector(dst, _distinct_row);
        }
    } else {
        vectorized::ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            auto distinct_column = key_columns[i]->clone_empty();
            key_columns[i]->append_data_by_selector(distinct_column, _distinct_row);
            columns_with_schema.emplace_back(
                    std::move(distinct_column),
                    _shared_state->probe_expr_ctxs[i]->root()->data_type(),
                    _shared_state->probe_expr_ctxs[i]->root()->expr_name());
        }
        out_block->swap(vectorized::Block(columns_with_schema));
    }
    return Status::OK();
}

void DistinctStreamingAggSinkLocalState::_emplace_into_hash_table_to_distinct(
        vectorized::IColumn::Selector& distinct_row, vectorized::ColumnRawPtrs& key_columns,
        const size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);
                size_t row = 0;
                auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                    HashMethodType::try_presis_key(key, origin, _arena);
                    ctor(key, dummy_mapped_data.get());
                    distinct_row.push_back(row);
                };
                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = dummy_mapped_data.get();
                    distinct_row.push_back(row);
                };

                SCOPED_TIMER(_hash_table_emplace_timer);
                for (; row < num_rows; ++row) {
                    agg_method.lazy_emplace(state, row, creator, creator_for_null_key);
                }

                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            _agg_data->method_variant);
}

DistinctStreamingAggSinkOperatorX::DistinctStreamingAggSinkOperatorX(ObjectPool* pool,
                                                                     const TPlanNode& tnode,
                                                                     const DescriptorTbl& descs)
        : AggSinkOperatorX<DistinctStreamingAggSinkLocalState>(pool, tnode, descs) {}

Status DistinctStreamingAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(AggSinkOperatorX<DistinctStreamingAggSinkLocalState>::init(tnode, state));
    _name = "DISTINCT_STREAMING_AGGREGATION_SINK_OPERATOR";
    return Status::OK();
}

Status DistinctStreamingAggSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                               SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_STATUS_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    local_state._shared_state->input_num_rows += in_block->rows();
    Status ret = Status::OK();
    if (in_block && in_block->rows() > 0) {
        if (local_state._output_block == nullptr) {
            local_state._output_block = local_state._shared_state->data_queue->get_free_block();
        }
        RETURN_IF_ERROR(local_state._distinct_pre_agg_with_serialized_key(
                in_block, local_state._output_block.get()));

        // get enough data or reached limit rows, need push block to queue
        if (_limit != -1 &&
            (local_state._output_block->rows() + local_state._output_distinct_rows) >= _limit) {
            auto limit_rows = _limit - local_state._output_distinct_rows;
            local_state._output_block->set_num_rows(limit_rows);
            local_state._output_distinct_rows += limit_rows;
            local_state._shared_state->data_queue->push_block(std::move(local_state._output_block));
        } else if (local_state._output_block->rows() >= state->batch_size()) {
            local_state._output_distinct_rows += local_state._output_block->rows();
            local_state._shared_state->data_queue->push_block(std::move(local_state._output_block));
        }
    }

    // reach limit or source finish
    if ((UNLIKELY(source_state == SourceState::FINISHED)) ||
        (_limit != -1 && local_state._output_distinct_rows >= _limit)) {
        if (local_state._output_block != nullptr) { //maybe the last block with eos
            local_state._output_distinct_rows += local_state._output_block->rows();
            local_state._shared_state->data_queue->push_block(std::move(local_state._output_block));
        }
        local_state._shared_state->data_queue->set_finish();
        return Status::Error<ErrorCode::END_OF_FILE>(""); // need given finish signal
    }
    return Status::OK();
}

Status DistinctStreamingAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_shared_state->data_queue && !_shared_state->data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _shared_state->data_queue->set_canceled();
    }
    return Base::close(state, exec_status);
}

} // namespace doris::pipeline
