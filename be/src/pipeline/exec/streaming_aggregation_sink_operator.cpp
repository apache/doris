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

#include "streaming_aggregation_sink_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

StreamingAggSinkOperator::StreamingAggSinkOperator(OperatorBuilderBase* operator_builder,
                                                   ExecNode* agg_node,
                                                   std::shared_ptr<DataQueue> queue)
        : StreamingOperator(operator_builder, agg_node), _data_queue(std::move(queue)) {}

Status StreamingAggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StreamingOperator::prepare(state));
    _queue_byte_size_counter =
            ADD_COUNTER(_node->runtime_profile(), "MaxSizeInBlockQueue", TUnit::BYTES);
    _queue_size_counter = ADD_COUNTER(_node->runtime_profile(), "MaxSizeOfBlockQueue", TUnit::UNIT);
    return Status::OK();
}

bool StreamingAggSinkOperator::can_write() {
    // sink and source in diff threads
    return _data_queue->has_enough_space_to_push();
}

Status StreamingAggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                                      SourceState source_state) {
    Status ret = Status::OK();
    if (in_block && in_block->rows() > 0) {
        auto block_from_ctx = _data_queue->get_free_block();
        RETURN_IF_ERROR(_node->do_pre_agg(in_block, block_from_ctx.get()));
        if (block_from_ctx->rows() == 0) {
            _data_queue->push_free_block(std::move(block_from_ctx));
        } else {
            _data_queue->push_block(std::move(block_from_ctx));
        }
    }

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        _data_queue->set_finish();
    }
    return Status::OK();
}

Status StreamingAggSinkOperator::close(RuntimeState* state) {
    if (_data_queue && !_data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _data_queue->set_canceled();
    }
    if (_data_queue) {
        COUNTER_SET(_queue_size_counter, _data_queue->max_size_of_queue());
        COUNTER_SET(_queue_byte_size_counter, _data_queue->max_bytes_in_queue());
    }
    return StreamingOperator::close(state);
}

StreamingAggSinkOperatorBuilder::StreamingAggSinkOperatorBuilder(int32_t id, ExecNode* exec_node,
                                                                 std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "StreamingAggSinkOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr StreamingAggSinkOperatorBuilder::build_operator() {
    return std::make_shared<StreamingAggSinkOperator>(this, _node, _data_queue);
}

/// The minimum reduction factor (input rows divided by output rows) to grow hash tables
/// in a streaming preaggregation, given that the hash tables are currently the given
/// size or above. The sizes roughly correspond to hash table sizes where the bucket
/// arrays will fit in  a cache level. Intuitively, we don't want the working set of the
/// aggregation to expand to the next level of cache unless we're reducing the input
/// enough to outweigh the increased memory latency we'll incur for each hash table
/// lookup.
///
/// Note that the current reduction achieved is not always a good estimate of the
/// final reduction. It may be biased either way depending on the ordering of the
/// input. If the input order is random, we will underestimate the final reduction
/// factor because the probability of a row having the same key as a previous row
/// increases as more input is processed.  If the input order is correlated with the
/// key, skew may bias the estimate. If high cardinality keys appear first, we
/// may overestimate and if low cardinality keys appear first, we underestimate.
/// To estimate the eventual reduction achieved, we estimate the final reduction
/// using the planner's estimated input cardinality and the assumption that input
/// is in a random order. This means that we assume that the reduction factor will
/// increase over time.
struct StreamingHtMinReductionEntry {
    // Use 'streaming_ht_min_reduction' if the total size of hash table bucket directories in
    // bytes is greater than this threshold.
    int min_ht_mem;
    // The minimum reduction factor to expand the hash tables.
    double streaming_ht_min_reduction;
};

// TODO: experimentally tune these values and also programmatically get the cache size
// of the machine that we're running on.
static constexpr StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
        // Expand up to L2 cache always.
        {0, 0.0},
        // Expand into L3 cache if we look like we're getting some reduction.
        // At present, The L2 cache is generally 1024k or more
        {1024 * 1024, 1.1},
        // Expand into main memory if we're getting a significant reduction.
        // The L3 cache is generally 16MB or more
        {16 * 1024 * 1024, 2.0},
};

static constexpr int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

StreamingAggSinkLocalState::StreamingAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                       RuntimeState* state)
        : Base(parent, state),
          _queue_byte_size_counter(nullptr),
          _queue_size_counter(nullptr),
          _streaming_agg_timer(nullptr) {}

Status StreamingAggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    _shared_state->data_queue.reset(new DataQueue(1, _dependency));
    _queue_byte_size_counter = ADD_COUNTER(profile(), "MaxSizeInBlockQueue", TUnit::BYTES);
    _queue_size_counter = ADD_COUNTER(profile(), "MaxSizeOfBlockQueue", TUnit::UNIT);
    _streaming_agg_timer = ADD_TIMER(profile(), "StreamingAggTime");
    return Status::OK();
}

Status StreamingAggSinkLocalState::do_pre_agg(vectorized::Block* input_block,
                                              vectorized::Block* output_block) {
    RETURN_IF_ERROR(_pre_agg_with_serialized_key(input_block, output_block));

    // pre stream agg need use _num_row_return to decide whether to do pre stream agg
    _num_rows_returned += output_block->rows();
    _dependency->_make_nullable_output_key(output_block);
    //    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    _executor.update_memusage();
    return Status::OK();
}

bool StreamingAggSinkLocalState::_should_expand_preagg_hash_tables() {
    if (!_should_expand_hash_table) {
        return false;
    }

    return std::visit(
            [&](auto&& agg_method) -> bool {
                auto& hash_tbl = *agg_method.hash_table;
                auto [ht_mem, ht_rows] =
                        std::pair {hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

                // Need some rows in tables to have valid statistics.
                if (ht_rows == 0) {
                    return true;
                }

                // Find the appropriate reduction factor in our table for the current hash table sizes.
                int cache_level = 0;
                while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
                       ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
                    ++cache_level;
                }

                // Compare the number of rows in the hash table with the number of input rows that
                // were aggregated into it. Exclude passed through rows from this calculation since
                // they were not in hash tables.
                const int64_t input_rows = _shared_state->input_num_rows;
                const int64_t aggregated_input_rows = input_rows - _num_rows_returned;
                // TODO chenhao
                //  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
                double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

                // TODO: workaround for IMPALA-2490: subplan node rows_returned counter may be
                // inaccurate, which could lead to a divide by zero below.
                if (aggregated_input_rows <= 0) {
                    return true;
                }

                // Extrapolate the current reduction factor (r) using the formula
                // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
                // set, N is the number of input rows, excluding passed-through rows, and n is the
                // number of rows inserted or merged into the hash tables. This is a very rough
                // approximation but is good enough to be useful.
                // TODO: consider collecting more statistics to better estimate reduction.
                //  double estimated_reduction = aggregated_input_rows >= expected_input_rows
                //      ? current_reduction
                //      : 1 + (expected_input_rows / aggregated_input_rows) * (current_reduction - 1);
                double min_reduction =
                        STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

                //  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
                //    COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
                //  return estimated_reduction > min_reduction;
                _should_expand_hash_table = current_reduction > min_reduction;
                return _should_expand_hash_table;
            },
            _agg_data->method_variant);
}

Status StreamingAggSinkLocalState::_pre_agg_with_serialized_key(
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
    _places.resize(rows);

    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In either case we should always use the remaining space in the hash table
    // to avoid wasting memory.
    // But for fixed hash map, it never need to expand
    bool ret_flag = false;
    RETURN_IF_ERROR(std::visit(
            [&](auto&& agg_method) -> Status {
                if (auto& hash_tbl = *agg_method.hash_table;
                    hash_tbl.add_elem_size_overflow(rows)) {
                    /// If too much memory is used during the pre-aggregation stage,
                    /// it is better to output the data directly without performing further aggregation.
                    const bool used_too_much_memory =
                            (_parent->cast<StreamingAggSinkOperatorX>()
                                             ._external_agg_bytes_threshold > 0 &&
                             _memory_usage() > _parent->cast<StreamingAggSinkOperatorX>()
                                                       ._external_agg_bytes_threshold);
                    // do not try to do agg, just init and serialize directly return the out_block
                    if (!_should_expand_preagg_hash_tables() || used_too_much_memory) {
                        SCOPED_TIMER(_streaming_agg_timer);
                        ret_flag = true;

                        // will serialize value data to string column.
                        // non-nullable column(id in `_make_nullable_keys`)
                        // will be converted to nullable.
                        bool mem_reuse =
                                _dependency->make_nullable_keys().empty() && out_block->mem_reuse();

                        std::vector<vectorized::DataTypePtr> data_types;
                        vectorized::MutableColumns value_columns;
                        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
                            auto data_type = _shared_state->aggregate_evaluators[i]
                                                     ->function()
                                                     ->get_serialized_type();
                            if (mem_reuse) {
                                value_columns.emplace_back(
                                        std::move(*out_block->get_by_position(i + key_size).column)
                                                .mutate());
                            } else {
                                // slot type of value it should always be string type
                                value_columns.emplace_back(_shared_state->aggregate_evaluators[i]
                                                                   ->function()
                                                                   ->create_serialize_column());
                            }
                            data_types.emplace_back(data_type);
                        }

                        for (int i = 0; i != _shared_state->aggregate_evaluators.size(); ++i) {
                            SCOPED_TIMER(_serialize_data_timer);
                            RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]
                                                    ->streaming_agg_serialize_to_column(
                                                            in_block, value_columns[i], rows,
                                                            _agg_arena_pool));
                        }

                        if (!mem_reuse) {
                            vectorized::ColumnsWithTypeAndName columns_with_schema;
                            for (int i = 0; i < key_size; ++i) {
                                columns_with_schema.emplace_back(
                                        key_columns[i]->clone_resized(rows),
                                        _shared_state->probe_expr_ctxs[i]->root()->data_type(),
                                        _shared_state->probe_expr_ctxs[i]->root()->expr_name());
                            }
                            for (int i = 0; i < value_columns.size(); ++i) {
                                columns_with_schema.emplace_back(std::move(value_columns[i]),
                                                                 data_types[i], "");
                            }
                            out_block->swap(vectorized::Block(columns_with_schema));
                        } else {
                            for (int i = 0; i < key_size; ++i) {
                                std::move(*out_block->get_by_position(i).column)
                                        .mutate()
                                        ->insert_range_from(*key_columns[i], 0, rows);
                            }
                        }
                    }
                }
                return Status::OK();
            },
            _agg_data->method_variant));

    if (!ret_flag) {
        RETURN_IF_CATCH_EXCEPTION(_emplace_into_hash_table(_places.data(), key_columns, rows));

        for (int i = 0; i < _shared_state->aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_shared_state->aggregate_evaluators[i]->execute_batch_add(
                    in_block, _dependency->offsets_of_aggregate_states()[i], _places.data(),
                    _agg_arena_pool, _should_expand_hash_table));
        }
    }

    return Status::OK();
}

StreamingAggSinkOperatorX::StreamingAggSinkOperatorX(ObjectPool* pool, int operator_id,
                                                     const TPlanNode& tnode,
                                                     const DescriptorTbl& descs)
        : AggSinkOperatorX<StreamingAggSinkLocalState>(pool, operator_id, tnode, descs) {}

Status StreamingAggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(AggSinkOperatorX<StreamingAggSinkLocalState>::init(tnode, state));
    _name = "STREAMING_AGGREGATION_SINK_OPERATOR";
    return Status::OK();
}

Status StreamingAggSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                       SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    local_state._shared_state->input_num_rows += in_block->rows();
    Status ret = Status::OK();
    if (in_block && in_block->rows() > 0) {
        auto block_from_ctx = local_state._shared_state->data_queue->get_free_block();
        RETURN_IF_ERROR(local_state.do_pre_agg(in_block, block_from_ctx.get()));
        if (block_from_ctx->rows() == 0) {
            local_state._shared_state->data_queue->push_free_block(std::move(block_from_ctx));
        } else {
            local_state._shared_state->data_queue->push_block(std::move(block_from_ctx));
        }
    }

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        local_state._shared_state->data_queue->set_finish();
    }
    return Status::OK();
}

Status StreamingAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    if (_shared_state->data_queue && !_shared_state->data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _shared_state->data_queue->set_canceled();
    }
    if (_shared_state->data_queue) {
        COUNTER_SET(_queue_size_counter, _shared_state->data_queue->max_size_of_queue());
        COUNTER_SET(_queue_byte_size_counter, _shared_state->data_queue->max_bytes_in_queue());
    }
    _preagg_block.clear();
    return Base::close(state, exec_status);
}

} // namespace doris::pipeline
