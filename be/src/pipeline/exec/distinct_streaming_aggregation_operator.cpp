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

#include "distinct_streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

DistinctStreamingAggLocalState::DistinctStreamingAggLocalState(RuntimeState* state,
                                                               OperatorXBase* parent)
        : PipelineXLocalState<FakeSharedState>(state, parent),
          dummy_mapped_data(std::make_shared<char>('A')),
          _agg_arena_pool(std::make_unique<vectorized::Arena>()),
          _agg_data(std::make_unique<vectorized::AggregatedDataVariants>()),
          _agg_profile_arena(std::make_unique<vectorized::Arena>()),
          _child_block(vectorized::Block::create_unique()),
          _child_source_state(SourceState::DEPEND_ON_SOURCE),
          _aggregated_block(vectorized::Block::create_unique()) {}

Status DistinctStreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    auto& p = Base::_parent->template cast<DistinctStreamingAggOperatorX>();
    for (auto& evaluator : p._aggregate_evaluators) {
        _aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }

    _build_timer = ADD_TIMER(Base::profile(), "BuildTime");
    _exec_timer = ADD_TIMER(Base::profile(), "ExecTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);

    if (_probe_expr_ctxs.empty()) {
        _agg_data->without_key = reinterpret_cast<vectorized::AggregateDataPtr>(
                _agg_profile_arena->alloc(p._total_size_of_aggregate_states));
    } else {
        _init_hash_method(_probe_expr_ctxs);
    }
    return Status::OK();
}

void DistinctStreamingAggLocalState::_init_hash_method(
        const vectorized::VExprContextSPtrs& probe_exprs) {
    DCHECK(probe_exprs.size() >= 1);

    using Type = vectorized::AggregatedDataVariants::Type;
    Type t(Type::serialized);

    if (probe_exprs.size() == 1) {
        auto is_nullable = probe_exprs[0]->root()->is_nullable();
        PrimitiveType type = probe_exprs[0]->root()->result_type();
        switch (type) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I: {
            size_t size = get_primitive_type_size(type);
            if (size == 1) {
                t = Type::int8_key;
            } else if (size == 2) {
                t = Type::int16_key;
            } else if (size == 4) {
                t = Type::int32_key;
            } else if (size == 8) {
                t = Type::int64_key;
            } else if (size == 16) {
                t = Type::int128_key;
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "meet invalid type size, size={}, type={}", size,
                                type_to_string(type));
            }
            break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            t = Type::string_key;
            break;
        }
        default:
            t = Type::serialized;
        }

        _agg_data->init(get_hash_key_type_with_phase(
                                t, !Base::_parent->template cast<DistinctStreamingAggOperatorX>()
                                            ._is_first_phase),
                        is_nullable);
    } else {
        if (!try_get_hash_map_context_fixed<PHNormalHashMap, HashCRC32,
                                            vectorized::AggregateDataPtr>(_agg_data->method_variant,
                                                                          probe_exprs)) {
            _agg_data->init(Type::serialized);
        }
    }
}

Status DistinctStreamingAggLocalState::_distinct_pre_agg_with_serialized_key(
        doris::vectorized::Block* in_block, doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
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

    bool mem_reuse = _parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys.empty() &&
                     out_block->mem_reuse();
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
            columns_with_schema.emplace_back(std::move(distinct_column),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        out_block->swap(vectorized::Block(columns_with_schema));
    }
    return Status::OK();
}

void DistinctStreamingAggLocalState::_make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : Base::_parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void DistinctStreamingAggLocalState::_emplace_into_hash_table_to_distinct(
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

DistinctStreamingAggOperatorX::DistinctStreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                                             const TPlanNode& tnode,
                                                             const DescriptorTbl& descs)
        : StatefulOperatorX<DistinctStreamingAggLocalState>(pool, tnode, operator_id, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
            DCHECK(_limit == -1) << "Preaggs have no limits";
        }
    } else {
        _is_streaming_preagg = false;
    }
}

Status DistinctStreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        vectorized::AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    _op_name = "DISTINCT_STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::prepare(state));
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child_x->row_desc()));

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, _child_x->row_desc(), intermediate_slot_desc, output_slot_desc));
    }

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        const auto& agg_function = _aggregate_evaluators[i]->function();
        _total_size_of_aggregate_states += agg_function->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _aggregate_evaluators.size()) {
            size_t alignment_of_next_state =
                    _aggregate_evaluators[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }

    return Status::OK();
}

Status DistinctStreamingAggOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    return Status::OK();
}

Status DistinctStreamingAggOperatorX::push(RuntimeState* state, vectorized::Block* in_block,
                                           SourceState source_state) const {
    auto& local_state = get_local_state(state);
    local_state._input_num_rows += in_block->rows();
    Status ret = Status::OK();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._distinct_pre_agg_with_serialized_key(
                in_block, local_state._aggregated_block.get()));

        // get enough data or reached limit rows, need push block to queue
        if (_limit != -1 &&
            (local_state._aggregated_block->rows() + local_state._output_distinct_rows) >= _limit) {
            auto limit_rows = _limit - local_state._output_distinct_rows;
            local_state._aggregated_block->set_num_rows(limit_rows);
            local_state._output_distinct_rows += limit_rows;
        } else if (local_state._aggregated_block->rows() >= state->batch_size()) {
            local_state._output_distinct_rows += local_state._aggregated_block->rows();
        }
    }

    // reach limit or source finish
    if ((UNLIKELY(source_state == SourceState::FINISHED)) ||
        (_limit != -1 && local_state._output_distinct_rows >= _limit)) {
        local_state._output_distinct_rows += local_state._aggregated_block->rows();
        return Status::OK(); // need given finish signal
    }
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::pull(RuntimeState* state, vectorized::Block* block,
                                           SourceState& source_state) const {
    auto& local_state = get_local_state(state);
    if (!local_state._aggregated_block->empty()) {
        block->swap(*local_state._aggregated_block);
        local_state._aggregated_block->clear_column_data(block->columns());
    }

    local_state._make_nullable_output_key(block);
    if (_is_streaming_preagg == false) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(
                vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    }

    if (UNLIKELY(local_state._child_source_state == SourceState::FINISHED ||
                 (_limit != -1 && local_state._output_distinct_rows >= _limit))) {
        source_state = SourceState::FINISHED;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }
    return Status::OK();
}

bool DistinctStreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._aggregated_block->empty() &&
           local_state._child_source_state != SourceState::FINISHED &&
           (_limit == -1 || local_state._output_distinct_rows < _limit);
}

Status DistinctStreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    _aggregated_block->clear();
    return Base::close(state);
}

} // namespace doris::pipeline
