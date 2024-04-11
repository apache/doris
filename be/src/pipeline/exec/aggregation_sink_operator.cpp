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

#include "aggregation_sink_operator.h"

#include <string>

#include "pipeline/exec/distinct_streaming_aggregation_sink_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/streaming_aggregation_sink_operator.h"
#include "runtime/primitive_type.h"
#include "vec/common/hash_table/hash.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(AggSinkOperator, StreamingOperator)

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
AggSinkLocalState::AggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : Base(parent, state), _agg_helper(this) {}

Status AggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    _agg_data = Base::_shared_state->agg_data.get();
    _agg_arena_pool = Base::_shared_state->agg_arena_pool.get();
    auto& p = Base::_parent->template cast<AggSinkOperatorX>();
    Base::_shared_state->align_aggregate_states = p._align_aggregate_states;
    Base::_shared_state->total_size_of_aggregate_states = p._total_size_of_aggregate_states;
    Base::_shared_state->offsets_of_aggregate_states = p.offsets_of_aggregate_states;
    Base::_shared_state->make_nullable_keys = p._make_nullable_keys;
    for (auto& evaluator : p._aggregate_evaluators) {
        Base::_shared_state->aggregate_evaluators.push_back(evaluator->clone(state, p._pool));
    }
    Base::_shared_state->probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < Base::_shared_state->probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                p._probe_expr_ctxs[i]->clone(state, Base::_shared_state->probe_expr_ctxs[i]));
    }
    _hash_table_memory_usage = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "HashTable",
                                                            TUnit::BYTES, "MemoryUsage", 1);
    _serialize_key_arena_memory_usage = Base::profile()->AddHighWaterMarkCounter(
            "SerializeKeyArena", TUnit::BYTES, "MemoryUsage", 1);

    _build_timer = ADD_TIMER(Base::profile(), "BuildTime");
    _serialize_key_timer = ADD_TIMER(Base::profile(), "SerializeKeyTime");
    _exec_timer = ADD_TIMER(Base::profile(), "ExecTime");
    _merge_timer = ADD_TIMER(Base::profile(), "MergeTime");
    _expr_timer = ADD_TIMER(Base::profile(), "ExprTime");
    _serialize_data_timer = ADD_TIMER(Base::profile(), "SerializeDataTime");
    _deserialize_data_timer = ADD_TIMER(Base::profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);
    _max_row_size_counter = ADD_COUNTER(Base::profile(), "MaxRowSizeInBytes", TUnit::UNIT);
    COUNTER_SET(_max_row_size_counter, (int64_t)0);

    for (auto& evaluator : Base::_shared_state->aggregate_evaluators) {
        evaluator->set_timer(_merge_timer, _expr_timer);
    }

    Base::_shared_state->agg_profile_arena = std::make_unique<vectorized::Arena>();

    if (Base::_shared_state->probe_expr_ctxs.empty()) {
        _agg_data->without_key = reinterpret_cast<vectorized::AggregateDataPtr>(
                Base::_shared_state->agg_profile_arena->alloc(p._total_size_of_aggregate_states));

        if (p._is_merge) {
            _executor = std::make_unique<Executor<true, true>>();
        } else {
            _executor = std::make_unique<Executor<true, false>>();
        }
    } else {
        _agg_helper._init_hash_method(Base::_shared_state->probe_expr_ctxs);

        std::visit(
                [&](auto&& agg_method) {
                    using HashTableType = std::decay_t<decltype(agg_method)>;
                    using KeyType = typename HashTableType::Key;

                    /// some aggregate functions (like AVG for decimal) have align issues.
                    Base::_shared_state->aggregate_data_container.reset(
                            new vectorized::AggregateDataContainer(
                                    sizeof(KeyType), ((p._total_size_of_aggregate_states +
                                                       p._align_aggregate_states - 1) /
                                                      p._align_aggregate_states) *
                                                             p._align_aggregate_states));
                },
                _agg_data->method_variant);
        if (p._is_merge) {
            _executor = std::make_unique<Executor<false, true>>();
        } else {
            _executor = std::make_unique<Executor<false, false>>();
        }

        _should_limit_output = p._limit != -1 &&       // has limit
                               (!p._have_conjuncts) && // no having conjunct
                               p._needs_finalize;      // agg's finalize step
    }

    return Status::OK();
}

Status AggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    _agg_data = Base::_shared_state->agg_data.get();
    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (Base::_shared_state->probe_expr_ctxs.empty()) {
        // _create_agg_status may acquire a lot of memory, may allocate failed when memory is very few
        RETURN_IF_CATCH_EXCEPTION(
                static_cast<void>(_agg_helper._create_agg_status(_agg_data->without_key)));
    }
    Base::_shared_state->ready_to_execute = true;
    return Status::OK();
}

size_t AggSinkLocalState::_memory_usage() const {
    if (0 == _agg_helper._get_hash_table_size()) {
        return 0;
    }
    size_t usage = 0;
    if (_agg_arena_pool) {
        usage += _agg_arena_pool->size();
    }

    if (Base::_shared_state->aggregate_data_container) {
        usage += Base::_shared_state->aggregate_data_container->memory_usage();
    }

    std::visit(
            [&](auto&& agg_method) -> void {
                auto data = agg_method.hash_table;
                usage += data->get_buffer_size_in_bytes();
            },
            _agg_data->method_variant);

    return usage;
}

AggSinkOperatorX::AggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                   const DescriptorTbl& descs)
        : DataSinkOperatorX<AggSinkLocalState>(operator_id, tnode.node_id),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _pool(pool),
          _limit(tnode.limit),
          _have_conjuncts((tnode.__isset.vconjunct && !tnode.vconjunct.nodes.empty()) ||
                          (tnode.__isset.conjuncts && !tnode.conjuncts.empty())),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {}

Status AggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AggSinkLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            vectorized::VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit =
            tnode.agg_node.aggregate_functions.empty() && state->enable_pipeline_exec();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        vectorized::AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(vectorized::AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _external_agg_bytes_threshold = state->external_agg_bytes_threshold();

    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });

    return Status::OK();
}

Status AggSinkOperatorX::prepare(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(vectorized::VExpr::prepare(
            _probe_expr_ctxs, state, DataSinkOperatorX<AggSinkLocalState>::_child_x->row_desc()));

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
                state, DataSinkOperatorX<AggSinkLocalState>::_child_x->row_desc(),
                intermediate_slot_desc, output_slot_desc));
    }

    offsets_of_aggregate_states.resize(_aggregate_evaluators.size());

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;

        const auto& agg_function = _aggregate_evaluators[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
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

Status AggSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    return Status::OK();
}

Status AggSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    local_state._shared_state->input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._executor->execute(&local_state, in_block));
        local_state._executor->update_memusage(&local_state);
    }
    if (eos) {
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t AggSinkOperatorX::get_revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._memory_usage();
}

Status AggSinkOperatorX::reset_hash_table(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto& ss = *local_state.Base::_shared_state;
    RETURN_IF_ERROR(ss.reset_hash_table());
    local_state._agg_arena_pool = ss.agg_arena_pool.get();
    return Status::OK();
}

Status AggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    _preagg_block.clear();
    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    Base::_mem_tracker->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
    return Base::close(state, exec_status);
}

} // namespace doris::pipeline
