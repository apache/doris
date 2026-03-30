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

#include "exec/operator/streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "exec/common/groupby_agg_context.h"
#include "exec/common/inline_count_agg_context.h"
#include "exec/operator/operator.h"
#include "exprs/aggregate/aggregate_function_count.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vslot_ref.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
} // namespace doris

namespace doris {

StreamingAggLocalState::StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _child_block(Block::create_unique()),
          _pre_aggregated_block(Block::create_unique()),
          _is_single_backend(state->get_query_ctx()->is_single_backend_query()) {}

StreamingAggLocalState::~StreamingAggLocalState() = default;

Status StreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _streaming_agg_timer = ADD_TIMER(custom_profile(), "StreamingAggTime");
    return Status::OK();
}

Status StreamingAggLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();

    // Clone evaluators and probe expression contexts
    std::vector<AggFnEvaluator*> evaluators;
    for (auto& evaluator : p._aggregate_evaluators) {
        evaluators.push_back(evaluator->clone(state, p._pool));
    }
    VExprContextSPtrs probe_expr_ctxs(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, probe_expr_ctxs[i]));
    }

    DCHECK(!probe_expr_ctxs.empty());

    // Determine whether to use simple count aggregation.
    // Note: Unlike AggSink, we do not check !enable_spill here because
    // StreamingAgg never calls merge_for_spill() — it only runs update+serialize.
    bool use_simple_count = evaluators.size() == 1 &&
                            evaluators[0]->function()->is_simple_count() && p._sort_limit == -1;
#ifndef NDEBUG
    // Randomly disable simple count in debug mode to test demotion correctness.
    // Only demote (true→false), never promote (false→true).
    if (use_simple_count) {
        use_simple_count = rand() % 2 == 0;
    }
#endif

    if (use_simple_count) {
        _groupby_agg_ctx = std::make_unique<InlineCountAggContext>(
                std::move(evaluators), std::move(probe_expr_ctxs), p._offsets_of_aggregate_states,
                p._total_size_of_aggregate_states, p._align_aggregate_states, p._is_first_phase);
    } else {
        _groupby_agg_ctx = std::make_unique<GroupByAggContext>(
                std::move(evaluators), std::move(probe_expr_ctxs), p._offsets_of_aggregate_states,
                p._total_size_of_aggregate_states, p._align_aggregate_states, p._is_first_phase);
    }

    // Configure sort-limit on context
    _groupby_agg_ctx->limit = p._sort_limit;
    _groupby_agg_ctx->do_sort_limit = p._do_sort_limit;
    _groupby_agg_ctx->order_directions = p._order_directions;
    _groupby_agg_ctx->null_directions = p._null_directions;

    _groupby_agg_ctx->init_hash_method();
    _groupby_agg_ctx->init_agg_data_container();
    // StreamingAgg combines sink and source in a single operator, so both profile inits
    // use the same profile object. ADD_TIMER returns the same counter for duplicate names,
    // which is the intended behavior here.
    _groupby_agg_ctx->init_sink_profile(custom_profile());
    _groupby_agg_ctx->init_source_profile(custom_profile());

    for (auto& evaluator : _groupby_agg_ctx->agg_evaluators()) {
        evaluator->set_timer(_groupby_agg_ctx->merge_timer(), _groupby_agg_ctx->expr_timer());
    }

    return Status::OK();
}

Status StreamingAggLocalState::do_pre_agg(RuntimeState* state, Block* input_block,
                                          Block* output_block) {
    if (low_memory_mode()) {
        auto& p = Base::_parent->template cast<StreamingAggOperatorX>();
        p.set_low_memory_mode(state);
    }
    RETURN_IF_ERROR(_pre_agg_with_serialized_key(input_block, output_block));

    // pre stream agg need use _num_row_return to decide whether to do pre stream agg
    _cur_num_rows_returned += output_block->rows();
    make_nullable_output_key(output_block);
    _groupby_agg_ctx->update_memusage();
    return Status::OK();
}

Status StreamingAggLocalState::_pre_agg_with_serialized_key(doris::Block* in_block,
                                                            doris::Block* out_block) {
    SCOPED_TIMER(_groupby_agg_ctx->build_timer());
    DCHECK(!_groupby_agg_ctx->groupby_expr_ctxs().empty());

    auto& p = Base::_parent->template cast<StreamingAggOperatorX>();

    size_t key_size = _groupby_agg_ctx->groupby_expr_ctxs().size();
    ColumnRawPtrs key_columns(key_size);
    RETURN_IF_ERROR(_groupby_agg_ctx->evaluate_groupby_keys(in_block, key_columns));

    uint32_t rows = (uint32_t)in_block->rows();

    if (_groupby_agg_ctx->should_skip_preagg(rows, p._spill_streaming_agg_mem_limit,
                                             _input_num_rows, _cur_num_rows_returned,
                                             _is_single_backend)) {
        // Sort limit handling for passthrough path
        if (_groupby_agg_ctx->limit > 0) {
            DCHECK(_groupby_agg_ctx->do_sort_limit);
            if (_need_do_sort_limit == -1) {
                const size_t hash_table_size = _groupby_agg_ctx->hash_table_size();
                _need_do_sort_limit = hash_table_size >= _groupby_agg_ctx->limit ? 1 : 0;
                if (_need_do_sort_limit == 1) {
                    _groupby_agg_ctx->build_limit_heap(hash_table_size);
                }
            }

            if (_need_do_sort_limit == 1) {
                if (_groupby_agg_ctx->do_limit_filter(rows, key_columns)) {
                    auto& need_computes = _groupby_agg_ctx->need_computes();
                    bool need_filter = std::find(need_computes.begin(), need_computes.end(), 1) !=
                                       need_computes.end();
                    if (need_filter) {
                        _groupby_agg_ctx->add_limit_heap_top(key_columns, rows);
                        Block::filter_block_internal(in_block, need_computes);
                        rows = (uint32_t)in_block->rows();
                    } else {
                        return Status::OK();
                    }
                }
            }
        }

        // Passthrough serialize: delegate to context
        bool mem_reuse = p._make_nullable_keys.empty() && out_block->mem_reuse();
        RETURN_IF_ERROR(_groupby_agg_ctx->streaming_serialize_passthrough(
                in_block, out_block, key_columns, rows, mem_reuse));
    } else {
        // Aggregation path: delegate to context
        if (_need_do_sort_limit != 1) {
            RETURN_IF_ERROR(
                    _groupby_agg_ctx->preagg_emplace_and_forward(key_columns, rows, in_block));
        } else {
            RETURN_IF_ERROR(
                    _groupby_agg_ctx->emplace_and_forward_limit(in_block, key_columns, rows));
        }
        if (_groupby_agg_ctx->limit > 0 && _need_do_sort_limit == -1 &&
            _groupby_agg_ctx->hash_table_size() >= _groupby_agg_ctx->limit) {
            _need_do_sort_limit = 1;
            _groupby_agg_ctx->build_limit_heap(_groupby_agg_ctx->hash_table_size());
        }
    }

    return Status::OK();
}

void StreamingAggLocalState::make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _parent->cast<StreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

StreamingAggOperatorX::StreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                             const TPlanNode& tnode, const DescriptorTbl& descs)
        : StatefulOperatorX<StreamingAggLocalState>(pool, tnode, operator_id, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _agg_fn_output_row_descriptor(descs, tnode.row_tuples) {}

void StreamingAggOperatorX::update_operator(const TPlanNode& tnode,
                                            bool followed_by_shuffled_operator,
                                            bool require_bucket_distribution) {
    _followed_by_shuffled_operator = followed_by_shuffled_operator;
    _require_bucket_distribution = require_bucket_distribution;
    _partition_exprs =
            tnode.__isset.distribute_expr_lists &&
                            (StatefulOperatorX<
                                     StreamingAggLocalState>::_followed_by_shuffled_operator ||
                             std::any_of(
                                     tnode.agg_node.aggregate_functions.begin(),
                                     tnode.agg_node.aggregate_functions.end(),
                                     [](const TExpr& texpr) -> bool {
                                         return texpr.nodes[0].fn.name.function_name.starts_with(
                                                 DISTINCT_FUNCTION_PREFIX);
                                     }))
                    ? tnode.distribute_expr_lists[0]
                    : tnode.agg_node.grouping_exprs;
}

Status StreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit = tnode.agg_node.aggregate_functions.empty();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                tnode.agg_node.grouping_exprs.empty(), false, &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    if (state->enable_spill()) {
        // If spill enabled, the streaming agg should not occupy too much memory.
        _spill_streaming_agg_mem_limit =
                state->query_options().__isset.spill_streaming_agg_mem_limit
                        ? state->query_options().spill_streaming_agg_mem_limit
                        : 0;
    } else {
        _spill_streaming_agg_mem_limit = 0;
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    auto is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                                [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    if (is_merge || _needs_finalize) {
        return Status::InvalidArgument(
                "StreamingAggLocalState only support no merge and no finalize, "
                "but got is_merge={}, needs_finalize={}",
                is_merge, _needs_finalize);
    }

    // Handle sort limit
    if (tnode.agg_node.__isset.agg_sort_info_by_group_key) {
        _sort_limit = _limit;
        _limit = -1;
        _do_sort_limit = true;
        const auto& agg_sort_info = tnode.agg_node.agg_sort_info_by_group_key;
        DCHECK_EQ(agg_sort_info.nulls_first.size(), agg_sort_info.is_asc_order.size());

        const size_t order_by_key_size = agg_sort_info.is_asc_order.size();
        _order_directions.resize(order_by_key_size);
        _null_directions.resize(order_by_key_size);
        for (int i = 0; i < order_by_key_size; ++i) {
            _order_directions[i] = agg_sort_info.is_asc_order[i] ? 1 : -1;
            _null_directions[i] =
                    agg_sort_info.nulls_first[i] ? -_order_directions[i] : _order_directions[i];
        }
    }

    _op_name = "STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status StreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<StreamingAggLocalState>::prepare(state));

    RETURN_IF_ERROR(_init_probe_expr_ctx(state));

    RETURN_IF_ERROR(_init_aggregate_evaluators(state));

    RETURN_IF_ERROR(_calc_aggregate_evaluators());

    return Status::OK();
}

Status StreamingAggOperatorX::_init_probe_expr_ctx(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    return Status::OK();
}

Status StreamingAggOperatorX::_init_aggregate_evaluators(RuntimeState* state) {
    size_t j = _probe_expr_ctxs.size();
    for (size_t i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, _child->row_desc(), intermediate_slot_desc, output_slot_desc));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }
    return Status::OK();
}

Status StreamingAggOperatorX::_calc_aggregate_evaluators() {
    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;

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

Status StreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    _pre_aggregated_block->clear();

    if (_groupby_agg_ctx) {
        _groupby_agg_ctx->close();
        _groupby_agg_ctx.reset();
    }
    return Base::close(state);
}

Status StreamingAggOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    if (!local_state._pre_aggregated_block->empty()) {
        local_state._pre_aggregated_block->swap(*block);
    } else {
        RETURN_IF_ERROR(local_state._groupby_agg_ctx->serialize(state, block, eos));
        local_state.make_nullable_output_key(block);
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    }
    local_state.reached_limit(block, eos);

    return Status::OK();
}

Status StreamingAggOperatorX::push(RuntimeState* state, Block* in_block, bool eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);

    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(
                local_state.do_pre_agg(state, in_block, local_state._pre_aggregated_block.get()));
    }
    in_block->clear_column_data(_child->row_desc().num_materialized_slots());
    return Status::OK();
}

bool StreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._pre_aggregated_block->empty() && !local_state._child_eos;
}

#include "common/compile_check_end.h"
} // namespace doris
