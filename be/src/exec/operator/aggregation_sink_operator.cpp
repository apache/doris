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

#include "exec/operator/aggregation_sink_operator.h"

#include <memory>
#include <string>

#include "common/cast_set.h"
#include "common/status.h"
#include "exec/common/groupby_agg_context.h"
#include "exec/common/inline_count_agg_context.h"
#include "exec/common/ungroupby_agg_context.h"
#include "exec/operator/operator.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/vectorized_agg_fn.h"
#include "runtime/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
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
        : Base(parent, state) {}

Status AggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    return Status::OK();
}

Status AggSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<AggSinkOperatorX>();
    Base::_shared_state->make_nullable_keys = p._make_nullable_keys;

    if (p._probe_expr_ctxs.empty()) {
        // ── Without GROUP BY → create UngroupByAggContext ──
        std::vector<AggFnEvaluator*> evaluators;
        for (auto& evaluator : p._aggregate_evaluators) {
            evaluators.push_back(evaluator->clone(state, p._pool));
        }
        auto ctx = std::make_unique<UngroupByAggContext>(
                std::move(evaluators), p._offsets_of_aggregate_states,
                p._total_size_of_aggregate_states, p._align_aggregate_states);
        ctx->init_profile(custom_profile());
        for (auto& evaluator : ctx->agg_evaluators()) {
            evaluator->set_timer(ctx->merge_timer(), nullptr);
        }
        Base::_shared_state->agg_ctx = std::move(ctx);
    } else {
        // ── With GROUP BY → create GroupByAggContext or InlineCountAggContext ──
        VExprContextSPtrs probe_expr_ctxs(p._probe_expr_ctxs.size());
        for (size_t i = 0; i < probe_expr_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, probe_expr_ctxs[i]));
        }
        std::vector<AggFnEvaluator*> evaluators;
        for (auto& evaluator : p._aggregate_evaluators) {
            evaluators.push_back(evaluator->clone(state, p._pool));
        }

        bool should_limit_output =
                p._limit != -1 && !p._have_conjuncts && !Base::_shared_state->enable_spill;
        bool use_simple_count = p._aggregate_evaluators.size() == 1 &&
                                p._aggregate_evaluators[0]->function()->is_simple_count() &&
                                !should_limit_output && !Base::_shared_state->enable_spill;
#ifndef NDEBUG
        // Randomly enable/disable in debug mode to verify correctness of multi-phase agg promotion/demotion.
        if (use_simple_count) {
            use_simple_count = rand() % 2 == 0;
        }
#endif

        std::unique_ptr<GroupByAggContext> ctx;
        if (use_simple_count) {
            ctx = std::make_unique<InlineCountAggContext>(
                    std::move(evaluators), std::move(probe_expr_ctxs),
                    p._offsets_of_aggregate_states, p._total_size_of_aggregate_states,
                    p._align_aggregate_states, p._is_first_phase);
        } else {
            ctx = std::make_unique<GroupByAggContext>(
                    std::move(evaluators), std::move(probe_expr_ctxs),
                    p._offsets_of_aggregate_states, p._total_size_of_aggregate_states,
                    p._align_aggregate_states, p._is_first_phase);
        }

        ctx->limit = p._limit;
        ctx->do_sort_limit = p._do_sort_limit;
        ctx->order_directions = p._order_directions;
        ctx->null_directions = p._null_directions;
        ctx->should_limit_output = should_limit_output;
        ctx->enable_spill = Base::_shared_state->enable_spill;
        ctx->make_nullable_keys = p._make_nullable_keys;

        ctx->init_hash_method();
        ctx->init_agg_data_container();
        ctx->init_sink_profile(custom_profile());
        for (auto& evaluator : ctx->agg_evaluators()) {
            evaluator->set_timer(ctx->merge_timer(), ctx->expr_timer());
        }

        Base::_shared_state->agg_ctx = std::move(ctx);
    }
    return Status::OK();
}

bool AggSinkLocalState::is_blockable() const {
    auto* ctx = Base::_shared_state->agg_ctx.get();
    if (!ctx) {
        return false;
    }
    return std::any_of(ctx->agg_evaluators().begin(), ctx->agg_evaluators().end(),
                       [](const AggFnEvaluator* evaluator) { return evaluator->is_blockable(); });
}

// TODO: Tricky processing if `multi_distinct_` exists which will be re-planed by optimizer.
AggSinkOperatorX::AggSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                   const TPlanNode& tnode, const DescriptorTbl& descs)
        : DataSinkOperatorX<AggSinkLocalState>(operator_id, tnode, dest_id),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _pool(pool),
          _limit(tnode.limit),
          _have_conjuncts(tnode.__isset.conjuncts && !tnode.conjuncts.empty()),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate),
          _agg_fn_output_row_descriptor(descs, tnode.row_tuples) {}

void AggSinkOperatorX::update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                                       bool require_bucket_distribution) {
    _followed_by_shuffled_operator = followed_by_shuffled_operator;
    _require_bucket_distribution = require_bucket_distribution;
    _partition_exprs =
            tnode.__isset.distribute_expr_lists &&
                            (_followed_by_shuffled_operator ||
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

Status AggSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AggSinkLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                tnode.agg_node.grouping_exprs.empty(), false, &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    if (tnode.agg_node.__isset.agg_sort_info_by_group_key) {
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

    return Status::OK();
}

Status AggSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<AggSinkLocalState>::prepare(state));

    RETURN_IF_ERROR(_init_probe_expr_ctx(state));

    RETURN_IF_ERROR(_init_aggregate_evaluators(state));

    RETURN_IF_ERROR(_calc_aggregate_evaluators());

    RETURN_IF_ERROR(_check_agg_fn_output());

    return Status::OK();
}

Status AggSinkOperatorX::_init_probe_expr_ctx(RuntimeState* state) {
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state,
                                   DataSinkOperatorX<AggSinkLocalState>::_child->row_desc()));

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    return Status::OK();
}

Status AggSinkOperatorX::_init_aggregate_evaluators(RuntimeState* state) {
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
                state, DataSinkOperatorX<AggSinkLocalState>::_child->row_desc(),
                intermediate_slot_desc, output_slot_desc));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    for (auto& _aggregate_evaluator : _aggregate_evaluators) {
        RETURN_IF_ERROR(_aggregate_evaluator->open(state));
    }
    return Status::OK();
}

Status AggSinkOperatorX::_calc_aggregate_evaluators() {
    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());
    _is_merge = false;
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
        if (_aggregate_evaluators[i]->is_merge()) {
            _is_merge = true;
        }
    }
    return Status::OK();
}

Status AggSinkOperatorX::_check_agg_fn_output() {
    if (_needs_finalize) {
        RETURN_IF_ERROR(AggFnEvaluator::check_agg_fn_output(
                cast_set<uint32_t>(_probe_expr_ctxs.size()), _aggregate_evaluators,
                _agg_fn_output_row_descriptor));
    }
    return Status::OK();
}

Status AggSinkOperatorX::sink(doris::RuntimeState* state, Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        auto* ctx = local_state.Base::_shared_state->agg_ctx.get();
        if (_is_merge) {
            RETURN_IF_ERROR(ctx->merge(in_block));
        } else {
            RETURN_IF_ERROR(ctx->update(in_block));
        }
        ctx->update_memusage();
    }
    if (eos) {
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

AggregatedDataVariants* AggSinkOperatorX::get_agg_data(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto* ctx = static_cast<GroupByAggContext*>(local_state.Base::_shared_state->agg_ctx.get());
    return ctx->hash_table_data();
}

size_t AggSinkOperatorX::get_revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state.Base::_shared_state->agg_ctx.get();
    return ctx ? ctx->memory_usage() : 0;
}

Status AggSinkOperatorX::reset_hash_table(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state.Base::_shared_state->agg_ctx.get();
    DCHECK(ctx);
    return ctx->reset_hash_table();
}

size_t AggSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state.Base::_shared_state->agg_ctx.get();
    return ctx ? ctx->get_reserve_mem_size(state) : 0;
}

size_t AggSinkOperatorX::get_hash_table_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state.Base::_shared_state->agg_ctx.get();
    return ctx ? ctx->hash_table_size() : 0;
}

Status AggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    _preagg_block.clear();
    return Base::close(state, exec_status);
}

} // namespace doris
