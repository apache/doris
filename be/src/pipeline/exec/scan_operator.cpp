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

#include "scan_operator.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>

#include <cstdint>
#include <memory>

#include "common/global_types.h"
#include "olap/null_predicate.h"
#include "olap/predicate_creator.h"
#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/group_commit_scan_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/meta_scan_operator.h"
#include "pipeline/exec/mock_scan_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "runtime_filter/runtime_filter_consumer_helper.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vtopn_pred.h"
#include "vec/functions/in.h"

namespace doris::pipeline {

#include "common/compile_check_begin.h"

#define RETURN_IF_PUSH_DOWN(stmt, status)    \
    if (pdt == PushDownType::UNACCEPTABLE) { \
        status = stmt;                       \
        if (!status.ok()) {                  \
            return;                          \
        }                                    \
    } else {                                 \
        return;                              \
    }

template <typename Derived>
bool ScanLocalState<Derived>::should_run_serial() const {
    return _parent->cast<typename Derived::Parent>()._should_run_serial;
}

Status ScanLocalStateBase::update_late_arrival_runtime_filter(RuntimeState* state,
                                                              int& arrived_rf_num) {
    // Lock needed because _conjuncts can be accessed concurrently by multiple scanner threads
    std::unique_lock lock(_conjuncts_lock);
    RETURN_IF_ERROR(_helper.try_append_late_arrival_runtime_filter(state, _parent->row_descriptor(),
                                                                   arrived_rf_num, _conjuncts));
    if (state->enable_adjust_conjunct_order_by_cost()) {
        std::ranges::sort(_conjuncts, [](const auto& a, const auto& b) {
            return a->execute_cost() < b->execute_cost();
        });
    };
    return Status::OK();
}

Status ScanLocalStateBase::clone_conjunct_ctxs(vectorized::VExprContextSPtrs& scanner_conjuncts) {
    // Lock needed because _conjuncts can be accessed concurrently by multiple scanner threads
    std::unique_lock lock(_conjuncts_lock);
    scanner_conjuncts.resize(_conjuncts.size());
    for (size_t i = 0; i != _conjuncts.size(); ++i) {
        RETURN_IF_ERROR(_conjuncts[i]->clone(_state, scanner_conjuncts[i]));
    }
    return Status::OK();
}

int ScanLocalStateBase::max_scanners_concurrency(RuntimeState* state) const {
    // For select * from table limit 10; should just use one thread.
    if (should_run_serial()) {
        return 1;
    }
    /*
     * The max concurrency of scanners for each ScanLocalStateBase is determined by:
     * 1. User specified max_scanners_concurrency which is set through session variable.
     * 2. Default: 4
     *
     * If this is a serial operator, the max concurrency should multiply by the number of parallel instances of the operator.
     */
    return (state->max_scanners_concurrency() > 0 ? state->max_scanners_concurrency() : 4) *
           (state->query_parallel_instance_num() / _parent->parallelism(state));
}

int ScanLocalStateBase::min_scanners_concurrency(RuntimeState* state) const {
    if (should_run_serial()) {
        return 1;
    }
    /*
     * The min concurrency of scanners for each ScanLocalStateBase is determined by:
     * 1. User specified min_scanners_concurrency which is set through session variable.
     * 2. Default: 1
     *
     * If this is a serial operator, the max concurrency should multiply by the number of parallel instances of the operator.
     */
    return (state->min_scanners_concurrency() > 0 ? state->min_scanners_concurrency() : 1) *
           (state->query_parallel_instance_num() / _parent->parallelism(state));
}

vectorized::ScannerScheduler* ScanLocalStateBase::scan_scheduler(RuntimeState* state) const {
    return state->get_query_ctx()->get_scan_scheduler();
}

template <typename Derived>
Status ScanLocalState<Derived>::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    _scan_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                 _parent->get_name() + "_DEPENDENCY");
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
            common_profile(), "WaitForDependency[" + _scan_dependency->name() + "]Time", 1);
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<typename Derived::Parent>();
    RETURN_IF_ERROR(_helper.init(state, p.is_serial_operator(), p.node_id(), p.operator_id(),
                                 _filter_dependencies, p.get_name() + "_FILTER_DEPENDENCY"));
    RETURN_IF_ERROR(_init_profile());
    set_scan_ranges(state, info.scan_ranges);

    _wait_for_rf_timer = ADD_TIMER(common_profile(), "WaitForRuntimeFilter");
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    RETURN_IF_ERROR(PipelineXLocalState<>::open(state));
    auto& p = _parent->cast<typename Derived::Parent>();

    // init id_file_map() for runtime state
    std::vector<SlotDescriptor*> slots = p._output_tuple_desc->slots();
    for (auto slot : slots) {
        if (slot->col_name().starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            state->set_id_file_map();
        }
    }

    _common_expr_ctxs_push_down.resize(p._common_expr_ctxs_push_down.size());
    for (size_t i = 0; i < _common_expr_ctxs_push_down.size(); i++) {
        RETURN_IF_ERROR(
                p._common_expr_ctxs_push_down[i]->clone(state, _common_expr_ctxs_push_down[i]));
    }
    RETURN_IF_ERROR(_helper.acquire_runtime_filter(state, _conjuncts, p.row_descriptor()));

    // Disable condition cache in topn filter valid. TODO:: Try to support the topn filter in condition cache
    if (state->query_options().condition_cache_digest && p._topn_filter_source_node_ids.empty()) {
        _condition_cache_digest = state->query_options().condition_cache_digest;
        for (auto& conjunct : _conjuncts) {
            _condition_cache_digest = conjunct->get_digest(_condition_cache_digest);
            if (!_condition_cache_digest) {
                break;
            }
        }
    } else {
        _condition_cache_digest = 0;
    }

    RETURN_IF_ERROR(_process_conjuncts(state));

    auto status = _eos ? Status::OK() : _prepare_scanners();
    RETURN_IF_ERROR(status);
    if (auto ctx = _scanner_ctx.load()) {
        DCHECK(!_eos && _num_scanners->value() > 0);
        RETURN_IF_ERROR(ctx->init());
    }
    _opened = true;
    return status;
}

static std::string predicates_to_string(
        const phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                slot_id_to_predicates) {
    fmt::memory_buffer debug_string_buffer;
    for (const auto& [slot_id, predicates] : slot_id_to_predicates) {
        if (predicates.empty()) {
            continue;
        }
        fmt::format_to(debug_string_buffer, "Slot ID: {}: [", slot_id);
        for (const auto& predicate : predicates) {
            fmt::format_to(debug_string_buffer, "{{{}}}, ", predicate->debug_string());
        }
        fmt::format_to(debug_string_buffer, "] ");
    }
    return fmt::to_string(debug_string_buffer);
}

static void init_slot_value_range(
        phmap::flat_hash_map<int, ColumnValueRangeType>& slot_id_to_value_range,
        SlotDescriptor* slot, const vectorized::DataTypePtr type_desc) {
    switch (type_desc->get_primitive_type()) {
#define M(NAME)                                                                        \
    case TYPE_##NAME: {                                                                \
        ColumnValueRange<TYPE_##NAME> range(slot->col_name(), slot->is_nullable(),     \
                                            cast_set<int>(type_desc->get_precision()), \
                                            cast_set<int>(type_desc->get_scale()));    \
        slot_id_to_value_range[slot->id()] = std::move(range);                         \
        break;                                                                         \
    }
#define APPLY_FOR_SCALAR_TYPE(M) \
    M(TINYINT)                   \
    M(SMALLINT)                  \
    M(INT)                       \
    M(BIGINT)                    \
    M(LARGEINT)                  \
    M(FLOAT)                     \
    M(DOUBLE)                    \
    M(CHAR)                      \
    M(DATE)                      \
    M(DATETIME)                  \
    M(DATEV2)                    \
    M(DATETIMEV2)                \
    M(TIMESTAMPTZ)               \
    M(VARCHAR)                   \
    M(STRING)                    \
    M(HLL)                       \
    M(DECIMAL32)                 \
    M(DECIMAL64)                 \
    M(DECIMAL128I)               \
    M(DECIMAL256)                \
    M(DECIMALV2)                 \
    M(BOOLEAN)                   \
    M(IPV4)                      \
    M(IPV6)
        APPLY_FOR_SCALAR_TYPE(M)
#undef M
    default: {
        break;
    }
    }
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_conjuncts(RuntimeState* state) {
    auto& p = _parent->cast<typename Derived::Parent>();
    // The conjuncts is always on output tuple, so use _output_tuple_desc;
    std::vector<SlotDescriptor*> slots = p._output_tuple_desc->slots();

    for (auto& slot : slots) {
        init_slot_value_range(_slot_id_to_value_range, slot, slot->type());
        _slot_id_to_predicates.insert(
                {slot->id(), std::vector<std::shared_ptr<ColumnPredicate>>()});
    }

    get_cast_types_for_variants();
    for (const auto& [colname, type] : _cast_types_for_variants) {
        auto* slot = p._slot_id_to_slot_desc[p._colname_to_slot_id[colname]];
        init_slot_value_range(_slot_id_to_value_range, slot, type);
        _slot_id_to_predicates.insert(
                {slot->id(), std::vector<std::shared_ptr<ColumnPredicate>>()});
    }

    RETURN_IF_ERROR(_get_topn_filters(state));

    for (auto it = _conjuncts.begin(); it != _conjuncts.end();) {
        auto& conjunct = *it;
        if (conjunct->root()) {
            vectorized::VExprSPtr new_root;
            RETURN_IF_ERROR(_normalize_predicate(conjunct.get(), conjunct->root(), new_root));
            if (new_root) {
                conjunct->set_root(new_root);
                if (_should_push_down_common_expr() &&
                    vectorized::VExpr::is_acting_on_a_slot(*(conjunct->root()))) {
                    _common_expr_ctxs_push_down.emplace_back(conjunct);
                    it = _conjuncts.erase(it);
                    continue;
                }
            } else { // All conjuncts are pushed down as predicate column
                _stale_expr_ctxs.emplace_back(
                        conjunct); // avoid function context and constant str being freed
                it = _conjuncts.erase(it);
                continue;
            }
        }
        ++it;
    }

    if (state->enable_profile()) {
        custom_profile()->add_info_string("PushDownPredicates",
                                          predicates_to_string(_slot_id_to_predicates));
        std::string message;
        for (auto& conjunct : _conjuncts) {
            if (conjunct->root()) {
                if (!message.empty()) {
                    message += ", ";
                }
                message += conjunct->root()->debug_string();
            }
        }
        custom_profile()->add_info_string("RemainedPredicates", message);
    }

    for (auto& it : _slot_id_to_value_range) {
        std::visit(
                [&](auto&& range) {
                    if (range.is_empty_value_range()) {
                        _eos = true;
                        _scan_dependency->set_ready();
                    }
                },
                it.second);
    }

    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_predicate(vectorized::VExprContext* context,
                                                     const vectorized::VExprSPtr& root,
                                                     vectorized::VExprSPtr& output_expr) {
    auto expr_root = root->is_rf_wrapper() ? root->get_impl() : root;
    PushDownType pdt = PushDownType::UNACCEPTABLE;
    if (dynamic_cast<vectorized::VirtualSlotRef*>(expr_root.get())) {
        // If the expr has virtual slot ref, we need to keep it in the tree.
        output_expr = expr_root;
        return Status::OK();
    }

    SlotDescriptor* slot = nullptr;
    ColumnValueRangeType* range = nullptr;
    RETURN_IF_ERROR(_eval_const_conjuncts(context, &pdt));
    if (pdt == PushDownType::ACCEPTABLE) {
        output_expr = nullptr;
        return Status::OK();
    }
    std::shared_ptr<vectorized::VSlotRef> slotref;
    for (const auto& child : expr_root->children()) {
        if (vectorized::VExpr::expr_without_cast(child)->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        slotref = std::dynamic_pointer_cast<vectorized::VSlotRef>(
                vectorized::VExpr::expr_without_cast(child));
    }
    if (_is_predicate_acting_on_slot(expr_root->children(), &slot, &range)) {
        Status status = Status::OK();
        std::visit(
                [&](auto& value_range) {
                    auto expr = root->is_rf_wrapper() ? root->get_impl() : root;
                    {
                        Defer attach_defer = [&]() {
                            if (pdt != PushDownType::UNACCEPTABLE && root->is_rf_wrapper()) {
                                auto* rf_expr =
                                        assert_cast<vectorized::VRuntimeFilterWrapper*>(root.get());
                                _slot_id_to_predicates[slot->id()].back()->attach_profile_counter(
                                        rf_expr->filter_id(),
                                        rf_expr->predicate_filtered_rows_counter(),
                                        rf_expr->predicate_input_rows_counter(),
                                        rf_expr->predicate_always_true_rows_counter(),
                                        context->get_runtime_filter_selectivity());
                            }
                        };
                        switch (expr->node_type()) {
                        case TExprNodeType::IN_PRED:
                            RETURN_IF_PUSH_DOWN(
                                    _normalize_in_predicate(context, expr, slot,
                                                            _slot_id_to_predicates[slot->id()],
                                                            value_range, &pdt),
                                    status);
                            break;
                        case TExprNodeType::BINARY_PRED:
                            RETURN_IF_PUSH_DOWN(
                                    _normalize_binary_predicate(context, expr, slot,
                                                                _slot_id_to_predicates[slot->id()],
                                                                value_range, &pdt),
                                    status);
                            break;
                        case TExprNodeType::FUNCTION_CALL:
                            if (expr->is_topn_filter()) {
                                RETURN_IF_PUSH_DOWN(
                                        _normalize_topn_filter(context, expr, slot,
                                                               _slot_id_to_predicates[slot->id()],
                                                               &pdt),
                                        status);
                            } else {
                                RETURN_IF_PUSH_DOWN(_normalize_is_null_predicate(
                                                            context, expr, slot,
                                                            _slot_id_to_predicates[slot->id()],
                                                            value_range, &pdt),
                                                    status);
                            }
                            break;
                        case TExprNodeType::BITMAP_PRED:
                            RETURN_IF_PUSH_DOWN(_normalize_bitmap_filter(
                                                        context, root, slot,
                                                        _slot_id_to_predicates[slot->id()], &pdt),
                                                status);
                            break;
                        case TExprNodeType::BLOOM_PRED:
                            RETURN_IF_PUSH_DOWN(_normalize_bloom_filter(
                                                        context, root, slot,
                                                        _slot_id_to_predicates[slot->id()], &pdt),
                                                status);
                            break;
                        default:
                            break;
                        }
                    }
                    // `node_type` of function filter is FUNCTION_CALL or COMPOUND_PRED
                    if (state()->enable_function_pushdown()) {
                        RETURN_IF_PUSH_DOWN(_normalize_function_filters(context, slot, &pdt),
                                            status);
                    }
                },
                *range);
        RETURN_IF_ERROR(status);
    }
    if (pdt == PushDownType::ACCEPTABLE && slotref != nullptr &&
        slotref->data_type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        // remaining it in the expr tree, in order to filter by function if the pushdown
        // predicate is not applied
        output_expr = expr_root; // remaining in conjunct tree
        return Status::OK();
    }

    if (pdt == PushDownType::ACCEPTABLE && (_is_key_column(slot->col_name()))) {
        output_expr = nullptr;
        return Status::OK();
    } else {
        // for PARTIAL_ACCEPTABLE and UNACCEPTABLE, do not remove expr from the tree
        output_expr = root;
        return Status::OK();
    }
    output_expr = root;
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_bloom_filter(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };
    DCHECK(TExprNodeType::BLOOM_PRED == root->node_type());
    auto expr = root->is_rf_wrapper() ? root->get_impl() : root;
    DCHECK(expr->get_num_children() == 1);
    DCHECK(root->is_rf_wrapper());
    *pdt = _should_push_down_bloom_filter();
    if (*pdt != PushDownType::UNACCEPTABLE) {
        pred = create_bloom_filter_predicate(
                _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                slot->type()->get_primitive_type() == TYPE_VARIANT ? expr->get_child(0)->data_type()
                                                                   : slot->type(),
                expr->get_bloom_filter_func());
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_topn_filter(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };
    DCHECK(root->is_topn_filter());
    *pdt = _should_push_down_topn_filter();
    if (*pdt != PushDownType::UNACCEPTABLE) {
        auto& p = _parent->cast<typename Derived::Parent>();
        auto& tmp = _state->get_query_ctx()->get_runtime_predicate(
                assert_cast<vectorized::VTopNPred*>(root.get())->source_node_id());
        if (_push_down_topn(tmp)) {
            pred = tmp.get_predicate(p.node_id());
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_bitmap_filter(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };
    DCHECK(TExprNodeType::BITMAP_PRED == root->node_type());
    auto expr = root->is_rf_wrapper() ? root->get_impl() : root;
    *pdt = _should_push_down_bitmap_filter();
    if (*pdt != PushDownType::UNACCEPTABLE) {
        DCHECK(expr->get_num_children() == 1);
        DCHECK(root->is_rf_wrapper());
        pred = create_bitmap_filter_predicate(
                _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                slot->type()->get_primitive_type() == TYPE_VARIANT ? expr->get_child(0)->data_type()
                                                                   : slot->type(),
                expr->get_bitmap_filter_func());
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_function_filters(vectorized::VExprContext* expr_ctx,
                                                            SlotDescriptor* slot,
                                                            PushDownType* pdt) {
    auto expr = expr_ctx->root()->is_rf_wrapper() ? expr_ctx->root()->get_impl() : expr_ctx->root();
    bool opposite = false;
    vectorized::VExpr* fn_expr = expr.get();
    if (TExprNodeType::COMPOUND_PRED == expr->node_type() &&
        expr->fn().name.function_name == "not") {
        fn_expr = fn_expr->children()[0].get();
        opposite = true;
    }

    if (fn_expr->is_like_expr()) {
        doris::FunctionContext* fn_ctx = nullptr;
        StringRef val;
        PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_function_filter(
                assert_cast<vectorized::VectorizedFnCall*>(fn_expr), expr_ctx, &val, &fn_ctx,
                temp_pdt));
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            std::string col = slot->col_name();
            _push_down_functions.emplace_back(opposite, col, fn_ctx, val);
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

// only one level cast expr could push down for variant type
// check if expr is cast and it's children is slot
static bool is_valid_push_down_cast(const vectorized::VExprSPtrs& children) {
    auto slot_expr = vectorized::VExpr::expr_without_cast(children[0]);
    return slot_expr->data_type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT &&
           children[0]->node_type() == TExprNodeType::CAST_EXPR &&
           children[0]->children().at(0)->is_slot_ref();
}

template <typename Derived>
bool ScanLocalState<Derived>::_is_predicate_acting_on_slot(const vectorized::VExprSPtrs& children,
                                                           SlotDescriptor** slot_desc,
                                                           ColumnValueRangeType** range) {
    // children[0] must be slot ref or cast(slot(variant) as type)
    if (children.empty() || (children[0]->node_type() != TExprNodeType::SLOT_REF &&
                             !is_valid_push_down_cast(children))) {
        // not a slot ref(column)
        return false;
    }
    std::shared_ptr<vectorized::VSlotRef> slot_ref =
            std::dynamic_pointer_cast<vectorized::VSlotRef>(
                    vectorized::VExpr::expr_without_cast(children[0]));
    *slot_desc =
            _parent->cast<typename Derived::Parent>()._slot_id_to_slot_desc[slot_ref->slot_id()];
    auto entry = _slot_id_to_predicates.find(slot_ref->slot_id());
    if (_slot_id_to_predicates.end() == entry) {
        return false;
    }
    auto sid_to_range = _slot_id_to_value_range.find(slot_ref->slot_id());
    if (_slot_id_to_value_range.end() == sid_to_range) {
        return false;
    }
    if (remove_nullable((*slot_desc)->type())->get_primitive_type() == TYPE_VARBINARY) {
        return false;
    }
    *range = &(sid_to_range->second);
    return true;
}

template <typename Derived>
std::string ScanLocalState<Derived>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, _eos = {} , _opened = {}",
                   PipelineXLocalState<>::debug_string(indentation_level), _eos.load(),
                   _opened.load());
    if (auto ctx = _scanner_ctx.load()) {
        fmt::format_to(debug_string_buffer, "");
        fmt::format_to(debug_string_buffer, ", Scanner Context: {}", ctx->debug_string());
    } else {
        fmt::format_to(debug_string_buffer, "");
        fmt::format_to(debug_string_buffer, ", Scanner Context: NULL");
    }

    return fmt::to_string(debug_string_buffer);
}

template <typename Derived>
Status ScanLocalState<Derived>::_eval_const_conjuncts(vectorized::VExprContext* expr_ctx,
                                                      PushDownType* pdt) {
    auto vexpr =
            expr_ctx->root()->is_rf_wrapper() ? expr_ctx->root()->get_impl() : expr_ctx->root();
    // Used to handle constant expressions, such as '1 = 1' _eval_const_conjuncts does not handle cases like 'colA = 1'
    const char* constant_val = nullptr;
    if (vexpr->is_constant()) {
        std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
        RETURN_IF_ERROR(vexpr->get_const_col(expr_ctx, &const_col_wrapper));
        if (const auto* const_column = check_and_get_column<vectorized::ColumnConst>(
                    const_col_wrapper->column_ptr.get())) {
            constant_val = const_column->get_data_at(0).data;
            if (constant_val == nullptr || !*reinterpret_cast<const bool*>(constant_val)) {
                *pdt = PushDownType::ACCEPTABLE;
                _eos = true;
                _scan_dependency->set_ready();
            }
        } else if (const auto* bool_column = check_and_get_column<vectorized::ColumnUInt8>(
                           const_col_wrapper->column_ptr.get())) {
            // TODO: If `vexpr->is_constant()` is true, a const column is expected here.
            //  But now we still don't cover all predicates for const expression.
            //  For example, for query `SELECT col FROM tbl WHERE 'PROMOTION' LIKE 'AAA%'`,
            //  predicate `like` will return a ColumnVector<UInt8> which contains a single value.
            LOG(WARNING) << "VExpr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << const_col_wrapper->column_ptr->get_name();
            DCHECK_EQ(bool_column->size(), 1);
            /// TODO: There is a DCHECK here, but an additional check is still needed. It should return an error code.
            if (bool_column->size() == 1) {
                constant_val = bool_column->get_data_at(0).data;
                if (constant_val == nullptr || !*reinterpret_cast<const bool*>(constant_val)) {
                    *pdt = PushDownType::ACCEPTABLE;
                    _eos = true;
                    _scan_dependency->set_ready();
                }
            } else {
                LOG(WARNING) << "Constant predicate in scan node should return a bool column with "
                                "`size == 1` but actually is "
                             << bool_column->size();
            }
        } else {
            LOG(WARNING) << "VExpr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << const_col_wrapper->column_ptr->get_name();
        }
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_in_predicate(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, ColumnValueRange<T>& range,
        PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };

    if (slot->get_virtual_column_expr() != nullptr) {
        // virtual column, do not push down
        return Status::OK();
    }

    DCHECK(!root->is_rf_wrapper()) << root->debug_string();
    DCHECK(TExprNodeType::IN_PRED == root->node_type()) << root->debug_string();
    *pdt = _should_push_down_in_predicate();
    if (*pdt == PushDownType::UNACCEPTABLE) {
        return Status::OK();
    }
    HybridSetBase::IteratorBase* iter = nullptr;
    auto hybrid_set = root->get_set_func();

    auto is_in = false;
    if (hybrid_set != nullptr) {
        // runtime filter produce VDirectInPredicate
        if (hybrid_set->size() <=
            _parent->cast<typename Derived::Parent>()._max_pushdown_conditions_per_column) {
            iter = hybrid_set->begin();
        }
        is_in = true;
    } else {
        // normal in predicate
        auto* tmp = assert_cast<vectorized::VInPredicate*>(root.get());

        // begin to push InPredicate value into ColumnValueRange
        auto* state = reinterpret_cast<vectorized::InState*>(
                expr_ctx->fn_context(tmp->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // xx in (col, xx, xx) should not be push down
        if (!state->use_set) {
            return Status::OK();
        }
        is_in = !tmp->is_not_in();

        if (state->hybrid_set->contain_null() && tmp->is_not_in()) {
            _eos = true;
            _scan_dependency->set_ready();
            return Status::OK();
        }
        hybrid_set = state->hybrid_set;
        iter = state->hybrid_set->begin();
    }

    if (iter) {
        auto empty_range = ColumnValueRange<T>::create_empty_column_value_range(
                slot->is_nullable(), range.precision(), range.scale());
        auto& temp_range = is_in ? empty_range : range;
        auto fn = is_in ? ColumnValueRange<T>::add_fixed_value_range
                        : (range.is_fixed_value_range()
                                   ? ColumnValueRange<T>::remove_fixed_value_range
                                   : ColumnValueRange<T>::empty_function);
        while (iter->has_next()) {
            // column in (nullptr) is always false so continue to
            // dispose next item
            DCHECK(iter->get_value() != nullptr);
            const auto* value = iter->get_value();
            if constexpr (is_string_type(T)) {
                const auto* str_value = reinterpret_cast<const StringRef*>(value);
                RETURN_IF_ERROR(_change_value_range(is_in, temp_range,
                                                    vectorized::Field::create_field<T>(std::string(
                                                            str_value->data, str_value->size)),
                                                    fn, is_in ? "in" : "not_in"));
            } else {
                RETURN_IF_ERROR(_change_value_range(
                        is_in, temp_range,
                        vectorized::Field::create_field<T>(
                                *reinterpret_cast<const typename PrimitiveTypeTraits<T>::CppType*>(
                                        value)),
                        fn, is_in ? "in" : "not_in"));
            }
            iter->next();
        }
        if (is_in) {
            range.intersection(temp_range);
        }
    }
    pred = is_in ? create_in_list_predicate<PredicateType::IN_LIST>(
                           _parent->intermediate_row_desc().get_column_id(slot->id()),
                           slot->col_name(),
                           slot->type()->get_primitive_type() == TYPE_VARIANT
                                   ? root->get_child(0)->data_type()
                                   : slot->type(),
                           hybrid_set, false)
                 : create_in_list_predicate<PredicateType::NOT_IN_LIST>(
                           _parent->intermediate_row_desc().get_column_id(slot->id()),
                           slot->col_name(),
                           slot->type()->get_primitive_type() == TYPE_VARIANT
                                   ? root->get_child(0)->data_type()
                                   : slot->type(),
                           hybrid_set, false);
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_binary_predicate(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, ColumnValueRange<T>& range,
        PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };

    if (slot->get_virtual_column_expr() != nullptr) {
        // virtual column, do not push down
        return Status::OK();
    }

    DCHECK(!root->is_rf_wrapper()) << root->debug_string();
    DCHECK(TExprNodeType::BINARY_PRED == root->node_type()) << root->debug_string();
    DCHECK(root->get_num_children() == 2);
    vectorized::Field value;
    *pdt = _should_push_down_binary_predicate(
            assert_cast<vectorized::VectorizedFnCall*>(root.get()), expr_ctx, value,
            {"eq", "ne", "lt", "gt", "le", "ge"});
    if (*pdt == PushDownType::UNACCEPTABLE) {
        return Status::OK();
    }
    const std::string& function_name =
            assert_cast<vectorized::VectorizedFnCall*>(root.get())->fn().name.function_name;
    auto op = to_olap_filter_type(function_name);
    auto is_equal_op = op == SQLFilterOp::FILTER_EQ || op == SQLFilterOp::FILTER_NE;
    auto empty_range = ColumnValueRange<T>::create_empty_column_value_range(
            slot->is_nullable(), range.precision(), range.scale());
    auto& temp_range = op == SQLFilterOp::FILTER_EQ ? empty_range : range;
    if (value.get_type() != TYPE_NULL) {
        switch (op) {
        case SQLFilterOp::FILTER_EQ:
            pred = create_comparison_predicate<PredicateType::EQ>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        case SQLFilterOp::FILTER_NE:
            pred = create_comparison_predicate<PredicateType::NE>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        case SQLFilterOp::FILTER_LESS:
            pred = create_comparison_predicate<PredicateType::LT>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        case SQLFilterOp::FILTER_LARGER:
            pred = create_comparison_predicate<PredicateType::GT>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        case SQLFilterOp::FILTER_LESS_OR_EQUAL:
            pred = create_comparison_predicate<PredicateType::LE>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        case SQLFilterOp::FILTER_LARGER_OR_EQUAL:
            pred = create_comparison_predicate<PredicateType::GE>(
                    _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(),
                    slot->type()->get_primitive_type() == TYPE_VARIANT
                            ? root->get_child(0)->data_type()
                            : slot->type(),
                    value, false);
            break;
        default:
            throw Exception(Status::InternalError("Unsupported function name: {}", function_name));
        }

        auto fn = op == SQLFilterOp::FILTER_EQ ? ColumnValueRange<T>::add_fixed_value_range
                  : op == SQLFilterOp::FILTER_NE
                          ? (range.is_fixed_value_range()
                                     ? ColumnValueRange<T>::remove_fixed_value_range
                                     : ColumnValueRange<T>::empty_function)
                          : ColumnValueRange<T>::add_value_range;
        RETURN_IF_ERROR(_change_value_range(is_equal_op, temp_range, value, fn, function_name));
        if (op == SQLFilterOp::FILTER_EQ) {
            range.intersection(temp_range);
        }
    } else {
        *pdt = PushDownType::UNACCEPTABLE;
        _eos = true;
        _scan_dependency->set_ready();
    }

    return Status::OK();
}

template <typename Derived>
template <PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
Status ScanLocalState<Derived>::_change_value_range(bool is_equal_op,
                                                    ColumnValueRange<PrimitiveType>& temp_range,
                                                    const vectorized::Field& value,
                                                    const ChangeFixedValueRangeFunc& func,
                                                    const std::string& fn_name) {
    if constexpr (PrimitiveType == TYPE_DATE) {
        auto tmp_value = value.template get<TYPE_DATE>();
        if (is_equal_op) {
            if (!tmp_value.check_loss_accuracy_cast_to_date()) {
                func(temp_range, to_olap_filter_type(fn_name), tmp_value);
            }
        } else {
            if (tmp_value.check_loss_accuracy_cast_to_date()) {
                if (fn_name == "lt" || fn_name == "ge") {
                    ++tmp_value;
                }
            }
            func(temp_range, to_olap_filter_type(fn_name), tmp_value);
        }
    } else if constexpr ((PrimitiveType == TYPE_DECIMALV2) || (PrimitiveType == TYPE_DATETIMEV2) ||
                         (PrimitiveType == TYPE_TINYINT) || (PrimitiveType == TYPE_SMALLINT) ||
                         (PrimitiveType == TYPE_INT) || (PrimitiveType == TYPE_BIGINT) ||
                         (PrimitiveType == TYPE_LARGEINT) || (PrimitiveType == TYPE_FLOAT) ||
                         (PrimitiveType == TYPE_DOUBLE) || (PrimitiveType == TYPE_IPV4) ||
                         (PrimitiveType == TYPE_IPV6) || (PrimitiveType == TYPE_DECIMAL32) ||
                         (PrimitiveType == TYPE_DECIMAL64) || (PrimitiveType == TYPE_DECIMAL128I) ||
                         (PrimitiveType == TYPE_DECIMAL256) || (PrimitiveType == TYPE_BOOLEAN) ||
                         (PrimitiveType == TYPE_DATEV2) || (PrimitiveType == TYPE_TIMESTAMPTZ) ||
                         (PrimitiveType == TYPE_DATETIME) || is_string_type(PrimitiveType)) {
        func(temp_range, to_olap_filter_type(fn_name), value.template get<PrimitiveType>());
    } else if constexpr (PrimitiveType == TYPE_HLL) {
        auto tmp = value.template get<PrimitiveType>();
        func(temp_range, to_olap_filter_type(fn_name),
             StringRef(reinterpret_cast<const char*>(&tmp), sizeof(tmp)));
    } else {
        static_assert(always_false_v<PrimitiveType>);
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_is_null_predicate(
        vectorized::VExprContext* expr_ctx, const vectorized::VExprSPtr& root, SlotDescriptor* slot,
        std::vector<std::shared_ptr<ColumnPredicate>>& predicates, ColumnValueRange<T>& range,
        PushDownType* pdt) {
    std::shared_ptr<ColumnPredicate> pred = nullptr;
    Defer defer = [&]() {
        if (pred) {
            DCHECK(*pdt != PushDownType::UNACCEPTABLE) << root->debug_string();
            predicates.emplace_back(pred);
        } else {
            // If exception occurs during processing, do not push down
            *pdt = PushDownType::UNACCEPTABLE;
        }
    };
    DCHECK(!root->is_rf_wrapper()) << root->debug_string();
    DCHECK(TExprNodeType::FUNCTION_CALL == root->node_type()) << root->debug_string();
    if (auto fn_call = dynamic_cast<vectorized::VectorizedFnCall*>(root.get())) {
        *pdt = _should_push_down_is_null_predicate(fn_call);
    } else {
        *pdt = PushDownType::UNACCEPTABLE;
    }

    if (*pdt == PushDownType::UNACCEPTABLE) {
        return Status::OK();
    }

    auto fn_call = assert_cast<vectorized::VectorizedFnCall*>(root.get());
    if (fn_call->fn().name.function_name == "is_null_pred") {
        pred = NullPredicate::create_shared(
                _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(), true,
                T);
        auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                slot->is_nullable(), range.precision(), range.scale());
        temp_range.set_contain_null(true);
        range.intersection(temp_range);
    } else if (fn_call->fn().name.function_name == "is_not_null_pred") {
        pred = NullPredicate::create_shared(
                _parent->intermediate_row_desc().get_column_id(slot->id()), slot->col_name(), false,
                T);
        auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                slot->is_nullable(), range.precision(), range.scale());
        temp_range.set_contain_null(false);
        range.intersection(temp_range);
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_prepare_scanners() {
    std::list<vectorized::ScannerSPtr> scanners;
    RETURN_IF_ERROR(_init_scanners(&scanners));
    // Init scanner wrapper
    for (auto it = scanners.begin(); it != scanners.end(); ++it) {
        _scanners.emplace_back(std::make_shared<vectorized::ScannerDelegate>(*it));
    }
    if (scanners.empty()) {
        _eos = true;
        _scan_dependency->set_always_ready();
    } else {
        COUNTER_SET(_num_scanners, static_cast<int64_t>(scanners.size()));
        RETURN_IF_ERROR(_start_scanners(_scanners));
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_start_scanners(
        const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners) {
    auto& p = _parent->cast<typename Derived::Parent>();
    _scanner_ctx.store(vectorized::ScannerContext::create_shared(
            state(), this, p._output_tuple_desc, p.output_row_descriptor(), scanners, p.limit(),
            _scan_dependency
#ifdef BE_TEST
            ,
            max_scanners_concurrency(state())
#endif
                    ));
    return Status::OK();
}

template <typename Derived>
const TupleDescriptor* ScanLocalState<Derived>::input_tuple_desc() const {
    return _parent->cast<typename Derived::Parent>()._input_tuple_desc;
}
template <typename Derived>
const TupleDescriptor* ScanLocalState<Derived>::output_tuple_desc() const {
    return _parent->cast<typename Derived::Parent>()._output_tuple_desc;
}

template <typename Derived>
TPushAggOp::type ScanLocalState<Derived>::get_push_down_agg_type() {
    return _parent->cast<typename Derived::Parent>()._push_down_agg_type;
}

template <typename Derived>
int64_t ScanLocalState<Derived>::limit_per_scanner() {
    return _parent->cast<typename Derived::Parent>()._limit_per_scanner;
}

template <typename Derived>
Status ScanLocalState<Derived>::_init_profile() {
    // 1. counters for scan node
    _rows_read_counter = ADD_COUNTER(custom_profile(), "RowsRead", TUnit::UNIT);
    _num_scanners = ADD_COUNTER(custom_profile(), "NumScanners", TUnit::UNIT);
    //custom_profile()->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES);

    // 2. counters for scanners
    _scanner_profile.reset(new RuntimeProfile("Scanner"));
    custom_profile()->add_child(_scanner_profile.get(), true, nullptr);

    _newly_create_free_blocks_num =
            ADD_COUNTER(_scanner_profile, "NewlyCreateFreeBlocksNum", TUnit::UNIT);
    _scan_timer = ADD_TIMER(_scanner_profile, "ScannerGetBlockTime");
    _scan_cpu_timer = ADD_TIMER(_scanner_profile, "ScannerCpuTime");
    _filter_timer = ADD_TIMER(_scanner_profile, "ScannerFilterTime");

    // time of scan thread to wait for worker thread of the thread pool
    _scanner_wait_worker_timer = ADD_TIMER(custom_profile(), "ScannerWorkerWaitTime");

    _max_scan_concurrency = ADD_COUNTER(custom_profile(), "MaxScanConcurrency", TUnit::UNIT);
    _min_scan_concurrency = ADD_COUNTER(custom_profile(), "MinScanConcurrency", TUnit::UNIT);

    _peak_running_scanner =
            _scanner_profile->AddHighWaterMarkCounter("RunningScanner", TUnit::UNIT);

    // Rows read from storage.
    // Include the rows read from doris page cache.
    _scan_rows = ADD_COUNTER_WITH_LEVEL(custom_profile(), "ScanRows", TUnit::UNIT, 1);
    // Size of data that read from storage.
    // Does not include rows that are cached by doris page cache.
    _scan_bytes = ADD_COUNTER_WITH_LEVEL(custom_profile(), "ScanBytes", TUnit::BYTES, 1);
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_get_topn_filters(RuntimeState* state) {
    auto& p = _parent->cast<typename Derived::Parent>();
    std::stringstream result;
    std::copy(p._topn_filter_source_node_ids.begin(), p._topn_filter_source_node_ids.end(),
              std::ostream_iterator<int>(result, ","));
    custom_profile()->add_info_string("TopNFilterSourceNodeIds", result.str());

    for (auto id : get_topn_filter_source_node_ids(state, false)) {
        const auto& pred = state->get_query_ctx()->get_runtime_predicate(id);
        vectorized::VExprSPtr topn_pred;
        RETURN_IF_ERROR(vectorized::VTopNPred::create_vtopn_pred(pred.get_texpr(p.node_id()), id,
                                                                 topn_pred));

        vectorized::VExprContextSPtr conjunct = vectorized::VExprContext::create_shared(topn_pred);
        RETURN_IF_ERROR(conjunct->prepare(
                state, _parent->cast<typename Derived::Parent>().row_descriptor()));
        RETURN_IF_ERROR(conjunct->open(state));
        _conjuncts.emplace_back(conjunct);
    }
    for (auto id : get_topn_filter_source_node_ids(state, true)) {
        const auto& pred = state->get_query_ctx()->get_runtime_predicate(id);
        vectorized::VExprSPtr topn_pred;
        RETURN_IF_ERROR(vectorized::VTopNPred::create_vtopn_pred(pred.get_texpr(p.node_id()), id,
                                                                 topn_pred));

        vectorized::VExprContextSPtr conjunct = vectorized::VExprContext::create_shared(topn_pred);
        RETURN_IF_ERROR(conjunct->prepare(
                state, _parent->cast<typename Derived::Parent>().row_descriptor()));
        RETURN_IF_ERROR(conjunct->open(state));
        _conjuncts.emplace_back(conjunct);
    }
    return Status::OK();
}

template <typename Derived>
void ScanLocalState<Derived>::_filter_and_collect_cast_type_for_variant(
        const vectorized::VExpr* expr,
        std::unordered_map<std::string, std::vector<vectorized::DataTypePtr>>&
                colname_to_cast_types) {
    auto& p = _parent->cast<typename Derived::Parent>();
    const auto* cast_expr = dynamic_cast<const vectorized::VCastExpr*>(expr);
    if (cast_expr != nullptr) {
        const auto* src_slot =
                cast_expr->get_child(0)->node_type() == TExprNodeType::SLOT_REF
                        ? dynamic_cast<const vectorized::VSlotRef*>(cast_expr->get_child(0).get())
                        : nullptr;
        if (src_slot == nullptr) {
            return;
        }
        std::vector<SlotDescriptor*> slots = output_tuple_desc()->slots();
        SlotDescriptor* src_slot_desc = p._slot_id_to_slot_desc[src_slot->slot_id()];
        auto type_desc = cast_expr->get_target_type();
        if (src_slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            colname_to_cast_types[src_slot_desc->col_name()].push_back(type_desc);
        }
    }
    for (const auto& child : expr->children()) {
        _filter_and_collect_cast_type_for_variant(child.get(), colname_to_cast_types);
    }
}

template <typename Derived>
void ScanLocalState<Derived>::get_cast_types_for_variants() {
    std::unordered_map<std::string, std::vector<vectorized::DataTypePtr>> colname_to_cast_types;
    for (auto it = _conjuncts.begin(); it != _conjuncts.end();) {
        auto& conjunct = *it;
        if (conjunct->root()) {
            _filter_and_collect_cast_type_for_variant(conjunct->root().get(),
                                                      colname_to_cast_types);
        }
        ++it;
    }
    // cast to one certain type for variant could utilize fully predicates performance
    // when storage layer type equals to cast type
    for (const auto& [slotid, types] : colname_to_cast_types) {
        if (types.size() == 1) {
            _cast_types_for_variants[slotid] = types[0];
        }
    }
}

template <typename LocalStateType>
ScanOperatorX<LocalStateType>::ScanOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                             int operator_id, const DescriptorTbl& descs,
                                             int parallel_tasks)
        : OperatorX<LocalStateType>(pool, tnode, operator_id, descs),
          _runtime_filter_descs(tnode.runtime_filters),
          _parallel_tasks(parallel_tasks) {
    if (tnode.__isset.push_down_count) {
        _push_down_count = tnode.push_down_count;
    }
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<LocalStateType>::init(tnode, state));

    const TQueryOptions& query_options = state->query_options();
    if (query_options.__isset.max_scan_key_num) {
        _max_scan_key_num = query_options.max_scan_key_num;
    }
    if (query_options.__isset.max_pushdown_conditions_per_column) {
        _max_pushdown_conditions_per_column = query_options.max_pushdown_conditions_per_column;
    }
    // tnode.olap_scan_node.push_down_agg_type_opt field is deprecated
    // Introduced a new field : tnode.push_down_agg_type_opt
    //
    // make it compatible here
    if (tnode.__isset.push_down_agg_type_opt) {
        _push_down_agg_type = tnode.push_down_agg_type_opt;
    } else if (tnode.olap_scan_node.__isset.push_down_agg_type_opt) {
        _push_down_agg_type = tnode.olap_scan_node.push_down_agg_type_opt;
    } else {
        _push_down_agg_type = TPushAggOp::type::NONE;
    }

    if (tnode.__isset.topn_filter_source_node_ids) {
        _topn_filter_source_node_ids = tnode.topn_filter_source_node_ids;
    }

    // Which means the request could be fullfilled in a single segment iterator request.
    // the unique_table has a condition of delete_sign = 0 awalys, so it's not have plan for one instance to scan table,
    // now add some check for unique_table let running only one instance for select limit n.
    if (query_options.enable_adaptive_pipeline_task_serial_read_on_limit) {
        DCHECK(query_options.__isset.adaptive_pipeline_task_serial_read_on_limit);
        if (!tnode.__isset.conjuncts || tnode.conjuncts.empty() ||
            (tnode.conjuncts.size() == 1 && tnode.__isset.olap_scan_node &&
             tnode.olap_scan_node.keyType == TKeysType::UNIQUE_KEYS)) {
            if (tnode.limit > 0 &&
                tnode.limit <= query_options.adaptive_pipeline_task_serial_read_on_limit) {
                _should_run_serial = true;
            }
        }
    }

    return Status::OK();
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::prepare(RuntimeState* state) {
    _input_tuple_desc = state->desc_tbl().get_tuple_descriptor(_input_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    RETURN_IF_ERROR(OperatorX<LocalStateType>::prepare(state));

    const auto slots = _output_tuple_desc->slots();
    for (auto* slot : slots) {
        _colname_to_slot_id[slot->col_name()] = slot->id();
        _slot_id_to_slot_desc[slot->id()] = slot;
    }
    for (auto id : _topn_filter_source_node_ids) {
        if (!state->get_query_ctx()->has_runtime_predicate(id)) {
            // compatible with older versions fe
            continue;
        }

        int cid = -1;
        if (state->get_query_ctx()->get_runtime_predicate(id).target_is_slot(node_id())) {
            auto s = _slot_id_to_slot_desc[state->get_query_ctx()
                                                   ->get_runtime_predicate(id)
                                                   .get_texpr(node_id())
                                                   .nodes[0]
                                                   .slot_ref.slot_id];
            DCHECK(s != nullptr);
            if (remove_nullable(s->type())->get_primitive_type() == TYPE_VARBINARY) {
                continue;
            }
            auto col_name = s->col_name();
            cid = get_column_id(col_name);
        }
        RETURN_IF_ERROR(state->get_query_ctx()->get_runtime_predicate(id).init_target(
                node_id(), _slot_id_to_slot_desc, cid));
    }

    RETURN_IF_CANCELLED(state);
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    COUNTER_UPDATE(exec_time_counter(), _scan_dependency->watcher_elapse_time());
    int64_t rf_time = 0;
    for (auto& dep : _filter_dependencies) {
        rf_time += dep->watcher_elapse_time();
    }
    COUNTER_UPDATE(exec_time_counter(), rf_time);
    SCOPED_TIMER(_close_timer);

    SCOPED_TIMER(exec_time_counter());
    if (auto ctx = _scanner_ctx.load()) {
        ctx->stop_scanners(state);
        // _scanner_ctx may be accessed in debug_string concurrently
        // so use atomic shared ptr to avoid use after free
        _scanner_ctx.store(nullptr);
    }
    std::list<std::shared_ptr<vectorized::ScannerDelegate>> {}.swap(_scanners);
    COUNTER_SET(_wait_for_dependency_timer, _scan_dependency->watcher_elapse_time());
    COUNTER_SET(_wait_for_rf_timer, rf_time);
    _helper.collect_realtime_profile(custom_profile());
    return PipelineXLocalState<>::close(state);
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    if (state->is_cancelled()) {
        if (auto ctx = local_state._scanner_ctx.load()) {
            ctx->stop_scanners(state);
        }
        return state->cancel_reason();
    }

    if (local_state._eos) {
        *eos = true;
        return Status::OK();
    }

    auto ctx = local_state._scanner_ctx.load();

    DCHECK(ctx != nullptr);
    RETURN_IF_ERROR(ctx->get_block_from_queue(state, block, eos, 0));

    local_state.reached_limit(block, eos);
    if (*eos) {
        // reach limit, stop the scanners.
        ctx->stop_scanners(state);
        local_state._scanner_profile->add_info_string("EOS", "True");
    }

    return Status::OK();
}

template <typename LocalStateType>
size_t ScanOperatorX<LocalStateType>::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto ctx = local_state._scanner_ctx.load();
    if (!local_state._opened || local_state._closed || !ctx) {
        return config::doris_scanner_row_bytes;
    }

    if (local_state.low_memory_mode()) {
        return ctx->low_memory_mode_scan_bytes_per_scanner() * ctx->low_memory_mode_scanners();
    } else {
        const auto peak_usage = local_state._memory_used_counter->value();
        const auto block_usage = ctx->block_memory_usage();
        if (peak_usage > 0) {
            // It is only a safty check, to avoid some counter not right.
            if (peak_usage > block_usage) {
                return peak_usage - block_usage;
            } else {
                return config::doris_scanner_row_bytes;
            }
        } else {
            // If the scan operator is first time to run, then we think it will occupy doris_scanner_row_bytes.
            // It maybe a little smaller than actual usage.
            return config::doris_scanner_row_bytes;
            // return local_state._scanner_ctx->max_bytes_in_queue();
        }
    }
}

template class ScanOperatorX<OlapScanLocalState>;
template class ScanLocalState<OlapScanLocalState>;
template class ScanOperatorX<JDBCScanLocalState>;
template class ScanLocalState<JDBCScanLocalState>;
template class ScanOperatorX<FileScanLocalState>;
template class ScanLocalState<FileScanLocalState>;
template class ScanOperatorX<EsScanLocalState>;
template class ScanLocalState<EsScanLocalState>;
template class ScanLocalState<MetaScanLocalState>;
template class ScanOperatorX<MetaScanLocalState>;
template class ScanOperatorX<GroupCommitLocalState>;
template class ScanLocalState<GroupCommitLocalState>;

#ifdef BE_TEST
template class ScanOperatorX<MockScanLocalState>;
template class ScanLocalState<MockScanLocalState>;
#endif

} // namespace doris::pipeline
