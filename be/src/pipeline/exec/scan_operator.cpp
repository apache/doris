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

#include <cstdint>
#include <memory>

#include "pipeline/common/runtime_filter_consumer.h"
#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/group_commit_scan_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/meta_scan_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vtopn_pred.h"
#include "vec/functions/in.h"

namespace doris::pipeline {

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

template <typename Derived>
Status ScanLocalState<Derived>::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    _scan_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                 _parent->get_name() + "_DEPENDENCY");
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(
            _runtime_profile, "WaitForDependency[" + _scan_dependency->name() + "]Time", 1);
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<typename Derived::Parent>();
    RETURN_IF_ERROR(RuntimeFilterConsumer::init(state, p.ignore_data_distribution()));
    // init profile for runtime filter
    RuntimeFilterConsumer::_init_profile(profile());
    init_runtime_filter_dependency(_filter_dependencies, p.operator_id(), p.node_id(),
                                   p.get_name() + "_FILTER_DEPENDENCY");

    // 1: running at not pipeline mode will init profile.
    // 2: the scan node should create scanner at pipeline mode will init profile.
    // during pipeline mode with more instances, olap scan node maybe not new VScanner object,
    // so the profile of VScanner and SegmentIterator infos are always empty, could not init those.
    RETURN_IF_ERROR(_init_profile());
    set_scan_ranges(state, info.scan_ranges);
    // if you want to add some profile in scan node, even it have not new VScanner object
    // could add here, not in the _init_profile() function
    _prepare_rf_timer(_runtime_profile.get());

    _wait_for_rf_timer = ADD_TIMER(_runtime_profile, "WaitForRuntimeFilter");
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
    _common_expr_ctxs_push_down.resize(p._common_expr_ctxs_push_down.size());
    for (size_t i = 0; i < _common_expr_ctxs_push_down.size(); i++) {
        RETURN_IF_ERROR(
                p._common_expr_ctxs_push_down[i]->clone(state, _common_expr_ctxs_push_down[i]));
    }
    RETURN_IF_ERROR(_acquire_runtime_filter());
    _stale_expr_ctxs.resize(p._stale_expr_ctxs.size());
    for (size_t i = 0; i < _stale_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._stale_expr_ctxs[i]->clone(state, _stale_expr_ctxs[i]));
    }
    RETURN_IF_ERROR(_process_conjuncts(state));

    auto status = _eos ? Status::OK() : _prepare_scanners();
    RETURN_IF_ERROR(status);
    if (_scanner_ctx) {
        DCHECK(!_eos && _num_scanners->value() > 0);
        RETURN_IF_ERROR(_scanner_ctx->init());
    }
    _opened = true;
    return status;
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_conjuncts(RuntimeState* state) {
    auto& p = _parent->cast<typename Derived::Parent>();
    // The conjuncts is always on output tuple, so use _output_tuple_desc;
    std::vector<SlotDescriptor*> slots = p._output_tuple_desc->slots();

    auto init_value_range = [&](SlotDescriptor* slot, PrimitiveType type) {
        switch (type) {
#define M(NAME)                                                                          \
    case TYPE_##NAME: {                                                                  \
        ColumnValueRange<TYPE_##NAME> range(slot->col_name(), slot->is_nullable(),       \
                                            slot->type().precision, slot->type().scale); \
        _slot_id_to_value_range[slot->id()] = std::pair {slot, range};                   \
        break;                                                                           \
    }
#define APPLY_FOR_PRIMITIVE_TYPE(M) \
    M(TINYINT)                      \
    M(SMALLINT)                     \
    M(INT)                          \
    M(BIGINT)                       \
    M(LARGEINT)                     \
    M(CHAR)                         \
    M(DATE)                         \
    M(DATETIME)                     \
    M(DATEV2)                       \
    M(DATETIMEV2)                   \
    M(VARCHAR)                      \
    M(STRING)                       \
    M(HLL)                          \
    M(DECIMAL32)                    \
    M(DECIMAL64)                    \
    M(DECIMAL128I)                  \
    M(DECIMAL256)                   \
    M(DECIMALV2)                    \
    M(BOOLEAN)                      \
    M(IPV4)                         \
    M(IPV6)
            APPLY_FOR_PRIMITIVE_TYPE(M)
#undef M
        default: {
            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slot->col_name() << "]";
            break;
        }
        }
    };

    for (auto& slot : slots) {
        auto type = slot->type().type;
        if (slot->type().type == TYPE_ARRAY) {
            type = slot->type().children[0].type;
            if (type == TYPE_ARRAY) {
                continue;
            }
        }
        init_value_range(slot, slot->type().type);
    }

    get_cast_types_for_variants();
    for (const auto& [colname, type] : _cast_types_for_variants) {
        init_value_range(p._slot_id_to_slot_desc[p._colname_to_slot_id[colname]], type);
    }

    RETURN_IF_ERROR(_get_topn_filters(state));

    for (auto it = _conjuncts.begin(); it != _conjuncts.end();) {
        auto& conjunct = *it;
        if (conjunct->root()) {
            vectorized::VExprSPtr new_root;
            RETURN_IF_ERROR(_normalize_predicate(conjunct->root(), conjunct.get(), new_root));
            if (new_root) {
                conjunct->set_root(new_root);
                if (_should_push_down_common_expr() &&
                    vectorized::VExpr::is_acting_on_a_slot(*(conjunct->root()))) {
                    _common_expr_ctxs_push_down.emplace_back(conjunct);
                    it = _conjuncts.erase(it);
                    continue;
                }
            } else { // All conjuncts are pushed down as predicate column
                _stale_expr_ctxs.emplace_back(conjunct);
                it = _conjuncts.erase(it);
                continue;
            }
        }
        ++it;
    }
    for (auto& it : _slot_id_to_value_range) {
        std::visit(
                [&](auto&& range) {
                    if (range.is_empty_value_range()) {
                        _eos = true;
                        _scan_dependency->set_ready();
                    }
                },
                it.second.second);
        _colname_to_value_range[it.second.first->col_name()] = it.second.second;
    }

    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_predicate(
        const vectorized::VExprSPtr& conjunct_expr_root, vectorized::VExprContext* context,
        vectorized::VExprSPtr& output_expr) {
    static constexpr auto is_leaf = [](auto&& expr) { return !expr->is_and_expr(); };
    auto in_predicate_checker = [](const vectorized::VExprSPtrs& children,
                                   std::shared_ptr<vectorized::VSlotRef>& slot,
                                   vectorized::VExprSPtr& child_contains_slot) {
        if (children.empty() || vectorized::VExpr::expr_without_cast(children[0])->node_type() !=
                                        TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            return false;
        }
        slot = std::dynamic_pointer_cast<vectorized::VSlotRef>(
                vectorized::VExpr::expr_without_cast(children[0]));
        child_contains_slot = children[0];
        return true;
    };
    auto eq_predicate_checker = [](const vectorized::VExprSPtrs& children,
                                   std::shared_ptr<vectorized::VSlotRef>& slot,
                                   vectorized::VExprSPtr& child_contains_slot) {
        for (const auto& child : children) {
            if (vectorized::VExpr::expr_without_cast(child)->node_type() !=
                TExprNodeType::SLOT_REF) {
                // not a slot ref(column)
                continue;
            }
            slot = std::dynamic_pointer_cast<vectorized::VSlotRef>(
                    vectorized::VExpr::expr_without_cast(child));
            CHECK(slot != nullptr);
            child_contains_slot = child;
            return true;
        }
        return false;
    };

    if (conjunct_expr_root != nullptr) {
        if (is_leaf(conjunct_expr_root)) {
            auto impl = conjunct_expr_root->get_impl();
            // If impl is not null, which means this a conjuncts from runtime filter.
            auto cur_expr = impl ? impl.get() : conjunct_expr_root.get();
            bool _is_runtime_filter_predicate = _rf_vexpr_set.contains(conjunct_expr_root);
            SlotDescriptor* slot = nullptr;
            ColumnValueRangeType* range = nullptr;
            PushDownType pdt = PushDownType::UNACCEPTABLE;
            RETURN_IF_ERROR(_eval_const_conjuncts(cur_expr, context, &pdt));
            if (pdt == PushDownType::ACCEPTABLE) {
                output_expr = nullptr;
                return Status::OK();
            }
            std::shared_ptr<vectorized::VSlotRef> slotref;
            for (const auto& child : cur_expr->children()) {
                if (vectorized::VExpr::expr_without_cast(child)->node_type() !=
                    TExprNodeType::SLOT_REF) {
                    // not a slot ref(column)
                    continue;
                }
                slotref = std::dynamic_pointer_cast<vectorized::VSlotRef>(
                        vectorized::VExpr::expr_without_cast(child));
            }
            if (_is_predicate_acting_on_slot(cur_expr, in_predicate_checker, &slot, &range) ||
                _is_predicate_acting_on_slot(cur_expr, eq_predicate_checker, &slot, &range)) {
                Status status = Status::OK();
                std::visit(
                        [&](auto& value_range) {
                            Defer mark_runtime_filter_flag {[&]() {
                                value_range.mark_runtime_filter_predicate(
                                        _is_runtime_filter_predicate);
                            }};
                            RETURN_IF_PUSH_DOWN(_normalize_in_and_eq_predicate(
                                                        cur_expr, context, slot, value_range, &pdt),
                                                status);
                            RETURN_IF_PUSH_DOWN(_normalize_not_in_and_not_eq_predicate(
                                                        cur_expr, context, slot, value_range, &pdt),
                                                status);
                            RETURN_IF_PUSH_DOWN(_normalize_is_null_predicate(
                                                        cur_expr, context, slot, value_range, &pdt),
                                                status);
                            RETURN_IF_PUSH_DOWN(_normalize_noneq_binary_predicate(
                                                        cur_expr, context, slot, value_range, &pdt),
                                                status);
                            RETURN_IF_PUSH_DOWN(_normalize_match_predicate(cur_expr, context, slot,
                                                                           value_range, &pdt),
                                                status);
                            if (_is_key_column(slot->col_name())) {
                                RETURN_IF_PUSH_DOWN(
                                        _normalize_bitmap_filter(cur_expr, context, slot, &pdt),
                                        status);
                                RETURN_IF_PUSH_DOWN(
                                        _normalize_bloom_filter(cur_expr, context, slot, &pdt),
                                        status);
                                if (state()->enable_function_pushdown()) {
                                    RETURN_IF_PUSH_DOWN(_normalize_function_filters(
                                                                cur_expr, context, slot, &pdt),
                                                        status);
                                }
                            }
                        },
                        *range);
                RETURN_IF_ERROR(status);
            }

            if (pdt == PushDownType::UNACCEPTABLE &&
                TExprNodeType::COMPOUND_PRED == cur_expr->node_type()) {
                _normalize_compound_predicate(cur_expr, context, &pdt, _is_runtime_filter_predicate,
                                              in_predicate_checker, eq_predicate_checker);
                output_expr = conjunct_expr_root; // remaining in conjunct tree
                return Status::OK();
            }

            if (pdt == PushDownType::ACCEPTABLE &&
                TExprNodeType::MATCH_PRED == cur_expr->node_type()) {
                // remaining it in the expr tree, in order to filter by function if the pushdown
                // match_predicate failed to apply inverted index in the storage layer
                output_expr = conjunct_expr_root; // remaining in conjunct tree
                return Status::OK();
            }

            if (pdt == PushDownType::ACCEPTABLE && slotref != nullptr &&
                slotref->type().is_variant_type()) {
                // remaining it in the expr tree, in order to filter by function if the pushdown
                // predicate is not applied
                output_expr = conjunct_expr_root; // remaining in conjunct tree
                return Status::OK();
            }

            if (pdt == PushDownType::ACCEPTABLE &&
                (_is_key_column(slot->col_name()) || _storage_no_merge())) {
                output_expr = nullptr;
                return Status::OK();
            } else {
                // for PARTIAL_ACCEPTABLE and UNACCEPTABLE, do not remove expr from the tree
                output_expr = conjunct_expr_root;
                return Status::OK();
            }
        } else {
            vectorized::VExprSPtr left_child;
            RETURN_IF_ERROR(
                    _normalize_predicate(conjunct_expr_root->children()[0], context, left_child));
            vectorized::VExprSPtr right_child;
            RETURN_IF_ERROR(
                    _normalize_predicate(conjunct_expr_root->children()[1], context, right_child));

            if (left_child != nullptr && right_child != nullptr) {
                conjunct_expr_root->set_children({left_child, right_child});
                output_expr = conjunct_expr_root;
                return Status::OK();
            } else {
                if (left_child == nullptr) {
                    conjunct_expr_root->children()[0]->close(context,
                                                             context->get_function_state_scope());
                }
                if (right_child == nullptr) {
                    conjunct_expr_root->children()[1]->close(context,
                                                             context->get_function_state_scope());
                }
                // here only close the and expr self, do not close the child
                conjunct_expr_root->set_children({});
                conjunct_expr_root->close(context, context->get_function_state_scope());
            }

            // here do not close VExpr* now
            output_expr = left_child != nullptr ? left_child : right_child;
            return Status::OK();
        }
    }
    output_expr = conjunct_expr_root;
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_bloom_filter(vectorized::VExpr* expr,
                                                        vectorized::VExprContext* expr_ctx,
                                                        SlotDescriptor* slot, PushDownType* pdt) {
    if (TExprNodeType::BLOOM_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        PushDownType temp_pdt = _should_push_down_bloom_filter();
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            _filter_predicates.bloom_filters.emplace_back(slot->col_name(),
                                                          expr->get_bloom_filter_func());
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_bitmap_filter(vectorized::VExpr* expr,
                                                         vectorized::VExprContext* expr_ctx,
                                                         SlotDescriptor* slot, PushDownType* pdt) {
    if (TExprNodeType::BITMAP_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        PushDownType temp_pdt = _should_push_down_bitmap_filter();
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            _filter_predicates.bitmap_filters.emplace_back(slot->col_name(),
                                                           expr->get_bitmap_filter_func());
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_function_filters(vectorized::VExpr* expr,
                                                            vectorized::VExprContext* expr_ctx,
                                                            SlotDescriptor* slot,
                                                            PushDownType* pdt) {
    bool opposite = false;
    vectorized::VExpr* fn_expr = expr;
    if (TExprNodeType::COMPOUND_PRED == expr->node_type() &&
        expr->fn().name.function_name == "not") {
        fn_expr = fn_expr->children()[0].get();
        opposite = true;
    }

    if (TExprNodeType::FUNCTION_CALL == fn_expr->node_type()) {
        doris::FunctionContext* fn_ctx = nullptr;
        StringRef val;
        PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_function_filter(
                reinterpret_cast<vectorized::VectorizedFnCall*>(fn_expr), expr_ctx, &val, &fn_ctx,
                temp_pdt));
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            std::string col = slot->col_name();
            _push_down_functions.emplace_back(opposite, col, fn_ctx, val);
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
bool ScanLocalState<Derived>::_is_predicate_acting_on_slot(
        vectorized::VExpr* expr,
        const std::function<bool(const vectorized::VExprSPtrs&,
                                 std::shared_ptr<vectorized::VSlotRef>&, vectorized::VExprSPtr&)>&
                checker,
        SlotDescriptor** slot_desc, ColumnValueRangeType** range) {
    std::shared_ptr<vectorized::VSlotRef> slot_ref;
    vectorized::VExprSPtr child_contains_slot;
    if (!checker(expr->children(), slot_ref, child_contains_slot)) {
        // not a slot ref(column)
        return false;
    }

    auto entry = _slot_id_to_value_range.find(slot_ref->slot_id());
    if (_slot_id_to_value_range.end() == entry) {
        return false;
    }
    // if the slot is a complex type(array/map/struct), we do not push down the predicate, because
    // we delete pack these type into predict column, and origin pack action is wrong. we should
    // make sense to push down this complex type after we delete predict column.
    if (is_complex_type(remove_nullable(slot_ref->data_type()))) {
        return false;
    }
    *slot_desc = entry->second.first;
    DCHECK(child_contains_slot != nullptr);
    if (child_contains_slot->type().type != (*slot_desc)->type().type ||
        child_contains_slot->type().precision != (*slot_desc)->type().precision ||
        child_contains_slot->type().scale != (*slot_desc)->type().scale) {
        if (!_ignore_cast(*slot_desc, child_contains_slot.get())) {
            // the type of predicate not match the slot's type
            return false;
        }
    } else if (child_contains_slot->type().is_datetime_type() &&
               child_contains_slot->node_type() == doris::TExprNodeType::CAST_EXPR) {
        // Expr `CAST(CAST(datetime_col AS DATE) AS DATETIME) = datetime_literal` should not be
        // push down.
        return false;
    }
    *range = &(entry->second.second);
    return true;
}

template <typename Derived>
std::string ScanLocalState<Derived>::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, _eos = {} , _opened = {}",
                   PipelineXLocalState<>::debug_string(indentation_level), _eos.load(),
                   _opened.load());
    if (_scanner_ctx) {
        fmt::format_to(debug_string_buffer, "");
        fmt::format_to(debug_string_buffer, ", Scanner Context: {}", _scanner_ctx->debug_string());
    } else {
        fmt::format_to(debug_string_buffer, "");
        fmt::format_to(debug_string_buffer, ", Scanner Context: NULL");
    }

    return fmt::to_string(debug_string_buffer);
}

template <typename Derived>
bool ScanLocalState<Derived>::_ignore_cast(SlotDescriptor* slot, vectorized::VExpr* expr) {
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    // Variant slot cast could be eliminated
    if (slot->type().is_variant_type()) {
        return true;
    }
    if (slot->type().is_array_type()) {
        if (slot->type().children[0].type == expr->type().type) {
            return true;
        }
        if (slot->type().children[0].is_string_type() && expr->type().is_string_type()) {
            return true;
        }
    }
    return false;
}

template <typename Derived>
Status ScanLocalState<Derived>::_eval_const_conjuncts(vectorized::VExpr* vexpr,
                                                      vectorized::VExprContext* expr_ctx,
                                                      PushDownType* pdt) {
    char* constant_val = nullptr;
    if (vexpr->is_constant()) {
        std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
        RETURN_IF_ERROR(vexpr->get_const_col(expr_ctx, &const_col_wrapper));
        if (const vectorized::ColumnConst* const_column =
                    check_and_get_column<vectorized::ColumnConst>(const_col_wrapper->column_ptr)) {
            constant_val = const_cast<char*>(const_column->get_data_at(0).data);
            if (constant_val == nullptr || !*reinterpret_cast<bool*>(constant_val)) {
                *pdt = PushDownType::ACCEPTABLE;
                _eos = true;
                _scan_dependency->set_ready();
            }
        } else if (const vectorized::ColumnVector<vectorized::UInt8>* bool_column =
                           check_and_get_column<vectorized::ColumnVector<vectorized::UInt8>>(
                                   const_col_wrapper->column_ptr)) {
            // TODO: If `vexpr->is_constant()` is true, a const column is expected here.
            //  But now we still don't cover all predicates for const expression.
            //  For example, for query `SELECT col FROM tbl WHERE 'PROMOTION' LIKE 'AAA%'`,
            //  predicate `like` will return a ColumnVector<UInt8> which contains a single value.
            LOG(WARNING) << "VExpr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << const_col_wrapper->column_ptr->get_name();
            DCHECK_EQ(bool_column->size(), 1);
            if (bool_column->size() == 1) {
                constant_val = const_cast<char*>(bool_column->get_data_at(0).data);
                if (constant_val == nullptr || !*reinterpret_cast<bool*>(constant_val)) {
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
Status ScanLocalState<Derived>::_normalize_in_and_eq_predicate(vectorized::VExpr* expr,
                                                               vectorized::VExprContext* expr_ctx,
                                                               SlotDescriptor* slot,
                                                               ColumnValueRange<T>& range,
                                                               PushDownType* pdt) {
    auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
            slot->is_nullable(), slot->type().precision, slot->type().scale);
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        HybridSetBase::IteratorBase* iter = nullptr;
        auto hybrid_set = expr->get_set_func();

        if (hybrid_set != nullptr) {
            // runtime filter produce VDirectInPredicate
            if (hybrid_set->size() <=
                _parent->cast<typename Derived::Parent>()._max_pushdown_conditions_per_column) {
                iter = hybrid_set->begin();
            } else if (_is_key_column(slot->col_name()) || _storage_no_merge()) {
                _filter_predicates.in_filters.emplace_back(slot->col_name(), expr->get_set_func());
                *pdt = PushDownType::ACCEPTABLE;
                return Status::OK();
            } else {
                *pdt = PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        } else {
            // normal in predicate
            vectorized::VInPredicate* pred = static_cast<vectorized::VInPredicate*>(expr);
            PushDownType temp_pdt = _should_push_down_in_predicate(pred, expr_ctx, false);
            if (temp_pdt == PushDownType::UNACCEPTABLE) {
                return Status::OK();
            }

            // begin to push InPredicate value into ColumnValueRange
            vectorized::InState* state = reinterpret_cast<vectorized::InState*>(
                    expr_ctx->fn_context(pred->fn_context_index())
                            ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

            // xx in (col, xx, xx) should not be push down
            if (!state->use_set) {
                return Status::OK();
            }

            iter = state->hybrid_set->begin();
        }

        while (iter->has_next()) {
            // column in (nullptr) is always false so continue to
            // dispose next item
            if (nullptr == iter->get_value()) {
                iter->next();
                continue;
            }
            auto value = const_cast<void*>(iter->get_value());
            RETURN_IF_ERROR(_change_value_range<true>(
                    temp_range, value, ColumnValueRange<T>::add_fixed_value_range, ""));
            iter->next();
        }
        range.intersection(temp_range);
        *pdt = PushDownType::ACCEPTABLE;
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };

        StringRef value;
        int slot_ref_child = -1;

        PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, eq_checker, temp_pdt));
        if (temp_pdt == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }
        DCHECK(slot_ref_child >= 0);
        // where A = nullptr should return empty result set
        auto fn_name = std::string("");
        if (value.data != nullptr) {
            if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                          T == TYPE_HLL) {
                auto val = StringRef(value.data, value.size);
                RETURN_IF_ERROR(_change_value_range<true>(
                        temp_range, reinterpret_cast<void*>(&val),
                        ColumnValueRange<T>::add_fixed_value_range, fn_name));
            } else {
                if (sizeof(typename PrimitiveTypeTraits<T>::CppType) != value.size) {
                    return Status::InternalError(
                            "PrimitiveType {} meet invalid input value size {}, expect size {}", T,
                            value.size, sizeof(typename PrimitiveTypeTraits<T>::CppType));
                }
                RETURN_IF_ERROR(_change_value_range<true>(
                        temp_range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                        ColumnValueRange<T>::add_fixed_value_range, fn_name));
            }
            range.intersection(temp_range);
        }
        *pdt = temp_pdt;
    }

    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_should_push_down_binary_predicate(
        vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
        StringRef* constant_val, int* slot_ref_child,
        const std::function<bool(const std::string&)>& fn_checker, PushDownType& pdt) {
    if (!fn_checker(fn_call->fn().name.function_name)) {
        pdt = PushDownType::UNACCEPTABLE;
        return Status::OK();
    }

    const auto& children = fn_call->children();
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (vectorized::VExpr::expr_without_cast(children[i])->node_type() !=
            TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            pdt = PushDownType::UNACCEPTABLE;
            return Status::OK();
        } else {
            std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
            RETURN_IF_ERROR(children[1 - i]->get_const_col(expr_ctx, &const_col_wrapper));
            if (const auto* const_column = check_and_get_column<vectorized::ColumnConst>(
                        const_col_wrapper->column_ptr)) {
                *slot_ref_child = i;
                *constant_val = const_column->get_data_at(0);
            } else {
                pdt = PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        }
    }
    pdt = PushDownType::ACCEPTABLE;
    return Status::OK();
}

template <typename Derived>
PushDownType ScanLocalState<Derived>::_should_push_down_in_predicate(
        vectorized::VInPredicate* pred, vectorized::VExprContext* expr_ctx, bool is_not_in) {
    if (pred->is_not_in() != is_not_in) {
        return PushDownType::UNACCEPTABLE;
    }
    return PushDownType::ACCEPTABLE;
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_not_in_and_not_eq_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, PushDownType* pdt) {
    bool is_fixed_range = range.is_fixed_value_range();
    auto not_in_range = ColumnValueRange<T>::create_empty_column_value_range(
            range.column_name(), slot->is_nullable(), slot->type().precision, slot->type().scale);
    PushDownType temp_pdt = PushDownType::UNACCEPTABLE;
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        /// `VDirectInPredicate` here should not be pushed down.
        /// here means the `VDirectInPredicate` is too big to be converted into `ColumnValueRange`.
        /// For non-key columns and `_storage_no_merge()` is false, this predicate should not be pushed down.
        if (expr->get_set_func() != nullptr) {
            *pdt = PushDownType::UNACCEPTABLE;
            return Status::OK();
        }

        vectorized::VInPredicate* pred = static_cast<vectorized::VInPredicate*>(expr);
        if ((temp_pdt = _should_push_down_in_predicate(pred, expr_ctx, true)) ==
            PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }

        // begin to push InPredicate value into ColumnValueRange
        vectorized::InState* state = reinterpret_cast<vectorized::InState*>(
                expr_ctx->fn_context(pred->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // xx in (col, xx, xx) should not be push down
        if (!state->use_set) {
            return Status::OK();
        }

        HybridSetBase::IteratorBase* iter = state->hybrid_set->begin();
        auto fn_name = std::string("");
        if (state->hybrid_set->contain_null()) {
            _eos = true;
            _scan_dependency->set_ready();
        }
        while (iter->has_next()) {
            // column not in (nullptr) is always true
            if (nullptr == iter->get_value()) {
                continue;
            }
            auto value = const_cast<void*>(iter->get_value());
            if (is_fixed_range) {
                RETURN_IF_ERROR(_change_value_range<true>(
                        range, value, ColumnValueRange<T>::remove_fixed_value_range, fn_name));
            } else {
                RETURN_IF_ERROR(_change_value_range<true>(
                        not_in_range, value, ColumnValueRange<T>::add_fixed_value_range, fn_name));
            }
            iter->next();
        }
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto ne_checker = [](const std::string& fn_name) { return fn_name == "ne"; };
        StringRef value;
        int slot_ref_child = -1;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, ne_checker, temp_pdt));
        if (temp_pdt == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }

        DCHECK(slot_ref_child >= 0);
        // where A = nullptr should return empty result set
        if (value.data != nullptr) {
            auto fn_name = std::string("");
            if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                          T == TYPE_HLL) {
                auto val = StringRef(value.data, value.size);
                if (is_fixed_range) {
                    RETURN_IF_ERROR(_change_value_range<true>(
                            range, reinterpret_cast<void*>(&val),
                            ColumnValueRange<T>::remove_fixed_value_range, fn_name));
                } else {
                    RETURN_IF_ERROR(_change_value_range<true>(
                            not_in_range, reinterpret_cast<void*>(&val),
                            ColumnValueRange<T>::add_fixed_value_range, fn_name));
                }
            } else {
                if (is_fixed_range) {
                    RETURN_IF_ERROR(_change_value_range<true>(
                            range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::remove_fixed_value_range, fn_name));
                } else {
                    RETURN_IF_ERROR(_change_value_range<true>(
                            not_in_range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::add_fixed_value_range, fn_name));
                }
            }
        }
    } else {
        return Status::OK();
    }

    if (is_fixed_range ||
        not_in_range.get_fixed_value_size() <=
                _parent->cast<typename Derived::Parent>()._max_pushdown_conditions_per_column) {
        if (!is_fixed_range) {
            _not_in_value_ranges.push_back(not_in_range);
        }
        *pdt = temp_pdt;
    }
    return Status::OK();
}

template <typename Derived>
template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
Status ScanLocalState<Derived>::_change_value_range(ColumnValueRange<PrimitiveType>& temp_range,
                                                    void* value,
                                                    const ChangeFixedValueRangeFunc& func,
                                                    const std::string& fn_name,
                                                    int slot_ref_child) {
    if constexpr (PrimitiveType == TYPE_DATE) {
        VecDateTimeValue tmp_value;
        memcpy(&tmp_value, value, sizeof(VecDateTimeValue));
        if constexpr (IsFixed) {
            if (!tmp_value.check_loss_accuracy_cast_to_date()) {
                func(temp_range,
                     reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                             &tmp_value));
            }
        } else {
            if (tmp_value.check_loss_accuracy_cast_to_date()) {
                if (fn_name == "lt" || fn_name == "ge") {
                    ++tmp_value;
                }
            }
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         &tmp_value));
        }
    } else if constexpr (PrimitiveType == TYPE_DATETIME) {
        if constexpr (IsFixed) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(value));
        } else {
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         reinterpret_cast<char*>(value)));
        }
    } else if constexpr ((PrimitiveType == TYPE_DECIMALV2) || (PrimitiveType == TYPE_CHAR) ||
                         (PrimitiveType == TYPE_VARCHAR) || (PrimitiveType == TYPE_HLL) ||
                         (PrimitiveType == TYPE_DATETIMEV2) || (PrimitiveType == TYPE_TINYINT) ||
                         (PrimitiveType == TYPE_SMALLINT) || (PrimitiveType == TYPE_INT) ||
                         (PrimitiveType == TYPE_BIGINT) || (PrimitiveType == TYPE_LARGEINT) ||
                         (PrimitiveType == TYPE_IPV4) || (PrimitiveType == TYPE_IPV6) ||
                         (PrimitiveType == TYPE_DECIMAL32) || (PrimitiveType == TYPE_DECIMAL64) ||
                         (PrimitiveType == TYPE_DECIMAL128I) ||
                         (PrimitiveType == TYPE_DECIMAL256) || (PrimitiveType == TYPE_STRING) ||
                         (PrimitiveType == TYPE_BOOLEAN) || (PrimitiveType == TYPE_DATEV2)) {
        if constexpr (IsFixed) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(value));
        } else {
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(value));
        }
    } else {
        static_assert(always_false_v<PrimitiveType>);
    }

    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_is_null_predicate(vectorized::VExpr* expr,
                                                             vectorized::VExprContext* expr_ctx,
                                                             SlotDescriptor* slot,
                                                             ColumnValueRange<T>& range,
                                                             PushDownType* pdt) {
    PushDownType temp_pdt = _should_push_down_is_null_predicate();
    if (temp_pdt == PushDownType::UNACCEPTABLE) {
        return Status::OK();
    }

    if (TExprNodeType::FUNCTION_CALL == expr->node_type()) {
        if (reinterpret_cast<vectorized::VectorizedFnCall*>(expr)->fn().name.function_name ==
            "is_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->is_nullable(), slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(true);
            range.intersection(temp_range);
            *pdt = temp_pdt;
        } else if (reinterpret_cast<vectorized::VectorizedFnCall*>(expr)->fn().name.function_name ==
                   "is_not_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->is_nullable(), slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(false);
            range.intersection(temp_range);
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_noneq_binary_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, PushDownType* pdt) {
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq" && fn_name != "eq_for_null";
        };
        StringRef value;
        int slot_ref_child = -1;
        PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, noneq_checker, temp_pdt));
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            DCHECK(slot_ref_child >= 0);
            const std::string& fn_name =
                    reinterpret_cast<vectorized::VectorizedFnCall*>(expr)->fn().name.function_name;

            // where A = nullptr should return empty result set
            if (value.data != nullptr) {
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringRef(value.data, value.size);
                    RETURN_IF_ERROR(_change_value_range<false>(range, reinterpret_cast<void*>(&val),
                                                               ColumnValueRange<T>::add_value_range,
                                                               fn_name, slot_ref_child));
                } else {
                    RETURN_IF_ERROR(_change_value_range<false>(
                            range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::add_value_range, fn_name, slot_ref_child));
                }
                *pdt = temp_pdt;
            }
        }
    }
    return Status::OK();
}

template <typename Derived>
void ScanLocalState<Derived>::_normalize_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, PushDownType* pdt,
        bool _is_runtime_filter_predicate,
        const std::function<bool(const vectorized::VExprSPtrs&,
                                 std::shared_ptr<vectorized::VSlotRef>&, vectorized::VExprSPtr&)>&
                in_predicate_checker,
        const std::function<bool(const vectorized::VExprSPtrs&,
                                 std::shared_ptr<vectorized::VSlotRef>&, vectorized::VExprSPtr&)>&
                eq_predicate_checker) {
    if (TExprNodeType::COMPOUND_PRED == expr->node_type()) {
        auto compound_fn_name = expr->fn().name.function_name;
        auto children_num = expr->children().size();
        for (auto i = 0; i < children_num; ++i) {
            auto* child_expr = expr->children()[i].get();
            if (TExprNodeType::BINARY_PRED == child_expr->node_type() ||
                TExprNodeType::IN_PRED == child_expr->node_type() ||
                TExprNodeType::MATCH_PRED == child_expr->node_type()) {
                SlotDescriptor* slot = nullptr;
                ColumnValueRangeType* range_on_slot = nullptr;
                if (_is_predicate_acting_on_slot(child_expr, in_predicate_checker, &slot,
                                                 &range_on_slot) ||
                    _is_predicate_acting_on_slot(child_expr, eq_predicate_checker, &slot,
                                                 &range_on_slot)) {
                    ColumnValueRangeType active_range =
                            *range_on_slot; // copy, in order not to affect the range in the _colname_to_value_range
                    std::visit(
                            [&](auto& value_range) {
                                Defer mark_runtime_filter_flag {[&]() {
                                    value_range.mark_runtime_filter_predicate(
                                            _is_runtime_filter_predicate);
                                }};
                                if (TExprNodeType::BINARY_PRED == child_expr->node_type()) {
                                    static_cast<void>(_normalize_binary_compound_predicate(
                                            child_expr, expr_ctx, slot, value_range, pdt));
                                } else if (TExprNodeType::IN_PRED == child_expr->node_type()) {
                                    static_cast<void>(_normalize_in_and_not_in_compound_predicate(
                                            child_expr, expr_ctx, slot, value_range, pdt));
                                } else {
                                    static_cast<void>(_normalize_match_compound_predicate(
                                            child_expr, expr_ctx, slot, value_range, pdt));
                                }
                            },
                            active_range);

                    _compound_value_ranges.emplace_back(active_range);
                }
            } else if (TExprNodeType::COMPOUND_PRED == child_expr->node_type()) {
                _normalize_compound_predicate(child_expr, expr_ctx, pdt,
                                              _is_runtime_filter_predicate, in_predicate_checker,
                                              eq_predicate_checker);
            }
        }
    }
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_binary_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, PushDownType* pdt) {
    DCHECK(expr->children().size() == 2);
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };
        auto ne_checker = [](const std::string& fn_name) { return fn_name == "ne"; };
        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq" && fn_name != "eq_for_null";
        };

        StringRef value;
        int slot_ref_child = -1;
        PushDownType eq_pdt;
        PushDownType ne_pdt;
        PushDownType noneq_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, eq_checker, eq_pdt));
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, ne_checker, ne_pdt));
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, noneq_checker, noneq_pdt));
        if (eq_pdt == PushDownType::UNACCEPTABLE && ne_pdt == PushDownType::UNACCEPTABLE &&
            noneq_pdt == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }
        DCHECK(slot_ref_child >= 0);
        const std::string& fn_name =
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr)->fn().name.function_name;
        if (eq_pdt == PushDownType::ACCEPTABLE || ne_pdt == PushDownType::ACCEPTABLE ||
            noneq_pdt == PushDownType::ACCEPTABLE) {
            if (value.data != nullptr) {
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringRef(value.data, value.size);
                    RETURN_IF_ERROR(_change_value_range<false>(
                            range, reinterpret_cast<void*>(&val),
                            ColumnValueRange<T>::add_compound_value_range, fn_name,
                            slot_ref_child));
                } else {
                    RETURN_IF_ERROR(_change_value_range<false>(
                            range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                            ColumnValueRange<T>::add_compound_value_range, fn_name,
                            slot_ref_child));
                }
            }
            *pdt = PushDownType::ACCEPTABLE;
        }
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_in_and_not_in_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, PushDownType* pdt) {
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        std::string fn_name = expr->op() == TExprOpcode::type::FILTER_IN ? "in" : "not_in";

        HybridSetBase::IteratorBase* iter = nullptr;
        auto hybrid_set = expr->get_set_func();

        if (hybrid_set != nullptr) {
            *pdt = PushDownType::UNACCEPTABLE;
            return Status::OK();
        } else {
            auto* pred = static_cast<vectorized::VInPredicate*>(expr);

            auto* state = reinterpret_cast<vectorized::InState*>(
                    expr_ctx->fn_context(pred->fn_context_index())
                            ->get_function_state(FunctionContext::FRAGMENT_LOCAL));

            if (!state->use_set) {
                return Status::OK();
            }

            iter = state->hybrid_set->begin();

            if (state->hybrid_set->contain_null()) {
                *pdt = PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        }

        while (iter->has_next()) {
            if (nullptr == iter->get_value()) {
                iter->next();
                continue;
            }
            auto* value = const_cast<void*>(iter->get_value());
            RETURN_IF_ERROR(_change_value_range<false>(
                    range, value, ColumnValueRange<T>::add_compound_value_range, fn_name, 0));
            iter->next();
        }
        *pdt = PushDownType::ACCEPTABLE;
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_match_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, PushDownType* pdt) {
    DCHECK(expr->children().size() == 2);
    if (TExprNodeType::MATCH_PRED == expr->node_type()) {
        RETURN_IF_ERROR(_normalize_match_predicate(expr, expr_ctx, slot, range, pdt));
    }

    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_match_predicate(vectorized::VExpr* expr,
                                                           vectorized::VExprContext* expr_ctx,
                                                           SlotDescriptor* slot,
                                                           ColumnValueRange<T>& range,
                                                           PushDownType* pdt) {
    if (TExprNodeType::MATCH_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        // create empty range as temp range, temp range should do intersection on range
        auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                slot->is_nullable(), slot->type().precision, slot->type().scale);
        // Normalize match conjuncts like 'where col match value'

        auto match_checker = [](const std::string& fn_name) { return is_match_condition(fn_name); };
        StringRef value;
        int slot_ref_child = -1;
        PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, match_checker, temp_pdt));
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            DCHECK(slot_ref_child >= 0);
            if (value.data != nullptr) {
                using CppType = typename PrimitiveTypeTraits<T>::CppType;
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringRef(value.data, value.size);
                    ColumnValueRange<T>::add_match_value_range(temp_range,
                                                               to_match_type(expr->op()),
                                                               reinterpret_cast<CppType*>(&val));
                } else {
                    ColumnValueRange<T>::add_match_value_range(
                            temp_range, to_match_type(expr->op()),
                            reinterpret_cast<CppType*>(const_cast<char*>(value.data)));
                }
                range.intersection(temp_range);
            }
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_prepare_scanners() {
    std::list<vectorized::VScannerSPtr> scanners;
    RETURN_IF_ERROR(_init_scanners(&scanners));
    // Init scanner wrapper
    for (auto it = scanners.begin(); it != scanners.end(); ++it) {
        _scanners.emplace_back(std::make_shared<vectorized::ScannerDelegate>(*it));
    }
    if (scanners.empty()) {
        _eos = true;
        _scan_dependency->set_always_ready();
    } else {
        for (auto& scanner : scanners) {
            scanner->set_query_statistics(_query_statistics.get());
        }
        COUNTER_SET(_num_scanners, static_cast<int64_t>(scanners.size()));
        RETURN_IF_ERROR(_start_scanners(_scanners));
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_start_scanners(
        const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners) {
    auto& p = _parent->cast<typename Derived::Parent>();
    _scanner_ctx = vectorized::ScannerContext::create_shared(
            state(), this, p._output_tuple_desc, p.output_row_descriptor(), scanners, p.limit(),
            state()->scan_queue_mem_limit(), _scan_dependency,
            // 1. If data distribution is ignored , we use 1 instance to scan.
            // 2. Else, file scanner will consume much memory so we use config::doris_scanner_thread_pool_thread_num / query_parallel_instance_num scanners to scan.
            p.ignore_data_distribution() && !p.is_file_scan_operator()
                    ? 1
                    : state()->query_parallel_instance_num());
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
int64_t ScanLocalState<Derived>::get_push_down_count() {
    return _parent->cast<typename Derived::Parent>()._push_down_count;
}

template <typename Derived>
int64_t ScanLocalState<Derived>::limit_per_scanner() {
    return _parent->cast<typename Derived::Parent>()._limit_per_scanner;
}

template <typename Derived>
Status ScanLocalState<Derived>::clone_conjunct_ctxs(vectorized::VExprContextSPtrs& conjuncts) {
    if (!_conjuncts.empty()) {
        std::unique_lock l(_rf_locks);
        conjuncts.resize(_conjuncts.size());
        for (size_t i = 0; i != _conjuncts.size(); ++i) {
            RETURN_IF_ERROR(_conjuncts[i]->clone(state(), conjuncts[i]));
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_init_profile() {
    // 1. counters for scan node
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _total_throughput_counter =
            profile()->add_rate_counter("TotalReadThroughput", _rows_read_counter);
    _num_scanners = ADD_COUNTER(_runtime_profile, "NumScanners", TUnit::UNIT);

    // 2. counters for scanners
    _scanner_profile.reset(new RuntimeProfile("VScanner"));
    profile()->add_child(_scanner_profile.get(), true, nullptr);

    _memory_usage_counter = ADD_LABEL_COUNTER_WITH_LEVEL(_scanner_profile, "MemoryUsage", 1);
    _free_blocks_memory_usage =
            _scanner_profile->AddHighWaterMarkCounter("FreeBlocks", TUnit::BYTES, "MemoryUsage", 1);
    _newly_create_free_blocks_num =
            ADD_COUNTER(_scanner_profile, "NewlyCreateFreeBlocksNum", TUnit::UNIT);
    _scale_up_scanners_counter = ADD_COUNTER(_scanner_profile, "NumScaleUpScanners", TUnit::UNIT);
    // time of transfer thread to wait for block from scan thread
    _scanner_wait_batch_timer = ADD_TIMER(_scanner_profile, "ScannerBatchWaitTime");
    _scanner_sched_counter = ADD_COUNTER(_scanner_profile, "ScannerSchedCount", TUnit::UNIT);
    _scanner_ctx_sched_time = ADD_TIMER(_scanner_profile, "ScannerCtxSchedTime");

    _scan_timer = ADD_TIMER(_scanner_profile, "ScannerGetBlockTime");
    _scan_cpu_timer = ADD_TIMER(_scanner_profile, "ScannerCpuTime");
    _convert_block_timer = ADD_TIMER(_scanner_profile, "ScannerConvertBlockTime");
    _filter_timer = ADD_TIMER(_scanner_profile, "ScannerFilterTime");

    // time of scan thread to wait for worker thread of the thread pool
    _scanner_wait_worker_timer = ADD_TIMER(_runtime_profile, "ScannerWorkerWaitTime");

    _max_scanner_thread_num = ADD_COUNTER(_runtime_profile, "MaxScannerThreadNum", TUnit::UNIT);

    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_get_topn_filters(RuntimeState* state) {
    auto& p = _parent->cast<typename Derived::Parent>();
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
    return Status::OK();
}

template <typename Derived>
void ScanLocalState<Derived>::_filter_and_collect_cast_type_for_variant(
        const vectorized::VExpr* expr,
        phmap::flat_hash_map<std::string, std::vector<PrimitiveType>>& colname_to_cast_types) {
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
        PrimitiveType cast_dst_type =
                cast_expr->get_target_type()->get_type_as_type_descriptor().type;
        if (src_slot_desc->type().is_variant_type()) {
            colname_to_cast_types[src_slot_desc->col_name()].push_back(cast_dst_type);
        }
    }
    for (const auto& child : expr->children()) {
        _filter_and_collect_cast_type_for_variant(child.get(), colname_to_cast_types);
    }
}

template <typename Derived>
void ScanLocalState<Derived>::get_cast_types_for_variants() {
    phmap::flat_hash_map<std::string, std::vector<PrimitiveType>> colname_to_cast_types;
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
    if (!tnode.__isset.conjuncts || tnode.conjuncts.empty()) {
        // Which means the request could be fullfilled in a single segment iterator request.
        if (tnode.limit > 0 && tnode.limit < 1024) {
            _should_run_serial = true;
        }
    }
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
    } else {
        _max_scan_key_num = config::doris_max_scan_key_num;
    }
    if (query_options.__isset.max_pushdown_conditions_per_column) {
        _max_pushdown_conditions_per_column = query_options.max_pushdown_conditions_per_column;
    } else {
        _max_pushdown_conditions_per_column = config::max_pushdown_conditions_per_column;
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
        topn_filter_source_node_ids = tnode.topn_filter_source_node_ids;
    }
    return Status::OK();
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::open(RuntimeState* state) {
    _input_tuple_desc = state->desc_tbl().get_tuple_descriptor(_input_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    RETURN_IF_ERROR(OperatorX<LocalStateType>::open(state));

    const auto slots = _output_tuple_desc->slots();
    for (auto* slot : slots) {
        _colname_to_slot_id[slot->col_name()] = slot->id();
        _slot_id_to_slot_desc[slot->id()] = slot;
    }
    for (auto id : topn_filter_source_node_ids) {
        if (!state->get_query_ctx()->has_runtime_predicate(id)) {
            // compatible with older versions fe
            continue;
        }

        state->get_query_ctx()->get_runtime_predicate(id).init_target(node_id(),
                                                                      _slot_id_to_slot_desc);
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
    if (_scanner_ctx) {
        _scanner_ctx->stop_scanners(state);
    }
    std::list<std::shared_ptr<vectorized::ScannerDelegate>> {}.swap(_scanners);
    COUNTER_SET(_wait_for_dependency_timer, _scan_dependency->watcher_elapse_time());
    COUNTER_SET(_wait_for_rf_timer, rf_time);

    return PipelineXLocalState<>::close(state);
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    // in inverted index apply logic, in order to optimize query performance,
    // we built some temporary columns into block, these columns only used in scan node level,
    // remove them when query leave scan node to avoid other nodes use block->columns() to make a wrong decision
    Defer drop_block_temp_column {[&]() {
        std::unique_lock l(local_state._block_lock);
        auto all_column_names = block->get_names();
        for (auto& name : all_column_names) {
            if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
                block->erase(name);
            }
        }
    }};

    if (state->is_cancelled()) {
        if (local_state._scanner_ctx) {
            local_state._scanner_ctx->stop_scanners(state);
        }
        return state->cancel_reason();
    }

    if (local_state._eos) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(local_state._scanner_ctx->get_block_from_queue(state, block, eos, 0));

    local_state.reached_limit(block, eos);
    if (*eos) {
        // reach limit, stop the scanners.
        local_state._scanner_ctx->stop_scanners(state);
    }

    return Status::OK();
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

} // namespace doris::pipeline
