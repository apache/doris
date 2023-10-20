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

#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/meta_scan_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/operator.h"
#include "vec/exec/runtime_filter_consumer.h"
#include "vec/exec/scan/pip_scanner_context.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/in.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(ScanOperator, SourceOperator)

bool ScanOperator::can_read() {
    if (!_node->_opened) {
        if (_node->_should_create_scanner || _node->ready_to_open()) {
            return true;
        } else {
            return false;
        }
    } else {
        if (_node->_eos || _node->_scanner_ctx->done()) {
            // _eos: need eos
            // _scanner_ctx->done(): need finish
            // _scanner_ctx->no_schedule(): should schedule _scanner_ctx
            return true;
        } else {
            if (_node->_scanner_ctx->get_num_running_scanners() == 0 &&
                _node->_scanner_ctx->should_be_scheduled()) {
                _node->_scanner_ctx->reschedule_scanner_ctx();
            }
            return _node->ready_to_read(); // there are some blocks to process
        }
    }
}

bool ScanOperator::is_pending_finish() const {
    return _node->_scanner_ctx && !_node->_scanner_ctx->no_schedule();
}

Status ScanOperator::try_close(RuntimeState* state) {
    return _node->try_close(state);
}

bool ScanOperator::runtime_filters_are_ready_or_timeout() {
    return _node->runtime_filters_are_ready_or_timeout();
}

std::string ScanOperator::debug_string() const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, scanner_ctx is null: {} ",
                   SourceOperator::debug_string(), _node->_scanner_ctx == nullptr);
    if (_node->_scanner_ctx) {
        fmt::format_to(debug_string_buffer, ", num_running_scanners = {}, num_scheduling_ctx = {} ",
                       _node->_scanner_ctx->get_num_running_scanners(),
                       _node->_scanner_ctx->get_num_scheduling_ctx());
    }
    return fmt::to_string(debug_string_buffer);
}

#define RETURN_IF_PUSH_DOWN(stmt, status)                           \
    if (pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) { \
        status = stmt;                                              \
        if (!status.ok()) {                                         \
            return;                                                 \
        }                                                           \
    } else {                                                        \
        return;                                                     \
    }

template <typename Derived>
ScanLocalState<Derived>::ScanLocalState(RuntimeState* state_, OperatorXBase* parent_)
        : ScanLocalStateBase(state_, parent_) {}

template <typename Derived>
bool ScanLocalState<Derived>::ready_to_read() {
    return !_scanner_ctx->empty_in_queue(0);
}

template <typename Derived>
bool ScanLocalState<Derived>::should_run_serial() const {
    return _parent->cast<typename Derived::Parent>()._should_run_serial;
}

template <typename Derived>
Status ScanLocalState<Derived>::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(RuntimeFilterConsumer::init(state));

    _source_dependency = OrDependency::create_shared(PipelineXLocalState<>::_parent->id());

    _open_dependency = OpenDependency::create_shared(PipelineXLocalState<>::_parent->id());
    _source_dependency->add_child(_open_dependency);
    _eos_dependency = EosDependency::create_shared(PipelineXLocalState<>::_parent->id());
    _source_dependency->add_child(_eos_dependency);

    auto& p = _parent->cast<typename Derived::Parent>();
    set_scan_ranges(info.scan_ranges);
    _common_expr_ctxs_push_down.resize(p._common_expr_ctxs_push_down.size());
    for (size_t i = 0; i < _common_expr_ctxs_push_down.size(); i++) {
        RETURN_IF_ERROR(
                p._common_expr_ctxs_push_down[i]->clone(state, _common_expr_ctxs_push_down[i]));
    }
    _stale_expr_ctxs.resize(p._stale_expr_ctxs.size());
    for (size_t i = 0; i < _stale_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._stale_expr_ctxs[i]->clone(state, _stale_expr_ctxs[i]));
    }
    // init profile for runtime filter
    RuntimeFilterConsumer::_init_profile(profile());

    // 1: running at not pipeline mode will init profile.
    // 2: the scan node should create scanner at pipeline mode will init profile.
    // during pipeline mode with more instances, olap scan node maybe not new VScanner object,
    // so the profile of VScanner and SegmentIterator infos are always empty, could not init those.
    RETURN_IF_ERROR(_init_profile());
    // if you want to add some profile in scan node, even it have not new VScanner object
    // could add here, not in the _init_profile() function
    _get_next_timer = ADD_TIMER(_runtime_profile, "GetNextTime");

    _prepare_rf_timer(_runtime_profile.get());
    _alloc_resource_timer = ADD_TIMER(_runtime_profile, "AllocateResourceTime");

    static const std::string timer_name = "WaitForDependencyTime";
    _wait_for_dependency_timer = ADD_TIMER(_runtime_profile, timer_name);
    _wait_for_data_timer = ADD_CHILD_TIMER(_runtime_profile, "WaitForData", timer_name);
    _wait_for_scanner_done_timer =
            ADD_CHILD_TIMER(_runtime_profile, "WaitForScannerDone", timer_name);
    _wait_for_eos_timer = ADD_CHILD_TIMER(_runtime_profile, "WaitForEos", timer_name);
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::open(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    if (_open_dependency == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_acquire_runtime_filter());
    RETURN_IF_ERROR(_process_conjuncts());

    auto status =
            _eos_dependency->read_blocked_by() == nullptr ? Status::OK() : _prepare_scanners();
    if (_scanner_ctx) {
        DCHECK(_eos_dependency->read_blocked_by() != nullptr && _num_scanners->value() > 0);
        RETURN_IF_ERROR(_scanner_ctx->init());
        RETURN_IF_ERROR(state->exec_env()->scanner_scheduler()->submit(_scanner_ctx.get()));
    }
    _source_dependency->remove_first_child();
    _open_dependency = nullptr;
    return status;
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_conjuncts() {
    auto& p = _parent->cast<typename Derived::Parent>();
    // The conjuncts is always on output tuple, so use _output_tuple_desc;
    std::vector<SlotDescriptor*> slots = p._output_tuple_desc->slots();

    for (int slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
        _colname_to_slot_id[slots[slot_idx]->col_name()] = slots[slot_idx]->id();

        auto type = slots[slot_idx]->type().type;
        if (slots[slot_idx]->type().type == TYPE_ARRAY) {
            type = slots[slot_idx]->type().children[0].type;
            if (type == TYPE_ARRAY) {
                continue;
            }
        }
        switch (type) {
#define M(NAME)                                                                              \
    case TYPE_##NAME: {                                                                      \
        ColumnValueRange<TYPE_##NAME> range(                                                 \
                slots[slot_idx]->col_name(), slots[slot_idx]->is_nullable(),                 \
                slots[slot_idx]->type().precision, slots[slot_idx]->type().scale);           \
        _slot_id_to_value_range[slots[slot_idx]->id()] = std::pair {slots[slot_idx], range}; \
        break;                                                                               \
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
    M(DECIMALV2)                    \
    M(BOOLEAN)
            APPLY_FOR_PRIMITIVE_TYPE(M)
#undef M
        default: {
            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slots[slot_idx]->col_name()
                          << "]";
            break;
        }
        }
    }

    for (auto it = _conjuncts.begin(); it != _conjuncts.end();) {
        auto& conjunct = *it;
        if (conjunct->root()) {
            vectorized::VExprSPtr new_root;
            RETURN_IF_ERROR(_normalize_predicate(conjunct->root(), conjunct.get(), new_root));
            if (new_root) {
                conjunct->set_root(new_root);
                if (_should_push_down_common_expr()) {
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
                        _eos_dependency->set_ready_for_read();
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
            bool _is_runtime_filter_predicate =
                    _rf_vexpr_set.find(conjunct_expr_root) != _rf_vexpr_set.end();
            SlotDescriptor* slot = nullptr;
            ColumnValueRangeType* range = nullptr;
            vectorized::VScanNode::PushDownType pdt =
                    vectorized::VScanNode::PushDownType::UNACCEPTABLE;
            RETURN_IF_ERROR(_eval_const_conjuncts(cur_expr, context, &pdt));
            if (pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE) {
                output_expr = nullptr;
                return Status::OK();
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

            if (pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE &&
                TExprNodeType::COMPOUND_PRED == cur_expr->node_type()) {
                static_cast<void>(_normalize_compound_predicate(
                        cur_expr, context, &pdt, _is_runtime_filter_predicate, in_predicate_checker,
                        eq_predicate_checker));
                output_expr = conjunct_expr_root; // remaining in conjunct tree
                return Status::OK();
            }

            if (pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE &&
                TExprNodeType::MATCH_PRED == cur_expr->node_type()) {
                // remaining it in the expr tree, in order to filter by function if the pushdown
                // match_predicate failed to apply inverted index in the storage layer
                output_expr = conjunct_expr_root; // remaining in conjunct tree
                return Status::OK();
            }

            if (pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE &&
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
                                                        SlotDescriptor* slot,
                                                        vectorized::VScanNode::PushDownType* pdt) {
    if (TExprNodeType::BLOOM_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        vectorized::VScanNode::PushDownType temp_pdt = _should_push_down_bloom_filter();
        if (temp_pdt != vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
                                                         SlotDescriptor* slot,
                                                         vectorized::VScanNode::PushDownType* pdt) {
    if (TExprNodeType::BITMAP_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        vectorized::VScanNode::PushDownType temp_pdt = _should_push_down_bitmap_filter();
        if (temp_pdt != vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
            _filter_predicates.bitmap_filters.emplace_back(slot->col_name(),
                                                           expr->get_bitmap_filter_func());
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_normalize_function_filters(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        vectorized::VScanNode::PushDownType* pdt) {
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
        vectorized::VScanNode::PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_function_filter(
                reinterpret_cast<vectorized::VectorizedFnCall*>(fn_expr), expr_ctx, &val, &fn_ctx,
                temp_pdt));
        if (temp_pdt != vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
bool ScanLocalState<Derived>::_ignore_cast(SlotDescriptor* slot, vectorized::VExpr* expr) {
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
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
                                                      vectorized::VScanNode::PushDownType* pdt) {
    char* constant_val = nullptr;
    if (vexpr->is_constant()) {
        std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
        RETURN_IF_ERROR(vexpr->get_const_col(expr_ctx, &const_col_wrapper));
        if (const vectorized::ColumnConst* const_column =
                    check_and_get_column<vectorized::ColumnConst>(const_col_wrapper->column_ptr)) {
            constant_val = const_cast<char*>(const_column->get_data_at(0).data);
            if (constant_val == nullptr || !*reinterpret_cast<bool*>(constant_val)) {
                *pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
                _eos_dependency->set_ready_for_read();
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
                    *pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
                    _eos_dependency->set_ready_for_read();
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
Status ScanLocalState<Derived>::_normalize_in_and_eq_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
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
            } else {
                _filter_predicates.in_filters.emplace_back(slot->col_name(), expr->get_set_func());
                *pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
                return Status::OK();
            }
        } else {
            // normal in predicate
            vectorized::VInPredicate* pred = static_cast<vectorized::VInPredicate*>(expr);
            vectorized::VScanNode::PushDownType temp_pdt =
                    _should_push_down_in_predicate(pred, expr_ctx, false);
            if (temp_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
        *pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };

        StringRef value;
        int slot_ref_child = -1;

        vectorized::VScanNode::PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, eq_checker, temp_pdt));
        if (temp_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
        const std::function<bool(const std::string&)>& fn_checker,
        vectorized::VScanNode::PushDownType& pdt) {
    if (!fn_checker(fn_call->fn().name.function_name)) {
        pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
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
            pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
            return Status::OK();
        } else {
            std::shared_ptr<ColumnPtrWrapper> const_col_wrapper;
            RETURN_IF_ERROR(children[1 - i]->get_const_col(expr_ctx, &const_col_wrapper));
            if (const vectorized::ColumnConst* const_column =
                        check_and_get_column<vectorized::ColumnConst>(
                                const_col_wrapper->column_ptr)) {
                *slot_ref_child = i;
                *constant_val = const_column->get_data_at(0);
            } else {
                pdt = vectorized::VScanNode::PushDownType::UNACCEPTABLE;
                return Status::OK();
            }
        }
    }
    pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
    return Status::OK();
}

template <typename Derived>
vectorized::VScanNode::PushDownType ScanLocalState<Derived>::_should_push_down_in_predicate(
        vectorized::VInPredicate* pred, vectorized::VExprContext* expr_ctx, bool is_not_in) {
    if (pred->is_not_in() != is_not_in) {
        return vectorized::VScanNode::PushDownType::UNACCEPTABLE;
    }
    return vectorized::VScanNode::PushDownType::ACCEPTABLE;
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_not_in_and_not_eq_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    bool is_fixed_range = range.is_fixed_value_range();
    auto not_in_range = ColumnValueRange<T>::create_empty_column_value_range(
            range.column_name(), slot->is_nullable(), slot->type().precision, slot->type().scale);
    vectorized::VScanNode::PushDownType temp_pdt =
            vectorized::VScanNode::PushDownType::UNACCEPTABLE;
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        vectorized::VInPredicate* pred = static_cast<vectorized::VInPredicate*>(expr);
        if ((temp_pdt = _should_push_down_in_predicate(pred, expr_ctx, true)) ==
            vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
        if (!is_fixed_range && state->null_in_set) {
            _eos_dependency->set_ready_for_read();
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
        if (temp_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
        vectorized::VecDateTimeValue tmp_value;
        memcpy(&tmp_value, value, sizeof(vectorized::VecDateTimeValue));
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
                         (PrimitiveType == TYPE_DECIMAL32) || (PrimitiveType == TYPE_DECIMAL64) ||
                         (PrimitiveType == TYPE_DECIMAL128I) || (PrimitiveType == TYPE_STRING) ||
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
Status ScanLocalState<Derived>::_normalize_is_null_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    vectorized::VScanNode::PushDownType temp_pdt = _should_push_down_is_null_predicate();
    if (temp_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq";
        };
        StringRef value;
        int slot_ref_child = -1;
        vectorized::VScanNode::PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, noneq_checker, temp_pdt));
        if (temp_pdt != vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
Status ScanLocalState<Derived>::_normalize_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
        vectorized::VScanNode::PushDownType* pdt, bool _is_runtime_filter_predicate,
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
            auto child_expr = expr->children()[i].get();
            if (TExprNodeType::BINARY_PRED == child_expr->node_type()) {
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
                                static_cast<void>(_normalize_binary_in_compound_predicate(
                                        child_expr, expr_ctx, slot, value_range, pdt));
                            },
                            active_range);

                    _compound_value_ranges.emplace_back(active_range);
                }
            } else if (TExprNodeType::MATCH_PRED == child_expr->node_type()) {
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
                                static_cast<void>(_normalize_match_in_compound_predicate(
                                        child_expr, expr_ctx, slot, value_range, pdt));
                            },
                            active_range);

                    _compound_value_ranges.emplace_back(active_range);
                }
            } else if (TExprNodeType::COMPOUND_PRED == child_expr->node_type()) {
                static_cast<void>(_normalize_compound_predicate(
                        child_expr, expr_ctx, pdt, _is_runtime_filter_predicate,
                        in_predicate_checker, eq_predicate_checker));
            }
        }
    }

    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_binary_in_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    DCHECK(expr->children().size() == 2);
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };
        auto ne_checker = [](const std::string& fn_name) { return fn_name == "ne"; };
        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq";
        };

        StringRef value;
        int slot_ref_child = -1;
        vectorized::VScanNode::PushDownType eq_pdt;
        vectorized::VScanNode::PushDownType ne_pdt;
        vectorized::VScanNode::PushDownType noneq_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, eq_checker, eq_pdt));
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, ne_checker, ne_pdt));
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, noneq_checker, noneq_pdt));
        if (eq_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE &&
            ne_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE &&
            noneq_pdt == vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }
        DCHECK(slot_ref_child >= 0);
        const std::string& fn_name =
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr)->fn().name.function_name;
        if (eq_pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE ||
            ne_pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE ||
            noneq_pdt == vectorized::VScanNode::PushDownType::ACCEPTABLE) {
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
            *pdt = vectorized::VScanNode::PushDownType::ACCEPTABLE;
        }
    }
    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_match_in_compound_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    DCHECK(expr->children().size() == 2);
    if (TExprNodeType::MATCH_PRED == expr->node_type()) {
        RETURN_IF_ERROR(_normalize_match_predicate(expr, expr_ctx, slot, range, pdt));
    }

    return Status::OK();
}

template <typename Derived>
template <PrimitiveType T>
Status ScanLocalState<Derived>::_normalize_match_predicate(
        vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
        ColumnValueRange<T>& range, vectorized::VScanNode::PushDownType* pdt) {
    if (TExprNodeType::MATCH_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        // create empty range as temp range, temp range should do intersection on range
        auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                slot->is_nullable(), slot->type().precision, slot->type().scale);
        // Normalize match conjuncts like 'where col match value'

        auto match_checker = [](const std::string& fn_name) { return is_match_condition(fn_name); };
        StringRef value;
        int slot_ref_child = -1;
        vectorized::VScanNode::PushDownType temp_pdt;
        RETURN_IF_ERROR(_should_push_down_binary_predicate(
                reinterpret_cast<vectorized::VectorizedFnCall*>(expr), expr_ctx, &value,
                &slot_ref_child, match_checker, temp_pdt));
        if (temp_pdt != vectorized::VScanNode::PushDownType::UNACCEPTABLE) {
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
    if (scanners.empty()) {
        _eos_dependency->set_ready_for_read();
    } else {
        COUNTER_SET(_num_scanners, static_cast<int64_t>(scanners.size()));
        RETURN_IF_ERROR(_start_scanners(scanners));
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::_start_scanners(
        const std::list<vectorized::VScannerSPtr>& scanners) {
    auto& p = _parent->cast<typename Derived::Parent>();
    _scanner_ctx = PipScannerContext::create_shared(state(), this, p._output_tuple_desc, scanners,
                                                    p.limit(), state()->scan_queue_mem_limit(),
                                                    p._col_distribute_ids, 1);
    _scanner_done_dependency = ScannerDoneDependency::create_shared(p.id(), _scanner_ctx.get());
    _source_dependency->add_child(_scanner_done_dependency);
    _data_ready_dependency = DataReadyDependency::create_shared(p.id(), _scanner_ctx.get());
    _source_dependency->add_child(_data_ready_dependency);

    _scanner_ctx->set_dependency(_data_ready_dependency, _scanner_done_dependency,
                                 _finish_dependency);
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

    _memory_usage_counter = ADD_LABEL_COUNTER(_scanner_profile, "MemoryUsage");
    _queued_blocks_memory_usage =
            _scanner_profile->AddHighWaterMarkCounter("QueuedBlocks", TUnit::BYTES, "MemoryUsage");
    _free_blocks_memory_usage =
            _scanner_profile->AddHighWaterMarkCounter("FreeBlocks", TUnit::BYTES, "MemoryUsage");
    _newly_create_free_blocks_num =
            ADD_COUNTER(_scanner_profile, "NewlyCreateFreeBlocksNum", TUnit::UNIT);
    // time of transfer thread to wait for block from scan thread
    _scanner_wait_batch_timer = ADD_TIMER(_scanner_profile, "ScannerBatchWaitTime");
    _scanner_sched_counter = ADD_COUNTER(_scanner_profile, "ScannerSchedCount", TUnit::UNIT);
    _scanner_ctx_sched_counter = ADD_COUNTER(_scanner_profile, "ScannerCtxSchedCount", TUnit::UNIT);
    _scanner_ctx_sched_time = ADD_TIMER(_scanner_profile, "ScannerCtxSchedTime");

    _scan_timer = ADD_TIMER(_scanner_profile, "ScannerGetBlockTime");
    _scan_cpu_timer = ADD_TIMER(_scanner_profile, "ScannerCpuTime");
    _prefilter_timer = ADD_TIMER(_scanner_profile, "ScannerPrefilterTime");
    _convert_block_timer = ADD_TIMER(_scanner_profile, "ScannerConvertBlockTime");
    _filter_timer = ADD_TIMER(_scanner_profile, "ScannerFilterTime");

    // time of scan thread to wait for worker thread of the thread pool
    _scanner_wait_worker_timer = ADD_TIMER(_runtime_profile, "ScannerWorkerWaitTime");

    _max_scanner_thread_num = ADD_COUNTER(_runtime_profile, "MaxScannerThreadNum", TUnit::UNIT);

    return Status::OK();
}

template <typename LocalStateType>
ScanOperatorX<LocalStateType>::ScanOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                             const DescriptorTbl& descs)
        : OperatorX<LocalStateType>(pool, tnode, descs),
          _runtime_filter_descs(tnode.runtime_filters) {
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
Dependency* ScanOperatorX<LocalStateType>::wait_for_dependency(RuntimeState* state) {
    CREATE_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
    return local_state._source_dependency->read_blocked_by();
}

template <typename LocalStateType>
FinishDependency* ScanOperatorX<LocalStateType>::finish_blocked_by(RuntimeState* state) const {
    auto& local_state = state->get_local_state(operator_id())->template cast<LocalStateType>();
    return local_state._finish_dependency->finish_blocked_by();
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
    return Status::OK();
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::open(RuntimeState* state) {
    _input_tuple_desc = state->desc_tbl().get_tuple_descriptor(_input_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    RETURN_IF_ERROR(OperatorX<LocalStateType>::open(state));

    RETURN_IF_CANCELLED(state);
    return Status::OK();
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::try_close(RuntimeState* state) {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    if (local_state._scanner_ctx.get()) {
        // mark this scanner ctx as should_stop to make sure scanners will not be scheduled anymore
        // TODO: there is a lock in `set_should_stop` may cause some slight impact
        local_state._scanner_ctx->set_should_stop();
    }
    return Status::OK();
}

template <typename Derived>
Status ScanLocalState<Derived>::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    if (_data_ready_dependency) {
        COUNTER_UPDATE(_wait_for_data_timer, _data_ready_dependency->read_watcher_elapse_time());
        COUNTER_UPDATE(profile()->total_time_counter(),
                       _data_ready_dependency->read_watcher_elapse_time());
    }
    if (_eos_dependency) {
        COUNTER_SET(_wait_for_eos_timer, _eos_dependency->read_watcher_elapse_time());
        COUNTER_UPDATE(profile()->total_time_counter(),
                       _eos_dependency->read_watcher_elapse_time());
    }
    if (_scanner_done_dependency) {
        COUNTER_SET(_wait_for_scanner_done_timer,
                    _scanner_done_dependency->read_watcher_elapse_time());
        COUNTER_UPDATE(profile()->total_time_counter(),
                       _scanner_done_dependency->read_watcher_elapse_time());
    }
    SCOPED_TIMER(profile()->total_time_counter());
    if (_scanner_ctx.get()) {
        _scanner_ctx->clear_and_join(reinterpret_cast<ScanLocalStateBase*>(this), state);
    }

    return PipelineXLocalState<>::close(state);
}

template <typename LocalStateType>
bool ScanOperatorX<LocalStateType>::runtime_filters_are_ready_or_timeout(
        RuntimeState* state) const {
    return state->get_local_state(operator_id())
            ->template cast<LocalStateType>()
            .runtime_filters_are_ready_or_timeout();
}

template <typename LocalStateType>
Status ScanOperatorX<LocalStateType>::get_block(RuntimeState* state, vectorized::Block* block,
                                                SourceState& source_state) {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state._get_next_timer);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
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
        // ISSUE: https://github.com/apache/doris/issues/16360
        // _scanner_ctx may be null here, see: `VScanNode::alloc_resource` (_eos == null)
        if (local_state._scanner_ctx) {
            local_state._scanner_ctx->set_status_on_error(Status::Cancelled("query cancelled"));
            return local_state._scanner_ctx->status();
        } else {
            return Status::Cancelled("query cancelled");
        }
    }

    if (local_state._eos_dependency->read_blocked_by() == nullptr) {
        source_state = SourceState::FINISHED;
        return Status::OK();
    }

    vectorized::BlockUPtr scan_block = nullptr;
    bool eos = false;
    RETURN_IF_ERROR(local_state._scanner_ctx->get_block_from_queue(state, &scan_block, &eos, 0));
    if (eos) {
        source_state = SourceState::FINISHED;
        DCHECK(scan_block == nullptr);
        return Status::OK();
    }

    // get scanner's block memory
    block->swap(*scan_block);
    local_state._scanner_ctx->return_free_block(std::move(scan_block));

    local_state.reached_limit(block, source_state);
    if (eos) {
        source_state = SourceState::FINISHED;
        // reach limit, stop the scanners.
        local_state._scanner_ctx->set_should_stop();
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

} // namespace doris::pipeline
