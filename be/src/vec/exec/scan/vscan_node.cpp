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

#include "vec/exec/scan/vscan_node.h"

#include "exprs/hybrid_set.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/stack_util.h"
#include "util/threadpool.h"
#include "vec/columns/column_const.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/in.h"

namespace doris::vectorized {

#define RETURN_IF_PUSH_DOWN(stmt)            \
    if (pdt == PushDownType::UNACCEPTABLE) { \
        stmt;                                \
    } else {                                 \
        return;                              \
    }

static bool ignore_cast(SlotDescriptor* slot, VExpr* expr) {
    if (slot->type().is_date_type() && expr->type().is_date_type()) {
        return true;
    }
    if (slot->type().is_string_type() && expr->type().is_string_type()) {
        return true;
    }
    return false;
}

Status VScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    START_AND_SCOPE_SPAN_IF(state->enable_profile(), state->get_tracer(), "VScanNode::init");
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _state = state;

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

    RETURN_IF_ERROR(_register_runtime_filter());

    return Status::OK();
}

Status VScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_init_profile());

    // init profile for runtime filter
    for (auto& rf_ctx : _runtime_filter_ctxs) {
        rf_ctx.runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status VScanNode::open(RuntimeState* state) {
    _input_tuple_desc = state->desc_tbl().get_tuple_descriptor(_input_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    START_AND_SCOPE_SPAN_IF(state->enable_profile(), state->get_tracer(), "VScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_acquire_runtime_filter());
    RETURN_IF_ERROR(_process_conjuncts());

    std::list<VScanner*> scanners;
    RETURN_IF_ERROR(_init_scanners(&scanners));
    if (scanners.empty()) {
        _eos = true;
    } else {
        COUNTER_SET(_num_scanners, static_cast<int64_t>(scanners.size()));
        RETURN_IF_ERROR(_start_scanners(scanners));
    }
    return Status::OK();
}

Status VScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    INIT_AND_SCOPE_REENTRANT_SPAN_IF(state->enable_profile(), state->get_tracer(), _get_next_span,
                                     "VScanNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    if (state->is_cancelled()) {
        _scanner_ctx->set_status_on_error(Status::Cancelled("query cancelled"));
        return _scanner_ctx->status();
    }

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    vectorized::Block* scan_block = nullptr;
    RETURN_IF_ERROR(_scanner_ctx->get_block_from_queue(&scan_block, eos));
    if (*eos) {
        DCHECK(scan_block == nullptr);
        return Status::OK();
    }

    // get scanner's block memory
    block->swap(*scan_block);
    _scanner_ctx->return_free_block(scan_block);

    reached_limit(block, eos);
    if (*eos) {
        // reach limit, stop the scanners.
        _scanner_ctx->set_should_stop();
    }

    return Status::OK();
}

Status VScanNode::_init_profile() {
    // 1. counters for scan node
    _rows_read_counter = ADD_COUNTER(_runtime_profile, "RowsRead", TUnit::UNIT);
    _total_throughput_counter =
            runtime_profile()->add_rate_counter("TotalReadThroughput", _rows_read_counter);
    _num_scanners = ADD_COUNTER(_runtime_profile, "NumScanners", TUnit::UNIT);

    // 2. counters for scanners
    _scanner_profile.reset(new RuntimeProfile("VScanner"));
    runtime_profile()->add_child(_scanner_profile.get(), true, nullptr);

    _scan_timer = ADD_TIMER(_scanner_profile, "ScannerGetBlockTime");
    _prefilter_timer = ADD_TIMER(_scanner_profile, "ScannerPrefilterTime");
    _convert_block_timer = ADD_TIMER(_scanner_profile, "ScannerConvertBlockTime");
    _filter_timer = ADD_TIMER(_scanner_profile, "ScannerFilterTime");

    _scanner_sched_counter = ADD_COUNTER(_runtime_profile, "ScannerSchedCount", TUnit::UNIT);
    _scanner_ctx_sched_counter = ADD_COUNTER(_runtime_profile, "ScannerCtxSchedCount", TUnit::UNIT);
    // time of transfer thread to wait for block from scan thread
    _scanner_wait_batch_timer = ADD_TIMER(_runtime_profile, "ScannerBatchWaitTime");
    // time of scan thread to wait for worker thread of the thread pool
    _scanner_wait_worker_timer = ADD_TIMER(_runtime_profile, "ScannerWorkerWaitTime");

    _pre_alloc_free_blocks_num =
            ADD_COUNTER(_runtime_profile, "PreAllocFreeBlocksNum", TUnit::UNIT);
    _newly_create_free_blocks_num =
            ADD_COUNTER(_runtime_profile, "NewlyCreateFreeBlocksNum", TUnit::UNIT);
    _max_scanner_thread_num = ADD_COUNTER(_runtime_profile, "MaxScannerThreadNum", TUnit::UNIT);

    return Status::OK();
}

Status VScanNode::_start_scanners(const std::list<VScanner*>& scanners) {
    _scanner_ctx.reset(new ScannerContext(_state, this, _input_tuple_desc, _output_tuple_desc,
                                          scanners, limit(),
                                          _state->query_options().mem_limit / 20));
    RETURN_IF_ERROR(_scanner_ctx->init());
    RETURN_IF_ERROR(_state->exec_env()->scanner_scheduler()->submit(_scanner_ctx.get()));
    return Status::OK();
}

Status VScanNode::_register_runtime_filter() {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.resize(filter_size);
    _runtime_filter_ready_flag.resize(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(_state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::CONSUMER, filter_desc, _state->query_options(), id()));
        RETURN_IF_ERROR(_state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                         &runtime_filter));
        _runtime_filter_ctxs[i].runtime_filter = runtime_filter;
        _runtime_filter_ready_flag[i] = false;
    }
    return Status::OK();
}

Status VScanNode::_acquire_runtime_filter() {
    std::vector<VExpr*> vexprs;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = _runtime_filter_ctxs[i].runtime_filter;
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&vexprs));
            _runtime_filter_ctxs[i].apply_mark = true;
        } else {
            _is_all_rf_applied = false;
        }
    }
    RETURN_IF_ERROR(_append_rf_into_conjuncts(vexprs));

    return Status::OK();
}

Status VScanNode::_append_rf_into_conjuncts(std::vector<VExpr*>& vexprs) {
    if (vexprs.empty()) {
        return Status::OK();
    }

    VExpr* last_expr = nullptr;
    if (_vconjunct_ctx_ptr) {
        last_expr = (*_vconjunct_ctx_ptr)->root();
    } else {
        DCHECK(_rf_vexpr_set.find(vexprs[0]) == _rf_vexpr_set.end());
        last_expr = vexprs[0];
        _rf_vexpr_set.insert(vexprs[0]);
    }
    for (size_t j = _vconjunct_ctx_ptr ? 0 : 1; j < vexprs.size(); j++) {
        if (_rf_vexpr_set.find(vexprs[j]) != _rf_vexpr_set.end()) {
            continue;
        }
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_db_name("");
        fn_name.__set_function_name("and");
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> arg_types;
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        arg_types.push_back(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        fn.__set_has_var_args(false);

        TExprNode texpr_node;
        texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        texpr_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        texpr_node.__set_opcode(TExprOpcode::COMPOUND_AND);
        texpr_node.__set_fn(fn);
        texpr_node.__set_is_nullable(last_expr->is_nullable() || vexprs[j]->is_nullable());
        VExpr* new_node = _pool->add(new VcompoundPred(texpr_node));
        new_node->add_child(last_expr);
        DCHECK((vexprs[j])->get_impl() != nullptr);
        new_node->add_child(vexprs[j]);
        last_expr = new_node;
        _rf_vexpr_set.insert(vexprs[j]);
    }
    auto new_vconjunct_ctx_ptr = _pool->add(new VExprContext(last_expr));
    if (_vconjunct_ctx_ptr) {
        (*_vconjunct_ctx_ptr)->clone_fn_contexts(new_vconjunct_ctx_ptr);
    }
    RETURN_IF_ERROR(new_vconjunct_ctx_ptr->prepare(_state, _row_descriptor));
    RETURN_IF_ERROR(new_vconjunct_ctx_ptr->open(_state));
    if (_vconjunct_ctx_ptr) {
        (*_vconjunct_ctx_ptr)->mark_as_stale();
        _stale_vexpr_ctxs.push_back(std::move(_vconjunct_ctx_ptr));
    }
    _vconjunct_ctx_ptr.reset(new doris::vectorized::VExprContext*);
    *_vconjunct_ctx_ptr = new_vconjunct_ctx_ptr;
    return Status::OK();
}

Status VScanNode::close(RuntimeState* state) {
    START_AND_SCOPE_SPAN_IF(state->enable_profile(), state->get_tracer(), "VScanNode::close");
    if (is_closed()) {
        return Status::OK();
    }
    if (_scanner_ctx.get()) {
        // stop and wait the scanner scheduler to be done
        // _scanner_ctx may not be created for some short circuit case.
        _scanner_ctx->set_should_stop();
        _scanner_ctx->clear_and_join();
    }

    for (auto& ctx : _runtime_filter_ctxs) {
        IRuntimeFilter* runtime_filter = ctx.runtime_filter;
        runtime_filter->consumer_close();
    }

    for (auto& ctx : _stale_vexpr_ctxs) {
        (*ctx)->close(state);
    }

    RETURN_IF_ERROR(ExecNode::close(state));
    return Status::OK();
}

Status VScanNode::_normalize_conjuncts() {
    // The conjuncts is always on output tuple, so use _output_tuple_desc;
    std::vector<SlotDescriptor*> slots = _output_tuple_desc->slots();

    for (int slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
        switch (slots[slot_idx]->type().type) {
#define M(NAME)                                                                              \
    case TYPE_##NAME: {                                                                      \
        ColumnValueRange<TYPE_##NAME> range(slots[slot_idx]->col_name(),                     \
                                            slots[slot_idx]->type().precision,               \
                                            slots[slot_idx]->type().scale);                  \
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
    M(DECIMAL128)                   \
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
    if (_vconjunct_ctx_ptr) {
        if ((*_vconjunct_ctx_ptr)->root()) {
            VExpr* new_root = _normalize_predicate((*_vconjunct_ctx_ptr)->root());
            if (new_root) {
                (*_vconjunct_ctx_ptr)->set_root(new_root);
            } else {
                (*_vconjunct_ctx_ptr)->mark_as_stale();
                _stale_vexpr_ctxs.push_back(std::move(_vconjunct_ctx_ptr));
                _vconjunct_ctx_ptr.reset(nullptr);
            }
        }
    }
    for (auto& it : _slot_id_to_value_range) {
        std::visit(
                [&](auto&& range) {
                    if (range.is_empty_value_range()) {
                        _eos = true;
                    }
                },
                it.second.second);
        _colname_to_value_range[it.second.first->col_name()] = it.second.second;
    }

    return Status::OK();
}

VExpr* VScanNode::_normalize_predicate(VExpr* conjunct_expr_root) {
    static constexpr auto is_leaf = [](VExpr* expr) { return !expr->is_and_expr(); };
    auto in_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
                                   VExpr** child_contains_slot) {
        if (children.empty() ||
            VExpr::expr_without_cast(children[0])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            return false;
        }
        *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(children[0]));
        *child_contains_slot = children[0];
        return true;
    };
    auto eq_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
                                   VExpr** child_contains_slot) {
        for (const VExpr* child : children) {
            if (VExpr::expr_without_cast(child)->node_type() != TExprNodeType::SLOT_REF) {
                // not a slot ref(column)
                continue;
            }
            *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(child));
            *child_contains_slot = const_cast<VExpr*>(child);
            return true;
        }
        return false;
    };

    if (conjunct_expr_root != nullptr) {
        if (is_leaf(conjunct_expr_root)) {
            auto impl = conjunct_expr_root->get_impl();
            // If impl is not null, which means this a conjuncts from runtime filter.
            VExpr* cur_expr = impl ? const_cast<VExpr*>(impl) : conjunct_expr_root;
            SlotDescriptor* slot = nullptr;
            ColumnValueRangeType* range = nullptr;
            PushDownType pdt = PushDownType::UNACCEPTABLE;
            _eval_const_conjuncts(cur_expr, *_vconjunct_ctx_ptr, &pdt);
            if (pdt == PushDownType::ACCEPTABLE) {
                return nullptr;
            }
            if (_is_predicate_acting_on_slot(cur_expr, in_predicate_checker, &slot, &range) ||
                _is_predicate_acting_on_slot(cur_expr, eq_predicate_checker, &slot, &range)) {
                std::visit(
                        [&](auto& value_range) {
                            RETURN_IF_PUSH_DOWN(_normalize_in_and_eq_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &pdt));
                            RETURN_IF_PUSH_DOWN(_normalize_not_in_and_not_eq_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &pdt));
                            RETURN_IF_PUSH_DOWN(_normalize_is_null_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &pdt));
                            RETURN_IF_PUSH_DOWN(_normalize_noneq_binary_predicate(
                                    cur_expr, *(_vconjunct_ctx_ptr.get()), slot, value_range,
                                    &pdt));
                            if (_is_key_column(slot->col_name())) {
                                RETURN_IF_PUSH_DOWN(_normalize_bloom_filter(
                                        cur_expr, *(_vconjunct_ctx_ptr.get()), slot, &pdt));
                                if (_state->enable_function_pushdown()) {
                                    RETURN_IF_PUSH_DOWN(_normalize_function_filters(
                                            cur_expr, *(_vconjunct_ctx_ptr.get()), slot, &pdt));
                                }
                            }
                        },
                        *range);
            }
            if (pdt == PushDownType::ACCEPTABLE && _is_key_column(slot->col_name())) {
                return nullptr;
            } else {
                // for PARTIAL_ACCEPTABLE and UNACCEPTABLE, do not remove expr from the tree
                return conjunct_expr_root;
            }
        } else {
            VExpr* left_child = _normalize_predicate(conjunct_expr_root->children()[0]);
            VExpr* right_child = _normalize_predicate(conjunct_expr_root->children()[1]);

            if (left_child != nullptr && right_child != nullptr) {
                conjunct_expr_root->set_children({left_child, right_child});
                return conjunct_expr_root;
            } else {
                // here only close the and expr self, do not close the child
                conjunct_expr_root->set_children({});
                conjunct_expr_root->close(_state, *_vconjunct_ctx_ptr,
                                          (*_vconjunct_ctx_ptr)->get_function_state_scope());
            }

            // here do not close Expr* now
            return left_child != nullptr ? left_child : right_child;
        }
    }
    return conjunct_expr_root;
}

Status VScanNode::_normalize_bloom_filter(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                          PushDownType* pdt) {
    if (TExprNodeType::BLOOM_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 1);
        PushDownType temp_pdt = _should_push_down_bloom_filter();
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            _bloom_filters_push_down.emplace_back(slot->col_name(), expr->get_bloom_filter_func());
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

Status VScanNode::_normalize_function_filters(VExpr* expr, VExprContext* expr_ctx,
                                              SlotDescriptor* slot, PushDownType* pdt) {
    bool opposite = false;
    VExpr* fn_expr = expr;
    if (TExprNodeType::COMPOUND_PRED == expr->node_type() &&
        expr->fn().name.function_name == "not") {
        fn_expr = fn_expr->children()[0];
        opposite = true;
    }

    if (TExprNodeType::FUNCTION_CALL == fn_expr->node_type()) {
        doris_udf::FunctionContext* fn_ctx = nullptr;
        StringVal val;
        PushDownType temp_pdt = _should_push_down_function_filter(
                reinterpret_cast<VectorizedFnCall*>(fn_expr), expr_ctx, &val, &fn_ctx);
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            std::string col = slot->col_name();
            _push_down_functions.emplace_back(opposite, col, fn_ctx, val);
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

bool VScanNode::_is_predicate_acting_on_slot(
        VExpr* expr,
        const std::function<bool(const std::vector<VExpr*>&, const VSlotRef**, VExpr**)>& checker,
        SlotDescriptor** slot_desc, ColumnValueRangeType** range) {
    const VSlotRef* slot_ref = nullptr;
    VExpr* child_contains_slot = nullptr;
    if (!checker(expr->children(), &slot_ref, &child_contains_slot)) {
        // not a slot ref(column)
        return false;
    }

    auto entry = _slot_id_to_value_range.find(slot_ref->slot_id());
    if (_slot_id_to_value_range.end() == entry) {
        return false;
    }
    *slot_desc = entry->second.first;
    DCHECK(child_contains_slot != nullptr);
    if (child_contains_slot->type().type != (*slot_desc)->type().type) {
        if (!ignore_cast(*slot_desc, child_contains_slot)) {
            // the type of predicate not match the slot's type
            return false;
        }
    }
    *range = &(entry->second.second);
    return true;
}

void VScanNode::_eval_const_conjuncts(VExpr* vexpr, VExprContext* expr_ctx, PushDownType* pdt) {
    char* constant_val = nullptr;
    if (vexpr->is_constant()) {
        if (const ColumnConst* const_column =
                    check_and_get_column<ColumnConst>(vexpr->get_const_col(expr_ctx)->column_ptr)) {
            constant_val = const_cast<char*>(const_column->get_data_at(0).data);
            if (constant_val == nullptr || *reinterpret_cast<bool*>(constant_val) == false) {
                *pdt = PushDownType::ACCEPTABLE;
                _eos = true;
            }
        } else if (const ColumnVector<UInt8>* bool_column =
                           check_and_get_column<ColumnVector<UInt8>>(
                                   vexpr->get_const_col(expr_ctx)->column_ptr)) {
            // TODO: If `vexpr->is_constant()` is true, a const column is expected here.
            //  But now we still don't cover all predicates for const expression.
            //  For example, for query `SELECT col FROM tbl WHERE 'PROMOTION' LIKE 'AAA%'`,
            //  predicate `like` will return a ColumnVector<UInt8> which contains a single value.
            LOG(WARNING) << "Expr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << vexpr->get_const_col(expr_ctx)->column_ptr->get_name();
            DCHECK_EQ(bool_column->size(), 1);
            if (bool_column->size() == 1) {
                constant_val = const_cast<char*>(bool_column->get_data_at(0).data);
                if (constant_val == nullptr || *reinterpret_cast<bool*>(constant_val) == false) {
                    *pdt = PushDownType::ACCEPTABLE;
                    _eos = true;
                }
            } else {
                LOG(WARNING) << "Constant predicate in scan node should return a bool column with "
                                "`size == 1` but actually is "
                             << bool_column->size();
            }
        } else {
            LOG(WARNING) << "Expr[" << vexpr->debug_string()
                         << "] should return a const column but actually is "
                         << vexpr->get_const_col(expr_ctx)->column_ptr->get_name();
        }
    }
}

template <PrimitiveType T>
Status VScanNode::_normalize_in_and_eq_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                 SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                 PushDownType* pdt) {
    auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(slot->type().precision,
                                                                           slot->type().scale);
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        VInPredicate* pred = static_cast<VInPredicate*>(expr);
        PushDownType temp_pdt = _should_push_down_in_predicate(pred, expr_ctx, false);
        if (temp_pdt == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }

        // begin to push InPredicate value into ColumnValueRange
        InState* state = reinterpret_cast<InState*>(
                expr_ctx->fn_context(pred->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        HybridSetBase::IteratorBase* iter = state->hybrid_set->begin();
        auto fn_name = std::string("");
        while (iter->has_next()) {
            // column in (nullptr) is always false so continue to
            // dispose next item
            if (nullptr == iter->get_value()) {
                iter->next();
                continue;
            }
            auto value = const_cast<void*>(iter->get_value());
            RETURN_IF_ERROR(_change_value_range<true>(
                    temp_range, value, ColumnValueRange<T>::add_fixed_value_range, fn_name));
            iter->next();
        }
        range.intersection(temp_range);
        *pdt = temp_pdt;
    } else if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);
        auto eq_checker = [](const std::string& fn_name) { return fn_name == "eq"; };

        StringRef value;
        int slot_ref_child = -1;

        PushDownType temp_pdt =
                _should_push_down_binary_predicate(reinterpret_cast<VectorizedFnCall*>(expr),
                                                   expr_ctx, &value, &slot_ref_child, eq_checker);
        if (temp_pdt == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }
        DCHECK(slot_ref_child >= 0);
        // where A = nullptr should return empty result set
        auto fn_name = std::string("");
        if (value.data != nullptr) {
            if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                          T == TYPE_HLL) {
                auto val = StringValue(value.data, value.size);
                RETURN_IF_ERROR(_change_value_range<true>(
                        temp_range, reinterpret_cast<void*>(&val),
                        ColumnValueRange<T>::add_fixed_value_range, fn_name));
            } else {
                RETURN_IF_ERROR(_change_value_range<true>(
                        temp_range, reinterpret_cast<void*>(const_cast<char*>(value.data)),
                        ColumnValueRange<T>::add_fixed_value_range, fn_name));
            }
            range.intersection(temp_range);
        }
        *pdt = temp_pdt;
    }

    // exceed limit, no conditions will be pushed down to storage engine.
    if (range.get_fixed_value_size() > _max_pushdown_conditions_per_column) {
        range.set_whole_value_range();
        *pdt = PushDownType::UNACCEPTABLE;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VScanNode::_normalize_not_in_and_not_eq_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                         SlotDescriptor* slot,
                                                         ColumnValueRange<T>& range,
                                                         PushDownType* pdt) {
    bool is_fixed_range = range.is_fixed_value_range();
    auto not_in_range = ColumnValueRange<T>::create_empty_column_value_range(range.column_name());
    PushDownType temp_pdt = PushDownType::UNACCEPTABLE;
    // 1. Normalize in conjuncts like 'where col in (v1, v2, v3)'
    if (TExprNodeType::IN_PRED == expr->node_type()) {
        VInPredicate* pred = static_cast<VInPredicate*>(expr);
        if ((temp_pdt = _should_push_down_in_predicate(pred, expr_ctx, true)) ==
            PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }

        // begin to push InPredicate value into ColumnValueRange
        InState* state = reinterpret_cast<InState*>(
                expr_ctx->fn_context(pred->fn_context_index())
                        ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        HybridSetBase::IteratorBase* iter = state->hybrid_set->begin();
        auto fn_name = std::string("");
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
        if ((temp_pdt = _should_push_down_binary_predicate(
                     reinterpret_cast<VectorizedFnCall*>(expr), expr_ctx, &value, &slot_ref_child,
                     ne_checker)) == PushDownType::UNACCEPTABLE) {
            return Status::OK();
        }

        DCHECK(slot_ref_child >= 0);
        // where A = nullptr should return empty result set
        if (value.data != nullptr) {
            auto fn_name = std::string("");
            if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                          T == TYPE_HLL) {
                auto val = StringValue(value.data, value.size);
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
        not_in_range.get_fixed_value_size() <= _max_pushdown_conditions_per_column) {
        if (!is_fixed_range) {
            _not_in_value_ranges.push_back(not_in_range);
        }
        *pdt = temp_pdt;
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VScanNode::_normalize_is_null_predicate(VExpr* expr, VExprContext* expr_ctx,
                                               SlotDescriptor* slot, ColumnValueRange<T>& range,
                                               PushDownType* pdt) {
    PushDownType temp_pdt = _should_push_down_is_null_predicate();
    if (temp_pdt == PushDownType::UNACCEPTABLE) {
        return Status::OK();
    }

    if (TExprNodeType::FUNCTION_CALL == expr->node_type()) {
        if (reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name == "is_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(true);
            range.intersection(temp_range);
            *pdt = temp_pdt;
        } else if (reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name ==
                   "is_not_null_pred") {
            auto temp_range = ColumnValueRange<T>::create_empty_column_value_range(
                    slot->type().precision, slot->type().scale);
            temp_range.set_contain_null(false);
            range.intersection(temp_range);
            *pdt = temp_pdt;
        }
    }
    return Status::OK();
}

template <PrimitiveType T>
Status VScanNode::_normalize_noneq_binary_predicate(VExpr* expr, VExprContext* expr_ctx,
                                                    SlotDescriptor* slot,
                                                    ColumnValueRange<T>& range, PushDownType* pdt) {
    if (TExprNodeType::BINARY_PRED == expr->node_type()) {
        DCHECK(expr->children().size() == 2);

        auto noneq_checker = [](const std::string& fn_name) {
            return fn_name != "ne" && fn_name != "eq";
        };
        StringRef value;
        int slot_ref_child = -1;
        PushDownType temp_pdt = _should_push_down_binary_predicate(
                reinterpret_cast<VectorizedFnCall*>(expr), expr_ctx, &value, &slot_ref_child,
                noneq_checker);
        if (temp_pdt != PushDownType::UNACCEPTABLE) {
            DCHECK(slot_ref_child >= 0);
            const std::string& fn_name =
                    reinterpret_cast<VectorizedFnCall*>(expr)->fn().name.function_name;

            // where A = nullptr should return empty result set
            if (value.data != nullptr) {
                if constexpr (T == TYPE_CHAR || T == TYPE_VARCHAR || T == TYPE_STRING ||
                              T == TYPE_HLL) {
                    auto val = StringValue(value.data, value.size);
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

template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
Status VScanNode::_change_value_range(ColumnValueRange<PrimitiveType>& temp_range, void* value,
                                      const ChangeFixedValueRangeFunc& func,
                                      const std::string& fn_name, int slot_ref_child) {
    if constexpr (PrimitiveType == TYPE_DATE) {
        DateTimeValue date_value;
        reinterpret_cast<VecDateTimeValue*>(value)->convert_vec_dt_to_dt(&date_value);
        if constexpr (IsFixed) {
            if (!date_value.check_loss_accuracy_cast_to_date()) {
                func(temp_range,
                     reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                             &date_value));
            }
        } else {
            if (date_value.check_loss_accuracy_cast_to_date()) {
                if (fn_name == "lt" || fn_name == "ge") {
                    ++date_value;
                }
            }
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         &date_value));
        }
    } else if constexpr (PrimitiveType == TYPE_DATETIME) {
        DateTimeValue date_value;
        reinterpret_cast<VecDateTimeValue*>(value)->convert_vec_dt_to_dt(&date_value);
        if constexpr (IsFixed) {
            func(temp_range,
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         &date_value));
        } else {
            func(temp_range, to_olap_filter_type(fn_name, slot_ref_child),
                 reinterpret_cast<typename PrimitiveTypeTraits<PrimitiveType>::CppType*>(
                         reinterpret_cast<char*>(&date_value)));
        }
    } else if constexpr ((PrimitiveType == TYPE_DECIMALV2) || (PrimitiveType == TYPE_CHAR) ||
                         (PrimitiveType == TYPE_VARCHAR) || (PrimitiveType == TYPE_HLL) ||
                         (PrimitiveType == TYPE_DATETIMEV2) || (PrimitiveType == TYPE_TINYINT) ||
                         (PrimitiveType == TYPE_SMALLINT) || (PrimitiveType == TYPE_INT) ||
                         (PrimitiveType == TYPE_BIGINT) || (PrimitiveType == TYPE_LARGEINT) ||
                         (PrimitiveType == TYPE_DECIMAL32) || (PrimitiveType == TYPE_DECIMAL64) ||
                         (PrimitiveType == TYPE_DECIMAL128) || (PrimitiveType == TYPE_STRING) ||
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

Status VScanNode::try_append_late_arrival_runtime_filter(int* arrived_rf_num) {
    if (_is_all_rf_applied) {
        *arrived_rf_num = _runtime_filter_descs.size();
        return Status::OK();
    }

    // This method will be called in scanner thread.
    // So need to add lock
    std::unique_lock<std::mutex> l(_rf_locks);
    if (_is_all_rf_applied) {
        *arrived_rf_num = _runtime_filter_descs.size();
        return Status::OK();
    }

    // 1. Check if are runtime filter ready but not applied.
    std::vector<VExpr*> vexprs;
    int current_arrived_rf_num = 0;
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        if (_runtime_filter_ctxs[i].apply_mark) {
            ++current_arrived_rf_num;
            continue;
        } else if (_runtime_filter_ctxs[i].runtime_filter->is_ready()) {
            _runtime_filter_ctxs[i].runtime_filter->get_prepared_vexprs(&vexprs, _row_descriptor);
            ++current_arrived_rf_num;
            _runtime_filter_ctxs[i].apply_mark = true;
        }
    }
    // 2. Append unapplied runtime filters to vconjunct_ctx_ptr
    if (!vexprs.empty()) {
        RETURN_IF_ERROR(_append_rf_into_conjuncts(vexprs));
    }
    if (current_arrived_rf_num == _runtime_filter_descs.size()) {
        _is_all_rf_applied = true;
    }

    *arrived_rf_num = current_arrived_rf_num;
    return Status::OK();
}

Status VScanNode::clone_vconjunct_ctx(VExprContext** _vconjunct_ctx) {
    if (_vconjunct_ctx_ptr) {
        std::unique_lock<std::mutex> l(_rf_locks);
        return (*_vconjunct_ctx_ptr)->clone(_state, _vconjunct_ctx);
    }
    return Status::OK();
}

VScanNode::PushDownType VScanNode::_should_push_down_binary_predicate(
        VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
        int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker) {
    if (!fn_checker(fn_call->fn().name.function_name)) {
        return PushDownType::UNACCEPTABLE;
    }

    const auto& children = fn_call->children();
    DCHECK(children.size() == 2);
    for (size_t i = 0; i < children.size(); i++) {
        if (VExpr::expr_without_cast(children[i])->node_type() != TExprNodeType::SLOT_REF) {
            // not a slot ref(column)
            continue;
        }
        if (!children[1 - i]->is_constant()) {
            // only handle constant value
            return PushDownType::UNACCEPTABLE;
        } else {
            if (const ColumnConst* const_column = check_and_get_column<ColumnConst>(
                        children[1 - i]->get_const_col(expr_ctx)->column_ptr)) {
                *slot_ref_child = i;
                *constant_val = const_column->get_data_at(0);
            } else {
                return PushDownType::UNACCEPTABLE;
            }
        }
    }
    return PushDownType::ACCEPTABLE;
}

VScanNode::PushDownType VScanNode::_should_push_down_in_predicate(VInPredicate* pred,
                                                                  VExprContext* expr_ctx,
                                                                  bool is_not_in) {
    if (pred->is_not_in() != is_not_in) {
        return PushDownType::UNACCEPTABLE;
    }
    InState* state = reinterpret_cast<InState*>(
            expr_ctx->fn_context(pred->fn_context_index())
                    ->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    HybridSetBase* set = state->hybrid_set.get();

    // if there are too many elements in InPredicate, exceed the limit,
    // we will not push any condition of this column to storage engine.
    // because too many conditions pushed down to storage engine may even
    // slow down the query process.
    // ATTN: This is just an experience value. You may need to try
    // different thresholds to improve performance.
    if (set->size() > _max_pushdown_conditions_per_column) {
        VLOG_NOTICE << "Predicate value num " << set->size() << " exceed limit "
                    << _max_pushdown_conditions_per_column;
        return PushDownType::UNACCEPTABLE;
    }
    return PushDownType::ACCEPTABLE;
}

} // namespace doris::vectorized
