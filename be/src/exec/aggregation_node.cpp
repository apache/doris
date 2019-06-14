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

#include "exec/aggregation_node.h"

#include <math.h>
#include <sstream>
#include <boost/functional/hash.hpp>
#include <thrift/protocol/TDebugProtocol.h>
#include <x86intrin.h>
#include <gperftools/profiler.h>

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "exec/hash_table.hpp"
#include "exprs/agg_fn_evaluator.h"
#include "exprs/expr.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

using llvm::BasicBlock;
using llvm::Function;
using llvm::PointerType;
using llvm::Type;
using llvm::Value;
using llvm::StructType;

namespace doris {

const char* AggregationNode::_s_llvm_class_name = "class.doris::AggregationNode";

// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
AggregationNode::AggregationNode(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) :
            ExecNode(pool, tnode, descs),
            _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
            _intermediate_tuple_desc(NULL),
            _output_tuple_id(tnode.agg_node.output_tuple_id),
            _output_tuple_desc(NULL),
            _singleton_output_tuple(NULL),
            //_tuple_pool(new MemPool()),
            //
            _codegen_process_row_batch_fn(NULL),
            _process_row_batch_fn(NULL),
            _needs_finalize(tnode.agg_node.need_finalize),
            _build_timer(NULL),
            _get_results_timer(NULL),
            _hash_table_buckets_counter(NULL) {
}

AggregationNode::~AggregationNode() {
}

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduct ExecNode::init()
    RETURN_IF_ERROR(Expr::create_expr_trees(
            _pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));

    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = NULL;
        AggFnEvaluator::create(
            _pool, tnode.agg_node.aggregate_functions[i], &evaluator);
        _aggregate_evaluators.push_back(evaluator);
    }
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _hash_table_buckets_counter =
        ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _hash_table_load_factor_counter =
        ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _intermediate_tuple_desc =
        state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(Expr::prepare(
            _probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    // Construct build exprs from _agg_tuple_desc
    for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
        SlotDescriptor* desc = _intermediate_tuple_desc->slots()[i];
        Expr* expr = new SlotRef(desc);
        state->obj_pool()->add(expr);
        _build_expr_ctxs.push_back(new ExprContext(expr));
        state->obj_pool()->add(_build_expr_ctxs.back());
    }

    // Construct a new row desc for preparing the build exprs because neither the child's
    // nor this node's output row desc may contain the intermediate tuple, e.g.,
    // in a single-node plan with an intermediate tuple different from the output tuple.
    RowDescriptor build_row_desc(_intermediate_tuple_desc, false);
    RETURN_IF_ERROR(Expr::prepare(
            _build_expr_ctxs, state, build_row_desc, expr_mem_tracker()));

    _tuple_pool.reset(new MemPool(mem_tracker()));

    _agg_fn_ctxs.resize(_aggregate_evaluators.size());
    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        // skip non-materialized slots; we don't have evaluators instantiated for those
        // while (!_agg_tuple_desc->slots()[j]->is_materialized()) {
        //     DCHECK_LT(j, _agg_tuple_desc->slots().size() - 1)
        //             << "#eval= " << _aggregate_evaluators.size()
        //             << " #probe=" << _probe_expr_ctxs.size();
        //     ++j;
        // }
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, child(0)->row_desc(), _tuple_pool.get(),
                intermediate_slot_desc, output_slot_desc, mem_tracker(), &_agg_fn_ctxs[i]));
        state->obj_pool()->add(_agg_fn_ctxs[i]);
    }

    // TODO: how many buckets?
    _hash_tbl.reset(new HashTable(
            _build_expr_ctxs, _probe_expr_ctxs, 1, true, id(), mem_tracker(), 1024));

    if (_probe_expr_ctxs.empty()) {
        // create single output tuple now; we need to output something
        // even if our input is empty
        _singleton_output_tuple = construct_intermediate_tuple();
    }

    if (state->codegen_level() > 0) {
        LlvmCodeGen* codegen = NULL;
        RETURN_IF_ERROR(state->get_codegen(&codegen));
        Function* update_tuple_fn = codegen_update_tuple(state);
        if (update_tuple_fn != NULL) {
            _codegen_process_row_batch_fn =
                codegen_process_row_batch(state, update_tuple_fn);
            if (_codegen_process_row_batch_fn != NULL) {
                // Update to using codegen'd process row batch.
                codegen->add_function_to_jit(_codegen_process_row_batch_fn,
                                             reinterpret_cast<void**>(&_process_row_batch_fn));
                // AddRuntimeExecOption("Codegen Enabled");
            }
        }
    }

    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state, _agg_fn_ctxs[i]));
    }

    RETURN_IF_ERROR(_children[0]->open(state));

    RowBatch batch(_children[0]->row_desc(), state->batch_size(), mem_tracker());
    int64_t num_input_rows = 0;
    int64_t num_agg_rows = 0;

    bool early_return = false;
    bool limit_with_no_agg = (limit() != -1 && (_aggregate_evaluators.size() == 0));
    DCHECK_EQ(_aggregate_evaluators.size(), _agg_fn_ctxs.size());

    while (true) {
        bool eos = false;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state());
        RETURN_IF_ERROR(_children[0]->get_next(state, &batch, &eos));
        // SCOPED_TIMER(_build_timer);
        if (VLOG_ROW_IS_ON) {
            for (int i = 0; i < batch.num_rows(); ++i) {
                TupleRow* row = batch.get_row(i);
                VLOG_ROW << "id=" << id() << " input row: "
                        << row->to_string(_children[0]->row_desc());
            }
        }

        int64_t agg_rows_before = _hash_tbl->size();

        if (_process_row_batch_fn != NULL) {
            _process_row_batch_fn(this, &batch);
        } else if (_singleton_output_tuple != NULL) {
            SCOPED_TIMER(_build_timer);
            process_row_batch_no_grouping(&batch, _tuple_pool.get());
        } else {
            process_row_batch_with_grouping(&batch, _tuple_pool.get());
            if (limit_with_no_agg) {
                if (_hash_tbl->size() >= limit()) {
                    early_return = true;
                }
            }
        }

        // RETURN_IF_LIMIT_EXCEEDED(state);
        RETURN_IF_ERROR(state->check_query_state());

        COUNTER_SET(_hash_table_buckets_counter, _hash_tbl->num_buckets());
        COUNTER_SET(memory_used_counter(),
                    _tuple_pool->peak_allocated_bytes() + _hash_tbl->byte_size());
        COUNTER_SET(_hash_table_load_factor_counter, _hash_tbl->load_factor());
        num_agg_rows += (_hash_tbl->size() - agg_rows_before);
        num_input_rows += batch.num_rows();

        batch.reset();

        RETURN_IF_ERROR(state->check_query_state());
        if (eos) {
            break;
        }
        if (early_return) {
            break;
        }
    }

    if (_singleton_output_tuple != NULL) {
        _hash_tbl->insert(reinterpret_cast<TupleRow*>(&_singleton_output_tuple));
        ++num_agg_rows;
    }

    VLOG_ROW << "id=" << id() << " aggregated " << num_input_rows << " input rows into "
              << num_agg_rows << " output rows";
    _output_iterator = _hash_tbl->begin();
    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state());
    SCOPED_TIMER(_get_results_timer);

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    ExprContext** ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    int count = 0;
    const int N = state->batch_size();
    while (!_output_iterator.at_end() && !row_batch->at_capacity()) {
        // This loop can go on for a long time if the conjuncts are very selective. Do query
        // maintenance every N iterations.
        if (count++ % N == 0) {
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(state->check_query_state());
        }
        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        Tuple* intermediate_tuple = _output_iterator.get_row()->get_tuple(0);
        Tuple* output_tuple =
            finalize_tuple(intermediate_tuple, row_batch->tuple_data_pool());
        row->set_tuple(0, output_tuple);

        if (ExecNode::eval_conjuncts(ctxs, num_ctxs, row)) {
            VLOG_ROW << "output row: " << row->to_string(row_desc());
            row_batch->commit_last_row();
            ++_num_rows_returned;

            if (reached_limit()) {
                break;
            }
        }

        _output_iterator.next<false>();
    }

    *eos = _output_iterator.at_end() || reached_limit();
    if (*eos) {
        if (memory_used_counter() != NULL && _hash_tbl.get() != NULL &&
                _hash_table_buckets_counter != NULL) {
            COUNTER_SET(memory_used_counter(),
                    _tuple_pool->peak_allocated_bytes() + _hash_tbl->byte_size());
            COUNTER_SET(_hash_table_buckets_counter, _hash_tbl->num_buckets());
        }
    }
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status AggregationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
    // them in order to free any memory allocated by UDAs. Finalize() requires a dst tuple
    // but we don't actually need the result, so allocate a single dummy tuple to avoid
    // accumulating memory.
    Tuple* dummy_dst = NULL;
    if (_needs_finalize && _output_tuple_desc != NULL) {
        dummy_dst = Tuple::create(_output_tuple_desc->byte_size(), _tuple_pool.get());
    }
    while (!_output_iterator.at_end()) {
        Tuple* tuple = _output_iterator.get_row()->get_tuple(0);
        if (_needs_finalize) {
            AggFnEvaluator::finalize(_aggregate_evaluators, _agg_fn_ctxs, tuple, dummy_dst);
        } else {
            AggFnEvaluator::serialize(_aggregate_evaluators, _agg_fn_ctxs, tuple);
        }
        _output_iterator.next<false>();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->close(state);
        if (!_agg_fn_ctxs.empty() && _agg_fn_ctxs[i] && _agg_fn_ctxs[i]->impl()) {
            _agg_fn_ctxs[i]->impl()->close();
        }
    }

    if (_tuple_pool.get() != NULL) {
        _tuple_pool->free_all();
    }
    if (_hash_tbl.get() != NULL) {
        _hash_tbl->close();
    }

    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_build_expr_ctxs, state);

    return ExecNode::close(state);
}

Tuple* AggregationNode::construct_intermediate_tuple() {
    Tuple* agg_tuple = Tuple::create(_intermediate_tuple_desc->byte_size(), _tuple_pool.get());
    vector<SlotDescriptor*>::const_iterator slot_desc = _intermediate_tuple_desc->slots().begin();

    // copy grouping values
    for (int i = 0; i < _probe_expr_ctxs.size(); ++i, ++slot_desc) {
        if (_hash_tbl->last_expr_value_null(i)) {
            agg_tuple->set_null((*slot_desc)->null_indicator_offset());
        } else {
            void* src = _hash_tbl->last_expr_value(i);
            void* dst = agg_tuple->get_slot((*slot_desc)->tuple_offset());
            RawValue::write(src, dst, (*slot_desc)->type(), _tuple_pool.get());
        }
    }

    // Initialize aggregate output.
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++slot_desc) {
        while (!(*slot_desc)->is_materialized()) {
            ++slot_desc;
        }

        AggFnEvaluator* evaluator = _aggregate_evaluators[i];
        evaluator->init(_agg_fn_ctxs[i], agg_tuple);

        // Codegen specific path.
        // To minimize branching on the UpdateAggTuple path, initialize the result value
        // so that UpdateAggTuple doesn't have to check if the aggregation
        // dst slot is null.
        //  - sum/count: 0
        //  - min: max_value
        //  - max: min_value
        // TODO: remove when we don't use the irbuilder for codegen here.
        // This optimization no longer applies with AnyVal
        if (!(*slot_desc)->type().is_string_type() &&
                !(*slot_desc)->type().is_date_type()) {
            ExprValue default_value;
            void* default_value_ptr = NULL;

            switch (evaluator->agg_op()) {
            case TAggregationOp::MIN:
                default_value_ptr = default_value.set_to_max((*slot_desc)->type());
                RawValue::write(default_value_ptr, agg_tuple, *slot_desc, NULL);
                break;

            case TAggregationOp::MAX:
                default_value_ptr = default_value.set_to_min((*slot_desc)->type());
                RawValue::write(default_value_ptr, agg_tuple, *slot_desc, NULL);
                break;

            default:
                break;
            }
        }
    }

    return agg_tuple;
}

void AggregationNode::update_tuple(Tuple* tuple, TupleRow* row) {
    DCHECK(tuple != NULL);

    AggFnEvaluator::add(_aggregate_evaluators, _agg_fn_ctxs, row, tuple);
#if 0
    vector<AggFnEvaluator*>::const_iterator evaluator;
    int i = 0;
    for (evaluator = _aggregate_evaluators.begin();
            evaluator != _aggregate_evaluators.end(); ++evaluator, ++i) {
        (*evaluator)->choose_update_or_merge(_agg_fn_ctxs[i], row, tuple);
        //if (_is_merge) {
        //    (*evaluator)->merge(_agg_fn_ctxs[i], row, tuple, pool);
        //} else {
        //    (*evaluator)->update(_agg_fn_ctxs[i], row, tuple, pool);
        //}
    }
#endif
}

Tuple* AggregationNode::finalize_tuple(Tuple* tuple, MemPool* pool) {
    DCHECK(tuple != NULL);

    Tuple* dst = tuple;
    if (_needs_finalize && _intermediate_tuple_id != _output_tuple_id) {
        dst = Tuple::create(_output_tuple_desc->byte_size(), pool);
    }
    if (_needs_finalize) {
        AggFnEvaluator::finalize(_aggregate_evaluators, _agg_fn_ctxs, tuple, dst);
    } else {
        AggFnEvaluator::serialize(_aggregate_evaluators, _agg_fn_ctxs, tuple);
    }
    // Copy grouping values from tuple to dst.
    // TODO: Codegen this.
    if (dst != tuple) {
        int num_grouping_slots = _probe_expr_ctxs.size();
        for (int i = 0; i < num_grouping_slots; ++i) {
            SlotDescriptor* src_slot_desc = _intermediate_tuple_desc->slots()[i];
            SlotDescriptor* dst_slot_desc = _output_tuple_desc->slots()[i];
            bool src_slot_null = tuple->is_null(src_slot_desc->null_indicator_offset());
            void* src_slot = NULL;
            if (!src_slot_null) src_slot = tuple->get_slot(src_slot_desc->tuple_offset());
            RawValue::write(src_slot, dst, dst_slot_desc, NULL);
        }
    }
    return dst;
}

void AggregationNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "AggregationNode(intermediate_tuple_id=" << _intermediate_tuple_id
         << " output_tuple_id=" << _output_tuple_id
         << " needs_finalize=" << _needs_finalize
         // << " probe_exprs=" << Expr::debug_string(_probe_exprs)
         << " agg_exprs=" << AggFnEvaluator::debug_string(_aggregate_evaluators);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

void AggregationNode::push_down_predicate(RuntimeState *state,
        std::list<ExprContext*> *expr_ctxs) {
    // groupby can pushdown, agg can't pushdown
    // Now we doesn't pushdown for easy.
    return;
}

static IRFunction::Type get_hll_update_function2(const TypeDescriptor& type) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        return IRFunction::HLL_UPDATE_BOOLEAN;
    case TYPE_TINYINT:
        return IRFunction::HLL_UPDATE_TINYINT;
    case TYPE_SMALLINT:
        return IRFunction::HLL_UPDATE_SMALLINT;
    case TYPE_INT:
        return IRFunction::HLL_UPDATE_INT;
    case TYPE_BIGINT:
        return IRFunction::HLL_UPDATE_BIGINT;
    case TYPE_FLOAT:
        return IRFunction::HLL_UPDATE_FLOAT;
    case TYPE_DOUBLE:
        return IRFunction::HLL_UPDATE_DOUBLE;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return IRFunction::HLL_UPDATE_STRING;
    case TYPE_DECIMAL:
        return IRFunction::HLL_UPDATE_DECIMAL;
    default:
        DCHECK(false) << "Unsupported type: " << type;
        return IRFunction::FN_END;
    }
}

// IR Generation for updating a single aggregation slot. Signature is:
// void update_slot(FunctionContext* fn_ctx, AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col) is:
// define void @update_slot(%"class.doris_udf::FunctionContext"* %fn_ctx,
//                         { i8, double }* %agg_tuple,
//                         %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %src = call { i8, double } @GetSlotRef(%"class.doris::ExprContext"* inttoptr
//     (i64 128241264 to %"class.doris::ExprContext"*), %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, double }* %agg_tuple)
//   %dst_val = load double* %dst_slot_ptr
//   %val = extractvalue { i8, double } %src, 1
//   %1 = fadd double %dst_val, %val
//   store double %1, double* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
//
// The IR for min(double_col) is:
// define void @update_slot(%"class.doris_udf::FunctionContext"* %fn_ctx,
//                         { i8, double }* %agg_tuple,
//                         %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %src = call { i8, double } @GetSlotRef(%"class.doris::ExprContext"* inttoptr
//     (i64 128241264 to %"class.doris::ExprContext"*), %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_is_null = call i8 @is_null(tuple);
//   br i1 %dst_is_null, label dst_null, label dst_not_null
//
// dst_null:            ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, double }* %agg_tuple)
//   %val = extractvalue { i8, double } %src, 1
//   store double %val, double* %dst_slot_ptr
//   br label %ret
//
// dst_not_null:                                     ; preds = %src_not_null
//   %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, double }* %agg_tuple)
//   %dst_val = load double* %dst_slot_ptr
//   %val = extractvalue { i8, double } %src, 1
//   %1 = fadd double %dst_val, %val
//   store double %1, double* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
// The IR for ndv(double_col) is:
// define void @update_slot(%"class.doris_udf::FunctionContext"* %fn_ctx,
//                         { i8, %"struct.doris::StringValue" }* %agg_tuple,
//                         %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i8* }
//   %src_lowered_ptr = alloca { i8, double }
//   %src = call { i8, double } @GetSlotRef(%"class.doris::ExprContext"* inttoptr
//     (i64 120530832 to %"class.doris::ExprContext"*), %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds
//     { i8, %"struct.doris::StringValue" }* %agg_tuple, i32 0, i32 1
//   call void @SetNotNull({ i8, %"struct.doris::StringValue" }* %agg_tuple)
//   %dst_val = load %"struct.doris::StringValue"* %dst_slot_ptr
//   store { i8, double } %src, { i8, double }* %src_lowered_ptr
//   %src_unlowered_ptr = bitcast { i8, double }* %src_lowered_ptr
//                        to %"struct.doris_udf::DoubleVal"*
//   %ptr = extractvalue %"struct.doris::StringValue" %dst_val, 0
//   %dst_stringval = insertvalue { i64, i8* } zeroinitializer, i8* %ptr, 1
//   %len = extractvalue %"struct.doris::StringValue" %dst_val, 1
//   %1 = extractvalue { i64, i8* } %dst_stringval, 0
//   %2 = zext i32 %len to i64
//   %3 = shl i64 %2, 32
//   %4 = and i64 %1, 4294967295
//   %5 = or i64 %4, %3
//   %dst_stringval1 = insertvalue { i64, i8* } %dst_stringval, i64 %5, 0
//   store { i64, i8* } %dst_stringval1, { i64, i8* }* %dst_lowered_ptr
//   %dst_unlowered_ptr = bitcast { i64, i8* }* %dst_lowered_ptr
//                        to %"struct.doris_udf::StringVal"*
//   call void @HllUpdate(%"class.doris_udf::FunctionContext"* %fn_ctx,
//                        %"struct.doris_udf::DoubleVal"* %src_unlowered_ptr,
//                        %"struct.doris_udf::StringVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i8* }* %dst_lowered_ptr
//   %6 = extractvalue { i64, i8* } %anyval_result, 1
//   %7 = insertvalue %"struct.doris::StringValue" zeroinitializer, i8* %6, 0
//   %8 = extractvalue { i64, i8* } %anyval_result, 0
//   %9 = ashr i64 %8, 32
//   %10 = trunc i64 %9 to i32
//   %11 = insertvalue %"struct.doris::StringValue" %7, i32 %10, 1
//   store %"struct.doris::StringValue" %11, %"struct.doris::StringValue"* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
llvm::Function* AggregationNode::codegen_update_slot(
        RuntimeState* state, AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
    DCHECK(slot_desc->is_materialized());
    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }

    DCHECK_EQ(evaluator->input_expr_ctxs().size(), 1);
    ExprContext* input_expr_ctx = evaluator->input_expr_ctxs()[0];
    Expr* input_expr = input_expr_ctx->root();
    // TODO: implement timestamp
    if (input_expr->type().type == TYPE_DATETIME
            || input_expr->type().type == TYPE_DATE
            || input_expr->type().type == TYPE_DECIMAL
            || input_expr->type().is_string_type()) {
        return NULL;
    }
    Function* agg_expr_fn = NULL;
    Status status = input_expr->get_codegend_compute_fn(state, &agg_expr_fn);
    if (!status.ok()) {
        LOG(INFO) << "Could not codegen update_slot(): " << status.get_error_msg();
        return NULL;
    }
    DCHECK(agg_expr_fn != NULL);

    PointerType* fn_ctx_type =
        codegen->get_ptr_type(FunctionContextImpl::_s_llvm_functioncontext_name);
    StructType* tuple_struct = _intermediate_tuple_desc->generate_llvm_struct(codegen);
    PointerType* tuple_ptr_type = PointerType::get(tuple_struct, 0);
    PointerType* tuple_row_ptr_type = codegen->get_ptr_type(TupleRow::_s_llvm_class_name);

    // Create update_slot prototype
    LlvmCodeGen::FnPrototype prototype(codegen, "update_slot", codegen->void_type());
    prototype.add_argument(LlvmCodeGen::NamedVariable("fn_ctx", fn_ctx_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

    LlvmCodeGen::LlvmBuilder builder(codegen->context());
    Value* args[3];
    Function* fn = prototype.generate_prototype(&builder, &args[0]);
    Value* fn_ctx_arg = args[0];
    Value* agg_tuple_arg = args[1];
    Value* row_arg = args[2];

    BasicBlock* src_not_null_block = NULL;
    BasicBlock* dst_null_block = NULL;
    BasicBlock* dst_not_null_block = NULL;
    if (evaluator->agg_op() == AggFnEvaluator::MIN
            || evaluator->agg_op() == AggFnEvaluator::MAX) {
        src_not_null_block = BasicBlock::Create(codegen->context(), "src_not_null", fn);
        dst_null_block = BasicBlock::Create(codegen->context(), "dst_null", fn);
    }
    dst_not_null_block = BasicBlock::Create(codegen->context(), "dst_not_null", fn);
    BasicBlock* ret_block = BasicBlock::Create(codegen->context(), "ret", fn);

    // Call expr function to get src slot value
    Value* ctx_arg = codegen->cast_ptr_to_llvm_ptr(
        codegen->get_ptr_type(ExprContext::_s_llvm_class_name), input_expr_ctx);
    Value* agg_expr_fn_args[] = { ctx_arg, row_arg };
    CodegenAnyVal src = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, input_expr->type(), agg_expr_fn, agg_expr_fn_args, "src", NULL);

    Value* src_is_null = src.get_is_null();
    if (evaluator->agg_op() == AggFnEvaluator::MIN
            || evaluator->agg_op() == AggFnEvaluator::MAX) {
        builder.CreateCondBr(src_is_null, ret_block, src_not_null_block);

        // Src slot is not null
        builder.SetInsertPoint(src_not_null_block);
        Function* is_null_fn = slot_desc->codegen_is_null(codegen, tuple_struct);
        Value* dst_is_null = builder.CreateCall(is_null_fn, agg_tuple_arg);
        builder.CreateCondBr(dst_is_null, dst_null_block, dst_not_null_block);
        // dst slot is null
        builder.SetInsertPoint(dst_null_block);
        Value* dst_ptr =
            builder.CreateStructGEP(agg_tuple_arg, slot_desc->field_idx(), "dst_slot_ptr");
        if (slot_desc->is_nullable()) {
            // Dst is NULL, just update dst slot to src slot and clear null bit
            Function* clear_null_fn = slot_desc->codegen_update_null(codegen, tuple_struct, false);
            builder.CreateCall(clear_null_fn, agg_tuple_arg);
        }
        builder.CreateStore(src.get_val(), dst_ptr);
        builder.CreateBr(ret_block);
    } else {
        builder.CreateCondBr(src_is_null, ret_block, dst_not_null_block);
    }


    // Src slot is not null, update dst_slot
    builder.SetInsertPoint(dst_not_null_block);
    Value* dst_ptr =
        builder.CreateStructGEP(agg_tuple_arg, slot_desc->field_idx(), "dst_slot_ptr");
    Value* result = NULL;

    if (slot_desc->is_nullable()) {
        // Dst is NULL, just update dst slot to src slot and clear null bit
        Function* clear_null_fn = slot_desc->codegen_update_null(codegen, tuple_struct, false);
        builder.CreateCall(clear_null_fn, agg_tuple_arg);
    }

    // Update the slot
    Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
    switch (evaluator->agg_op()) {
    case AggFnEvaluator::COUNT:
        if (evaluator->is_merge()) {
            result = builder.CreateAdd(dst_value, src.get_val(), "count_sum");
        } else {
            result = builder.CreateAdd(
                dst_value, codegen->get_int_constant(TYPE_BIGINT, 1), "count_inc");
        }
        break;
    case AggFnEvaluator::MIN: {
        Function* min_fn = codegen->codegen_min_max(slot_desc->type(), true);
        Value* min_args[] = { dst_value, src.get_val() };
        result = builder.CreateCall(min_fn, min_args, "min_value");
        break;
    }
    case AggFnEvaluator::MAX: {
        Function* max_fn = codegen->codegen_min_max(slot_desc->type(), false);
        Value* max_args[] = { dst_value, src.get_val() };
        result = builder.CreateCall(max_fn, max_args, "max_value");
        break;
    }
    case AggFnEvaluator::SUM:
        if (slot_desc->type().type == TYPE_FLOAT || slot_desc->type().type == TYPE_DOUBLE) {
            result = builder.CreateFAdd(dst_value, src.get_val());
        } else {
            result = builder.CreateAdd(dst_value, src.get_val());
        }
        break;
    case AggFnEvaluator::NDV: {
        DCHECK_EQ(slot_desc->type().type, TYPE_VARCHAR);
        IRFunction::Type ir_function_type = evaluator->is_merge() ? IRFunction::HLL_MERGE
            : get_hll_update_function2(input_expr->type());
        Function* hll_fn = codegen->get_function(ir_function_type);

        // Create pointer to src_anyval to pass to HllUpdate() function. We must use the
        // unlowered type.
        Value* src_lowered_ptr = codegen->create_entry_block_alloca(
            fn, LlvmCodeGen::NamedVariable("src_lowered_ptr", src.value()->getType()));
        builder.CreateStore(src.value(), src_lowered_ptr);
        Type* unlowered_ptr_type =
            CodegenAnyVal::get_unlowered_type(codegen, input_expr->type())->getPointerTo();
        Value* src_unlowered_ptr =
            builder.CreateBitCast(src_lowered_ptr, unlowered_ptr_type, "src_unlowered_ptr");

        // Create StringVal* intermediate argument from dst_value
        CodegenAnyVal dst_stringval = CodegenAnyVal::get_non_null_val(
            codegen, &builder, TypeDescriptor(TYPE_VARCHAR), "dst_stringval");
        dst_stringval.set_from_raw_value(dst_value);
        // Create pointer to dst_stringval to pass to HllUpdate() function. We must use
        // the unlowered type.
        Value* dst_lowered_ptr = codegen->create_entry_block_alloca(
            fn, LlvmCodeGen::NamedVariable("dst_lowered_ptr",
                                           dst_stringval.value()->getType()));
        builder.CreateStore(dst_stringval.value(), dst_lowered_ptr);
        unlowered_ptr_type =
            codegen->get_ptr_type(CodegenAnyVal::get_unlowered_type(
                    codegen, TypeDescriptor(TYPE_VARCHAR)));
        Value* dst_unlowered_ptr =
            builder.CreateBitCast(dst_lowered_ptr, unlowered_ptr_type, "dst_unlowered_ptr");

        // Call 'hll_fn'
        builder.CreateCall3(hll_fn, fn_ctx_arg, src_unlowered_ptr, dst_unlowered_ptr);

        // Convert StringVal intermediate 'dst_arg' back to StringValue
        Value* anyval_result = builder.CreateLoad(dst_lowered_ptr, "anyval_result");
        result = CodegenAnyVal(codegen, &builder, TypeDescriptor(TYPE_VARCHAR), anyval_result)
            .to_native_value();
        break;
    }
    default:
        DCHECK(false) << "bad aggregate operator: " << evaluator->agg_op();
    }

    builder.CreateStore(result, dst_ptr);
    builder.CreateBr(ret_block);

    builder.SetInsertPoint(ret_block);
    builder.CreateRetVoid();

    fn = codegen->finalize_function(fn);
    return fn;
}

// IR codegen for the update_tuple loop.  This loop is query specific and
// based on the aggregate functions.  The function signature must match the non-
// codegen'd update_tuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @update_tuple(%"class.doris::AggregationNode"* %this_ptr,
//                          %"class.doris::Tuple"* %agg_tuple,
//                          %"class.doris::TupleRow"* %tuple_row) #20 {
// entry:
//   %tuple = bitcast %"class.doris::Tuple"* %agg_tuple to { i8, i64, i64, double }*
//   %src_slot = getelementptr inbounds { i8, i64, i64, double }* %tuple, i32 0, i32 1
//   %count_star_val = load i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   call void @update_slot(%"class.doris_udf::FunctionContext"* inttoptr
//                           (i64 44521296 to %"class.doris_udf::FunctionContext"*),
//                         { i8, i64, i64, double }* %tuple,
//                         %"class.doris::TupleRow"* %tuple_row)
//   call void @UpdateSlot5(%"class.doris_udf::FunctionContext"* inttoptr
//                            (i64 44521328 to %"class.doris_udf::FunctionContext"*),
//                          { i8, i64, i64, double }* %tuple,
//                          %"class.doris::TupleRow"* %tuple_row)
//   ret void
// }
Function* AggregationNode::codegen_update_tuple(RuntimeState* state) {
    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }
    SCOPED_TIMER(codegen->codegen_timer());

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        // skip non-materialized slots; we don't have evaluators instantiated for those
        while (!_intermediate_tuple_desc->slots()[j]->is_materialized()) {
            DCHECK_LT(j, _intermediate_tuple_desc->slots().size() - 1);
            ++j;
        }
        SlotDescriptor* slot_desc = _intermediate_tuple_desc->slots()[j];
        AggFnEvaluator* evaluator = _aggregate_evaluators[i];

        // Timestamp and char are never supported. NDV supports decimal and string but no
        // other functions.
        // TODO: the other aggregate functions might work with decimal as-is
        // TODO(zc)
        if (slot_desc->type().type == TYPE_DATETIME || slot_desc->type().type == TYPE_CHAR ||
            (evaluator->agg_op() != AggFnEvaluator::NDV &&
             (slot_desc->type().type == TYPE_DECIMAL ||
              slot_desc->type().type == TYPE_CHAR ||
              slot_desc->type().type == TYPE_VARCHAR))) {
            LOG(INFO) << "Could not codegen UpdateIntermediateTuple because "
                << "string, char, timestamp and decimal are not yet supported.";
            return NULL;
        }
        if (evaluator->agg_op() == AggFnEvaluator::COUNT_DISTINCT
                || evaluator->agg_op() == AggFnEvaluator::SUM_DISTINCT) {
            return NULL;
        }

        // Don't codegen things that aren't builtins (for now)
        if (!evaluator->is_builtin()) {
            return NULL;
        }
    }

    if (_intermediate_tuple_desc->generate_llvm_struct(codegen) == NULL) {
        LOG(INFO) << "Could not codegen update_tuple because we could"
            << "not generate a matching llvm struct for the intermediate tuple.";
        return NULL;
    }

    // Get the types to match the update_tuple signature
    Type* agg_node_type = codegen->get_type(AggregationNode::_s_llvm_class_name);
    Type* agg_tuple_type = codegen->get_type(Tuple::_s_llvm_class_name);
    Type* tuple_row_type = codegen->get_type(TupleRow::_s_llvm_class_name);

    DCHECK(agg_node_type != NULL);
    DCHECK(agg_tuple_type != NULL);
    DCHECK(tuple_row_type != NULL);

    PointerType* agg_node_ptr_type = PointerType::get(agg_node_type, 0);
    PointerType* agg_tuple_ptr_type = PointerType::get(agg_tuple_type, 0);
    PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

    // Signature for update_tuple is
    // void update_tuple(AggregationNode* this, Tuple* tuple, TupleRow* row)
    // This signature needs to match the non-codegen'd signature exactly.
    StructType* tuple_struct = _intermediate_tuple_desc->generate_llvm_struct(codegen);
    PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
    LlvmCodeGen::FnPrototype prototype(codegen, "update_tuple", codegen->void_type());
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("agg_tuple", agg_tuple_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));

    LlvmCodeGen::LlvmBuilder builder(codegen->context());
    Value* args[3];
    Function* fn = prototype.generate_prototype(&builder, &args[0]);

    // Cast the parameter types to the internal llvm runtime types.
    // TODO: get rid of this by using right type in function signature
    args[1] = builder.CreateBitCast(args[1], tuple_ptr, "tuple");

    // Loop over each expr and generate the IR for that slot.  If the expr is not
    // count(*), generate a helper IR function to update the slot and call that.
    j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        // skip non-materialized slots; we don't have evaluators instantiated for those
        while (!_intermediate_tuple_desc->slots()[j]->is_materialized()) {
            DCHECK_LT(j, _intermediate_tuple_desc->slots().size() - 1);
            ++j;
        }
        SlotDescriptor* slot_desc = _intermediate_tuple_desc->slots()[j];
        AggFnEvaluator* evaluator = _aggregate_evaluators[i];
        if (evaluator->is_count_star()) {
            // TODO: we should be able to hoist this up to the loop over the batch and just
            // increment the slot by the number of rows in the batch.
            int field_idx = slot_desc->field_idx();
            Value* const_one = codegen->get_int_constant(TYPE_BIGINT, 1);
            Value* slot_ptr = builder.CreateStructGEP(args[1], field_idx, "src_slot");
            Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
            Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
            builder.CreateStore(count_inc, slot_ptr);
        } else {
            Function* update_slot_fn = codegen_update_slot(state, evaluator, slot_desc);
            if (update_slot_fn == NULL) {
                return NULL;
            }
            Value* fn_ctx_arg = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(FunctionContextImpl::_s_llvm_functioncontext_name),
                _agg_fn_ctxs[i]);
            builder.CreateCall3(update_slot_fn, fn_ctx_arg, args[1], args[2]);
        }
    }
    builder.CreateRetVoid();

    // CodegenProcessRowBatch() does the final optimizations.
    return codegen->finalize_function(fn);
}

Function* AggregationNode::codegen_process_row_batch(
        RuntimeState* state, Function* update_tuple_fn) {
    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }
    SCOPED_TIMER(codegen->codegen_timer());
    DCHECK(update_tuple_fn != NULL);

    // Get the cross compiled update row batch function
    IRFunction::Type ir_fn =
        (!_probe_expr_ctxs.empty() ?  IRFunction::AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING
            : IRFunction::AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING);
    Function* process_batch_fn = codegen->get_function(ir_fn);
    if (process_batch_fn == NULL) {
        LOG(ERROR) << "Could not find AggregationNode::ProcessRowBatch in module.";
        return NULL;
    }

    int replaced = 0;
    if (!_probe_expr_ctxs.empty()) {
        // Aggregation w/o grouping does not use a hash table.

        // Codegen for hash
        Function* hash_fn = _hash_tbl->codegen_hash_current_row(state);
        if (hash_fn == NULL) {
            return NULL;
        }

        // Codegen HashTable::Equals
        Function* equals_fn = _hash_tbl->codegen_equals(state);
        if (equals_fn == NULL) {
            return NULL;
        }

        // Codegen for evaluating build rows
        Function* eval_build_row_fn = _hash_tbl->codegen_eval_tuple_row(state, true);
        if (eval_build_row_fn == NULL) {
            return NULL;
        }

        // Codegen for evaluating probe rows
        Function* eval_probe_row_fn = _hash_tbl->codegen_eval_tuple_row(state, false);
        if (eval_probe_row_fn == NULL) {
            return NULL;
        }

        // Replace call sites
        process_batch_fn = codegen->replace_call_sites(
            process_batch_fn, false, eval_build_row_fn, "eval_build_row", &replaced);
        DCHECK_EQ(replaced, 1);

        process_batch_fn = codegen->replace_call_sites(
            process_batch_fn, false, eval_probe_row_fn, "eval_probe_row", &replaced);
        DCHECK_EQ(replaced, 1);

        process_batch_fn = codegen->replace_call_sites(
            process_batch_fn, false, hash_fn, "hash_current_row", &replaced);
        DCHECK_EQ(replaced, 2);

        process_batch_fn = codegen->replace_call_sites(
            process_batch_fn, false, equals_fn, "equals", &replaced);
        DCHECK_EQ(replaced, 1);
    }

    process_batch_fn = codegen->replace_call_sites(
        process_batch_fn, false, update_tuple_fn, "update_tuple", &replaced);
    DCHECK_EQ(replaced, 1) << "One call site should be replaced.";
    DCHECK(process_batch_fn != NULL);
    return codegen->optimize_function_with_exprs(process_batch_fn);
}
}

