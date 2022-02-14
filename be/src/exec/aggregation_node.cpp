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

#include <gperftools/profiler.h>
#include <math.h>

#include <sstream>

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

namespace doris {

// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _singleton_output_tuple(nullptr),
          //_tuple_pool(new MemPool()),
          //
          _process_row_batch_fn(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _build_timer(nullptr),
          _get_results_timer(nullptr),
          _hash_table_buckets_counter(nullptr) {}

AggregationNode::~AggregationNode() {}

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            Expr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));

    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        AggFnEvaluator::create(_pool, tnode.agg_node.aggregate_functions[i], &evaluator);
        _aggregate_evaluators.push_back(evaluator);
    }
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _hash_table_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _hash_table_load_factor_counter =
            ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(
            Expr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

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
    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state, build_row_desc, expr_mem_tracker()));

    _tuple_pool.reset(new MemPool(mem_tracker().get()));

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
                state, child(0)->row_desc(), _tuple_pool.get(), intermediate_slot_desc,
                output_slot_desc, mem_tracker(), &_agg_fn_ctxs[i]));
        state->obj_pool()->add(_agg_fn_ctxs[i]);
    }

    // TODO: how many buckets?
    _hash_tbl.reset(new HashTable(_build_expr_ctxs, _probe_expr_ctxs, 1, true,
                                  std::vector<bool>(_build_expr_ctxs.size(), false), id(),
                                  mem_tracker(), 1024));

    if (_probe_expr_ctxs.empty()) {
        // create single output tuple now; we need to output something
        // even if our input is empty
        _singleton_output_tuple = construct_intermediate_tuple();
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

    RowBatch batch(_children[0]->row_desc(), state->batch_size(), mem_tracker().get());
    int64_t num_input_rows = 0;
    int64_t num_agg_rows = 0;

    bool early_return = false;
    bool limit_with_no_agg = (limit() != -1 && (_aggregate_evaluators.size() == 0));
    DCHECK_EQ(_aggregate_evaluators.size(), _agg_fn_ctxs.size());

    while (true) {
        bool eos = false;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("Aggregation, before getting next from child 0."));
        RETURN_IF_ERROR(_children[0]->get_next(state, &batch, &eos));
        // SCOPED_TIMER(_build_timer);
        if (VLOG_ROW_IS_ON) {
            for (int i = 0; i < batch.num_rows(); ++i) {
                TupleRow* row = batch.get_row(i);
                VLOG_ROW << "id=" << id()
                         << " input row: " << row->to_string(_children[0]->row_desc());
            }
        }

        int64_t agg_rows_before = _hash_tbl->size();

        if (_process_row_batch_fn != nullptr) {
            _process_row_batch_fn(this, &batch);
        } else if (_singleton_output_tuple != nullptr) {
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
        RETURN_IF_ERROR(state->check_query_state("Aggregation, after hashing the child 0."));

        COUNTER_SET(_hash_table_buckets_counter, _hash_tbl->num_buckets());
        COUNTER_SET(_hash_table_load_factor_counter, _hash_tbl->load_factor());
        num_agg_rows += (_hash_tbl->size() - agg_rows_before);
        num_input_rows += batch.num_rows();

        batch.reset();

        RETURN_IF_ERROR(state->check_query_state("Aggregation, after setting the counter."));
        if (eos) {
            break;
        }
        if (early_return) {
            break;
        }
    }

    if (_singleton_output_tuple != nullptr) {
        _hash_tbl->insert(reinterpret_cast<TupleRow*>(&_singleton_output_tuple));
        ++num_agg_rows;
    }

    VLOG_ROW << "id=" << id() << " aggregated " << num_input_rows << " input rows into "
             << num_agg_rows << " output rows";
    _output_iterator = _hash_tbl->begin();
    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    // 1. `!need_finalize` means this aggregation node not the level two aggregation node
    // 2. `_singleton_output_tuple != nullptr` means is not group by
    // 3. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return nullptr result
    //    level one aggregation node set `eos = true` return directly
    if (UNLIKELY(!_needs_finalize && _singleton_output_tuple != nullptr &&
                 child(0)->rows_returned() == 0)) {
        *eos = true;
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Aggregation, before evaluating conjuncts."));
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
            RETURN_IF_ERROR(state->check_query_state("Aggregation, while evaluating conjuncts."));
        }
        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        Tuple* intermediate_tuple = _output_iterator.get_row()->get_tuple(0);
        Tuple* output_tuple = finalize_tuple(intermediate_tuple, row_batch->tuple_data_pool());
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
        if (_hash_tbl.get() != nullptr && _hash_table_buckets_counter != nullptr) {
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
    Tuple* dummy_dst = nullptr;
    if (_needs_finalize && _output_tuple_desc != nullptr) {
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

    if (_tuple_pool.get() != nullptr) {
        _tuple_pool->free_all();
    }
    if (_hash_tbl.get() != nullptr) {
        _hash_tbl->close();
    }

    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_build_expr_ctxs, state);

    return ExecNode::close(state);
}

Tuple* AggregationNode::construct_intermediate_tuple() {
    Tuple* agg_tuple = Tuple::create(_intermediate_tuple_desc->byte_size(), _tuple_pool.get());
    std::vector<SlotDescriptor*>::const_iterator slot_desc =
            _intermediate_tuple_desc->slots().begin();

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
        if (!(*slot_desc)->type().is_string_type() && !(*slot_desc)->type().is_date_type()) {
            ExprValue default_value;
            void* default_value_ptr = nullptr;

            switch (evaluator->agg_op()) {
            case TAggregationOp::MIN:
                default_value_ptr = default_value.set_to_max((*slot_desc)->type());
                RawValue::write(default_value_ptr, agg_tuple, *slot_desc, nullptr);
                break;

            case TAggregationOp::MAX:
                default_value_ptr = default_value.set_to_min((*slot_desc)->type());
                RawValue::write(default_value_ptr, agg_tuple, *slot_desc, nullptr);
                break;

            default:
                break;
            }
        }
    }

    return agg_tuple;
}

void AggregationNode::update_tuple(Tuple* tuple, TupleRow* row) {
    DCHECK(tuple != nullptr);

    AggFnEvaluator::add(_aggregate_evaluators, _agg_fn_ctxs, row, tuple);
}

Tuple* AggregationNode::finalize_tuple(Tuple* tuple, MemPool* pool) {
    DCHECK(tuple != nullptr);

    Tuple* dst = tuple;
    if (_needs_finalize && _intermediate_tuple_id != _output_tuple_id) {
        dst = Tuple::create(_output_tuple_desc->byte_size(), pool);
    }
    if (_needs_finalize) {
        AggFnEvaluator::finalize(
                _aggregate_evaluators, _agg_fn_ctxs, tuple, dst,
                _singleton_output_tuple != nullptr && child(0)->rows_returned() == 0);
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
            void* src_slot = nullptr;
            if (!src_slot_null) src_slot = tuple->get_slot(src_slot_desc->tuple_offset());
            RawValue::write(src_slot, dst, dst_slot_desc, nullptr);
        }
    }
    return dst;
}

void AggregationNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << std::string(indentation_level * 2, ' ');
    *out << "AggregationNode(intermediate_tuple_id=" << _intermediate_tuple_id
         << " output_tuple_id=" << _output_tuple_id << " needs_finalize="
         << _needs_finalize
         // << " probe_exprs=" << Expr::debug_string(_probe_exprs)
         << " agg_exprs=" << AggFnEvaluator::debug_string(_aggregate_evaluators);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

void AggregationNode::push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    // groupby can pushdown, agg can't pushdown
    // Now we doesn't pushdown for easy.
    return;
}

void AggregationNode::process_row_batch_no_grouping(RowBatch* batch, MemPool* pool) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        update_tuple(_singleton_output_tuple, batch->get_row(i));
    }
}

void AggregationNode::process_row_batch_with_grouping(RowBatch* batch, MemPool* pool) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* row = batch->get_row(i);
        Tuple* agg_tuple = nullptr;
        HashTable::Iterator it = _hash_tbl->find(row);

        if (it.at_end()) {
            agg_tuple = construct_intermediate_tuple();
            _hash_tbl->insert(reinterpret_cast<TupleRow*>(&agg_tuple));
        } else {
            agg_tuple = it.get_row()->get_tuple(0);
        }

        update_tuple(agg_tuple, row);
    }
}

} // namespace doris
