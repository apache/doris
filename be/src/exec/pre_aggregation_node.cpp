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

#include "exec/pre_aggregation_node.h"

#include <boost/functional/hash.hpp>
#include <math.h>
#include <sstream>
#include <x86intrin.h>

#include "exec/hash_table.hpp"
#include "exprs/expr.h"
#include "exprs/agg_expr.h"
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
#include "util/debug_util.h"
#include "util/runtime_profile.h"

// This object appends n-int32s to the end of a normal tuple object to maintain the
// lengths of the string buffers in the tuple.
namespace doris {
// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
PreAggregationNode::PreAggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
    : ExecNode(pool, tnode, descs),
      _construct_fail(false),
      _is_init(false),
      _use_aggregate(true),
      _child_eos(false),
      _input_record_num(0),
      _input_record_num_sum(0),
      _agg_record_num(0),
      _agg_record_num_sum(0),
      _bad_agg_num(0),
      _bad_agg_latch(3),
      _agg_record_latch(5000),
      _agg_rate_latch(150),
      _singleton_agg_row(NULL),
      _tuple_pool(NULL),
      _build_timer(NULL),
      _get_results_timer(NULL),
      _hash_table_buckets_counter(NULL),
      _hash_table_load_factor_counter(NULL) {
    if (NULL == pool) {
        _construct_fail = true;
        LOG(WARNING) << "input pool is NULL";
        return;
    }

    if (!tnode.__isset.pre_agg_node) {
        _construct_fail = true;
        LOG(WARNING) << "there is no pre agg node";
        return;
    }

    Status status = Expr::create_expr_trees(pool, tnode.pre_agg_node.group_exprs, &_probe_exprs);

    if (!status.ok()) {
        _construct_fail = true;
        LOG(WARNING) << "construct group exprs failed.";
        return;
    }

    status = Expr::create_expr_trees(pool, tnode.pre_agg_node.group_exprs, &_build_exprs);

    if (!status.ok()) {
        _construct_fail = true;
        LOG(WARNING) << "construct build exprs failed.";
        return;
    }

    status = Expr::create_expr_trees(pool, tnode.pre_agg_node.aggregate_exprs, &_aggregate_exprs);

    if (!status.ok()) {
        _construct_fail = true;
        LOG(WARNING) << "construct aggregate exprs failed.";
        return;
    }
}

PreAggregationNode::~PreAggregationNode() {
}

Status PreAggregationNode::prepare(RuntimeState* state) {
    if (_construct_fail) {
        return Status::InternalError("construct failed.");
    }

    if (_is_init) {
        return Status::OK();
    }

    if (NULL == state || _children.size() != 1) {
        return Status::InternalError("input parameter is not OK.");
    }

    RETURN_IF_ERROR(ExecNode::prepare(state));

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _hash_table_buckets_counter =
        ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _hash_table_load_factor_counter =
        ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);

    if (NULL == _build_timer || NULL == _get_results_timer
            || NULL == _hash_table_buckets_counter || NULL == _hash_table_load_factor_counter) {
        return Status::InternalError("construct timer and counter failed.");
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // prepare _probe_exprs and _aggregate_exprs
    RETURN_IF_ERROR(Expr::prepare(_probe_exprs, state, _children[0]->row_desc(), false));
    RETURN_IF_ERROR(Expr::prepare(_build_exprs, state, _children[0]->row_desc(), false));
    RETURN_IF_ERROR(Expr::prepare(_aggregate_exprs, state, _children[0]->row_desc(), false));

    // TODO: how many buckets?
    // new one hash table
    _build_tuple_size = child(0)->row_desc().tuple_descriptors().size();
    _hash_tbl.reset(new(std::nothrow) HashTable(_build_exprs, _probe_exprs, _build_tuple_size,
                     true, id(), *state->mem_trackers(), 16384));

    if (NULL == _hash_tbl.get()) {
        return Status::InternalError("new one hash table failed.");
    }

    // Determine the number of string slots in the output
    for (std::vector<Expr*>::const_iterator expr = _aggregate_exprs.begin();
            expr != _aggregate_exprs.end(); ++expr) {
        AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

        // only support sum
        if (agg_expr->agg_op() != TAggregationOp::SUM) {
            _use_aggregate = false;
            break;
        }

        // when there is String Type or is Count star, this node do nothing.
        if (agg_expr->type() == TYPE_STRING ||
                agg_expr->type() == TYPE_CHAR ||
                agg_expr->type() == TYPE_VARCHAR ||
                agg_expr->is_star()) {
            _use_aggregate = false;
            break;
        }

        if (agg_expr->get_child(0)->node_type() != TExprNodeType::SLOT_REF) {
            _use_aggregate = false;
            break;
        }
    }

    for (int i = 0; i < child(0)->get_tuple_ids().size(); ++i) {
        TupleDescriptor* child_tuple
            = state->desc_tbl().get_tuple_descriptor(child(0)->get_tuple_ids()[i]);
        _children_tuple.push_back(_row_descriptor.get_tuple_idx(child_tuple->id()));
    }

    if (0 == _children_tuple.size()) {
        return Status::InternalError("children tuple size is zero.");
    }

    _tuple_row_size = _row_descriptor.tuple_descriptors().size() * sizeof(Tuple*);

    // new before construct single row.
    _tuple_pool.reset(new(std::nothrow) MemPool());

    if (NULL == _tuple_pool.get()) {
        return Status::InternalError("no memory for Mempool.");
    }

    _tuple_pool->set_limits(*state->mem_trackers());

    // _singleton_agg_row
    if (0 == _probe_exprs.size()) {
        construct_single_row();
    }

    _is_init = true;
    return Status::OK();
}

// Open Function, used to build hash table, Do not read in this Operation.
Status PreAggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // open child ready to read data, only open, do nothing.
    RETURN_IF_ERROR(_children[0]->open(state));

    return Status::OK();
}

// GetNext interface, read data from _hash_tbl
Status PreAggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_get_results_timer);
    int read_time = 0;

    if ((!_output_iterator.has_next() && _child_eos) || reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // if hash table hash data not read, read hash table first.
    if (_output_iterator.has_next()) {
        // read all data in HashTable
        goto read_from_hash;
    }

    // if not use aggregate, read child data directly.
    if (!_use_aggregate) {
        Status status = _children[0]->get_next(state, row_batch, eos);
        _num_rows_returned += row_batch->num_rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        return status;
    }

    // reach here, need use _hash_tbl to aggregate
    // read data from child
    while (true) {
        //RETURN_IF_CANCELLED(state);
        RowBatch child_batch(_children[0]->row_desc(), state->batch_size());
        // read _children data
        //RETURN_IF_ERROR(_children[0]->get_next(state, &child_batch, &_child_eos));
        _children[0]->get_next(state, &child_batch, &_child_eos);

        SCOPED_TIMER(_build_timer);
        read_time++;

        // handle
        if (_singleton_agg_row) {
            RETURN_IF_ERROR(process_row_batch_no_grouping(&child_batch));
        } else {
            RETURN_IF_ERROR(process_row_batch_with_grouping(&child_batch));
            _tuple_pool->acquire_data(child_batch.tuple_data_pool(), false);
        }

        _input_record_num += child_batch.num_rows();
        _agg_record_num = _hash_tbl->size() ? _hash_tbl->size() : 1;

        // if hash table is too big, break to read
        if (_agg_record_num > _agg_record_latch) {
            if ((_input_record_num * 100 / _agg_record_num) < _agg_rate_latch) {
                _bad_agg_num++;

                if (_bad_agg_num >= _bad_agg_latch) {
                    _use_aggregate = false;
                }
            }

            _agg_record_num_sum += _agg_record_num;
            _input_record_num_sum += _input_record_num;
            _input_record_num = 0;
            _agg_record_num = 0;
            _output_iterator = _hash_tbl->begin();
            break;
        }

        // no data to read child, break to read data
        if (_child_eos || read_time >= 5) {
            // now, push the only row into hash table.
            if (_singleton_agg_row) {
                _hash_tbl->insert(_singleton_agg_row);
                construct_single_row();
            }

            _output_iterator = _hash_tbl->begin();
            break;
        }

        //RETURN_IF_LIMIT_EXCEEDED(state);
        COUNTER_SET(_hash_table_buckets_counter, _hash_tbl->num_buckets());
        COUNTER_SET(memory_used_counter(),
                    _tuple_pool->peak_allocated_bytes() + _hash_tbl->byte_size());
        COUNTER_SET(_hash_table_load_factor_counter, _hash_tbl->load_factor());
    }

read_from_hash:

    while (_output_iterator.has_next() && !row_batch->is_full()) {
        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        row_batch->copy_row(_output_iterator.get_row(), row);
        // there is no need to compute conjuncts.
        row_batch->commit_last_row();
        ++_num_rows_returned;

        // if Reach this Node Limit
        if (reached_limit()) {
            break;
        }

        _output_iterator.Next<false>();
    }

    // if no data, clear hash table
    if (!_output_iterator.has_next()) {
        _hash_tbl.reset(new HashTable(_build_exprs, _probe_exprs, _build_tuple_size, true,
                                       id(), *state->mem_trackers(), 16384));
        row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), false);
    }

    *eos = (!_output_iterator.has_next() && _child_eos) || reached_limit();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    return Status::OK();
}

Status PreAggregationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    if (memory_used_counter() != NULL && _hash_tbl.get() != NULL &&
            _hash_table_buckets_counter != NULL) {
        COUNTER_SET(memory_used_counter(),
                    _tuple_pool->peak_allocated_bytes() + _hash_tbl->byte_size());
        COUNTER_SET(_hash_table_buckets_counter, _hash_tbl->num_buckets());
    }

    LOG(INFO) << "_input_record_num is " << _input_record_num_sum
              << " aggregate:" << _agg_record_num_sum
              << " bad_agg_num:" <<  _bad_agg_num;
    return ExecNode::close(state);
}

// used to construct one tuple row insert into hash table.
Status PreAggregationNode::construct_single_row() {
    // alloc single row
    _singleton_agg_row = reinterpret_cast<TupleRow*>(_tuple_pool->allocate(_tuple_row_size));

    if (NULL == _singleton_agg_row) {
        return Status::InternalError("new memory failed.");
    }

    for (int i = 0; i < _row_descriptor.tuple_descriptors().size(); ++i) {
        Tuple* tuple = reinterpret_cast<Tuple*>(_tuple_pool->allocate(
                _row_descriptor.tuple_descriptors()[i]->byte_size()));

        if (NULL == tuple) {
            return Status::InternalError("new one tuple failed.");
        }

        _singleton_agg_row->set_tuple(i, tuple);
    }

    // set agg value
    for (int i = 0; i < _aggregate_exprs.size(); ++i) {
        AggregateExpr* agg_expr = static_cast<AggregateExpr*>(_aggregate_exprs[i]);
        // determine value of aggregate's child expr
        void* value = agg_expr->get_child(0)->get_value(_singleton_agg_row);

        if (value == NULL) {
            // NULLs don't get aggregated
            continue;
        }

        // To minimize branching on the UpdateAggTuple path, initialize the result value
        // so that UpdateAggTuple doesn't have to check if the aggregation dst slot is null.
        //  - sum/count: 0
        //  - min: max_value
        //  - max: min_value
        ExprValue default_value;
        void* default_value_ptr = NULL;

        switch (agg_expr->agg_op()) {
        case TAggregationOp::MIN:
            default_value_ptr = default_value.set_to_max(agg_expr->type());
            break;

        case TAggregationOp::MAX:
            default_value_ptr = default_value.set_to_min(agg_expr->type());
            break;

        default:
            default_value_ptr = default_value.set_to_zero(agg_expr->type());
            break;
        }

        RawValue::write(default_value_ptr, value, agg_expr->get_child(0)->type(), NULL);
    }

    return Status::OK();
}

/**
 * used to construct one tuple row insert into hash table.
 */
TupleRow* PreAggregationNode::construct_row(TupleRow* in_row) {
    TupleRow* out_row = NULL;
    out_row = reinterpret_cast<TupleRow*>(_tuple_pool->allocate(_tuple_row_size));

    if (NULL == out_row) {
        return NULL;
    }

    for (int i = 0; i < _children_tuple.size(); ++i) {
        out_row->set_tuple(_children_tuple[i], in_row->get_tuple(_children_tuple[i]));
    }

    return out_row;
}

template <typename T>
void update_min_slot(void* slot, void* value) {
    T* t_slot = static_cast<T*>(slot);
    *t_slot = min(*t_slot, *static_cast<T*>(value));
}

template <typename T>
void update_max_slot(void* slot, void* value) {
    T* t_slot = static_cast<T*>(slot);
    *t_slot = max(*t_slot, *static_cast<T*>(value));
}

template <typename T>
void update_sum_slot(void* slot, void* value) {
    T* t_slot = static_cast<T*>(slot);
    *t_slot += *static_cast<T*>(value);
}

Status PreAggregationNode::process_row_batch_no_grouping(RowBatch* batch) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        RETURN_IF_ERROR(update_agg_row(_singleton_agg_row, batch->get_row(i)));
    }

    return Status::OK();
}

Status PreAggregationNode::process_row_batch_with_grouping(RowBatch* batch) {
    for (int i = 0; i < batch->num_rows(); ++i) {
        TupleRow* probe_row = batch->get_row(i);
        TupleRow* agg_row = NULL;
        HashTable::Iterator entry = _hash_tbl->find(probe_row);

        if (!entry.has_next()) {
            agg_row = construct_row(probe_row);

            if (NULL == agg_row) {
                return Status::InternalError("Construct row failed.");
            }

            _hash_tbl->insert(agg_row);
        } else {
            agg_row = entry.get_row();
            RETURN_IF_ERROR(update_agg_row(agg_row, probe_row));
        }
    }

    return Status::OK();
}

// Function used to update Aggregate Row in hash table
Status PreAggregationNode::update_agg_row(TupleRow* agg_row, TupleRow* probe_row) {
    if (NULL == agg_row) {
        return Status::InternalError("internal error: input pointer is NULL");
    }

    // compute all aggregate expr
    for (std::vector<Expr*>::const_iterator expr = _aggregate_exprs.begin();
            expr != _aggregate_exprs.end(); ++expr) {
        AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

        // determine value of aggregate's child expr
        void* value = agg_expr->get_child(0)->get_value(probe_row);

        if (value == NULL) {
            // NULLs don't get aggregated
            continue;
        }

        //FIXME : now this Node handle Expr is SlotRef, if Not is Slot Ref, MayBe Error
        void* slot = agg_expr->get_child(0)->get_value(agg_row);
        DCHECK(slot != NULL);

        if (NULL == slot) {
            return Status::InternalError("get slot pointer is NULL.");
        }

        // switch opcode
        switch (agg_expr->agg_op()) {
        case TAggregationOp::MIN:
            switch (agg_expr->type()) {
            case TYPE_BOOLEAN:
                UpdateMinSlot<bool>(slot, value);
                break;

            case TYPE_TINYINT:
                UpdateMinSlot<int8_t>(slot, value);
                break;

            case TYPE_SMALLINT:
                UpdateMinSlot<int16_t>(slot, value);
                break;

            case TYPE_INT:
                UpdateMinSlot<int32_t>(slot, value);
                break;

            case TYPE_BIGINT:
                UpdateMinSlot<int64_t>(slot, value);
                break;

            case TYPE_FLOAT:
                UpdateMinSlot<float>(slot, value);
                break;

            case TYPE_DOUBLE:
                UpdateMinSlot<double>(slot, value);
                break;

            case TYPE_DATE:
            case TYPE_DATETIME:
                UpdateMinSlot<TimestampValue>(slot, value);
                break;

            case TYPE_DECIMAL:
                UpdateMinSlot<DecimalValue>(slot, value);
                break;

            case TYPE_DECIMALV2:
                UpdateMinSlot<DecimalV2Value>(slot, value);
                break;

            default:
                LOG(WARNING) << "invalid type: " << type_to_string(agg_expr->type());
                return Status::InternalError("unknown type");
            }

            break;

        case TAggregationOp::MAX:
            switch (agg_expr->type()) {
            case TYPE_BOOLEAN:
                UpdateMaxSlot<bool>(slot, value);
                break;

            case TYPE_TINYINT:
                UpdateMaxSlot<int8_t>(slot, value);
                break;

            case TYPE_SMALLINT:
                UpdateMaxSlot<int16_t>(slot, value);
                break;

            case TYPE_INT:
                UpdateMaxSlot<int32_t>(slot, value);
                break;

            case TYPE_BIGINT:
                UpdateMaxSlot<int64_t>(slot, value);
                break;

            case TYPE_FLOAT:
                UpdateMaxSlot<float>(slot, value);
                break;

            case TYPE_DOUBLE:
                UpdateMaxSlot<double>(slot, value);
                break;

            case TYPE_DATE:
            case TYPE_DATETIME:
                UpdateMaxSlot<TimestampValue>(slot, value);
                break;

            case TYPE_DECIMAL:
                UpdateMaxSlot<DecimalValue>(slot, value);
                break;

            case TYPE_DECIMALV2:
                UpdateMaxSlot<DecimalV2Value>(slot, value);
                break;

            default:
                LOG(WARNING) << "invalid type: " << type_to_string(agg_expr->type());
                return Status::InternalError("unknown type");
            }

            break;

        case TAggregationOp::SUM:
            switch (agg_expr->type()) {
            case TYPE_BIGINT:
                UpdateSumSlot<int64_t>(slot, value);
                break;

            case TYPE_DOUBLE:
                UpdateSumSlot<double>(slot, value);
                break;

            case TYPE_DECIMAL:
                UpdateSumSlot<DecimalValue>(slot, value);
                break;

            case TYPE_DECIMALV2:
                UpdateSumSlot<DecimalV2Value>(slot, value);
                break;

            default:
                LOG(WARNING) << "invalid type: " << type_to_string(agg_expr->type());
                return Status::InternalError("Aggsum not valid.");
            }

            break;

        default:
            LOG(WARNING) << "bad aggregate operator: " << agg_expr->agg_op();
            return Status::InternalError("unknown agg op.");
        }
    }

    return Status::OK();
}

void PreAggregationNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "AggregationNode(tuple_id=xxx"
         << " probe_exprs=" << Expr::debug_string(_probe_exprs)
         << " build_exprs=" << Expr::debug_string(_build_exprs)
         << " agg_exprs=" << Expr::debug_string(_aggregate_exprs);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

}

