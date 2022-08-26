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

#include "exec/table_function_node.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/table_function/table_function_factory.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "vec/exprs/vexpr.h"

namespace doris {

TableFunctionNode::TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

TableFunctionNode::~TableFunctionNode() {}

Status TableFunctionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    for (const TExpr& texpr : tnode.table_function_node.fnCallExprList) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, texpr, &ctx));
        _fn_ctxs.push_back(ctx);

        Expr* root = ctx->root();
        const std::string& tf_name = root->fn().name.function_name;
        TableFunction* fn = nullptr;
        RETURN_IF_ERROR(TableFunctionFactory::get_fn(tf_name, false, _pool, &fn));
        fn->set_expr_context(ctx);
        _fns.push_back(fn);
    }
    _fn_num = _fns.size();
    _fn_values.resize(_fn_num);
    _fn_value_lengths.resize(_fn_num);

    // Prepare output slot ids
    RETURN_IF_ERROR(_prepare_output_slot_ids(tnode));
    return Status::OK();
}

Status TableFunctionNode::_prepare_output_slot_ids(const TPlanNode& tnode) {
    // Prepare output slot ids
    if (tnode.table_function_node.outputSlotIds.empty()) {
        return Status::InternalError("Output slots of table function node is empty");
    }
    SlotId max_id = -1;
    for (auto slot_id : tnode.table_function_node.outputSlotIds) {
        if (slot_id > max_id) {
            max_id = slot_id;
        }
    }
    _output_slot_ids = std::vector<bool>(max_id + 1, false);
    for (auto slot_id : tnode.table_function_node.outputSlotIds) {
        _output_slot_ids[slot_id] = true;
    }

    return Status::OK();
}

bool TableFunctionNode::_is_inner_and_empty() {
    for (int i = 0; i < _fn_num; i++) {
        // if any table function is not outer and has empty result, go to next child row
        if (!_fns[i]->is_outer() && _fns[i]->current_empty()) {
            return true;
        }
    }
    return false;
}

Status TableFunctionNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _num_rows_filtered_counter = ADD_COUNTER(_runtime_profile, "RowsFiltered", TUnit::UNIT);

    RETURN_IF_ERROR(Expr::prepare(_fn_ctxs, state, _row_descriptor));
    for (auto fn : _fns) {
        RETURN_IF_ERROR(fn->prepare());
    }
    return Status::OK();
}

Status TableFunctionNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "TableFunctionNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(Expr::open(_fn_ctxs, state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_vfn_ctxs, state));

    for (auto fn : _fns) {
        RETURN_IF_ERROR(fn->open());
    }

    RETURN_IF_ERROR(_children[0]->open(state));
    return Status::OK();
}

Status TableFunctionNode::_process_next_child_row() {
    if (_cur_child_offset == _cur_child_batch->num_rows()) {
        _cur_child_batch->reset();
        _child_batch_exhausted = true;
        return Status::OK();
    }
    _cur_child_tuple_row = _cur_child_batch->get_row(_cur_child_offset++);
    for (TableFunction* fn : _fns) {
        RETURN_IF_ERROR(fn->process(_cur_child_tuple_row));
    }

    _child_batch_exhausted = false;
    return Status::OK();
}

// Returns the index of fn of the last eos counted from back to front
// eg: there are 3 functions in `_fns`
//      eos:    false, true, true
//      return: 1
//
//      eos:    false, false, true
//      return: 2
//
//      eos:    false, false, false
//      return: -1
//
//      eos:    true, true, true
//      return: 0
//
// return:
//  0: all fns are eos
// -1: all fns are not eos
// >0: some of fns are eos
int TableFunctionNode::_find_last_fn_eos_idx() {
    for (int i = _fn_num - 1; i >= 0; --i) {
        if (!_fns[i]->eos()) {
            if (i == _fn_num - 1) {
                return -1;
            } else {
                return i + 1;
            }
        }
    }
    // all eos
    return 0;
}

// Roll to reset the table function.
// Eg:
//  There are 3 functions f1, f2 and f3 in `_fns`.
//  If `last_eos_idx` is 1, which means f2 and f3 are eos.
//  So we need to forward f1, and reset f2 and f3.
bool TableFunctionNode::_roll_table_functions(int last_eos_idx) {
    bool fn_eos = false;
    int i = last_eos_idx - 1;
    for (; i >= 0; --i) {
        _fns[i]->forward(&fn_eos);
        if (!fn_eos) {
            break;
        }
    }
    if (i == -1) {
        // after forward, all functions are eos.
        // we should process next child row to get more table function results.
        return false;
    }

    for (int j = i + 1; j < _fn_num; ++j) {
        _fns[j]->reset();
    }

    return true;
}

// There are 2 while loops in this method.
// The outer loop is to get the next batch from child node.
// And the inner loop is to expand the row by table functions, and output row by row.
Status TableFunctionNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    const RowDescriptor& parent_rowdesc = row_batch->row_desc();
    const RowDescriptor& child_rowdesc = _children[0]->row_desc();
    if (_parent_tuple_desc_size == -1) {
        _parent_tuple_desc_size = parent_rowdesc.tuple_descriptors().size();
        _child_tuple_desc_size = child_rowdesc.tuple_descriptors().size();
        for (int i = 0; i < _child_tuple_desc_size; ++i) {
            _child_slot_sizes.push_back(child_rowdesc.tuple_descriptors()[i]->slots().size());
        }
    }

    uint8_t* tuple_buffer = nullptr;
    Tuple* tuple_ptr = nullptr;
    Tuple* pre_tuple_ptr = nullptr;

    while (true) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("TableFunctionNode, while getting next batch."));

        if (_cur_child_batch == nullptr) {
            _cur_child_batch.reset(new RowBatch(child_rowdesc, state->batch_size()));
        }
        if (_child_batch_exhausted) {
            if (_child_eos) {
                // current child batch is exhausted, and no more batch from child node
                break;
            }
            // current child batch is exhausted, get next batch from child
            RETURN_IF_ERROR(_children[0]->get_next(state, _cur_child_batch.get(), &_child_eos));
            if (_cur_child_batch->num_rows() == 0) {
                // no more batch from child node
                break;
            }

            _cur_child_offset = 0;
            RETURN_IF_ERROR(_process_next_child_row());
            if (_child_batch_exhausted) {
                continue;
            }
        }

        bool skip_child_row = false;
        while (true) {
            int idx = _find_last_fn_eos_idx();
            if (idx == 0 || skip_child_row) {
                // all table functions' results are exhausted, process next child row
                RETURN_IF_ERROR(_process_next_child_row());
                if (_child_batch_exhausted) {
                    break;
                }
            } else if (idx < _fn_num && idx != -1) {
                // some of table functions' results are exhausted
                if (!_roll_table_functions(idx)) {
                    // continue to process next child row
                    continue;
                }
            }

            // if any table function is not outer and has empty result, go to next child row
            if (skip_child_row = _is_inner_and_empty(); skip_child_row) {
                continue;
            }

            // get slots from every table function
            // Notice that _fn_values[i] may be null if the table function has empty result set.
            for (int i = 0; i < _fn_num; i++) {
                RETURN_IF_ERROR(_fns[i]->get_value(&_fn_values[i]));
            }

            // allocate memory for row batch for the first time
            if (tuple_buffer == nullptr) {
                int64_t tuple_buffer_size;
                RETURN_IF_ERROR(row_batch->resize_and_allocate_tuple_buffer(
                        state, &tuple_buffer_size, &tuple_buffer));
                tuple_ptr = reinterpret_cast<Tuple*>(tuple_buffer);
            }

            pre_tuple_ptr = tuple_ptr;
            // The tuples order in parent row batch should be
            //      child1, child2, tf1, tf2, ...
            TupleRow* parent_tuple_row = row_batch->get_row(row_batch->add_row());
            // 1. copy child tuples
            int tuple_idx = 0;
            for (int i = 0; i < _child_tuple_desc_size; tuple_idx++, i++) {
                TupleDescriptor* child_tuple_desc = child_rowdesc.tuple_descriptors()[tuple_idx];
                TupleDescriptor* parent_tuple_desc = parent_rowdesc.tuple_descriptors()[tuple_idx];

                auto tuple_idx = child_rowdesc.get_tuple_idx(child_tuple_desc->id());
                RETURN_IF_INVALID_TUPLE_IDX(child_tuple_desc->id(), tuple_idx);
                Tuple* child_tuple = _cur_child_tuple_row->get_tuple(tuple_idx);

                // The child tuple is nullptr, only when the child tuple is from outer join. so we directly set
                // parent_tuple have same tuple_idx nullptr to mock the behavior
                if (child_tuple != nullptr) {
                    // copy the child tuple to parent_tuple
                    memcpy(tuple_ptr, child_tuple, parent_tuple_desc->byte_size());
                    // only deep copy the child slot if it is selected and is var len (Eg: string, bitmap, hll)
                    for (int j = 0; j < _child_slot_sizes[i]; ++j) {
                        SlotDescriptor* child_slot_desc = child_tuple_desc->slots()[j];
                        SlotDescriptor* parent_slot_desc = parent_tuple_desc->slots()[j];

                        if (child_tuple->is_null(child_slot_desc->null_indicator_offset())) {
                            continue;
                        }
                        if (child_slot_desc->type().is_string_type()) {
                            void* dest_slot = tuple_ptr->get_slot(parent_slot_desc->tuple_offset());
                            if (_output_slot_ids[parent_slot_desc->id()]) {
                                // deep coopy
                                RawValue::write(
                                        child_tuple->get_slot(child_slot_desc->tuple_offset()),
                                        dest_slot, parent_slot_desc->type(),
                                        row_batch->tuple_data_pool());
                            } else {
                                // clear for unused slot
                                StringValue* dest = reinterpret_cast<StringValue*>(dest_slot);
                                dest->replace(nullptr, 0);
                            }
                        }
                    }
                    parent_tuple_row->set_tuple(tuple_idx, tuple_ptr);
                } else {
                    parent_tuple_row->set_tuple(tuple_idx, nullptr);
                }
                tuple_ptr = reinterpret_cast<Tuple*>(reinterpret_cast<uint8_t*>(tuple_ptr) +
                                                     parent_tuple_desc->byte_size());
            }

            // 2. copy function result
            for (int i = 0; tuple_idx < _parent_tuple_desc_size; tuple_idx++, i++) {
                TupleDescriptor* parent_tuple_desc = parent_rowdesc.tuple_descriptors()[tuple_idx];
                SlotDescriptor* parent_slot_desc = parent_tuple_desc->slots()[0];
                void* dest_slot = tuple_ptr->get_slot(parent_slot_desc->tuple_offset());
                if (_fn_values[i] != nullptr) {
                    RawValue::write(_fn_values[i], dest_slot, parent_slot_desc->type(),
                                    row_batch->tuple_data_pool());
                    tuple_ptr->set_not_null(parent_slot_desc->null_indicator_offset());
                } else {
                    tuple_ptr->set_null(parent_slot_desc->null_indicator_offset());
                }
                parent_tuple_row->set_tuple(tuple_idx, tuple_ptr);
                tuple_ptr = reinterpret_cast<Tuple*>(reinterpret_cast<uint8_t*>(tuple_ptr) +
                                                     parent_tuple_desc->byte_size());
            }

            // 3. eval conjuncts
            if (eval_conjuncts(&_conjunct_ctxs[0], _conjunct_ctxs.size(), parent_tuple_row)) {
                row_batch->commit_last_row();
                ++_num_rows_returned;
            } else {
                tuple_ptr = pre_tuple_ptr;
                ++_num_rows_filtered;
            }

            // Forward after write success.
            // Because data in `_fn_values` points to the data saved in functions.
            // And `forward` will change the data in functions.
            bool tmp = false;
            _fns[_fn_num - 1]->forward(&tmp);

            if (row_batch->at_capacity()) {
                break;
            }
        } // end while true

        if (row_batch->at_capacity()) {
            break;
        }
    } // end while cur_eos

    if (reached_limit()) {
        int num_rows_over = _num_rows_returned - _limit;
        row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
        _num_rows_returned -= num_rows_over;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        *eos = true;
    } else {
        *eos = row_batch->num_rows() == 0;
    }

    return Status::OK();
}

Status TableFunctionNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "TableFunctionNode::close");
    Expr::close(_fn_ctxs, state);
    vectorized::VExpr::close(_vfn_ctxs, state);

    if (_num_rows_filtered_counter != nullptr) {
        COUNTER_SET(_num_rows_filtered_counter, static_cast<int64_t>(_num_rows_filtered));
    }

    return ExecNode::close(state);
}

}; // namespace doris
