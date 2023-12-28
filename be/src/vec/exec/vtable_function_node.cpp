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

#include "vec/exec/vtable_function_node.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/table_function/table_function_factory.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VTableFunctionNode::VTableFunctionNode(doris::ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    _child_block = Block::create_shared();
}

Status VTableFunctionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    for (const TExpr& texpr : tnode.table_function_node.fnCallExprList) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(texpr, ctx));
        _vfn_ctxs.push_back(ctx);

        auto root = ctx->root();
        const std::string& tf_name = root->fn().name.function_name;
        TableFunction* fn = nullptr;
        RETURN_IF_ERROR(TableFunctionFactory::get_fn(tf_name, _pool, &fn));
        fn->set_expr_context(ctx);
        _fns.push_back(fn);
    }
    _fn_num = _fns.size();

    // Prepare output slot ids
    RETURN_IF_ERROR(_prepare_output_slot_ids(tnode));
    return Status::OK();
}

Status VTableFunctionNode::_prepare_output_slot_ids(const TPlanNode& tnode) {
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

bool VTableFunctionNode::_is_inner_and_empty() {
    for (int i = 0; i < _fn_num; i++) {
        // if any table function is not outer and has empty result, go to next child row
        if (!_fns[i]->is_outer() && _fns[i]->current_empty()) {
            return true;
        }
    }
    return false;
}

Status VTableFunctionNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_exec_timer);

    _num_rows_filtered_counter = ADD_COUNTER(_runtime_profile, "RowsFiltered", TUnit::UNIT);
    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->prepare());
    }
    RETURN_IF_ERROR(VExpr::prepare(_vfn_ctxs, state, _row_descriptor));

    // get current all output slots
    for (const auto& tuple_desc : this->_row_descriptor.tuple_descriptors()) {
        for (const auto& slot_desc : tuple_desc->slots()) {
            _output_slots.push_back(slot_desc);
        }
    }

    // get all input slots
    for (const auto& child_tuple_desc : child(0)->row_desc().tuple_descriptors()) {
        for (const auto& child_slot_desc : child_tuple_desc->slots()) {
            _child_slots.push_back(child_slot_desc);
        }
    }

    for (size_t i = 0; i < _child_slots.size(); i++) {
        if (_slot_need_copy(i)) {
            _output_slot_indexs.push_back(i);
        } else {
            _useless_slot_indexs.push_back(i);
        }
    }

    _cur_child_offset = -1;

    return Status::OK();
}

Status VTableFunctionNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_CANCELLED(state);

    // if child_block is empty, get data from child.
    while (need_more_input_data()) {
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, _child_block.get(), &_child_eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, Block*, bool*)) & ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));

        RETURN_IF_ERROR(push(state, _child_block.get(), _child_eos));
    }

    return pull(state, block, eos);
}

Status VTableFunctionNode::_get_expanded_block(RuntimeState* state, Block* output_block,
                                               bool* eos) {
    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, _output_slots);
    MutableColumns& columns = m_block.mutable_columns();
    for (int i = 0; i < _fn_num; i++) {
        if (columns[i + _child_slots.size()]->is_nullable()) {
            _fns[i]->set_nullable();
        }
    }

    while (columns[_child_slots.size()]->size() < state->batch_size()) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("VTableFunctionNode, while getting next batch."));

        if (_child_block->rows() == 0) {
            break;
        }

        bool skip_child_row = false;
        while (columns[_child_slots.size()]->size() < state->batch_size()) {
            int idx = _find_last_fn_eos_idx();
            if (idx == 0 || skip_child_row) {
                _copy_output_slots(columns);
                // all table functions' results are exhausted, process next child row.
                _process_next_child_row();
                if (_cur_child_offset == -1) {
                    break;
                }
            } else if (idx < _fn_num && idx != -1) {
                // some of table functions' results are exhausted.
                if (!_roll_table_functions(idx)) {
                    // continue to process next child row.
                    continue;
                }
            }

            // if any table function is not outer and has empty result, go to next child row
            if (skip_child_row = _is_inner_and_empty(); skip_child_row) {
                continue;
            }
            if (_fn_num == 1) {
                _current_row_insert_times += _fns[0]->get_value(
                        columns[_child_slots.size()],
                        state->batch_size() - columns[_child_slots.size()]->size());
            } else {
                for (int i = 0; i < _fn_num; i++) {
                    _fns[i]->get_value(columns[i + _child_slots.size()]);
                }
                _current_row_insert_times++;
                _fns[_fn_num - 1]->forward();
            }
        }
    }

    _copy_output_slots(columns);

    size_t row_size = columns[_child_slots.size()]->size();
    for (auto index : _useless_slot_indexs) {
        columns[index]->insert_many_defaults(row_size - columns[index]->size());
    }
    output_block->set_columns(std::move(columns));
    // 3. eval conjuncts
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));

    *eos = _child_eos && _cur_child_offset == -1;
    return Status::OK();
}

void VTableFunctionNode::_process_next_child_row() {
    _cur_child_offset++;

    if (_cur_child_offset >= _child_block->rows()) {
        // release block use count.
        for (TableFunction* fn : _fns) {
            fn->process_close();
        }

        release_block_memory(*_child_block);
        _cur_child_offset = -1;
        return;
    }

    for (TableFunction* fn : _fns) {
        fn->process_row(_cur_child_offset);
    }
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
int VTableFunctionNode::_find_last_fn_eos_idx() {
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
bool VTableFunctionNode::_roll_table_functions(int last_eos_idx) {
    int i = last_eos_idx - 1;
    for (; i >= 0; --i) {
        _fns[i]->forward();
        if (!_fns[i]->eos()) {
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

} // namespace doris::vectorized
