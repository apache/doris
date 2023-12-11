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

#include "table_function_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/core/block.h"
#include "vec/exprs/table_function/table_function_factory.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(TableFunctionOperator, StatefulOperator)

Status TableFunctionOperator::prepare(doris::RuntimeState* state) {
    // just for speed up, the way is dangerous
    _child_block = _node->get_child_block();
    return StatefulOperator::prepare(state);
}

Status TableFunctionOperator::close(doris::RuntimeState* state) {
    return StatefulOperator::close(state);
}

TableFunctionLocalState::TableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<>(state, parent), _child_block(vectorized::Block::create_unique()) {}

Status TableFunctionLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    auto& p = _parent->cast<TableFunctionOperatorX>();
    _vfn_ctxs.resize(p._vfn_ctxs.size());
    for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));

        const std::string& tf_name = _vfn_ctxs[i]->root()->fn().name.function_name;
        vectorized::TableFunction* fn = nullptr;
        RETURN_IF_ERROR(vectorized::TableFunctionFactory::get_fn(tf_name, state->obj_pool(), &fn));
        fn->set_expr_context(_vfn_ctxs[i]);
        _fns.push_back(fn);
    }

    _cur_child_offset = -1;
    return Status::OK();
}

void TableFunctionLocalState::_copy_output_slots(
        std::vector<vectorized::MutableColumnPtr>& columns) {
    if (!_current_row_insert_times) {
        return;
    }
    auto& p = _parent->cast<TableFunctionOperatorX>();
    for (auto index : p._output_slot_indexs) {
        auto src_column = _child_block->get_by_position(index).column;
        columns[index]->insert_many_from(*src_column, _cur_child_offset, _current_row_insert_times);
    }
    _current_row_insert_times = 0;
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
int TableFunctionLocalState::_find_last_fn_eos_idx() const {
    for (int i = _parent->cast<TableFunctionOperatorX>()._fn_num - 1; i >= 0; --i) {
        if (!_fns[i]->eos()) {
            if (i == _parent->cast<TableFunctionOperatorX>()._fn_num - 1) {
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
bool TableFunctionLocalState::_roll_table_functions(int last_eos_idx) {
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

    for (int j = i + 1; j < _parent->cast<TableFunctionOperatorX>()._fn_num; ++j) {
        _fns[j]->reset();
    }

    return true;
}

bool TableFunctionLocalState::_is_inner_and_empty() {
    for (int i = 0; i < _parent->cast<TableFunctionOperatorX>()._fn_num; i++) {
        // if any table function is not outer and has empty result, go to next child row
        if (!_fns[i]->is_outer() && _fns[i]->current_empty()) {
            return true;
        }
    }
    return false;
}

Status TableFunctionLocalState::get_expanded_block(RuntimeState* state,
                                                   vectorized::Block* output_block,
                                                   SourceState& source_state) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    vectorized::MutableBlock m_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
            output_block, p._output_slots);
    vectorized::MutableColumns& columns = m_block.mutable_columns();

    for (int i = 0; i < p._fn_num; i++) {
        if (columns[i + p._child_slots.size()]->is_nullable()) {
            _fns[i]->set_nullable();
        }
    }

    while (columns[p._child_slots.size()]->size() < state->batch_size()) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("VTableFunctionNode, while getting next batch."));

        if (_child_block->rows() == 0) {
            break;
        }

        bool skip_child_row = false;
        while (columns[p._child_slots.size()]->size() < state->batch_size()) {
            int idx = _find_last_fn_eos_idx();
            if (idx == 0 || skip_child_row) {
                _copy_output_slots(columns);
                // all table functions' results are exhausted, process next child row.
                process_next_child_row();
                if (_cur_child_offset == -1) {
                    break;
                }
            } else if (idx < p._fn_num && idx != -1) {
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
            if (p._fn_num == 1) {
                _current_row_insert_times += _fns[0]->get_value(
                        columns[p._child_slots.size()],
                        state->batch_size() - columns[p._child_slots.size()]->size());
            } else {
                for (int i = 0; i < p._fn_num; i++) {
                    _fns[i]->get_value(columns[i + p._child_slots.size()]);
                }
                _current_row_insert_times++;
                _fns[p._fn_num - 1]->forward();
            }
        }
    }

    _copy_output_slots(columns);

    size_t row_size = columns[p._child_slots.size()]->size();
    for (auto index : p._useless_slot_indexs) {
        columns[index]->insert_many_defaults(row_size - columns[index]->size());
    }

    // 3. eval conjuncts
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                           output_block->columns()));

    if (_child_source_state == SourceState::FINISHED && _cur_child_offset == -1) {
        source_state = SourceState::FINISHED;
    }
    return Status::OK();
}

void TableFunctionLocalState::process_next_child_row() {
    _cur_child_offset++;

    if (_cur_child_offset >= _child_block->rows()) {
        // release block use count.
        for (vectorized::TableFunction* fn : _fns) {
            fn->process_close();
        }

        _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                ._child_x->row_desc()
                                                .num_materialized_slots());
        _cur_child_offset = -1;
        return;
    }

    for (vectorized::TableFunction* fn : _fns) {
        fn->process_row(_cur_child_offset);
    }
}

TableFunctionOperatorX::TableFunctionOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {}

Status TableFunctionOperatorX::_prepare_output_slot_ids(const TPlanNode& tnode) {
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

Status TableFunctionOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));

    for (const TExpr& texpr : tnode.table_function_node.fnCallExprList) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(texpr, ctx));
        _vfn_ctxs.push_back(ctx);

        auto root = ctx->root();
        const std::string& tf_name = root->fn().name.function_name;
        vectorized::TableFunction* fn = nullptr;
        RETURN_IF_ERROR(vectorized::TableFunctionFactory::get_fn(tf_name, _pool, &fn));
        fn->set_expr_context(ctx);
        _fns.push_back(fn);
    }
    _fn_num = _fns.size();

    // Prepare output slot ids
    RETURN_IF_ERROR(_prepare_output_slot_ids(tnode));
    return Status::OK();
}

Status TableFunctionOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));

    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->prepare());
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_vfn_ctxs, state, _row_descriptor));

    // get current all output slots
    for (const auto& tuple_desc : _row_descriptor.tuple_descriptors()) {
        for (const auto& slot_desc : tuple_desc->slots()) {
            _output_slots.push_back(slot_desc);
        }
    }

    // get all input slots
    for (const auto& child_tuple_desc : _child_x->row_desc().tuple_descriptors()) {
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

    return Status::OK();
}

Status TableFunctionOperatorX::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    return vectorized::VExpr::open(_vfn_ctxs, state);
}

} // namespace doris::pipeline
