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

#include <limits>
#include <memory>

#include "pipeline/exec/operator.h"
#include "util/simd/bits.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/exprs/table_function/table_function_factory.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

TableFunctionLocalState::TableFunctionLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<>(state, parent), _child_block(vectorized::Block::create_unique()) {}

Status TableFunctionLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _init_function_timer = ADD_TIMER(custom_profile(), "InitTableFunctionTime");
    _process_rows_timer = ADD_TIMER(custom_profile(), "ProcessRowsTime");
    _filter_timer = ADD_TIMER(custom_profile(), "FilterTime");
    return Status::OK();
}

Status TableFunctionLocalState::_clone_table_function(RuntimeState* state) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    _vfn_ctxs.resize(p._vfn_ctxs.size());
    for (size_t i = 0; i < _vfn_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._vfn_ctxs[i]->clone(state, _vfn_ctxs[i]));

        vectorized::TableFunction* fn = nullptr;
        RETURN_IF_ERROR(vectorized::TableFunctionFactory::get_fn(
                _vfn_ctxs[i]->root()->fn(), state->obj_pool(), &fn, state->be_exec_version()));
        fn->set_expr_context(_vfn_ctxs[i]);
        _fns.push_back(fn);
    }
    _expand_conjuncts_ctxs.resize(p._expand_conjuncts_ctxs.size());
    for (size_t i = 0; i < _expand_conjuncts_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._expand_conjuncts_ctxs[i]->clone(state, _expand_conjuncts_ctxs[i]));
    }
    _need_to_handle_outer_conjuncts = (!_expand_conjuncts_ctxs.empty() && _fns[0]->is_outer());
    return Status::OK();
}

Status TableFunctionLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(PipelineXLocalState<>::exec_time_counter());
    SCOPED_TIMER(PipelineXLocalState<>::_open_timer);
    RETURN_IF_ERROR(PipelineXLocalState<>::open(state));
    RETURN_IF_ERROR(_clone_table_function(state));
    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->open());
    }
    _cur_child_offset = -1;
    return Status::OK();
}

void TableFunctionLocalState::_copy_output_slots(
        std::vector<vectorized::MutableColumnPtr>& columns, const TableFunctionOperatorX& p,
        size_t output_row_count, std::vector<int64_t>& child_row_to_output_rows_indices,
        std::vector<size_t>& handled_row_indices) {
    if (!_current_row_insert_times) {
        return;
    }
    for (auto index : p._output_slot_indexs) {
        auto src_column = _child_block->get_by_position(index).column;
        columns[index]->insert_many_from(*src_column, _cur_child_offset, _current_row_insert_times);
    }
    _current_row_insert_times = 0;

    if (_need_to_handle_outer_conjuncts) {
        handled_row_indices.push_back(_cur_child_offset);
        child_row_to_output_rows_indices.push_back(output_row_count);
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
        // if it's outer function, will be insert into one row NULL
        if (!_fns[i]->is_outer() && _fns[i]->current_empty()) {
            return true;
        }
    }
    return false;
}

Status TableFunctionLocalState::get_expanded_block(RuntimeState* state,
                                                   vectorized::Block* output_block, bool* eos) {
    auto& p = _parent->cast<TableFunctionOperatorX>();
    vectorized::MutableBlock m_block = vectorized::VectorizedUtils::build_mutable_mem_reuse_block(
            output_block, p._output_slots);
    vectorized::MutableColumns& columns = m_block.mutable_columns();
    auto child_slot_count = p._child_slots.size();
    for (int i = 0; i < p._fn_num; i++) {
        if (columns[i + child_slot_count]->is_nullable()) {
            _fns[i]->set_nullable();
        }
    }
    std::vector<int64_t> child_row_to_output_rows_indices;
    std::vector<size_t> handled_row_indices;
    bool child_block_empty = _child_block->empty();
    if (_need_to_handle_outer_conjuncts && !child_block_empty) {
        child_row_to_output_rows_indices.push_back(0);
    }
    SCOPED_TIMER(_process_rows_timer);
    auto batch_size = state->batch_size();
    int32_t max_batch_size = std::numeric_limits<int32_t>::max();
    while (columns[child_slot_count]->size() < batch_size) {
        RETURN_IF_CANCELLED(state);

        // finished handling current child block
        if (_child_block->rows() == 0 || _cur_child_offset == -1) {
            break;
        }

        bool skip_child_row = false;
        size_t cur_row_count = 0;
        while (cur_row_count = columns[child_slot_count]->size(), cur_row_count < batch_size) {
            int idx = _find_last_fn_eos_idx();
            if (idx == 0 || skip_child_row) {
                _copy_output_slots(columns, p, cur_row_count, child_row_to_output_rows_indices,
                                   handled_row_indices);
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

            DCHECK_LE(1, p._fn_num);
            // It may take multiple iterations of this while loop to process a child row if
            // any table function produces a large number of rows.
            // But for table function with outer conjuncts, we output all rows in one iteration
            // even if it may exceed batch_size, because we need to make sure at least one row
            // is output for the child row after evaluating the outer conjuncts.
            auto repeat_times = _fns[p._fn_num - 1]->get_value(
                    columns[child_slot_count + p._fn_num - 1],
                    //// It has already been checked that
                    // columns[p._child_slots.size()]->size() < state->batch_size(),
                    // so columns[p._child_slots.size()]->size() will not exceed the range of int.
                    _need_to_handle_outer_conjuncts
                            ? max_batch_size
                            : batch_size - (int)columns[child_slot_count]->size());
            _current_row_insert_times += repeat_times;
            for (int i = 0; i < p._fn_num - 1; i++) {
                LOG(INFO) << "xxxx Query: " << print_id(state->query_id())
                          << " get_same_many_values for fn index: " << i;
                _fns[i]->get_same_many_values(columns[i + child_slot_count], repeat_times);
            }
        }
    }

    _copy_output_slots(columns, p, columns[child_slot_count]->size(),
                       child_row_to_output_rows_indices, handled_row_indices);

    size_t row_size = columns[child_slot_count]->size();
    for (auto index : p._useless_slot_indexs) {
        columns[index]->insert_many_defaults(row_size - columns[index]->size());
    }
    // LOG(INFO) << "xxxx before filter output_block data: "
    //           << output_block->dump_data(0, output_block->rows());

    /**
    Handle the outer conjuncts after unnest. Currently, only left outer is supported.
    e.g., for the following example data,
    select id, name, tags from items_dict_unnest_t order by id;
    +------+---------------------+-------------------------------------------------+
    | id   | name                | tags                                            |
    +------+---------------------+-------------------------------------------------+
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 |
    +------+---------------------+-------------------------------------------------+
    
    for this query: ``` SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;```
    
    after unnest, before evaluating the outer conjuncts, the result is:
    +------+---------------------+-------------------------------------------------+--------------+
    | id   | name                | tags                                            | unnest(tags) |
    +------+---------------------+-------------------------------------------------+--------------+
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Electronics  |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Office       |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | High-End     |
    |    1 | Laptop              | ["Electronics", "Office", "High-End", "Laptop"] | Laptop       |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  | Electronics  |
    |    2 | Mechanical Keyboard | ["Electronics", "Accessories"]                  | Accessories  |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           | Sports       |
    |    3 | Basketball          | ["Sports", "Outdoor"]                           | Outdoor      |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         | Sports       |
    |    4 | Badminton Racket    | ["Sports", "Equipment"]                         | Equipment    |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Clothing     |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Office       |
    |    5 | Shirt               | ["Clothing", "Office", "Shirt"]                 | Shirt        |
    +------+---------------------+-------------------------------------------------+--------------+
    13 rows in set (0.47 sec)
    
    the vector child_row_to_output_rows_indices is used to record the mapping relationship,
    between child row and output rows, for example:
    child row 0 -> output rows [0, 4)
    child row 1 -> output rows [4, 6)
    child row 2 -> output rows [6, 8)
    child row 3 -> output rows [8, 10)
    child row 4 -> output rows [10, 13)
    it's contents are: [0, 4, 6, 8, 10, 13].
    
    After evaluating the left join conjuncts `t.tag = name`,
    the content of filter is: [0, 0, 0, 1, // child row 0
                               0, 0,       // child row 1
                               0, 0,       // child row 2
                               0, 0,       // child row 3
                               0, 0, 1     // child row 4
                               ]
    child rows 1, 2, 3 are all filtered out, so we need to insert one row with NULL tag value for each of them.
    */
    if (!_expand_conjuncts_ctxs.empty() && !child_block_empty) {
        auto output_row_count = output_block->rows();
        vectorized::IColumn::Filter filter;
        auto column_count = output_block->columns();
        vectorized::ColumnNumbers columns_to_filter(column_count);
        std::iota(columns_to_filter.begin(), columns_to_filter.end(), 0);
        RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts_and_filter_block(
                _expand_conjuncts_ctxs, output_block, columns_to_filter, column_count, filter));
        size_t remain_row_count = output_block->rows();
        /*
        LOG(INFO) << "xxxx after filter output_block row count: " << remain_row_count;
        std::string debug_string("xxxx handled child rows: ");
        for (auto v : handled_row_indices) {
            debug_string += std::to_string(v) + ",";
        }
        debug_string += ", filter: ";
        for (auto v : filter) {
            debug_string += std::to_string(v) + ",";
        }
        debug_string += ", child_row_to_output_rows_indices: ";
        for (auto v : child_row_to_output_rows_indices) {
            debug_string += ", " + std::to_string(v);
        }
        LOG(INFO) << debug_string;
        */
        // for outer table function, need to handle those child rows which all expanded rows are filtered out
        if (_need_to_handle_outer_conjuncts && remain_row_count < output_row_count) {
            auto handled_child_row_count = handled_row_indices.size();
            for (size_t i = 0; i < handled_child_row_count; ++i) {
                auto start_row_idx = child_row_to_output_rows_indices[i];
                auto end_row_idx = child_row_to_output_rows_indices[i + 1];
                if (!simd::contain_byte((uint8_t*)filter.data() + start_row_idx,
                                        end_row_idx - start_row_idx, 1)) {
                    for (auto index : p._output_slot_indexs) {
                        auto src_column = _child_block->get_by_position(index).column;
                        columns[index]->insert_from(*src_column, handled_row_indices[i]);
                    }
                    for (auto index : p._useless_slot_indexs) {
                        columns[index]->insert_default();
                    }
                    for (int j = 0; j != p._fn_num; j++) {
                        columns[j + child_slot_count]->insert_default();
                    }
                }
            }
        }
        if (_cur_child_offset == -1) {
            _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                    ._child->row_desc()
                                                    .num_materialized_slots());
        }
    }

    {
        SCOPED_TIMER(_filter_timer); // 3. eval conjuncts
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                               output_block->columns()));
    }

    *eos = _child_eos && _cur_child_offset == -1;
    // LOG(INFO) << "xxxx output_block rows: " << output_block->rows();
    return Status::OK();
}

void TableFunctionLocalState::process_next_child_row() {
    _cur_child_offset++;

    if (_cur_child_offset >= _child_block->rows()) {
        // release block use count.
        for (vectorized::TableFunction* fn : _fns) {
            fn->process_close();
        }

        // if there are any _expand_conjuncts_ctxs, don't clear child block here,
        // because we still need _child_block to output NULL rows for outer table function
        if (_expand_conjuncts_ctxs.empty()) {
            _child_block->clear_column_data(_parent->cast<TableFunctionOperatorX>()
                                                    ._child->row_desc()
                                                    .num_materialized_slots());
        }
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
        vectorized::TableFunction* fn = nullptr;
        RETURN_IF_ERROR(vectorized::TableFunctionFactory::get_fn(root->fn(), _pool, &fn,
                                                                 state->be_exec_version()));
        fn->set_expr_context(ctx);
        _fns.push_back(fn);
    }
    _fn_num = cast_set<int>(_fns.size());

    for (const TExpr& texpr : tnode.table_function_node.expand_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(texpr, ctx));
        _expand_conjuncts_ctxs.push_back(ctx);
    }
    if (!_expand_conjuncts_ctxs.empty()) {
        DCHECK(1 == _fn_num) << "Only support one table function when there are expand conjuncts.";
    }

    // Prepare output slot ids
    RETURN_IF_ERROR(_prepare_output_slot_ids(tnode));
    return Status::OK();
}

Status TableFunctionOperatorX::prepare(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (auto* fn : _fns) {
        RETURN_IF_ERROR(fn->prepare());
    }
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_vfn_ctxs, state, row_descriptor()));

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_expand_conjuncts_ctxs, state, row_descriptor()));

    // get current all output slots
    for (const auto& tuple_desc : row_descriptor().tuple_descriptors()) {
        for (const auto& slot_desc : tuple_desc->slots()) {
            _output_slots.push_back(slot_desc);
        }
    }

    // get all input slots
    for (const auto& child_tuple_desc : _child->row_desc().tuple_descriptors()) {
        for (const auto& child_slot_desc : child_tuple_desc->slots()) {
            _child_slots.push_back(child_slot_desc);
        }
    }

    for (int i = 0; i < _child_slots.size(); i++) {
        if (_slot_need_copy(i)) {
            _output_slot_indexs.push_back(i);
        } else {
            _useless_slot_indexs.push_back(i);
        }
    }

    RETURN_IF_ERROR(vectorized::VExpr::open(_expand_conjuncts_ctxs, state));
    return vectorized::VExpr::open(_vfn_ctxs, state);
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
