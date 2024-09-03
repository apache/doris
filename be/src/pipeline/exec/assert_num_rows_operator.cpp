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

#include "assert_num_rows_operator.h"

#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::pipeline {

AssertNumRowsOperatorX::AssertNumRowsOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : StreamingOperatorX<AssertNumRowsLocalState>(pool, tnode, operator_id, descs),
          _desired_num_rows(tnode.assert_num_rows_node.desired_num_rows),
          _subquery_string(tnode.assert_num_rows_node.subquery_string) {
    if (tnode.assert_num_rows_node.__isset.assertion) {
        _assertion = tnode.assert_num_rows_node.assertion;
    } else {
        _assertion = TAssertion::LE; // just compatible for the previous code
    }

    _should_convert_output_to_nullable =
            tnode.assert_num_rows_node.__isset.should_convert_output_to_nullable &&
            tnode.assert_num_rows_node.should_convert_output_to_nullable;
}

Status AssertNumRowsOperatorX::pull(doris::RuntimeState* state, vectorized::Block* block,
                                    bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    local_state.add_num_rows_returned(block->rows());
    int64_t num_rows_returned = local_state.num_rows_returned();
    bool assert_res = false;
    const auto has_more_rows = !(*eos);
    switch (_assertion) {
    case TAssertion::EQ:
        assert_res = num_rows_returned == _desired_num_rows ||
                     (has_more_rows && num_rows_returned < _desired_num_rows);
        break;
    case TAssertion::NE:
        assert_res = num_rows_returned != _desired_num_rows || (has_more_rows);
        break;
    case TAssertion::LT:
        assert_res = num_rows_returned < _desired_num_rows;
        break;
    case TAssertion::LE:
        assert_res = num_rows_returned <= _desired_num_rows;
        break;
    case TAssertion::GT:
        assert_res = num_rows_returned > _desired_num_rows || has_more_rows;
        break;
    case TAssertion::GE:
        assert_res = num_rows_returned >= _desired_num_rows || has_more_rows;
        break;
    default:
        break;
    }

    /**
     * For nereids planner:
     * The output of `AssertNumRowsOperatorX` should be nullable.
     * If the `num_rows_returned` is 0 and `_desired_num_rows` is 1,
     * here need to insert one row of null.
     */
    if (_should_convert_output_to_nullable) {
        if (block->rows() > 0) {
            for (size_t i = 0; i != block->columns(); ++i) {
                auto& data = block->get_by_position(i);
                data.type = vectorized::make_nullable(data.type);
                data.column = vectorized::make_nullable(data.column);
            }
        } else if (!has_more_rows && _assertion == TAssertion::EQ && num_rows_returned == 0 &&
                   _desired_num_rows == 1) {
            auto new_block =
                    vectorized::VectorizedUtils::create_columns_with_type_and_name(_row_descriptor);
            block->swap(new_block);
            for (size_t i = 0; i != block->columns(); ++i) {
                auto& column = block->get_by_position(i).column;
                auto& type = block->get_by_position(i).type;
                type = vectorized::make_nullable(type);
                column = type->create_column();
                column->assume_mutable()->insert_default();
            }
            assert_res = true;
        }
    }

    if (!assert_res) {
        auto to_string_lambda = [](TAssertion::type assertion) {
            auto it = _TAssertion_VALUES_TO_NAMES.find(assertion);

            if (it == _TAggregationOp_VALUES_TO_NAMES.end()) {
                return "NULL";
            } else {
                return it->second;
            }
        };
        LOG(INFO) << "Expected " << to_string_lambda(_assertion) << " " << _desired_num_rows
                  << " to be returned by expression " << _subquery_string;
        return Status::Cancelled("Expected {} {} to be returned by expression {}",
                                 to_string_lambda(_assertion), _desired_num_rows, _subquery_string);
    }
    COUNTER_SET(local_state.rows_returned_counter(), local_state.num_rows_returned());
    COUNTER_UPDATE(local_state.blocks_returned_counter(), 1);
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                           block->columns()));
    return Status::OK();
}

} // namespace doris::pipeline
