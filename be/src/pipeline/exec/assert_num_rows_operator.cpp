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

namespace doris::pipeline {

OperatorPtr AssertNumRowsOperatorBuilder::build_operator() {
    return std::make_shared<AssertNumRowsOperator>(this, _node);
}

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
}

Status AssertNumRowsOperatorX::pull(doris::RuntimeState* state, vectorized::Block* block,
                                    SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    local_state.add_num_rows_returned(block->rows());
    int64_t num_rows_returned = local_state.num_rows_returned();
    bool assert_res = false;
    switch (_assertion) {
    case TAssertion::EQ:
        assert_res = num_rows_returned == _desired_num_rows;
        break;
    case TAssertion::NE:
        assert_res = num_rows_returned != _desired_num_rows;
        break;
    case TAssertion::LT:
        assert_res = num_rows_returned < _desired_num_rows;
        break;
    case TAssertion::LE:
        assert_res = num_rows_returned <= _desired_num_rows;
        break;
    case TAssertion::GT:
        assert_res = num_rows_returned > _desired_num_rows;
        break;
    case TAssertion::GE:
        assert_res = num_rows_returned >= _desired_num_rows;
        break;
    default:
        break;
    }

    if (!assert_res) {
        auto to_string_lambda = [](TAssertion::type assertion) {
            std::map<int, const char*>::const_iterator it =
                    _TAssertion_VALUES_TO_NAMES.find(assertion);

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
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    return Status::OK();
}

} // namespace doris::pipeline
