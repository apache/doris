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

#include "vec/exec/vassert_num_rows_node.h"

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

VAssertNumRowsNode::VAssertNumRowsNode(ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _desired_num_rows(tnode.assert_num_rows_node.desired_num_rows),
          _subquery_string(tnode.assert_num_rows_node.subquery_string) {
    if (tnode.assert_num_rows_node.__isset.assertion) {
        _assertion = tnode.assert_num_rows_node.assertion;
    } else {
        _assertion = TAssertion::LE; // just compatible for the previous code
    }
}

Status VAssertNumRowsNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    // ISSUE-3435
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status VAssertNumRowsNode::pull(doris::RuntimeState* state, vectorized::Block* block, bool* eos) {
    _num_rows_returned += block->rows();
    bool assert_res = false;
    switch (_assertion) {
    case TAssertion::EQ:
        assert_res = _num_rows_returned == _desired_num_rows;
        break;
    case TAssertion::NE:
        assert_res = _num_rows_returned != _desired_num_rows;
        break;
    case TAssertion::LT:
        assert_res = _num_rows_returned < _desired_num_rows;
        break;
    case TAssertion::LE:
        assert_res = _num_rows_returned <= _desired_num_rows;
        break;
    case TAssertion::GT:
        assert_res = _num_rows_returned > _desired_num_rows;
        break;
    case TAssertion::GE:
        assert_res = _num_rows_returned >= _desired_num_rows;
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
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
    return Status::OK();
}

Status VAssertNumRowsNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(child(0)->get_next_after_projects(
            state, block, eos,
            std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                              ExecNode::get_next,
                      _children[0], std::placeholders::_1, std::placeholders::_2,
                      std::placeholders::_3)));

    return pull(state, block, eos);
}

} // namespace doris::vectorized
