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

#include "exec/assert_num_rows_node.h"

#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

AssertNumRowsNode::AssertNumRowsNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) :
        ExecNode(pool, tnode, descs),
        _desired_num_rows(tnode.assert_num_rows_node.desired_num_rows),
        _subquery_string(tnode.assert_num_rows_node.subquery_string) {
}

Status AssertNumRowsNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

Status AssertNumRowsNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    return Status::OK();
}

Status AssertNumRowsNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    return Status::OK();
}

Status AssertNumRowsNode::get_next(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    child(0)->get_next(state, output_batch, eos);
    int num_rows_returned_before = _num_rows_returned;
    _num_rows_returned += output_batch->num_rows();
    if (_num_rows_returned > _desired_num_rows) {
        _num_rows_returned = num_rows_returned_before;
        LOG(INFO) << "Expected no more than " << _desired_num_rows << " to be returned by expression "
                  << _subquery_string;
        std::stringstream ss;
        ss << "Expected no more than " << _desired_num_rows << " to be returned by expression "
                  << _subquery_string;
        return Status::Cancelled(ss.str());
    }
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status AssertNumRowsNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

}