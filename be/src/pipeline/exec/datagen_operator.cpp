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

#include "datagen_operator.h"

#include <memory>

#include "exprs/runtime_filter.h"
#include "pipeline/common/data_gen_functions/vdata_gen_function_inf.h"
#include "pipeline/common/data_gen_functions/vnumbers_tvf.h"
#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

DataGenSourceOperatorX::DataGenSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : OperatorX<DataGenLocalState>(pool, tnode, operator_id, descs),
          _tuple_id(tnode.data_gen_scan_node.tuple_id),
          _tuple_desc(nullptr),
          _runtime_filter_descs(tnode.runtime_filters) {}

Status DataGenSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<DataGenLocalState>::init(tnode, state));
    // set _table_func here
    switch (tnode.data_gen_scan_node.func_name) {
    case TDataGenFunctionName::NUMBERS:
        break;
    default:
        return Status::InternalError("Unsupported function type");
    }
    return Status::OK();
}

Status DataGenSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<DataGenLocalState>::prepare(state));
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }
    return Status::OK();
}

Status DataGenSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    if (state == nullptr || block == nullptr) {
        return Status::InternalError("input is NULL pointer");
    }
    RETURN_IF_CANCELLED(state);
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    Status res = local_state._table_func->get_next(state, block, eos);
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                           block->columns()));
    local_state.reached_limit(block, eos);
    return res;
}

Status DataGenLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    auto& p = _parent->cast<DataGenSourceOperatorX>();
    _table_func = std::make_shared<VNumbersTVF>(p._tuple_id, p._tuple_desc);
    _table_func->set_tuple_desc(p._tuple_desc);
    RETURN_IF_ERROR(_table_func->set_scan_ranges(info.scan_ranges));

    // TODO: use runtime filter to filte result block, maybe this node need derive from vscan_node.
    for (const auto& filter_desc : p._runtime_filter_descs) {
        std::shared_ptr<IRuntimeFilter> runtime_filter;
        RETURN_IF_ERROR(state->register_consumer_runtime_filter(
                filter_desc, p.ignore_data_distribution(), p.node_id(), &runtime_filter));
        runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status DataGenLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_table_func->close(state));
    return PipelineXLocalState<>::close(state);
}

} // namespace doris::pipeline
