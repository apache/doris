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

#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"
#include "vec/exec/data_gen_functions/vdata_gen_function_inf.h"
#include "vec/exec/data_gen_functions/vnumbers_tvf.h"
#include "vec/exec/vdata_gen_scan_node.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(DataGenOperator, SourceOperator)

Status DataGenOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::open(state));
    return _node->open(state);
}

Status DataGenOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::close(state));
    static_cast<void>(_node->close(state));
    return Status::OK();
}

DataGenSourceOperatorX::DataGenSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               const DescriptorTbl& descs)
        : OperatorX<DataGenLocalState>(pool, tnode, descs),
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

Status DataGenSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                         SourceState& source_state) {
    if (state == nullptr || block == nullptr) {
        return Status::InternalError("input is NULL pointer");
    }
    RETURN_IF_CANCELLED(state);
    CREATE_LOCAL_STATE_RETURN_STATUS_IF_ERROR(local_state);
    bool eos = false;
    Status res = local_state._table_func->get_next(state, block, &eos);
    source_state = eos ? SourceState::FINISHED : source_state;
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(local_state._conjuncts, block,
                                                           block->columns()));
    local_state.reached_limit(block, source_state);
    return res;
}

Status DataGenLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    auto& p = _parent->cast<DataGenSourceOperatorX>();
    _table_func = std::make_shared<vectorized::VNumbersTVF>(p._tuple_id, p._tuple_desc);
    _table_func->set_tuple_desc(p._tuple_desc);
    RETURN_IF_ERROR(_table_func->set_scan_ranges(info.scan_ranges));

    // TODO: use runtime filter to filte result block, maybe this node need derive from vscan_node.
    for (const auto& filter_desc : p._runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        if (filter_desc.__isset.opt_remote_rf && filter_desc.opt_remote_rf) {
            RETURN_IF_ERROR(state->get_query_ctx()->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, state->query_options(), p.id(), false));
            RETURN_IF_ERROR(state->get_query_ctx()->runtime_filter_mgr()->get_consume_filter(
                    filter_desc.filter_id, p.id(), &runtime_filter));
        } else {
            RETURN_IF_ERROR(state->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, state->query_options(), p.id(), false));
            RETURN_IF_ERROR(state->runtime_filter_mgr()->get_consume_filter(
                    filter_desc.filter_id, p.id(), &runtime_filter));
        }
        runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status DataGenLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    static_cast<void>(_table_func->close(state));
    return PipelineXLocalState<>::close(state);
}

} // namespace doris::pipeline
