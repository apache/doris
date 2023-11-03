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

#include "vec/exec/vdata_gen_scan_node.h"

#include <gen_cpp/PlanNodes_types.h>

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "exprs/runtime_filter.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/data_gen_functions/vdata_gen_function_inf.h"
#include "vec/exec/data_gen_functions/vnumbers_tvf.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class TScanRangeParams;
} // namespace doris

namespace doris::vectorized {

VDataGenFunctionScanNode::VDataGenFunctionScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _tuple_id(tnode.data_gen_scan_node.tuple_id),
          _tuple_desc(nullptr),
          _runtime_filter_descs(tnode.runtime_filters) {
    // set _table_func here
    switch (tnode.data_gen_scan_node.func_name) {
    case TDataGenFunctionName::NUMBERS:
        _table_func = std::make_shared<VNumbersTVF>(_tuple_id, _tuple_desc);
        break;
    default:
        LOG(FATAL) << "Unsupported function type";
    }
}

Status VDataGenFunctionScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VDataGenFunctionScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    _table_func->set_tuple_desc(_tuple_desc);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    // TODO: use runtime filter to filte result block, maybe this node need derive from vscan_node.
    for (const auto& filter_desc : _runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        if (filter_desc.__isset.opt_remote_rf && filter_desc.opt_remote_rf) {
            RETURN_IF_ERROR(state->get_query_ctx()->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, state->query_options(), id(), false));
            RETURN_IF_ERROR(state->get_query_ctx()->runtime_filter_mgr()->get_consume_filter(
                    filter_desc.filter_id, id(), &runtime_filter));
        } else {
            RETURN_IF_ERROR(state->runtime_filter_mgr()->register_consumer_filter(
                    filter_desc, state->query_options(), id(), false));
            RETURN_IF_ERROR(state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                            id(), &runtime_filter));
        }
        runtime_filter->init_profile(_runtime_profile.get());
    }

    _is_init = true;
    return Status::OK();
}

Status VDataGenFunctionScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return Status::OK();
}

Status VDataGenFunctionScanNode::get_next(RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
    if (state == nullptr || block == nullptr || eos == nullptr) {
        return Status::InternalError("input is NULL pointer");
    }
    RETURN_IF_CANCELLED(state);
    Status res = _table_func->get_next(state, block, eos);
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
    reached_limit(block, eos);
    return res;
}

Status VDataGenFunctionScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _table_func->close(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return ExecNode::close(state);
}

Status VDataGenFunctionScanNode::set_scan_ranges(RuntimeState* state,
                                                 const std::vector<TScanRangeParams>& scan_ranges) {
    return _table_func->set_scan_ranges(scan_ranges);
}

} // namespace doris::vectorized
