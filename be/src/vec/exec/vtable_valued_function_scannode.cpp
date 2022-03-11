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

#include "vec/exec/vnumbers_tbf_scannode.h"

#include <sstream>
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

VTableValuedFunctionScanNode::VTableValuedFunctionScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _tuple_id(tnode.table_valued_func_scan_node.tuple_id),
          _tuple_desc(nullptr) {
    
    // set _table_func here
    switch (tnode.table_valued_func_scan_node.func_name) {
    case TTVFunctionName::NUMBERS:
        _table_func = std::make_shared<VNumbersTBF>(*this);
        break;
    default:
        LOG(FATAL) << "Unsupported function type";
    }
}

VTableValuedFunctionScanNode::~VTableValuedFunctionScanNode() {}

Status VTableValuedFunctionScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VTableValuedFunctionScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _is_init = true;
    return Status::OK();
}

Status VTableValuedFunctionScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return Status::OK();
}

Status VTableValuedFunctionScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    LOG(FATAL) << "VTableValuedFunctionScanNode only support vectorized execution";
    return Status::OK();
}

Status VTableValuedFunctionScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    return _table_func->get_next(state, block, eos);
}

Status VTableValuedFunctionScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    _table_func->close(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return ExecNode::close(state);
}

Status VTableValuedFunctionScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return _table_func->set_scan_ranges(scan_ranges);
}

} // namespace doris
