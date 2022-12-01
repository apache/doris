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

#include "vec/sink/vtable_sink.h"

#include <sstream>

#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {

VTableSink::VTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                       const std::vector<TExpr>& t_exprs)
        : _pool(pool), _row_desc(row_desc), _t_output_expr(t_exprs) {}

Status VTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, _t_output_expr, &_output_vexpr_ctxs));
    return Status::OK();
}

Status VTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    std::stringstream title;
    title << _name << " (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status VTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VTableSink::open");
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::open(_output_vexpr_ctxs, state));
    return Status::OK();
}

Status VTableSink::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported(
            "Not Implemented VTableSink::send(RuntimeState* state, RowBatch* batch)");
}

Status VTableSink::send(RuntimeState* state, Block* block, bool eos) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VTableSink::send");
    return Status::OK();
}
Status VTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    VExpr::close(_output_vexpr_ctxs, state);
    return Status::OK();
}
} // namespace vectorized
} // namespace doris