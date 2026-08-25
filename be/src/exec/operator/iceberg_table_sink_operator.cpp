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

#include "exec/operator/iceberg_table_sink_operator.h"

#include "common/status.h"

namespace doris {
#include "common/compile_check_begin.h"
Status IcebergTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _writer = std::make_unique<VIcebergTableWriter>(info.tsink, _output_vexpr_ctxs);
    auto& parent = _parent->cast<Parent>();
    RETURN_IF_ERROR(_writer->init_properties(parent._pool, parent._row_desc));
    return Status::OK();
}

Status IcebergTableSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    auto& parent = _parent->cast<Parent>();
    _output_vexpr_ctxs.resize(parent._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(parent._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    return _writer->open(state, operator_profile());
}

Status IcebergTableSinkLocalState::sink(RuntimeState* state, Block* block, bool /*eos*/) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    DCHECK(_writer);
    return _writer->write(state, *block);
}

Status IcebergTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    DCHECK(_writer);
    Status final_status = exec_status;
    if (final_status.ok() && state->is_cancelled()) {
        final_status = state->cancel_reason();
    }
    Status writer_status = _writer->close(final_status);
    if (final_status.ok() && !writer_status.ok()) {
        final_status = writer_status;
    }
    _writer.reset();

    Status base_status = Base::close(state, final_status);
    if (final_status.ok() && !base_status.ok()) {
        final_status = base_status;
    }
    return final_status;
}

} // namespace doris
