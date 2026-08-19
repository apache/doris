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

#include "exec/operator/paimon_table_sink_operator.h"

#include "common/logging.h"

namespace doris {

Status PaimonTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& parent = _parent->cast<Parent>();
    DCHECK(parent._memory_allocator != nullptr);
    std::unique_ptr<PaimonWriterMemoryLease> memory_lease;
    RETURN_IF_ERROR(parent._memory_allocator->create_lease(&memory_lease));
    _writer = std::make_unique<PaimonTableWriter>(info.tsink, _output_vexpr_ctxs,
                                                  std::move(memory_lease));
    return Status::OK();
}

Status PaimonTableSinkLocalState::open(RuntimeState* state) {
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

Status PaimonTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    Status final_status = exec_status;
    if (_writer) {
        Status writer_status = _writer->close(exec_status);
        if (final_status.ok() && !writer_status.ok()) {
            final_status = writer_status;
        }
        _writer.reset();
    }

    Status base_status = Base::close(state, final_status);
    if (final_status.ok() && !base_status.ok()) {
        final_status = base_status;
    }
    return final_status;
}

Status PaimonTableSinkOperatorX::sink_impl(RuntimeState* state, Block* in_block, bool /*eos*/) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), static_cast<int64_t>(in_block->rows()));

    if (in_block->rows() == 0) {
        return Status::OK();
    }

    // This is a synchronous SDK call. The LocalState is marked blockable, so
    // the whole pipeline task (including open and close) runs on the blocking
    // scheduler instead of occupying a regular pipeline worker.
    DCHECK(local_state._writer);
    return local_state._writer->write(state, *in_block);
}

} // namespace doris
