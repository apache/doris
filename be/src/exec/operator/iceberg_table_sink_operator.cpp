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
Status IcebergTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _writer = std::make_unique<VIcebergTableWriter>(info.tsink, _output_vexpr_ctxs);
    _writer->defer_file_cleanup_until_outer_close();
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

Status IcebergTableSinkLocalState::sink(RuntimeState* state, Block* block, bool eos) {
    if (block->rows() > 0) {
        DCHECK(_writer);
        RETURN_IF_ERROR(_writer->write(state, *block));
    }
    if (!eos) {
        return Status::OK();
    }

    // The pipeline task holds the sink reservation only while sink() runs. Closing the writer
    // flushes what each partition file still buffers, e.g. the last half-filled Parquet row group,
    // whose final pages grow the page and compression buffers. Done in close(), that flush would
    // run after the EOS reservation was released, so close the writer here.
    Status close_status = Status::OK();
    if (state->is_cancelled()) {
        close_status = state->cancel_reason();
    }
    return _close_writer(close_status);
}

Status IcebergTableSinkLocalState::_close_writer(Status close_status) {
    DCHECK(_writer);
    if (!_writer_closed) {
        _writer_close_status = _writer->close(close_status);
        _writer_closed = true;
    }
    if (close_status.ok() && !_writer_close_status.ok()) {
        close_status = _writer_close_status;
    }
    return close_status;
}

Status IcebergTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    DCHECK(_writer);
    Status final_status = exec_status;
    // Observe cancellation before close so a known-cancelled write is not finalized as successful.
    if (final_status.ok() && state->is_cancelled()) {
        final_status = state->cancel_reason();
    }
    // A task that reached EOS already closed the writer in sink(); an error or cancellation before
    // EOS closes it here with that status.
    final_status = _close_writer(final_status);
    // close() may block on sort/merge/file I/O, so cancellation can arrive while it is running.
    if (final_status.ok() && state->is_cancelled()) {
        final_status = state->cancel_reason();
    }
    _writer->finish_deferred_file_cleanup(final_status);
    _writer.reset();

    Status base_status = Base::close(state, final_status);
    if (final_status.ok() && !base_status.ok()) {
        final_status = base_status;
    }
    return final_status;
}

} // namespace doris
