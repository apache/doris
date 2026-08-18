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

#include "exec/sink/writer/paimon/paimon_table_writer.h"

#include "common/check.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/block/materialize_block.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {

PaimonTableWriter::PaimonTableWriter(TDataSink t_sink, const VExprContextSPtrs& output_exprs,
                                     std::shared_ptr<PaimonWriterMemoryLease> memory_lease)
        : _t_sink(std::move(t_sink)),
          _output_expr_ctxs(output_exprs),
          _memory_lease(std::move(memory_lease)) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

Status PaimonTableWriter::_projection_block(Block& input_block, Block* output_block) {
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_expr_ctxs,
                                                                       input_block, output_block));
    materialize_block_inplace(*output_block);
    return Status::OK();
}

Status PaimonTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _operator_profile = profile;

    // Register profile counters
    _written_rows_counter = ADD_COUNTER(_operator_profile, "WrittenRows", TUnit::UNIT);
    _written_bytes_counter = ADD_COUNTER(_operator_profile, "WrittenBytes", TUnit::BYTES);
    _send_data_timer = ADD_TIMER(_operator_profile, "SendDataTime");
    _project_timer = ADD_CHILD_TIMER(_operator_profile, "ProjectTime", "SendDataTime");
    _file_store_write_timer =
            ADD_CHILD_TIMER(_operator_profile, "FileStoreWriteTime", "SendDataTime");
    _open_timer = ADD_TIMER(_operator_profile, "OpenTime");
    _close_timer = ADD_TIMER(_operator_profile, "CloseTime");
    _prepare_commit_timer = ADD_TIMER(_operator_profile, "PrepareCommitTime");
    _commit_payload_count = ADD_COUNTER(_operator_profile, "CommitPayloadCount", TUnit::UNIT);
    _commit_payload_bytes_counter =
            ADD_COUNTER(_operator_profile, "CommitPayloadBytes", TUnit::BYTES);

    SCOPED_TIMER(_open_timer);

    // Step 1: Create the backend (JNI or FFI) based on the sink configuration.
    RETURN_IF_ERROR(
            PaimonWriteBackendFactory::create(_t_sink.paimon_table_sink, _memory_lease, &_backend));
    DCHECK(_backend);
    // Step 2: Open the backend — for JNI this loads the Java class and calls PaimonJniWriter.open().
    RETURN_IF_ERROR(_backend->open(_t_sink.paimon_table_sink, state, profile));
    // Step 3: Create a lightweight writer adapter that delegates to the opened backend.
    RETURN_IF_ERROR(_backend->create_writer(&_writer));
    DCHECK(_writer);

    LOG(INFO) << "PaimonTableWriter opened: backend=" << static_cast<int>(_backend->type())
              << ", writer_scope=local_state";
    return Status::OK();
}

Status PaimonTableWriter::write(RuntimeState* state, Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    SCOPED_TIMER(_send_data_timer);

    // Step 1: Apply output expressions to produce the columns selected by FE.
    Block output_block;
    {
        SCOPED_TIMER(_project_timer);
        RETURN_IF_ERROR(_projection_block(block, &output_block));
    }

    COUNTER_UPDATE(_written_rows_counter, block.rows());
    COUNTER_UPDATE(_written_bytes_counter, block.bytes());
    _state->update_num_rows_load_total(block.rows());
    _state->update_num_bytes_load_total(block.bytes());

    // Step 2: Delegate to the backend writer (JNI or FFI). For the JNI path
    // this converts Block → Arrow IPC → direct buffer → Java PaimonJniWriter.
    DCHECK(_writer);
    {
        SCOPED_TIMER(_file_store_write_timer);
        RETURN_IF_ERROR(_writer->write(_state, output_block));
    }
    _written_rows += block.rows();
    return Status::OK();
}

Status PaimonTableWriter::close(Status status) {
    SCOPED_TIMER(_close_timer);

    // Prepare messages first, but do not publish them until the backend confirms
    // that every SDK user has stopped and its native backing memory is safe to release.
    std::vector<TPaimonCommitMessage> messages;
    if (status.ok()) {
        DCHECK(_writer);
        {
            SCOPED_TIMER(_prepare_commit_timer);
            Status prep_st = _writer->prepare_commit(messages);
            if (!prep_st.ok()) {
                status = prep_st;
            }
        }
    }

    // If prepare_commit failed or the incoming status was already an error,
    // abort the writer to clean up uncommitted data files.
    if (!status.ok()) {
        LOG(WARNING) << "Paimon writer closing with error: " << status.to_string();
        if (_writer) {
            Status abort_st = _writer->abort();
            if (!abort_st.ok()) {
                LOG(WARNING) << "Paimon writer abort failed: " << abort_st.to_string();
            }
        }
    }

    // The adapter only owns Arrow conversion resources. Release it before closing
    // the backend, whose Java close is the authoritative SDK shutdown boundary.
    _writer.reset();

    if (_backend) {
        Status close_st = _backend->close();
        if (!close_st.ok()) {
            if (status.ok()) {
                status = close_st;
            } else {
                LOG(WARNING) << "Paimon backend close also failed: " << close_st.to_string();
            }
        }
    }

    // Only a fully prepared and cleanly stopped writer may contribute payloads
    // to the FE transaction. A Java close failure therefore aborts the Doris
    // transaction instead of allowing it to commit potentially unsafe output.
    if (status.ok()) {
        COUNTER_UPDATE(_commit_payload_count, static_cast<int64_t>(messages.size()));
        for (const auto& msg : messages) {
            DORIS_CHECK(msg.__isset.payload);
            COUNTER_UPDATE(_commit_payload_bytes_counter, static_cast<int64_t>(msg.payload.size()));
        }
        if (!messages.empty()) {
            _state->add_paimon_commit_messages(messages);
            LOG(INFO) << "Paimon writer closed: " << messages.size()
                      << " commit messages, total rows=" << _written_rows;
        }
    }

    _backend.reset();
    return status;
}

} // namespace doris
