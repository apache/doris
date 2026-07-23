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

#include "exec/sink/writer/paimon/vpaimon_table_writer.h"

#include "common/check.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"

namespace doris {

VPaimonTableWriter::VPaimonTableWriter(TDataSink t_sink, const VExprContextSPtrs& output_exprs,
                                       std::shared_ptr<Dependency> dep,
                                       std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, std::move(dep), std::move(fin_dep)),
          _t_sink(std::move(t_sink)) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

Status VPaimonTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
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
    RETURN_IF_ERROR(PaimonWriteBackendFactory::create(_t_sink.paimon_table_sink, &_backend));
    DCHECK(_backend);
    // Step 2: Open the backend — for JNI this loads the Java class and calls PaimonJniWriter.open().
    RETURN_IF_ERROR(_backend->open(_t_sink.paimon_table_sink, state));
    // Step 3: Create a lightweight writer adapter that delegates to the opened backend.
    RETURN_IF_ERROR(_backend->create_writer(&_writer));
    DCHECK(_writer);

    LOG(INFO) << "VPaimonTableWriter opened: backend=" << static_cast<int>(_backend->type())
              << ", writer_scope=local_state";
    return Status::OK();
}

Status VPaimonTableWriter::write(RuntimeState* state, Block& block) {
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

Status VPaimonTableWriter::close(Status status) {
    SCOPED_TIMER(_close_timer);

    // On success: prepare commit messages via the SDK writer.
    // On failure: abort the writer to discard any written data files.
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

        if (status.ok()) {
            COUNTER_UPDATE(_commit_payload_count, static_cast<int64_t>(messages.size()));
            for (const auto& msg : messages) {
                DORIS_CHECK(msg.__isset.payload);
                COUNTER_UPDATE(_commit_payload_bytes_counter,
                               static_cast<int64_t>(msg.payload.size()));
            }

            // Forward commit messages to RuntimeState; they are collected by the
            // FE Coordinator via RPC and committed through PaimonTransaction.
            if (!messages.empty()) {
                _state->add_paimon_commit_messages(messages);
                LOG(INFO) << "Paimon writer closed: " << messages.size()
                          << " commit messages, total rows=" << _written_rows;
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

    // Release writer first (may hold SDK resources), then the backend (JNI refs).
    _writer.reset();
    _backend.reset();
    return status;
}

} // namespace doris
