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
#include "util/debug_points.h"

namespace doris {

PaimonTableWriter::PaimonTableWriter(TDataSink t_sink, const VExprContextSPtrs& output_exprs)
        : _t_sink(std::move(t_sink)), _output_expr_ctxs(output_exprs) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

Status PaimonTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    // Register profile counters
    _written_rows_counter = ADD_COUNTER(profile, "WrittenRows", TUnit::UNIT);
    _written_bytes_counter = ADD_COUNTER(profile, "WrittenBytes", TUnit::BYTES);
    _send_data_timer = ADD_TIMER(profile, "SendDataTime");
    _project_timer = ADD_CHILD_TIMER(profile, "ProjectTime", "SendDataTime");
    _file_store_write_timer = ADD_CHILD_TIMER(profile, "FileStoreWriteTime", "SendDataTime");
    _open_timer = ADD_TIMER(profile, "OpenTime");
    _close_timer = ADD_TIMER(profile, "CloseTime");
    _prepare_commit_timer = ADD_TIMER(profile, "PrepareCommitTime");
    _commit_payload_count = ADD_COUNTER(profile, "CommitPayloadCount", TUnit::UNIT);
    _commit_payload_bytes_counter = ADD_COUNTER(profile, "CommitPayloadBytes", TUnit::BYTES);

    SCOPED_TIMER(_open_timer);

    // Step 1: Create the backend selected by FE.
    RETURN_IF_ERROR(PaimonWriteBackendFactory::create(_t_sink.paimon_table_sink, &_backend));
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
        RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_expr_ctxs, block,
                                                                           &output_block));
        materialize_block_inplace(output_block);
    }

    COUNTER_UPDATE(_written_rows_counter, block.rows());
    COUNTER_UPDATE(_written_bytes_counter, block.bytes());
    state->update_num_rows_load_total(block.rows());
    state->update_num_bytes_load_total(block.bytes());

    // Step 2: Delegate to the backend writer. For the JNI path
    // this converts Block → Arrow RecordBatch → Arrow C Data → Java PaimonJniWriter.
    DCHECK(_writer);
    {
        SCOPED_TIMER(_file_store_write_timer);
        RETURN_IF_ERROR(_writer->write(state, output_block));
    }
    return Status::OK();
}

Status PaimonTableWriter::close(Status status) {
    SCOPED_TIMER(_close_timer);

    // Local ownership guarantees destruction on every return, including message-retention OOM.
    // Repeated close has no resources left to release; the task retains the original error.
    auto backend = std::move(_backend);
    auto writer = std::move(_writer);
    if (!backend) return status;

    std::vector<TPaimonCommitMessage> messages;
    if (status.ok()) {
        DCHECK(writer);
        status = paimon_write_call([&] {
            SCOPED_TIMER(_prepare_commit_timer);
            return writer->prepare_commit(messages);
        });
    }

    if (!status.ok() && writer) {
        // Abort failure must not skip backend close or replace the original task error.
        (void)paimon_write_call([&] {
            auto abort_st = writer->abort();
            if (!abort_st.ok()) {
                LOG(WARNING) << "Paimon writer abort failed: " << abort_st.to_string();
            }
            return abort_st;
        });
    }

    writer.reset();
    auto close_st = paimon_write_call([&] { return backend->close(); });
    if (status.ok()) status = std::move(close_st);
    if (!status.ok()) return status;

    // Only publish after SDK shutdown. Until handoff, backend destruction cleans native files.
    return paimon_write_call([&] {
        COUNTER_UPDATE(_commit_payload_count, static_cast<int64_t>(messages.size()));
        for (const auto& msg : messages) {
            DORIS_CHECK(msg.__isset.payload);
            COUNTER_UPDATE(_commit_payload_bytes_counter, static_cast<int64_t>(msg.payload.size()));
        }
        if (!messages.empty()) {
            DBUG_EXECUTE_IF("PaimonTableWriter.close.store_messages_oom",
                            { throw std::bad_alloc(); });
            _state->add_paimon_commit_messages(messages);
        }
        backend->on_commit_messages_transferred();
        return Status::OK();
    });
}

} // namespace doris
