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

#include "common/logging.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"

namespace doris {

static constexpr const char* PAIMON_OUTPUT_COLUMN_NAMES_KEY =
        "doris.output_column_names";
static constexpr char PAIMON_COLUMN_NAME_SEPARATOR = '\x01';

VPaimonTableWriter::VPaimonTableWriter(const TDataSink& t_sink,
                                         const VExprContextSPtrs& output_exprs,
                                         std::shared_ptr<Dependency> dep,
                                         std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, std::move(dep), std::move(fin_dep)),
          _t_sink(t_sink) {
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
    _arrow_convert_timer = ADD_CHILD_TIMER(_operator_profile, "ArrowConvertTime", "SendDataTime");
    _file_store_write_timer =
            ADD_CHILD_TIMER(_operator_profile, "FileStoreWriteTime", "SendDataTime");
    _open_timer = ADD_TIMER(_operator_profile, "OpenTime");
    _close_timer = ADD_TIMER(_operator_profile, "CloseTime");
    _prepare_commit_timer = ADD_TIMER(_operator_profile, "PrepareCommitTime");
    _serialize_commit_messages_timer =
            ADD_TIMER(_operator_profile, "SerializeCommitMessagesTime");
    _commit_payload_count = ADD_COUNTER(_operator_profile, "CommitPayloadCount", TUnit::UNIT);
    _commit_payload_bytes_counter =
            ADD_COUNTER(_operator_profile, "CommitPayloadBytes", TUnit::BYTES);
    _buffer_flush_count = ADD_COUNTER(_operator_profile, "BufferFlushCount", TUnit::UNIT);

    SCOPED_TIMER(_open_timer);

    // Create the backend via factory
    RETURN_IF_ERROR(
            PaimonWriteBackendFactory::create(_t_sink.paimon_table_sink, &_backend));
    RETURN_IF_ERROR(_backend->open(_t_sink.paimon_table_sink, state));

    // Create a single writer (the actual per-partition-bucket routing is
    // handled inside Paimon's Java BatchTableWrite).
    RETURN_IF_ERROR(_backend->create_writer("" /* partition */, 0 /* bucket */, &_writer));

    LOG(INFO) << "VPaimonTableWriter opened: table=" << _t_sink.paimon_table_sink.tb_name
              << ", backend=" << static_cast<int>(_backend->type());
    return Status::OK();
}

Status VPaimonTableWriter::write(RuntimeState* state, Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    SCOPED_TIMER(_send_data_timer);

    // Project: apply output expressions
    Block output_block;
    {
        SCOPED_TIMER(_project_timer);
        RETURN_IF_ERROR(_projection_block(block, &output_block));
    }

    // Rename columns using the output column names from the sink config
    const auto& paimon_sink = _t_sink.paimon_table_sink;
    if (paimon_sink.__isset.paimon_options) {
        auto it = paimon_sink.paimon_options.find(PAIMON_OUTPUT_COLUMN_NAMES_KEY);
        if (it != paimon_sink.paimon_options.end()) {
            auto output_names = _split_column_names(it->second);
            if (!output_names.empty() && output_names.size() == output_block.columns()) {
                for (size_t i = 0; i < output_names.size(); ++i) {
                    output_block.get_by_position(i).name = output_names[i];
                }
            }
        }
    }

    // If the block is already large enough, send it directly.
    // Otherwise, buffer it to reduce JNI call overhead.
    if (output_block.rows() >= BATCH_MAX_ROWS ||
        output_block.bytes() >= BATCH_MAX_BYTES) {
        RETURN_IF_ERROR(_flush_buffer());
        return _write_projected_block(output_block);
    }

    // Check if appending would overflow the buffer
    if (_buffered_rows > 0 &&
        (_buffered_rows + output_block.rows() >= BATCH_MAX_ROWS ||
         _buffered_bytes + output_block.bytes() >= BATCH_MAX_BYTES)) {
        RETURN_IF_ERROR(_flush_buffer());
    }

    RETURN_IF_ERROR(_append_to_buffer(output_block));
    if (_buffered_rows >= BATCH_MAX_ROWS || _buffered_bytes >= BATCH_MAX_BYTES) {
        return _flush_buffer();
    }
    return Status::OK();
}

Status VPaimonTableWriter::_append_to_buffer(const Block& block) {
    if (!_buffer) {
        _buffer = Block::create_unique(block.clone_empty());
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }
    auto columns = std::move(*_buffer).mutate_columns();
    const int cols = block.columns();
    const size_t rows = block.rows();
    for (int col = 0; col < cols; ++col) {
        columns[col]->insert_range_from(*block.get_by_position(col).column, 0, rows);
    }
    _buffer->set_columns(std::move(columns));
    _buffered_rows += rows;
    _buffered_bytes += block.bytes();
    return Status::OK();
}

Status VPaimonTableWriter::_flush_buffer() {
    if (!_buffer || _buffered_rows == 0) {
        return Status::OK();
    }
    Status st = _write_projected_block(*_buffer);
    COUNTER_UPDATE(_buffer_flush_count, 1);
    _buffer.reset();
    _buffered_rows = 0;
    _buffered_bytes = 0;
    return st;
}

Status VPaimonTableWriter::_write_projected_block(Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    COUNTER_UPDATE(_written_rows_counter, block.rows());
    COUNTER_UPDATE(_written_bytes_counter, block.bytes());
    _state->update_num_rows_load_total(block.rows());
    _state->update_num_bytes_load_total(block.bytes());

    if (_writer) {
        SCOPED_TIMER(_file_store_write_timer);
        RETURN_IF_ERROR(_writer->write(_state, block));
    }
    _written_rows += block.rows();
    return Status::OK();
}

Status VPaimonTableWriter::close(Status status) {
    SCOPED_TIMER(_close_timer);

    // Flush any remaining buffered data
    if (status.ok()) {
        Status flush_st = _flush_buffer();
        if (!flush_st.ok()) {
            status = flush_st;
        }
    } else {
        _buffer.reset();
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }

    // Collect commit messages from the writer
    std::vector<TPaimonCommitMessage> messages;
    if (status.ok() && _writer) {
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
                if (msg.__isset.payload) {
                    COUNTER_UPDATE(_commit_payload_bytes_counter,
                                   static_cast<int64_t>(msg.payload.size()));
                }
            }

            if (!messages.empty()) {
                _state->add_paimon_commit_messages(messages);
                LOG(INFO) << "Paimon writer closed with " << messages.size()
                          << " commit messages, total rows=" << _written_rows;
            }
        }
    }

    // On error, abort the writer
    if (!status.ok()) {
        LOG(WARNING) << "Paimon writer closing with error: " << status.to_string();
        if (_writer) {
            Status abort_st = _writer->abort();
            if (!abort_st.ok()) {
                LOG(WARNING) << "Paimon writer abort failed: " << abort_st.to_string();
            }
        }
    }

    _writer.reset();
    _backend.reset();
    return status;
}

// ────────────────────────────────────────────────────────────
// Helper: split output column names
// ────────────────────────────────────────────────────────────

std::vector<std::string> split_paimon_output_column_names(const std::string& column_names) {
    std::vector<std::string> names;
    size_t begin = 0;
    while (begin <= column_names.size()) {
        size_t end = column_names.find(PAIMON_COLUMN_NAME_SEPARATOR, begin);
        if (end == std::string::npos) {
            names.emplace_back(column_names.substr(begin));
            break;
        }
        names.emplace_back(column_names.substr(begin, end - begin));
        begin = end + 1;
    }
    return names;
}

} // namespace doris
