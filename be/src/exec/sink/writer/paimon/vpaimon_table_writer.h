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

#pragma once

#include <gen_cpp/DataSinks_types.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/sink/writer/async_result_writer.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

namespace doris {

class ObjectPool;
class RuntimeState;

/// VPaimonTableWriter is the main entry point for writing data to Paimon
/// tables from Doris. It inherits AsyncResultWriter to leverage the
/// producer-consumer queue pattern for async I/O.
///
/// Architecture:
///
///   VPaimonTableWriter (this class)
///     │
///     │ holds a std::unique_ptr<IPaimonWriteBackend>
///     │
///     ├── JniPaimonWriteBackend  (v1, production)
///     └── FfiPaimonWriteBackend  (v2, stub in v1)
///
/// Data flow:
///   Block → _route_block() → partition/bucket groups
///         → IPaimonWriter::write() per group
///         → (JNI: Arrow IPC → Java; FFI: Arrow C Data → Rust)
///
/// Commit flow:
///   close() → prepare_commit() on all writers
///          → collect TPaimonCommitMessage[]
///          → RuntimeState::add_paimon_commit_messages()
///          → RPC to FE Coordinator
///          → FE PaimonTransaction.commit()
class VPaimonTableWriter final : public AsyncResultWriter {
public:
    VPaimonTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                       std::shared_ptr<Dependency> dep, std::shared_ptr<Dependency> fin_dep);

    ~VPaimonTableWriter() override = default;

    Status init_properties(ObjectPool* pool) { return Status::OK(); }

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, Block& block) override;

    Status close(Status status) override;

private:
    // Append block rows to the internal buffer. Flushes if buffer is full.
    Status _append_to_buffer(const Block& block);
    // Flush the buffered block through the backend.
    Status _flush_buffer();
    // Write a single projected block (after projection).
    Status _write_projected_block(Block& block);

    TDataSink _t_sink;
    RuntimeState* _state = nullptr;

    // The backend abstraction — JNI or FFI
    std::unique_ptr<IPaimonWriteBackend> _backend;

    // Active writer (one per BE worker; the actual per-partition-bucket
    // routing happens inside Paimon's Java BatchTableWrite).
    std::unique_ptr<IPaimonWriter> _writer;

    // Output column names, extracted from sink options
    std::vector<std::string> _output_column_names;

    // Buffering: small blocks are accumulated before sending to Java
    // to reduce JNI call overhead. Large blocks bypass the buffer.
    std::unique_ptr<Block> _buffer;
    size_t _buffered_rows = 0;
    size_t _buffered_bytes = 0;
    static constexpr size_t BATCH_MAX_ROWS = 32768;            // 32K rows
    static constexpr size_t BATCH_MAX_BYTES = 4 * 1024 * 1024; // 4 MB

    // Statistics
    int64_t _written_rows = 0;

    // Profile counters
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_bytes_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _project_timer = nullptr;
    RuntimeProfile::Counter* _arrow_convert_timer = nullptr;
    RuntimeProfile::Counter* _file_store_write_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _prepare_commit_timer = nullptr;
    RuntimeProfile::Counter* _serialize_commit_messages_timer = nullptr;
    RuntimeProfile::Counter* _commit_payload_count = nullptr;
    RuntimeProfile::Counter* _commit_payload_bytes_counter = nullptr;
    RuntimeProfile::Counter* _buffer_flush_count = nullptr;
};

} // namespace doris
