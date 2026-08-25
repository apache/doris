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

#include "common/status.h"
#include "core/block/block.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
enum class ExternalFileReportOutcome;

class PaimonPreparedCommitOwner {
public:
    PaimonPreparedCommitOwner(std::unique_ptr<IPaimonWriter> writer,
                              std::unique_ptr<IPaimonWriteBackend> backend);
    ~PaimonPreparedCommitOwner();

    Status prepare_for_report();
    void finalize(ExternalFileReportOutcome outcome);

private:
    void _close();

    std::unique_ptr<IPaimonWriter> _writer;
    std::unique_ptr<IPaimonWriteBackend> _backend;
    bool _finalized = false;
};

/// Each PaimonTableSinkLocalState owns one PaimonTableWriter, which in turn
/// owns one IPaimonWriteBackend and one IPaimonWriter. Pipeline parallelism
/// therefore determines the number of independent Paimon writer sessions;
/// each writer session delegates partition and bucket routing to the Paimon
/// SDK (Java via JNI, or Rust via FFI in the future).
///
/// Doris does NOT compute partition values or bucket ids — it passes complete
/// Blocks through the selected backend (JNI/FFI) to the Paimon SDK, which
/// internally computes partition values, bucket ids, and routes rows to the
/// correct file writers.
///
/// Architecture:
///   PaimonTableSinkOperatorX
///     │  sink_impl() → PaimonTableWriter::write()  (synchronous, no routing)
///     ▼
///   PaimonTableWriter (one per LocalState / pipeline instance)
///     │  owns IPaimonWriteBackend (JNI or FFI)
///     │    └─ create_writer() → IPaimonWriter
///     │  write()
///     │    → JNI backend: Block → Arrow C Data → Java Paimon SDK
///     │    → FFI backend: Block → Rust writer (future)
///     │    → selected SDK owns row normalization, routing, buffering,
///     │      file writing, and compaction
///     ▼
///   close() → prepareCommit() → retain abort owner → CommitMessage[]
///
/// Commit flow (BE only prepares messages; FE is the commit coordinator):
///   close() → writer->prepare_commit()
///          → collect TPaimonCommitMessage[] (DPCM-framed serialized messages)
///          → retain writer/backend until final report ACK or rejection
///          → RuntimeState::add_paimon_commit_messages()
///          → RPC to FE Coordinator → PaimonTransaction
class PaimonTableWriter final {
public:
    PaimonTableWriter(TDataSink t_sink, const VExprContextSPtrs& output_exprs);

    ~PaimonTableWriter() = default;

    Status open(RuntimeState* state, RuntimeProfile* profile);

    Status write(RuntimeState* state, Block& block);

    Status close(Status status);

private:
    TDataSink _t_sink;
    const VExprContextSPtrs& _output_expr_ctxs;
    RuntimeState* _state = nullptr;
    int64_t _written_rows = 0;

    // Backend owns the JNI/FFI connection and creates the writer adapter.
    // Both are scoped to this PaimonTableWriter (one per LocalState).
    std::unique_ptr<IPaimonWriteBackend> _backend;
    std::unique_ptr<IPaimonWriter> _writer;

    // Profile counters
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_bytes_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _project_timer = nullptr;
    RuntimeProfile::Counter* _file_store_write_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _prepare_commit_timer = nullptr;
    RuntimeProfile::Counter* _commit_payload_count = nullptr;
    RuntimeProfile::Counter* _commit_payload_bytes_counter = nullptr;
};

} // namespace doris
