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
#include <vector>

#include "common/exception.h"
#include "core/block/block.h"

namespace doris {

class RuntimeState;
class RuntimeProfile;

// Keep synchronous SDK calls and commit-message handoff inside the task's error path.
// Resource owners must still clean up on failure; this cannot catch a throwing noexcept destructor.
template <typename F>
Status paimon_write_call(F&& call) {
    try {
        return call();
    } catch (const doris::Exception& e) {
        return e.to_status();
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Paimon write allocation failed");
    } catch (const std::exception& e) {
        return Status::InternalError("Paimon write exception: {}", e.what());
    } catch (...) {
        return Status::InternalError("Paimon write unknown exception");
    }
}

enum class PaimonBackendType {
    JNI = 0, // Java via JNI (PaimonJniWriter)
    CPP = 2, // Paimon native SDK via Arrow C Data; keep existing diagnostic IDs
    UNKNOWN,
};

/// Writer contract implemented by one SDK writer adapter. Each
/// PaimonTableWriter owns one IPaimonWriter, which delegates to the
/// underlying Paimon SDK. Partition and bucket
/// routing happens inside the selected SDK backend.
///
/// Lifecycle: created by IPaimonWriteBackend::create_writer() after the
/// backend is opened; used for the duration of one pipeline instance.
/// The pipeline stops writing on error and closes the backend before destruction.
/// Adapters do not independently track execution phases or retry failed SDK operations.
class IPaimonWriter {
public:
    virtual ~IPaimonWriter() = default;

    /// Write a projected Block to the Paimon SDK.
    /// For the JNI path: Block → Arrow RecordBatch → Arrow C Data → Java.
    virtual Status write(RuntimeState* state, Block& block) = 0;

    /// Flush all buffered data, close files, and collect serialized commit
    /// messages (DPCM-framed). Called once at EOS.
    virtual Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) = 0;

    /// Discard written data files on error. Called when write or prepare_commit fails.
    virtual Status abort() = 0;
};

/// Backend boundary for creating JNI or native SDK writers.
///
/// The backend owns the connection/session to the external runtime:
/// - JNI: owns the JVM class reference, method IDs, and the Java writer object.
/// - CPP: owns the native SDK writer and its memory/filesystem adapters.
///
/// Each backend creates one or more IPaimonWriter adapters that share the
/// same underlying connection. Snapshot commit is deliberately excluded from
/// this boundary: BE only prepares commit messages (byte payloads), while FE
/// PaimonTransaction is the single commit coordinator.
class IPaimonWriteBackend {
public:
    virtual ~IPaimonWriteBackend() = default;

    /// Initialize the backend connection. For JNI this loads the writer class,
    /// creates the Java object, and calls PaimonJniWriter.open().
    virtual Status open(const TPaimonTableSink& sink, RuntimeState* state,
                        RuntimeProfile* profile) = 0;

    /// Create a lightweight writer adapter that delegates to this backend.
    virtual Status create_writer(std::unique_ptr<IPaimonWriter>* writer) = 0;

    /// Stop all SDK users and release backend resources.
    ///
    /// A successful return is the ownership boundary after which native memory
    /// backing SDK buffers can be reclaimed safely. Callers must not publish
    /// prepared commit messages until this succeeds.
    virtual Status close() = 0;

    /// Called only after successful close and retention of all messages in RuntimeState.
    /// Native backends keep cleanup responsibility until this point. This is a local
    /// handoff to the existing reporting/transaction path, NOT a durable FE commit ACK.
    virtual void on_commit_messages_transferred() {}

    virtual PaimonBackendType type() const = 0;
};

/// Factory that selects and creates the appropriate write backend.
///
/// Backend selection is based on TPaimonTableSink.backend_type:
/// - JNI: JniPaimonWriteBackend (also used for legacy plans without backend_type)
/// - CPP: CppPaimonWriteBackend
/// FE resolves the session preference and capability fallback before sending the plan.
class PaimonWriteBackendFactory {
public:
    /// Create a backend instance based on the sink configuration.
    static Status create(const TPaimonTableSink& sink,
                         std::unique_ptr<IPaimonWriteBackend>* backend);

    /// Determine which backend type to use for the given sink.
    static PaimonBackendType select_backend_type(const TPaimonTableSink& sink);
};

} // namespace doris
