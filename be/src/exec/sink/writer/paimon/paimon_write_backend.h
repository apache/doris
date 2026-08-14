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

#include "common/status.h"
#include "core/block/block.h"

namespace doris {

class RuntimeState;
class RuntimeProfile;

/// Writer contract implemented by one SDK writer adapter. Each
/// PaimonTableWriter owns one IPaimonWriter, which delegates to the
/// Paimon Java SDK through JNI. Partition and bucket routing happens
/// inside the SDK.
///
/// Lifecycle: created by IPaimonWriteBackend::create_writer() after the
/// backend is opened; used for the duration of one pipeline instance.
class IPaimonWriter {
public:
    virtual ~IPaimonWriter() = default;

    /// Write a projected Block to the Paimon SDK.
    /// For the JNI path: Block → Arrow IPC → direct buffer → Java.
    virtual Status write(RuntimeState* state, Block& block) = 0;

    /// Flush all buffered data, close files, and collect serialized commit
    /// messages (DPCM-framed). Called once at EOS.
    virtual Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) = 0;

    /// Discard written data files on error. Called when write or prepare_commit fails.
    virtual Status abort() = 0;
};

/// Backend boundary for creating writers through the Paimon Java SDK.
///
/// The backend owns the JVM class reference, method IDs, and Java writer object.
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
};

} // namespace doris
