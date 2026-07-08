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
#include <vector>

#include "common/status.h"
#include "core/block/block.h"

namespace doris {

class RuntimeState;

// ────────────────────────────────────────────────────────────
// Backend type enumeration
// ────────────────────────────────────────────────────────────

enum class PaimonBackendType {
    JNI,  // Java SDK via JNI
    FFI   // Rust via C FFI
};

// ────────────────────────────────────────────────────────────
// IPaimonWriter — per-(partition, bucket) writer
// ────────────────────────────────────────────────────────────

class IPaimonWriter {
public:
    virtual ~IPaimonWriter() = default;

    /// Write a Doris columnar block to this writer.
    virtual Status write(RuntimeState* state, Block& block) = 0;

    /// Prepare commit: close files and produce commit messages.
    /// The caller owns the returned messages and is responsible for forwarding
    /// them to the commit coordinator (FE).
    virtual Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) = 0;

    /// Trigger compaction for this (partition, bucket).
    /// Only supported by the JNI backend; FFI backend returns NotSupported.
    virtual Status compact(bool full_compaction) = 0;

    /// Abort: discard all data written by this writer.
    /// Best-effort cleanup; errors are logged but not propagated.
    virtual Status abort() = 0;
};

// ────────────────────────────────────────────────────────────
// IPaimonCommitter — snapshot commit coordinator
// ────────────────────────────────────────────────────────────

class IPaimonCommitter {
public:
    virtual ~IPaimonCommitter() = default;

    /// Commit new files in APPEND mode.
    /// All messages are committed atomically in one snapshot.
    virtual Status commit(const std::vector<TPaimonCommitMessage>& messages) = 0;

    /// Commit in OVERWRITE mode, optionally scoped to static partitions.
    virtual Status overwrite(
            const std::vector<TPaimonCommitMessage>& messages,
            const std::map<std::string, std::string>& static_partition) = 0;

    /// Truncate the entire table.
    virtual Status truncate_table() = 0;

    /// Truncate specific partitions.
    virtual Status truncate_partitions(
            const std::vector<std::map<std::string, std::string>>& partitions) = 0;

    /// Abort: delete data files that were prepared but not yet committed.
    virtual Status abort(const std::vector<TPaimonCommitMessage>& messages) = 0;
};

// ────────────────────────────────────────────────────────────
// IPaimonWriteBackend — factory for writers and committers
// ────────────────────────────────────────────────────────────
//
// Java (JNI) and Rust (FFI) are interchangeable implementations of
// this interface. Rust is a functional subset of Java. The selection
// logic prefers Rust (performance) and falls back to Java when the
// table requires features that Rust does not yet support.
//
// v1 only implements JniPaimonWriteBackend. FfiPaimonWriteBackend
// exists as a stub that returns NotSupported for all methods.

class IPaimonWriteBackend {
public:
    virtual ~IPaimonWriteBackend() = default;

    /// Initialize the backend from the Thrift sink description.
    virtual Status open(const TPaimonTableSink& sink, RuntimeState* state) = 0;

    /// Create a writer for a specific (partition, bucket) pair.
    /// May be called multiple times for the same (partition, bucket);
    /// each call produces an independent file writer.
    virtual Status create_writer(const std::string& partition_bytes, int32_t bucket,
                                 std::unique_ptr<IPaimonWriter>* writer) = 0;

    /// Create a committer. One per sink; shared across all writers.
    virtual Status create_committer(std::unique_ptr<IPaimonCommitter>* committer) = 0;

    /// Which backend type is this?
    virtual PaimonBackendType type() const = 0;

    // ──── Capability queries ────────────────────────────────

    virtual bool supports_compaction() const = 0;
    virtual bool supports_lookup_changelog_producer() const = 0;
    virtual bool supports_full_compaction_changelog_producer() const = 0;
    virtual bool supports_partial_update_with_dv() const = 0;
    virtual bool supports_aggregation_with_dv() const = 0;
};

// ────────────────────────────────────────────────────────────
// Factory
// ────────────────────────────────────────────────────────────

class PaimonWriteBackendFactory {
public:
    /// Create the appropriate backend based on the sink configuration.
    static Status create(const TPaimonTableSink& sink,
                         std::unique_ptr<IPaimonWriteBackend>* backend);

    /// Select the best backend type for the given table configuration.
    /// v1: always returns JNI.
    /// v2: returns FFI when the table does not require Java-only features.
    static PaimonBackendType select_backend_type(const TPaimonTableSink& sink);
};

} // namespace doris
