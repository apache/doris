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

#include "exec/sink/writer/paimon/paimon_write_backend.h"

namespace doris {

/// Stub implementation of Paimon write backend via Rust C FFI.
///
/// This is a placeholder for future use. In v1, all methods return
/// Status::NotSupported. When paimon-rust is integrated (v2), this
/// will be replaced with a real implementation that:
///
///   1. Dynamically loads the paimon-rust shared library (libpaimon_c.so)
///   2. Converts Doris Block → Arrow C Data Interface (zero-copy)
///   3. Calls Rust TableWrite::write_arrow_batch() via FFI
///   4. Calls Rust TableCommit::commit() via FFI
///
/// v1 scope: stub only. Upper layers (VPaimonTableWriter) are coded
/// against IPaimonWriteBackend and never need to change.
class FfiPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    FfiPaimonWriteBackend() = default;
    ~FfiPaimonWriteBackend() override = default;

    Status open(const TPaimonTableSink& sink, RuntimeState* state) override;

    Status create_writer(const std::string& partition_bytes, int32_t bucket,
                         std::unique_ptr<IPaimonWriter>* writer) override;

    Status create_committer(std::unique_ptr<IPaimonCommitter>* committer) override;

    PaimonBackendType type() const override { return PaimonBackendType::FFI; }

    // Rust currently supports a subset of Java's features
    bool supports_compaction() const override { return false; }
    bool supports_lookup_changelog_producer() const override { return false; }
    bool supports_full_compaction_changelog_producer() const override { return false; }
    bool supports_partial_update_with_dv() const override { return false; }
    bool supports_aggregation_with_dv() const override { return false; }
};

} // namespace doris
