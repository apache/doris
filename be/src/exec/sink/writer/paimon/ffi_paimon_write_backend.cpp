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

#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"

namespace doris {

Status FfiPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state) {
    return Status::NotSupported(
            "FFI (Rust) Paimon write backend is not yet implemented. "
            "Currently only the JNI (Java) backend is available. "
            "The FFI backend will be introduced in v2 for high-throughput append-only writes.");
}

Status FfiPaimonWriteBackend::create_writer(const std::string& partition_bytes, int32_t bucket,
                                            std::unique_ptr<IPaimonWriter>* writer) {
    return Status::NotSupported("FFI backend: create_writer not yet implemented");
}

Status FfiPaimonWriteBackend::create_committer(std::unique_ptr<IPaimonCommitter>* committer) {
    return Status::NotSupported("FFI backend: create_committer not yet implemented");
}

} // namespace doris
