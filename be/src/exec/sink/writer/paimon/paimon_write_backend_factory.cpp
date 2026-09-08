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

#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"
#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"
#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"

namespace doris {

Status PaimonWriteBackendFactory::create(const TPaimonTableSink& sink,
                                         std::unique_ptr<IPaimonWriteBackend>* backend) {
    switch (select_backend_type(sink)) {
    case PaimonBackendType::JNI:
        *backend = std::make_unique<JniPaimonWriteBackend>();
        return Status::OK();
    case PaimonBackendType::FFI:
        *backend = std::make_unique<FfiPaimonWriteBackend>();
        return Status::OK();
    case PaimonBackendType::CPP:
        *backend = std::make_unique<CppPaimonWriteBackend>();
        return Status::OK();
    case PaimonBackendType::UNKNOWN:
        return Status::NotSupported("Unknown Paimon write backend; refusing implicit JNI fallback");
    }
    return Status::InternalError("Unknown Paimon write backend");
}

PaimonBackendType PaimonWriteBackendFactory::select_backend_type(const TPaimonTableSink& sink) {
    if (!sink.__isset.backend_type) {
        return PaimonBackendType::JNI;
    }
    switch (sink.backend_type) {
    case TPaimonWriteBackendType::JNI:
        return PaimonBackendType::JNI;
    case TPaimonWriteBackendType::FFI:
        return PaimonBackendType::FFI;
    case TPaimonWriteBackendType::CPP:
        return PaimonBackendType::CPP;
    default:
        return PaimonBackendType::UNKNOWN;
    }
}

} // namespace doris
