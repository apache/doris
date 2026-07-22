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

/// Placeholder for the future paimon-rust writer implementation. Keeping this
/// backend in the factory makes the integration boundary explicit without
/// introducing a BE commit contract that the Rust writer will not own.
class FfiPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state) override;
    Status create_writer(std::unique_ptr<IPaimonWriter>* writer) override;
    PaimonBackendType type() const override { return PaimonBackendType::FFI; }
};

} // namespace doris
