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

#include <memory>
#include <string>

#include "exec/sink/writer/paimon/paimon_write_backend.h"

namespace doris {

// Implemented behind USE_PAIMON_CPP; a CPP plan fails explicitly on a BE without the library.
class CppPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    CppPaimonWriteBackend();
    ~CppPaimonWriteBackend() override;
    Status open(const TPaimonTableSink&, RuntimeState*, RuntimeProfile*) override;
    Status create_writer(std::unique_ptr<IPaimonWriter>*) override;
    Status close() override;
    PaimonBackendType type() const override { return PaimonBackendType::CPP; }

private:
    class Impl;
    class Writer;
    std::shared_ptr<Impl> _impl;
};

// DPCM: magic, big-endian serializer version, big-endian payload length, payload.
Status frame_paimon_cpp_commit(const std::string& data, int32_t version,
                               TPaimonCommitMessage* message);

} // namespace doris
