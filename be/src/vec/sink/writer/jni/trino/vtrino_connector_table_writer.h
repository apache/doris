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

#include <fmt/format.h>

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "vec/exec/jni_connector.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris::vectorized {

class Block;

class VTrinoConnectorTableWriter final : public AsyncResultWriter {
public:
    VTrinoConnectorTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                               std::shared_ptr<pipeline::Dependency> dep,
                               std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VTrinoConnectorTableWriter() override = default;

    Status open(RuntimeState* state, RuntimeProfile* profile) override {
        _state = state;
        RETURN_IF_ERROR(_set_spi_plugins_dir());
        return _init_writer();
    }

    Status write(RuntimeState* state, vectorized::Block& block) override;
    Status finish(RuntimeState* state) override;
    Status close(Status s) override {
        if (_jni_connector) {
            return _jni_connector->close();
        }
        return Status::OK();
    }

private:
    Status _set_spi_plugins_dir();
    Status _init_writer();

    std::map<std::string, std::string> params;
    std::unique_ptr<JniConnector> _jni_connector;
    RuntimeState* _state = nullptr;
    static const std::string TRINO_CONNECTOR_OPTION_PREFIX;
};
} // namespace doris::vectorized
