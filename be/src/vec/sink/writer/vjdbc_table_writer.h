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
#include <stddef.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "vec/exec/vjdbc_connector.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
namespace vectorized {

class Block;

class VJdbcTableWriter final : public AsyncResultWriter, public JdbcConnector {
public:
    static JdbcConnectorParam create_connect_param(const TDataSink&);

    VJdbcTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    // connect to jdbc server
    Status open(RuntimeState* state, RuntimeProfile* profile) override {
        RETURN_IF_ERROR(JdbcConnector::open(state, false));
        return init_to_write(profile);
    }

    Status append_block(vectorized::Block& block) override;

    Status close() override { return JdbcConnector::close(); }

    bool in_transaction() override { return TableConnector::_is_in_transaction; }

private:
    JdbcConnectorParam _param;
};
} // namespace vectorized
} // namespace doris