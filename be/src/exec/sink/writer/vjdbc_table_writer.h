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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/sink/writer/async_result_writer.h"
#include "format/transformer/vjni_format_transformer.h"

namespace doris {

class Block;
class RuntimeState;

/**
 * VJdbcTableWriter writes data to external JDBC targets via JNI.
 *
 * Refactored to use VJniFormatTransformer (same pattern as VMCPartitionWriter for MaxCompute).
 * The Java side writer is JdbcJniWriter which extends JniWriter.
 *
 * Explicit transactions are finalized by Java close(boolean), using the terminal BE status.
 */
class VJdbcTableWriter final : public AsyncResultWriter {
public:
    VJdbcTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                     std::shared_ptr<Dependency> dep, std::shared_ptr<Dependency> fin_dep);

    Status open(RuntimeState* state, RuntimeProfile* operator_profile) override;

    Status write(RuntimeState* state, Block& block) override;

    Status finish(RuntimeState* state) override;

    Status close(Status s) override;

private:
    // Build the writer_params map from TDataSink
    static std::map<std::string, std::string> _build_writer_params(const TDataSink& t_sink);

    Status _transaction_status(Status status);

    std::unique_ptr<VJniFormatTransformer> _writer;
    std::map<std::string, std::string> _writer_params;
    bool _use_transaction = false;
    // Only the async writer thread accesses these; its task context owns the RuntimeState.
    RuntimeState* _state = nullptr;
    bool _successful_eos = false;
};
} // namespace doris
