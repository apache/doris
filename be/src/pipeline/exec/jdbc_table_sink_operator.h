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

#include <stdint.h>

#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/sink/vresult_sink.h"
#include "vec/sink/writer/vjdbc_table_writer.h"

namespace doris {
class DataSink;

namespace pipeline {

class JdbcTableSinkOperatorX;
class JdbcTableSinkLocalState final
        : public AsyncWriterSink<vectorized::VJdbcTableWriter, JdbcTableSinkOperatorX> {
    ENABLE_FACTORY_CREATOR(JdbcTableSinkLocalState);

public:
    using Base = AsyncWriterSink<vectorized::VJdbcTableWriter, JdbcTableSinkOperatorX>;
    JdbcTableSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : AsyncWriterSink<vectorized::VJdbcTableWriter, JdbcTableSinkOperatorX>(parent, state) {
    }

private:
    friend class JdbcTableSinkOperatorX;
};

class JdbcTableSinkOperatorX final : public DataSinkOperatorX<JdbcTableSinkLocalState> {
public:
    JdbcTableSinkOperatorX(const RowDescriptor& row_desc, int operator_id,
                           const std::vector<TExpr>& select_exprs);
    Status init(const TDataSink& thrift_sink) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

private:
    friend class JdbcTableSinkLocalState;
    template <typename Writer, typename Parent>
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
};

} // namespace pipeline
} // namespace doris
