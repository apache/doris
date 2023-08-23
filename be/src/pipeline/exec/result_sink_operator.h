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
#include "vec/sink/vresult_sink.h"

namespace doris {
class DataSink;

namespace pipeline {

class ResultSinkOperatorBuilder final : public DataSinkOperatorBuilder<vectorized::VResultSink> {
public:
    ResultSinkOperatorBuilder(int32_t id, DataSink* sink);

    OperatorPtr build_operator() override;
};

class ResultSinkOperator final : public DataSinkOperator<ResultSinkOperatorBuilder> {
public:
    ResultSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink);

    bool can_write() override;
};

class ResultSinkLocalState : public PipelineXSinkLocalState {
    ENABLE_FACTORY_CREATOR(ResultSinkLocalState);

public:
    ResultSinkLocalState(DataSinkOperatorX* parent, RuntimeState* state)
            : PipelineXSinkLocalState(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class ResultSinkOperatorX;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<ResultWriter> _writer;
    RuntimeProfile* _profile; // Allocated from _pool
};

class ResultSinkOperatorX final : public DataSinkOperatorX {
public:
    ResultSinkOperatorX(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                        const TResultSink& sink, int buffer_size);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    Status close(RuntimeState* state) override;
    bool can_write(RuntimeState* state) override;

private:
    friend class ResultSinkLocalState;

    Status _second_phase_fetch_data(RuntimeState* state, vectorized::Block* final_block);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<vectorized::ResultFileOptions> _file_opts;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    int _buf_size; // Allocated from _pool

    // for fetch data by rowids
    TFetchOption _fetch_option;
};

} // namespace pipeline
} // namespace doris