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

namespace doris {
class PipBufferControlBlock;

namespace pipeline {

class ResultSinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
    ENABLE_FACTORY_CREATOR(ResultSinkLocalState);
    using Base = PipelineXSinkLocalState<BasicSharedState>;

public:
    ResultSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* rows_sent_counter() { return _rows_sent_counter; }

private:
    friend class ResultSinkOperatorX;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender = nullptr;
    std::shared_ptr<ResultWriter> _writer = nullptr;
    RuntimeProfile::Counter* _blocks_sent_counter = nullptr;
    RuntimeProfile::Counter* _rows_sent_counter = nullptr;
};

class ResultSinkOperatorX final : public DataSinkOperatorX<ResultSinkLocalState> {
public:
    ResultSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                        const std::vector<TExpr>& select_exprs, const TResultSink& sink);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class ResultSinkLocalState;

    Status _second_phase_fetch_data(RuntimeState* state, vectorized::Block* final_block);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<vectorized::ResultFileOptions> _file_opts = nullptr;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    // for fetch data by rowids
    TFetchOption _fetch_option;

    std::shared_ptr<BufferControlBlock> _sender = nullptr;
};

} // namespace pipeline
} // namespace doris
