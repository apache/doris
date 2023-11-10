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

namespace doris {
class DataSink;
class PipBufferControlBlock;

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

class ResultBufferDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(ResultBufferDependency);
    ResultBufferDependency(int id) : WriteDependency(id, "ResultBufferDependency") {}
    ~ResultBufferDependency() override = default;

    void* shared_state() override { return nullptr; }
};

class ResultQueueDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(ResultQueueDependency);
    ResultQueueDependency(int id) : WriteDependency(id, "ResultQueueDependency") {}
    ~ResultQueueDependency() override = default;

    void* shared_state() override { return nullptr; }
};

class CancelDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(CancelDependency);
    CancelDependency(int id) : WriteDependency(id, "CancelDependency") { _ready_for_write = false; }
    ~CancelDependency() override = default;

    void* shared_state() override { return nullptr; }
};

class ResultSinkLocalState final : public PipelineXSinkLocalState<> {
    ENABLE_FACTORY_CREATOR(ResultSinkLocalState);

public:
    ResultSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    WriteDependency* dependency() override { return _result_sink_dependency.get(); }

private:
    friend class ResultSinkOperatorX;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<ResultWriter> _writer;
    std::shared_ptr<OrDependency> _result_sink_dependency;
    std::shared_ptr<pipeline::ResultBufferDependency> _buffer_dependency;
    std::shared_ptr<pipeline::ResultQueueDependency> _queue_dependency;
    std::shared_ptr<pipeline::CancelDependency> _cancel_dependency;

    RuntimeProfile::Counter* _wait_for_queue_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_buffer_timer = nullptr;
    // time of prefilter input block from scanner
    RuntimeProfile::Counter* _wait_for_cancel_timer = nullptr;
};

class ResultSinkOperatorX final : public DataSinkOperatorX<ResultSinkLocalState> {
public:
    ResultSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                        const std::vector<TExpr>& select_exprs, const TResultSink& sink);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

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

    // for fetch data by rowids
    TFetchOption _fetch_option;
};

} // namespace pipeline
} // namespace doris
