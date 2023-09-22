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
#include "vec/sink/vresult_file_sink.h"

namespace doris {
class DataSink;

namespace pipeline {

class ResultFileSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::VResultFileSink> {
public:
    ResultFileSinkOperatorBuilder(int32_t id, DataSink* sink);

    OperatorPtr build_operator() override;
};

class ResultFileSinkOperator final : public DataSinkOperator<ResultFileSinkOperatorBuilder> {
public:
    ResultFileSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink);

    bool can_write() override { return true; }
};

class ResultFileSinkOperatorX;
class ResultFileSinkLocalState final : public AsyncWriterSink<vectorized::VFileResultWriter> {
public:
    using Base = AsyncWriterSink<vectorized::VFileResultWriter>;
    ENABLE_FACTORY_CREATOR(ResultFileSinkLocalState);
    ResultFileSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class ResultFileSinkOperatorX;

    std::unique_ptr<vectorized::Block> _output_block = nullptr;
    std::shared_ptr<BufferControlBlock> _sender;
};

class ResultFileSinkOperatorX final : public DataSinkOperatorX<ResultFileSinkLocalState> {
public:
    ResultFileSinkOperatorX(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr);
    ResultFileSinkOperatorX(RuntimeState* state, ObjectPool* pool, int sender_id,
                            const RowDescriptor& row_desc, const TResultFileSink& sink,
                            const std::vector<TPlanFragmentDestination>& destinations,
                            bool send_query_statistics_with_every_batch,
                            const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs);
    Status init(const TDataSink& thrift_sink) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    WriteDependency* wait_for_dependency(RuntimeState* state) override;

    bool is_pending_finish(RuntimeState* state) const override;

private:
    friend class ResultFileSinkLocalState;

    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    // set file options when sink type is FILE
    std::unique_ptr<vectorized::ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    RowDescriptor _output_row_descriptor;
    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
    std::string _header;
    std::string _header_type;
};

} // namespace pipeline
} // namespace doris
