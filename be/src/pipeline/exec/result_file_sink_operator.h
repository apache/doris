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
class ResultFileSinkLocalState final
        : public AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX>;
    ENABLE_FACTORY_CREATOR(ResultFileSinkLocalState);
    ResultFileSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] int sender_id() const { return _sender_id; }

    RuntimeProfile::Counter* brpc_wait_timer() { return _brpc_wait_timer; }

private:
    friend class ResultFileSinkOperatorX;

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st);

    std::unique_ptr<vectorized::Block> _output_block = nullptr;
    std::shared_ptr<BufferControlBlock> _sender;

    std::vector<vectorized::Channel<ResultFileSinkLocalState>*> _channels;
    bool _only_local_exchange = false;
    vectorized::BlockSerializer<ResultFileSinkLocalState> _serializer;
    std::unique_ptr<vectorized::BroadcastPBlockHolder> _block_holder;
    RuntimeProfile::Counter* _brpc_wait_timer;

    int _sender_id;
};

class ResultFileSinkOperatorX final : public DataSinkOperatorX<ResultFileSinkLocalState> {
public:
    ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                            const std::vector<TExpr>& t_output_expr);
    ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                            const TResultFileSink& sink,
                            const std::vector<TPlanFragmentDestination>& destinations,
                            bool send_query_statistics_with_every_batch,
                            const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs);
    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    WriteDependency* wait_for_dependency(RuntimeState* state) override;

    FinishDependency* finish_blocked_by(RuntimeState* state) const override;

private:
    friend class ResultFileSinkLocalState;
    template <typename Writer, typename Parent>
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    const std::vector<TPlanFragmentDestination> _dests;
    bool _send_query_statistics_with_every_batch;

    // set file options when sink type is FILE
    std::unique_ptr<vectorized::ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    RowDescriptor _output_row_descriptor;
    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
    std::string _header;
    std::string _header_type;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
};

} // namespace pipeline
} // namespace doris
