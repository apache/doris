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

#include "operator.h"
#include "vec/sink/writer/vfile_result_writer.h"

namespace doris::vectorized {
template <typename Parent>
class BlockSerializer;
template <typename Parent>
class Channel;
class BroadcastPBlockHolder;
} // namespace doris::vectorized

namespace doris::pipeline {

class ResultFileSinkOperatorX;
class ResultFileSinkLocalState final
        : public AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX>;
    ENABLE_FACTORY_CREATOR(ResultFileSinkLocalState);
    ResultFileSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~ResultFileSinkLocalState() override;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] int sender_id() const { return _sender_id; }

    RuntimeProfile::Counter* brpc_wait_timer() { return _brpc_wait_timer; }
    RuntimeProfile::Counter* local_send_timer() { return _local_send_timer; }
    RuntimeProfile::Counter* brpc_send_timer() { return _brpc_send_timer; }
    RuntimeProfile::Counter* merge_block_timer() { return _merge_block_timer; }
    RuntimeProfile::Counter* split_block_distribute_by_channel_timer() {
        return _split_block_distribute_by_channel_timer;
    }

private:
    friend class ResultFileSinkOperatorX;

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st);

    std::unique_ptr<vectorized::Block> _output_block;
    std::shared_ptr<BufferControlBlock> _sender;

    std::vector<vectorized::Channel<ResultFileSinkLocalState>*> _channels;
    bool _only_local_exchange = false;
    std::unique_ptr<vectorized::BlockSerializer<ResultFileSinkLocalState>> _serializer;
    std::shared_ptr<vectorized::BroadcastPBlockHolder> _block_holder;
    RuntimeProfile::Counter* _brpc_wait_timer = nullptr;
    RuntimeProfile::Counter* _local_send_timer = nullptr;
    RuntimeProfile::Counter* _brpc_send_timer = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;
    RuntimeProfile::Counter* _split_block_distribute_by_channel_timer = nullptr;

    int _sender_id;
};

class ResultFileSinkOperatorX final : public DataSinkOperatorX<ResultFileSinkLocalState> {
public:
    ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                            const std::vector<TExpr>& t_output_expr);
    ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                            const TResultFileSink& sink,
                            const std::vector<TPlanFragmentDestination>& destinations,
                            const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs);
    Status init(const TDataSink& thrift_sink) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class ResultFileSinkLocalState;
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;

    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    const std::vector<TPlanFragmentDestination> _dests;

    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    RowDescriptor _output_row_descriptor;
    int _buf_size = 4096; // Allocated from _pool
    bool _is_top_sink = true;
    std::string _header;
    std::string _header_type;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
};

} // namespace doris::pipeline
