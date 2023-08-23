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

#include <memory>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "operator.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class DataSink;
class RuntimeState;
class TDataSink;

namespace pipeline {
class PipelineFragmentContext;
class PipelineXFragmentContext;

class ExchangeSinkOperatorBuilder final
        : public DataSinkOperatorBuilder<vectorized::VDataStreamSender> {
public:
    ExchangeSinkOperatorBuilder(int32_t id, DataSink* sink, PipelineFragmentContext* context,
                                int mult_cast_id = -1);

    OperatorPtr build_operator() override;

private:
    PipelineFragmentContext* _context;
    int _mult_cast_id = -1;
};

// Now local exchange is not supported since VDataStreamRecvr is considered as a pipeline broker.
class ExchangeSinkOperator final : public DataSinkOperator<ExchangeSinkOperatorBuilder> {
public:
    ExchangeSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink,
                         PipelineFragmentContext* context, int mult_cast_id);
    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override;
    bool can_write() override;
    bool is_pending_finish() const override;

    Status close(RuntimeState* state) override;

    RuntimeState* state() { return _state; }

private:
    std::unique_ptr<ExchangeSinkBuffer<vectorized::VDataStreamSender>> _sink_buffer;
    int _dest_node_id = -1;
    RuntimeState* _state = nullptr;
    PipelineFragmentContext* _context;
    int _mult_cast_id = -1;
};

class ExchangeSinkLocalState : public PipelineXSinkLocalState {
    ENABLE_FACTORY_CREATOR(ExchangeSinkLocalState);

public:
    ExchangeSinkLocalState(DataSinkOperatorX* parent, RuntimeState* state)
            : PipelineXSinkLocalState(parent, state),
              current_channel_idx(0),
              only_local_exchange(false),
              _serializer(this) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status serialize_block(vectorized::Block* src, PBlock* dest, int num_receivers = 1);
    void register_channels(pipeline::ExchangeSinkBuffer<ExchangeSinkLocalState>* buffer);
    bool channel_all_can_write();
    Status get_next_available_buffer(vectorized::BroadcastPBlockHolder** holder);

    RuntimeProfile::Counter* brpc_wait_timer() { return _brpc_wait_timer; }
    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* local_send_timer() { return _local_send_timer; }
    RuntimeProfile::Counter* local_bytes_send_counter() { return _local_bytes_send_counter; }
    RuntimeProfile::Counter* local_sent_rows() { return _local_sent_rows; }
    RuntimeProfile::Counter* brpc_send_timer() { return _brpc_send_timer; }
    RuntimeProfile::Counter* serialize_batch_timer() { return _serialize_batch_timer; }
    RuntimeProfile::Counter* split_block_distribute_by_channel_timer() {
        return _split_block_distribute_by_channel_timer;
    }
    RuntimeProfile::Counter* bytes_sent_counter() { return _bytes_sent_counter; }
    RuntimeProfile::Counter* split_block_hash_compute_timer() {
        return _split_block_hash_compute_timer;
    }
    RuntimeProfile::Counter* merge_block_timer() { return _merge_block_timer; }
    RuntimeProfile::Counter* compress_timer() { return _compress_timer; }
    RuntimeProfile::Counter* uncompressed_bytes_counter() { return _uncompressed_bytes_counter; }
    [[nodiscard]] bool transfer_large_data_by_brpc() const;

    [[nodiscard]] int sender_id() const { return _sender_id; }

    segment_v2::CompressionTypePB& compression_type();

    vectorized::VExprContextSPtrs partition_expr_ctxs;

    std::vector<vectorized::PipChannel<ExchangeSinkLocalState>*> channels;
    std::vector<std::shared_ptr<vectorized::PipChannel<ExchangeSinkLocalState>>>
            channel_shared_ptrs;
    int current_channel_idx; // index of current channel to send to if _random == true
    bool only_local_exchange;

private:
    friend class ExchangeSinkOperatorX;
    friend class vectorized::Channel<ExchangeSinkLocalState>;
    friend class vectorized::PipChannel<ExchangeSinkLocalState>;
    friend class vectorized::BlockSerializer<ExchangeSinkLocalState>;

    std::unique_ptr<ExchangeSinkBuffer<ExchangeSinkLocalState>> _sink_buffer;
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _compress_timer;
    RuntimeProfile::Counter* _brpc_send_timer;
    RuntimeProfile::Counter* _brpc_wait_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeProfile::Counter* _local_sent_rows;
    RuntimeProfile::Counter* _local_send_timer;
    RuntimeProfile::Counter* _split_block_hash_compute_timer;
    RuntimeProfile::Counter* _split_block_distribute_by_channel_timer;
    RuntimeProfile::Counter* _blocks_sent_counter;
    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput;
    // Used to counter send bytes under local data exchange
    RuntimeProfile::Counter* _local_bytes_send_counter;
    RuntimeProfile::Counter* _merge_block_timer;
    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _peak_memory_usage_counter;

    // Sender instance id, unique within a fragment.
    int _sender_id;
    std::vector<vectorized::BroadcastPBlockHolder> _broadcast_pb_blocks;
    int _broadcast_pb_block_idx;

    vectorized::BlockSerializer<ExchangeSinkLocalState> _serializer;
};

class ExchangeSinkOperatorX final : public DataSinkOperatorX {
public:
    ExchangeSinkOperatorX(RuntimeState* state, ObjectPool* pool, const RowDescriptor& row_desc,
                          const TDataStreamSink& sink,
                          const std::vector<TPlanFragmentDestination>& destinations,
                          bool send_query_statistics_with_every_batch,
                          PipelineXFragmentContext* context);
    ExchangeSinkOperatorX(ObjectPool* pool, const RowDescriptor& row_desc, PlanNodeId dest_node_id,
                          const std::vector<TPlanFragmentDestination>& destinations,
                          bool send_query_statistics_with_every_batch,
                          PipelineXFragmentContext* context);
    ExchangeSinkOperatorX(ObjectPool* pool, const RowDescriptor& row_desc,
                          bool send_query_statistics_with_every_batch,
                          PipelineXFragmentContext* context);
    Status init(const TDataSink& tsink) override;

    RuntimeState* state() { return _state; }

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    Status serialize_block(ExchangeSinkLocalState& stete, vectorized::Block* src, PBlock* dest,
                           int num_receivers = 1);

    Status close(RuntimeState* state) override;
    Status try_close(RuntimeState* state) override;
    bool can_write(RuntimeState* state) override;
    bool is_pending_finish(RuntimeState* state) const override;

private:
    friend class ExchangeSinkLocalState;

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st);

    Status get_partition_column_result(vectorized::Block* block, int* result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }

    template <typename Channels>
    Status channel_add_rows(RuntimeState* state, Channels& channels, int num_channels,
                            const uint64_t* channel_ids, int rows, vectorized::Block* block,
                            bool eos);
    RuntimeState* _state = nullptr;
    PipelineXFragmentContext* _context;

    ObjectPool* _pool;
    const RowDescriptor& _row_desc;

    TPartitionType::type _part_type;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PBlock _pb_block1;
    PBlock _pb_block2;
    PBlock* _cur_pb_block = nullptr;

    // compute per-row partition values
    vectorized::VExprContextSPtrs _partition_expr_ctxs;

    const std::vector<TPlanFragmentDestination> _dests;
    const bool _send_query_statistics_with_every_batch;

    std::unique_ptr<MemTracker> _mem_tracker;
    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    segment_v2::CompressionTypePB _compression_type;
};

} // namespace pipeline
} // namespace doris