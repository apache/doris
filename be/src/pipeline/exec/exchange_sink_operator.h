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
#include "vec/sink/scale_writer_partitioning_exchanger.hpp"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
class RuntimeState;
class TDataSink;

namespace pipeline {

class ExchangeSinkLocalState final : public PipelineXSinkLocalState<> {
    ENABLE_FACTORY_CREATOR(ExchangeSinkLocalState);
    using Base = PipelineXSinkLocalState<>;

private:
    class HashPartitionFunction {
    public:
        HashPartitionFunction(vectorized::PartitionerBase* partitioner)
                : _partitioner(partitioner) {}

        int get_partition(vectorized::Block* block, int position) {
            return _partitioner->get_channel_ids().get<uint32_t>()[position];
        }

    private:
        vectorized::PartitionerBase* _partitioner;
    };

public:
    ExchangeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state),
              current_channel_idx(0),
              only_local_exchange(false),
              _serializer(this) {
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", true);
    }

    std::vector<Dependency*> dependencies() const override {
        std::vector<Dependency*> dep_vec;
        dep_vec.push_back(_queue_dependency.get());
        if (_broadcast_dependency) {
            dep_vec.push_back(_broadcast_dependency.get());
        }
        std::for_each(_local_channels_dependency.begin(), _local_channels_dependency.end(),
                      [&](std::shared_ptr<Dependency> dep) { dep_vec.push_back(dep.get()); });
        return dep_vec;
    }
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    Status serialize_block(vectorized::Block* src, PBlock* dest, int num_receivers = 1);
    void register_channels(pipeline::ExchangeSinkBuffer* buffer);

    RuntimeProfile::Counter* brpc_wait_timer() { return _brpc_wait_timer; }
    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* rows_sent_counter() { return _rows_sent_counter; }
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
    bool is_finished() const override { return _reach_limit.load(); }
    void set_reach_limit() { _reach_limit = true; };

    [[nodiscard]] int sender_id() const { return _sender_id; }

    std::string name_suffix() override;
    segment_v2::CompressionTypePB compression_type() const;
    std::string debug_string(int indentation_level) const override;
    static Status empty_callback_function(void* sender, TCreatePartitionResult* result) {
        return Status::OK();
    }
    Status _send_new_partition_batch();
    std::vector<vectorized::PipChannel*> channels;
    std::vector<std::shared_ptr<vectorized::PipChannel>> channel_shared_ptrs;
    int current_channel_idx; // index of current channel to send to if _random == true
    bool only_local_exchange;

    // for external table sink hash partition
    std::unique_ptr<vectorized::ScaleWriterPartitioningExchanger<HashPartitionFunction>>
            scale_writer_partitioning_exchanger;

private:
    friend class ExchangeSinkOperatorX;
    friend class vectorized::Channel<ExchangeSinkLocalState>;
    friend class vectorized::PipChannel;
    friend class vectorized::BlockSerializer<ExchangeSinkLocalState>;

    std::unique_ptr<ExchangeSinkBuffer> _sink_buffer = nullptr;
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _brpc_send_timer = nullptr;
    RuntimeProfile::Counter* _brpc_wait_timer = nullptr;
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    RuntimeProfile::Counter* _uncompressed_bytes_counter = nullptr;
    RuntimeProfile::Counter* _local_sent_rows = nullptr;
    RuntimeProfile::Counter* _local_send_timer = nullptr;
    RuntimeProfile::Counter* _split_block_hash_compute_timer = nullptr;
    RuntimeProfile::Counter* _split_block_distribute_by_channel_timer = nullptr;
    RuntimeProfile::Counter* _blocks_sent_counter = nullptr;
    RuntimeProfile::Counter* _rows_sent_counter = nullptr;
    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput = nullptr;
    // Used to counter send bytes under local data exchange
    RuntimeProfile::Counter* _local_bytes_send_counter = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;

    RuntimeProfile::Counter* _wait_queue_timer = nullptr;
    RuntimeProfile::Counter* _wait_broadcast_buffer_timer = nullptr;
    std::vector<RuntimeProfile::Counter*> _wait_channel_timer;

    // Sender instance id, unique within a fragment.
    int _sender_id;
    std::shared_ptr<vectorized::BroadcastPBlockHolderMemLimiter> _broadcast_pb_mem_limiter;

    vectorized::BlockSerializer<ExchangeSinkLocalState> _serializer;

    std::shared_ptr<Dependency> _queue_dependency = nullptr;
    std::shared_ptr<Dependency> _broadcast_dependency = nullptr;

    /**
     * We use this to control the execution for local exchange.
     *              +---------------+                                    +---------------+                               +---------------+
     *              | ExchangeSink1 |                                    | ExchangeSink2 |                               | ExchangeSink3 |
     *              +---------------+                                    +---------------+                               +---------------+
     *                     |                                                    |                                               |
     *                     |                       +----------------------------+----------------------------------+            |
     *                     +----+------------------|------------------------------------------+                    |            |
     *                          |                  |                 +------------------------|--------------------|------------+-----+
     *          Dependency 1-1  |   Dependency 2-1 |  Dependency 3-1 |         Dependency 1-2 |    Dependency 2-2  |  Dependency 3-2  |
     *                    +----------------------------------------------+               +----------------------------------------------+
     *                    |  queue1              queue2          queue3  |               |  queue1              queue2          queue3  |
     *                    |                   LocalRecvr                 |               |                   LocalRecvr                 |
     *                    +----------------------------------------------+               +----------------------------------------------+
     *                         +-----------------+                                                        +------------------+
     *                         | ExchangeSource1 |                                                        | ExchangeSource2 |
     *                         +-----------------+                                                        +------------------+
     */
    std::vector<std::shared_ptr<Dependency>> _local_channels_dependency;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    int _partition_count;

    std::shared_ptr<Dependency> _finish_dependency;

    // for shuffle data by partition and tablet
    int64_t _txn_id = -1;
    vectorized::VExprContextSPtrs _fake_expr_ctxs;
    std::unique_ptr<VOlapTablePartitionParam> _vpartition = nullptr;
    std::unique_ptr<vectorized::OlapTabletFinder> _tablet_finder = nullptr;
    std::shared_ptr<OlapTableSchemaParam> _schema = nullptr;
    std::unique_ptr<vectorized::OlapTableBlockConvertor> _block_convertor = nullptr;
    TupleDescriptor* _tablet_sink_tuple_desc = nullptr;
    RowDescriptor* _tablet_sink_row_desc = nullptr;
    OlapTableLocationParam* _location = nullptr;
    vectorized::VRowDistribution _row_distribution;
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    std::vector<vectorized::RowPartTabletIds> _row_part_tablet_ids;
    int64_t _number_input_rows = 0;
    TPartitionType::type _part_type;

    // for external table sink hash partition
    std::unique_ptr<HashPartitionFunction> _partition_function = nullptr;
    std::atomic<bool> _reach_limit = false;
};

class ExchangeSinkOperatorX final : public DataSinkOperatorX<ExchangeSinkLocalState> {
public:
    ExchangeSinkOperatorX(RuntimeState* state, const RowDescriptor& row_desc, int operator_id,
                          const TDataStreamSink& sink,
                          const std::vector<TPlanFragmentDestination>& destinations);
    Status init(const TDataSink& tsink) override;

    RuntimeState* state() { return _state; }

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    Status serialize_block(ExchangeSinkLocalState& stete, vectorized::Block* src, PBlock* dest,
                           int num_receivers = 1);
    DataDistribution required_data_distribution() const override;

private:
    friend class ExchangeSinkLocalState;

    template <typename ChannelPtrType>
    void _handle_eof_channel(RuntimeState* state, ChannelPtrType channel, Status st);

    template <typename Channels, typename HashValueType>
    Status channel_add_rows(RuntimeState* state, Channels& channels, int num_channels,
                            const HashValueType* channel_ids, int rows, vectorized::Block* block,
                            bool eos);

    template <typename Channels>
    Status channel_add_rows_with_idx(RuntimeState* state, Channels& channels, int num_channels,
                                     std::vector<std::vector<uint32_t>>& channel2rows,
                                     vectorized::Block* block, bool eos);
    RuntimeState* _state = nullptr;

    const std::vector<TExpr> _texprs;

    const RowDescriptor& _row_desc;

    TPartitionType::type _part_type;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PBlock _pb_block1;
    PBlock _pb_block2;

    const std::vector<TPlanFragmentDestination> _dests;

    std::unique_ptr<MemTracker> _mem_tracker;
    // Identifier of the destination plan node.
    const PlanNodeId _dest_node_id;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    segment_v2::CompressionTypePB _compression_type;

    // for tablet sink shuffle
    const TOlapTableSchemaParam _tablet_sink_schema;
    const TOlapTablePartitionParam _tablet_sink_partition;
    const TOlapTableLocationParam _tablet_sink_location;
    const TTupleId _tablet_sink_tuple_id;
    int64_t _tablet_sink_txn_id = -1;
    std::shared_ptr<ObjectPool> _pool;

    // for external table sink random partition
    // Control the number of channels according to the flow, thereby controlling the number of table sink writers.
    size_t _data_processed = 0;
    int _writer_count = 1;
    const bool _enable_local_merge_sort;
};

} // namespace pipeline
} // namespace doris
