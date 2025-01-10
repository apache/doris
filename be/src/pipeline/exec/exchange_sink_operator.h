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

#include <atomic>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "exchange_sink_buffer.h"
#include "operator.h"
#include "pipeline/shuffle/writer.h"
#include "vec/sink/scale_writer_partitioning_exchanger.hpp"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
class TDataSink;

namespace pipeline {

using ChannelId = int;
using ChannelIds = std::vector<ChannelId>;
class ChannelSelector {
public:
    virtual ~ChannelSelector() = default;
    virtual ChannelIds& next_channel_ids() = 0;
    virtual void process_next_block(size_t data_size) = 0;
    virtual void reset_selected_channel() {}
    virtual void select_channel(ChannelId idx) {}
};

class AllChannelsSelector final : public ChannelSelector {
public:
    AllChannelsSelector(size_t num_channels) : ChannelSelector(), _all_channel_ids(num_channels) {
        for (int i = 0; i < num_channels; i++) {
            _all_channel_ids[i] = i;
        }
    }
    ~AllChannelsSelector() override = default;
    ChannelIds& next_channel_ids() override { return _all_channel_ids; }
    void process_next_block(size_t data_size) override {}

private:
    ChannelIds _all_channel_ids;
};

class SelectedChannelsSelector final : public ChannelSelector {
public:
    SelectedChannelsSelector(size_t num_channels)
            : ChannelSelector(), _selected_channel_ids(num_channels) {}
    ~SelectedChannelsSelector() override = default;
    ChannelIds& next_channel_ids() override { return _selected_channel_ids; }
    void process_next_block(size_t data_size) override {}
    void reset_selected_channel() override { _selected_channel_ids.clear(); }
    void select_channel(ChannelId idx) override { _selected_channel_ids.push_back(idx); }

private:
    ChannelIds _selected_channel_ids;
};

class RoundRobinSelector final : public ChannelSelector {
public:
    RoundRobinSelector(const size_t num_channels)
            : ChannelSelector(), _next_channel_ids(1, -1), _num_channels(num_channels) {}
    ~RoundRobinSelector() override = default;
    ChannelIds& next_channel_ids() override { return _next_channel_ids; }

    void process_next_block(size_t data_size) override {
        _next_channel_ids[0] = (_next_channel_ids[0] + 1) % _num_channels;
    }

private:
    ChannelIds _next_channel_ids;
    const size_t _num_channels;
};

class TableSinkRandomSelector final : public ChannelSelector {
public:
    TableSinkRandomSelector(const size_t num_channels)
            : ChannelSelector(), _next_channel_ids(1, -1), _num_channels(num_channels) {}
    ~TableSinkRandomSelector() override = default;
    ChannelIds& next_channel_ids() override { return _next_channel_ids; }
    void process_next_block(size_t data_size) override {
        _data_processed += data_size;
        _next_channel_ids[0] = (_next_channel_ids[0] + 1) % _writer_count;
        if (_writer_count < _num_channels) {
            if (_data_processed >=
                _writer_count *
                        config::table_sink_non_partition_write_scaling_data_processed_threshold) {
                _writer_count++;
            }
        }
    }

private:
    ChannelIds _next_channel_ids;
    const size_t _num_channels;
    int _writer_count = 1;
    size_t _data_processed = 0;
};

class ExchangeSinkLocalState final : public PipelineXSinkLocalState<> {
    ENABLE_FACTORY_CREATOR(ExchangeSinkLocalState);
    using Base = PipelineXSinkLocalState<>;

public:
    ExchangeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _serializer(this) {
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", false);
    }

#ifdef BE_TEST
    ExchangeSinkLocalState(RuntimeState* state) : Base(nullptr, state) {
        _profile = state->obj_pool()->add(new RuntimeProfile("mock"));
        _memory_used_counter =
                _profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
    }
#endif

    std::vector<Dependency*> dependencies() const override {
        std::vector<Dependency*> dep_vec;
        if (_queue_dependency) {
            dep_vec.push_back(_queue_dependency.get());
        }
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
    void register_channels(pipeline::ExchangeSinkBuffer* buffer);

    RuntimeProfile::Counter* blocks_sent_counter() { return _blocks_sent_counter; }
    RuntimeProfile::Counter* local_send_timer() { return _local_send_timer; }
    RuntimeProfile::Counter* local_bytes_send_counter() { return _local_bytes_send_counter; }
    RuntimeProfile::Counter* local_sent_rows() { return _local_sent_rows; }
    RuntimeProfile::Counter* merge_block_timer() { return _merge_block_timer; }
    [[nodiscard]] bool transfer_large_data_by_brpc() const;
    bool is_finished() const override { return _reach_limit.load(); }
    void set_reach_limit() { _reach_limit = true; };

    // sender_id indicates which instance within a fragment, while be_number indicates which instance
    // across all fragments. For example, with 3 BEs and 8 instances, the range of sender_id would be 0 to 24,
    // and the range of be_number would be from n + 0 to n + 24.
    // Since be_number is a required field, it still needs to be set for compatibility with older code.
    [[nodiscard]] int sender_id() const { return _sender_id; }
    [[nodiscard]] int be_number() const { return _state->be_number(); }

    std::string name_suffix() override;
    segment_v2::CompressionTypePB compression_type() const;
    std::string debug_string(int indentation_level) const override;
    RuntimeProfile::Counter* send_new_partition_timer() { return _send_new_partition_timer; }
    RuntimeProfile::Counter* add_partition_request_timer() { return _add_partition_request_timer; }
    RuntimeProfile::Counter* split_block_hash_compute_timer() {
        return _split_block_hash_compute_timer;
    }
    RuntimeProfile::Counter* distribute_rows_into_channels_timer() {
        return _distribute_rows_into_channels_timer;
    }
    std::vector<std::shared_ptr<vectorized::Channel>> channels;
    bool only_local_exchange = false;
    int channel_id() const { return _channel_id; }
    ChannelSelector* channel_selector() { return _channel_selector.get(); }
    RuntimeProfile::Counter* copy_shuffled_data_timer() { return _copy_shuffled_data_timer; }

    void on_channel_finished(InstanceLoId channel_id);
    vectorized::PartitionerBase* partitioner() const { return _partitioner.get(); }
    ExchangerBase* exchanger() { return _exchanger.get(); }
    RuntimeProfile::Counter* enqueue_blocks_counter() { return _enqueue_blocks_counter; }
    RuntimeProfile::Counter* dequeue_blocks_counter() { return _dequeue_blocks_counter; }
    RuntimeProfile::Counter* get_block_failed_counter() { return _get_block_failed_counter; }
    TPartitionType::type& part_type() { return _part_type; }

private:
    friend class ExchangeSinkOperatorX;
    friend class vectorized::Channel;
    friend class vectorized::BlockSerializer;

    std::shared_ptr<ExchangeSinkBuffer> _sink_buffer = nullptr;
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _compress_timer = nullptr;
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    RuntimeProfile::Counter* _uncompressed_bytes_counter = nullptr;
    RuntimeProfile::Counter* _local_sent_rows = nullptr;
    RuntimeProfile::Counter* _local_send_timer = nullptr;
    RuntimeProfile::Counter* _split_block_hash_compute_timer = nullptr;
    RuntimeProfile::Counter* _distribute_rows_into_channels_timer = nullptr;
    RuntimeProfile::Counter* _blocks_sent_counter = nullptr;
    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput = nullptr;
    // Used to counter send bytes under local data exchange
    RuntimeProfile::Counter* _local_bytes_send_counter = nullptr;
    RuntimeProfile::Counter* _merge_block_timer = nullptr;
    RuntimeProfile::Counter* _send_new_partition_timer = nullptr;

    RuntimeProfile::Counter* _wait_queue_timer = nullptr;
    RuntimeProfile::Counter* _wait_broadcast_buffer_timer = nullptr;
    std::vector<RuntimeProfile::Counter*> _wait_channel_timer;
    RuntimeProfile::Counter* _copy_shuffled_data_timer = nullptr;
    RuntimeProfile::Counter* _enqueue_blocks_counter = nullptr;
    RuntimeProfile::Counter* _dequeue_blocks_counter = nullptr;
    RuntimeProfile::Counter* _get_block_failed_counter = nullptr;

    RuntimeProfile::Counter* _test_timer1 = nullptr;
    RuntimeProfile::Counter* _test_timer2 = nullptr;
    RuntimeProfile::Counter* _test_timer3 = nullptr;
    RuntimeProfile::Counter* _test_timer4 = nullptr;
    RuntimeProfile::Counter* _test_timer5 = nullptr;

    // Sender instance id, unique within a fragment.
    int _sender_id;
    std::shared_ptr<vectorized::BroadcastPBlockHolderMemLimiter> _broadcast_pb_mem_limiter;

    size_t _rpc_channels_num = 0;
    vectorized::BlockSerializer _serializer;

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
    size_t _partition_count;

    std::shared_ptr<Dependency> _finish_dependency;

    // for shuffle data by partition and tablet

    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    TPartitionType::type _part_type;

    std::atomic<bool> _reach_limit = false;

    std::atomic_int _working_channels_count = 0;
    std::set<InstanceLoId> _finished_channels;
    std::mutex _finished_channels_mutex;
    std::unique_ptr<ChannelSelector> _channel_selector;
    int _channel_id = 0;
    std::shared_ptr<ExchangerBase> _exchanger = nullptr;
    bool _could_be_moved = false;
    bool _init_block = false;
};

class ExchangeSinkOperatorX final : public DataSinkOperatorX<ExchangeSinkLocalState> {
public:
    ExchangeSinkOperatorX(RuntimeState* state, const RowDescriptor& row_desc, int operator_id,
                          const TDataStreamSink& sink,
                          const std::vector<TPlanFragmentDestination>& destinations,
                          const std::vector<TUniqueId>& fragment_instance_ids);
    Status init(const TDataSink& tsink) override;

    RuntimeState* state() { return _state; }

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override;
    bool is_serial_operator() const override { return true; }

    // For a normal shuffle scenario, if the concurrency is n,
    // there can be up to n * n RPCs in the current fragment.
    // Therefore, a shared sink buffer is used here to limit the number of concurrent RPCs.
    // (Note: This does not reduce the total number of RPCs.)
    // In a merge sort scenario, there are only n RPCs, so a shared sink buffer is not needed.
    std::shared_ptr<ExchangeSinkBuffer> get_sink_buffer(InstanceLoId sender_ins_id);
    vectorized::VExprContextSPtrs& tablet_sink_expr_ctxs() { return _tablet_sink_expr_ctxs; }
    bool is_broadcast() const {
        return _part_type == TPartitionType::UNPARTITIONED || _dests.size() == 1;
    }

private:
    friend class ExchangeSinkLocalState;
    // Use ExchangeSinkOperatorX to create a sink buffer.
    // The sink buffer can be shared among multiple ExchangeSinkLocalState instances,
    // or each ExchangeSinkLocalState can have its own sink buffer.
    std::shared_ptr<ExchangeSinkBuffer> _create_buffer(
            const std::vector<InstanceLoId>& sender_ins_ids);
    std::shared_ptr<ExchangeSinkBuffer> _sink_buffer = nullptr;
    RuntimeState* _state = nullptr;

    const std::vector<TExpr> _texprs;

    const RowDescriptor& _row_desc;
    TTupleId _output_tuple_id = -1;

    TPartitionType::type _part_type;

    const std::vector<TPlanFragmentDestination> _dests;

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
    const int64_t _tablet_sink_txn_id = -1;
    std::shared_ptr<ObjectPool> _pool;
    vectorized::VExprContextSPtrs _tablet_sink_expr_ctxs;
    const std::vector<TExpr>* _t_tablet_sink_exprs = nullptr;

    const bool _enable_local_merge_sort;
    // If dest_is_merge is true, it indicates that the corresponding receiver is a VMERGING-EXCHANGE.
    // The receiver will sort the collected data, so the sender must ensure that the data sent is ordered.
    const bool _dest_is_merge;
    const std::vector<TUniqueId>& _fragment_instance_ids;
    std::unique_ptr<Writer> _writer = nullptr;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
