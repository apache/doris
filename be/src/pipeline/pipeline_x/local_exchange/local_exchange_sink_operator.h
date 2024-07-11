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

#include "pipeline/pipeline_x/operator.h"

namespace doris::pipeline {

class ExchangerBase;
class ShuffleExchanger;
class PassthroughExchanger;
class BroadcastExchanger;
class PassToOneExchanger;
class LocalExchangeSinkOperatorX;
class LocalExchangeSinkLocalState final : public PipelineXSinkLocalState<LocalExchangeSharedState> {
public:
    using Base = PipelineXSinkLocalState<LocalExchangeSharedState>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSinkLocalState);

    LocalExchangeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~LocalExchangeSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    std::string debug_string(int indentation_level) const override;

private:
    friend class LocalExchangeSinkOperatorX;
    friend class ShuffleExchanger;
    friend class BucketShuffleExchanger;
    friend class PassthroughExchanger;
    friend class BroadcastExchanger;
    friend class PassToOneExchanger;
    friend class AdaptivePassthroughExchanger;

    ExchangerBase* _exchanger = nullptr;

    // Used by shuffle exchanger
    RuntimeProfile::Counter* _compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* _distribute_timer = nullptr;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner = nullptr;
    std::vector<size_t> _partition_rows_histogram;

    // Used by random passthrough exchanger
    int _channel_id = 0;
    bool _release_count = false;
};

// A single 32-bit division on a recent x64 processor has a throughput of one instruction every six cycles with a latency of 26 cycles.
// In contrast, a multiplication has a throughput of one instruction every cycle and a latency of 3 cycles.
// So we prefer to this algorithm instead of modulo.
// Reference: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
struct LocalExchangeChannelIds {
    static constexpr auto SHIFT_BITS = 32;
    uint32_t operator()(uint32_t l, uint32_t r) {
        return ((uint64_t)l * (uint64_t)r) >> SHIFT_BITS;
    }
};

class LocalExchangeSinkOperatorX final : public DataSinkOperatorX<LocalExchangeSinkLocalState> {
public:
    using Base = DataSinkOperatorX<LocalExchangeSinkLocalState>;
    LocalExchangeSinkOperatorX(int sink_id, int dest_id, int num_partitions,
                               const std::vector<TExpr>& texprs,
                               const std::map<int, int>& bucket_seq_to_instance_idx)
            : Base(sink_id, dest_id, dest_id),
              _num_partitions(num_partitions),
              _texprs(texprs),
              _bucket_seq_to_instance_idx(bucket_seq_to_instance_idx) {}

    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(ExchangeType type, const int num_buckets, const bool is_shuffled_hash_join,
                const std::map<int, int>& shuffle_idx_to_instance_idx) override {
        _name = "LOCAL_EXCHANGE_SINK_OPERATOR (" + get_exchange_type_name(type) + ")";
        _type = type;
        if (_type == ExchangeType::HASH_SHUFFLE) {
            // For shuffle join, if data distribution has been broken by previous operator, we
            // should use a HASH_SHUFFLE local exchanger to shuffle data again. To be mentioned,
            // we should use map shuffle idx to instance idx because all instances will be
            // distributed to all BEs. Otherwise, we should use shuffle idx directly.
            if (is_shuffled_hash_join) {
                std::for_each(shuffle_idx_to_instance_idx.begin(),
                              shuffle_idx_to_instance_idx.end(), [&](const auto& item) {
                                  DCHECK(item.first != -1);
                                  _shuffle_idx_to_instance_idx.push_back({item.first, item.second});
                              });
            } else {
                _shuffle_idx_to_instance_idx.resize(_num_partitions);
                for (int i = 0; i < _num_partitions; i++) {
                    _shuffle_idx_to_instance_idx[i] = {i, i};
                }
            }
            _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                    _num_partitions));
            RETURN_IF_ERROR(_partitioner->init(_texprs));
        } else if (_type == ExchangeType::BUCKET_HASH_SHUFFLE) {
            _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                    num_buckets));
            RETURN_IF_ERROR(_partitioner->init(_texprs));
        }

        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        if (_type == ExchangeType::HASH_SHUFFLE || _type == ExchangeType::BUCKET_HASH_SHUFFLE) {
            RETURN_IF_ERROR(_partitioner->prepare(state, _child_x->row_desc()));
        }

        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        if (_type == ExchangeType::HASH_SHUFFLE || _type == ExchangeType::BUCKET_HASH_SHUFFLE) {
            RETURN_IF_ERROR(_partitioner->open(state));
        }

        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class LocalExchangeSinkLocalState;
    friend class ShuffleExchanger;
    ExchangeType _type;
    const int _num_partitions;
    const std::vector<TExpr>& _texprs;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    const std::map<int, int> _bucket_seq_to_instance_idx;
    std::vector<std::pair<int, int>> _shuffle_idx_to_instance_idx;
};

} // namespace doris::pipeline
