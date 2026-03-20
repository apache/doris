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

#include "pipeline/exec/operator.h"

namespace doris::vectorized {
class PartitionerBase;
}

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
    ~LocalExchangeSinkLocalState() override;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const override;
    std::vector<Dependency*> dependencies() const override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class LocalExchangeSinkOperatorX;
    friend class ShuffleExchanger;
    friend class BucketShuffleExchanger;
    friend class PassthroughExchanger;
    friend class BroadcastExchanger;
    friend class PassToOneExchanger;
    friend class AdaptivePassthroughExchanger;
    template <typename BlockType>
    friend class Exchanger;

    ExchangerBase* _exchanger = nullptr;

    // Used by shuffle exchanger
    RuntimeProfile::Counter* _compute_hash_value_timer = nullptr;
    RuntimeProfile::Counter* _distribute_timer = nullptr;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner = nullptr;

    // Used by random passthrough exchanger
    int _channel_id = 0;
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
              _partitioned_exprs_num(texprs.size()),
              _shuffle_idx_to_instance_idx(bucket_seq_to_instance_idx) {}

    LocalExchangeSinkOperatorX(int operator_id, int dest_id, const TPlanNode& tnode,
                               int num_partitions,
                               const std::map<int, int>& shuffle_id_to_instance_idx)
            : Base(operator_id, tnode, dest_id),
              _type(tnode.local_exchange_node.partition_type),
              _num_partitions(num_partitions),
              _texprs(tnode.local_exchange_node.distribute_expr_lists),
              _partitioned_exprs_num(tnode.local_exchange_node.distribute_expr_lists.size()),
              _shuffle_idx_to_instance_idx(shuffle_id_to_instance_idx),
              _planned_by_fe(true) {}
#ifdef BE_TEST
    LocalExchangeSinkOperatorX(const std::vector<TExpr>& texprs,
                               const std::map<int, int>& bucket_seq_to_instance_idx)
            : Base(),
              _num_partitions(0),
              _texprs(texprs),
              _partitioned_exprs_num(texprs.size()),
              _shuffle_idx_to_instance_idx(bucket_seq_to_instance_idx) {}
#endif

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode", Base::_name);
    }

    Status init(RuntimeState* state, TLocalPartitionType::type type, const int num_buckets,
                const std::map<int, int>& shuffle_idx_to_instance_idx) override;

    // Initialize partitioner for FE-planned local exchange nodes. The FE-planned constructor
    // already sets _type, _num_partitions, _texprs, and _shuffle_idx_to_instance_idx from the
    // TPlanNode, but does not create the partitioner. This method creates the partitioner so
    // that prepare() can call _partitioner->prepare() without null dereference.
    Status init_partitioner(RuntimeState* state);

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        SCOPED_TIMER(local_state.exec_time_counter());
        local_state._shared_state->set_low_memory_mode(state);
        local_state._exchanger->set_low_memory_mode();
    }

private:
    friend class LocalExchangeSinkLocalState;
    friend class ShuffleExchanger;
    TLocalPartitionType::type _type;
    const int _num_partitions;
    const std::vector<TExpr>& _texprs;
    const size_t _partitioned_exprs_num;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::map<int, int> _shuffle_idx_to_instance_idx;
    const bool _planned_by_fe = false;
};

} // namespace doris::pipeline
