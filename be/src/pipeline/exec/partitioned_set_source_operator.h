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

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/set_source_operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {

template <bool is_intersect>
class PartitionedSetSourceOperatorX;

template <bool is_intersect>
class PartitionedSetSourceLocalState final
        : public PipelineXSpillLocalState<PartitionedSetSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedSetSourceLocalState);
    using Base = PipelineXSpillLocalState<PartitionedSetSharedState>;
    using Parent = PartitionedSetSourceOperatorX<is_intersect>;
    PartitionedSetSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {};
    Status init(RuntimeState* state, LocalStateInfo& infos) override;
    Status open(RuntimeState* state) override;

    Status do_partitioned_probe(RuntimeState* state);
    Status setup_internal_operators(RuntimeState* state);

    Status recovery_build_blocks(RuntimeState* state);

private:
    friend class PartitionedSetSourceOperatorX<is_intersect>;
    friend class OperatorX<PartitionedSetSourceLocalState<is_intersect>>;
    std::vector<vectorized::MutableColumnPtr> _mutable_cols;
    //record build column type
    vectorized::DataTypes _left_table_data_types;

    uint32_t _partition_index;
    const uint32_t _partition_count {32};
    uint32_t _partition_cursor {0};
    std::atomic_uint32_t _probe_child_cursor {0};
    bool _need_to_setup_internal_operators {true};

    AtomicStatus _spill_status;

    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    std::vector<std::unique_ptr<RuntimeState>> _probe_runtime_states;

    RuntimeProfile::Counter* _build_rows_counter {nullptr};
    std::map<uint32_t, RuntimeProfile::Counter*> _probe_rows_counter_map;
};

template <bool is_intersect>
class PartitionedSetSourceOperatorX final
        : public OperatorX<PartitionedSetSourceLocalState<is_intersect>> {
public:
    using Base = OperatorX<PartitionedSetSourceLocalState<is_intersect>>;
    // for non-delay template instantiation
    using OperatorXBase::operator_id;
    using Base::get_local_state;
    using typename Base::LocalState;

    PartitionedSetSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs),
              _child_quantity(tnode.node_type == TPlanNodeType::type::INTERSECT_NODE
                                      ? tnode.intersect_node.result_expr_lists.size()
                                      : tnode.except_node.result_expr_lists.size()) {};
    ~PartitionedSetSourceOperatorX() override = default;

    [[nodiscard]] Status init(const TPlanNode& tnode, RuntimeState* state) override;

    [[nodiscard]] Status prepare(RuntimeState* state) override;

    [[nodiscard]] Status open(RuntimeState* state) override;

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    void set_inner_source_operator(
            std::shared_ptr<SetSourceOperatorX<is_intersect>> inner_operator) {
        _inner_source_operator = std::move(inner_operator);
    }

    void set_inner_sink_operator(std::shared_ptr<SetSinkOperatorX<is_intersect>> inner_operator) {
        _inner_sink_operator = std::move(inner_operator);
    }

    void set_inner_probe_sink_operators(
            std::vector<std::shared_ptr<SetProbeSinkOperatorX<is_intersect>>>&& operators) {
        _inner_probe_sink_operators = std::move(operators);
    }

private:
    friend class PartitionedSetSourceLocalState<is_intersect>;
    const int _child_quantity;

    std::shared_ptr<SetSourceOperatorX<is_intersect>> _inner_source_operator;
    std::shared_ptr<SetSinkOperatorX<is_intersect>> _inner_sink_operator;
    std::vector<std::shared_ptr<SetProbeSinkOperatorX<is_intersect>>> _inner_probe_sink_operators;
};

} // namespace pipeline
} // namespace doris
