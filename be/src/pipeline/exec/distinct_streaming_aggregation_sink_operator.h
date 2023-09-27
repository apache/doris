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

#include <cstdint>
#include <memory>

#include "aggregation_sink_operator.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
class DataQueue;

class DistinctStreamingAggSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::DistinctAggregationNode> {
public:
    DistinctStreamingAggSinkOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; }
    bool is_source() const override { return false; }

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class DistinctStreamingAggSinkOperator final
        : public StreamingOperator<DistinctStreamingAggSinkOperatorBuilder> {
public:
    DistinctStreamingAggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode*,
                                     std::shared_ptr<DataQueue>);

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    bool can_write() override;

    Status close(RuntimeState* state) override;

    bool reached_limited_rows() {
        return _node->limit() != -1 && _output_distinct_rows >= _node->limit();
    }

private:
    int64_t _output_distinct_rows = 0;
    std::shared_ptr<DataQueue> _data_queue;
    std::unique_ptr<vectorized::Block> _output_block = vectorized::Block::create_unique();
};

class DistinctStreamingAggSinkOperatorX;

class DistinctStreamingAggSinkLocalState final
        : public AggSinkLocalState<AggDependency, DistinctStreamingAggSinkLocalState> {
public:
    using Parent = DistinctStreamingAggSinkOperatorX;
    using Base = AggSinkLocalState<AggDependency, DistinctStreamingAggSinkLocalState>;
    ENABLE_FACTORY_CREATOR(DistinctStreamingAggSinkLocalState);
    DistinctStreamingAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        _shared_state->data_queue.reset(new DataQueue(1, _dependency));
        return Status::OK();
    }

    Status close(RuntimeState* state, Status exec_status) override;
    Status _distinct_pre_agg_with_serialized_key(vectorized::Block* in_block,
                                                 vectorized::Block* out_block);

private:
    friend class DistinctStreamingAggSinkOperatorX;
    void _emplace_into_hash_table_to_distinct(vectorized::IColumn::Selector& distinct_row,
                                              vectorized::ColumnRawPtrs& key_columns,
                                              const size_t num_rows);

    std::unique_ptr<vectorized::Block> _output_block = vectorized::Block::create_unique();
    std::shared_ptr<char> dummy_mapped_data = nullptr;
    vectorized::IColumn::Selector _distinct_row;
    vectorized::Arena _arena;
    int64_t _output_distinct_rows = 0;
};

class DistinctStreamingAggSinkOperatorX final
        : public AggSinkOperatorX<DistinctStreamingAggSinkLocalState> {
public:
    DistinctStreamingAggSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                      const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    WriteDependency* wait_for_dependency(RuntimeState* state) override {
        CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
        return local_state._dependency->write_blocked_by();
    }
};

} // namespace pipeline
} // namespace doris
