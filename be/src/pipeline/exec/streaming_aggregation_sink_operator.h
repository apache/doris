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

#include "aggregation_sink_operator.h"
#include "aggregation_source_operator.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
class DataQueue;

class StreamingAggSinkOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    StreamingAggSinkOperatorBuilder(int32_t, ExecNode*, std::shared_ptr<DataQueue>);

    OperatorPtr build_operator() override;

    bool is_sink() const override { return true; }
    bool is_source() const override { return false; }

private:
    std::shared_ptr<DataQueue> _data_queue;
};

class StreamingAggSinkOperator final : public StreamingOperator<StreamingAggSinkOperatorBuilder> {
public:
    StreamingAggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode*,
                             std::shared_ptr<DataQueue>);

    Status prepare(RuntimeState*) override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    bool can_write() override;

    Status close(RuntimeState* state) override;

private:
    vectorized::Block _preagg_block = vectorized::Block();

    RuntimeProfile::Counter* _queue_byte_size_counter;
    RuntimeProfile::Counter* _queue_size_counter;

    std::shared_ptr<DataQueue> _data_queue;
};

class StreamingAggSinkOperatorX;

class StreamingAggSinkLocalState final
        : public AggSinkLocalState<AggDependency, StreamingAggSinkLocalState> {
public:
    using Parent = StreamingAggSinkOperatorX;
    using Base = AggSinkLocalState<AggDependency, StreamingAggSinkLocalState>;
    ENABLE_FACTORY_CREATOR(StreamingAggSinkLocalState);
    StreamingAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~StreamingAggSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Status do_pre_agg(vectorized::Block* input_block, vectorized::Block* output_block);

private:
    friend class StreamingAggSinkOperatorX;

    Status _pre_agg_with_serialized_key(doris::vectorized::Block* in_block,
                                        doris::vectorized::Block* out_block);
    bool _should_expand_preagg_hash_tables();

    vectorized::Block _preagg_block = vectorized::Block();

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;

    RuntimeProfile::Counter* _queue_byte_size_counter;
    RuntimeProfile::Counter* _queue_size_counter;
    RuntimeProfile::Counter* _streaming_agg_timer;

    bool _should_expand_hash_table = true;
    int64_t _num_rows_returned = 0;
};

class StreamingAggSinkOperatorX final : public AggSinkOperatorX<StreamingAggSinkLocalState> {
public:
    StreamingAggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                              const DescriptorTbl& descs);
    ~StreamingAggSinkOperatorX() override = default;
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
