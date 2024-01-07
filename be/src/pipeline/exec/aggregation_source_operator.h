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
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class AggSourceOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    AggSourceOperatorBuilder(int32_t, ExecNode*);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class AggSourceOperator final : public SourceOperator<vectorized::AggregationNode> {
public:
    AggSourceOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
};

class AggSourceDependency final : public Dependency {
public:
    using SharedState = AggSharedState;
    AggSourceDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "AggSourceDependency", query_ctx) {}
    ~AggSourceDependency() override = default;

    void block() override {
        if (_is_streaming_agg_state()) {
            Dependency::block();
        }
    }

private:
    bool _is_streaming_agg_state() {
        return ((SharedState*)Dependency::_shared_state.get())->data_queue != nullptr;
    }
};

class AggSourceOperatorX;

class AggLocalState final : public PipelineXLocalState<AggSourceDependency> {
public:
    using Base = PipelineXLocalState<AggSourceDependency>;
    ENABLE_FACTORY_CREATOR(AggLocalState);
    AggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~AggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

    void make_nullable_output_key(vectorized::Block* block);

protected:
    friend class AggSourceOperatorX;
    friend class StreamingAggSourceOperatorX;
    friend class StreamingAggSinkOperatorX;
    friend class DistinctStreamingAggSourceOperatorX;
    friend class DistinctStreamingAggSinkOperatorX;

    Status _get_without_key_result(RuntimeState* state, vectorized::Block* block,
                                   SourceState& source_state);
    Status _serialize_without_key(RuntimeState* state, vectorized::Block* block,
                                  SourceState& source_state);
    Status _get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                           SourceState& source_state);
    Status _serialize_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                                 SourceState& source_state);
    Status _get_result_with_serialized_key_non_spill(RuntimeState* state, vectorized::Block* block,
                                                     SourceState& source_state);
    Status _get_result_with_spilt_data(RuntimeState* state, vectorized::Block* block,
                                       SourceState& source_state);

    Status _serialize_with_serialized_key_result_non_spill(RuntimeState* state,
                                                           vectorized::Block* block,
                                                           SourceState& source_state);
    Status _serialize_with_serialized_key_result_with_spilt_data(RuntimeState* state,
                                                                 vectorized::Block* block,
                                                                 SourceState& source_state);
    Status _destroy_agg_status(vectorized::AggregateDataPtr data);
    Status _reset_hash_table();
    Status _merge_spilt_data();
    void _make_nullable_output_key(vectorized::Block* block) {
        if (block->rows() != 0) {
            auto& shared_state = *Base ::_shared_state;
            for (auto cid : shared_state.make_nullable_keys) {
                block->get_by_position(cid).column =
                        make_nullable(block->get_by_position(cid).column);
                block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
            }
        }
    }

    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _serialize_result_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _serialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;

    using vectorized_get_result = std::function<Status(
            RuntimeState* state, vectorized::Block* block, SourceState& source_state)>;

    struct executor {
        vectorized_get_result get_result;
    };

    executor _executor;

    vectorized::AggregatedDataVariants* _agg_data = nullptr;
};

class AggSourceOperatorX : public OperatorX<AggLocalState> {
public:
    using Base = OperatorX<AggLocalState>;
    AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs, bool is_streaming = false);
    ~AggSourceOperatorX() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class AggLocalState;
    const bool _is_streaming;

    bool _needs_finalize;
    bool _without_key;

    // left / full join will change the key nullable make output/input solt
    // nullable diff. so we need make nullable of it.
    std::vector<size_t> _make_nullable_keys;
};

} // namespace pipeline
} // namespace doris
