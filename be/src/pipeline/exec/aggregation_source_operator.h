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

class AggSourceOperator final : public SourceOperator<AggSourceOperatorBuilder> {
public:
    AggSourceOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
};

class AggSourceOperatorX;

class AggLocalState final : public PipelineXLocalState<AggDependency> {
public:
    using Base = PipelineXLocalState<AggDependency>;
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

    void _close_without_key();
    void _close_with_serialized_key();
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

    RuntimeProfile::Counter* _get_results_timer;
    RuntimeProfile::Counter* _serialize_result_timer;
    RuntimeProfile::Counter* _hash_table_iterate_timer;
    RuntimeProfile::Counter* _insert_keys_to_column_timer;
    RuntimeProfile::Counter* _serialize_data_timer;
    RuntimeProfile::Counter* _hash_table_size_counter;

    using vectorized_get_result = std::function<Status(
            RuntimeState* state, vectorized::Block* block, SourceState& source_state)>;
    using vectorized_closer = std::function<void()>;

    struct executor {
        vectorized_get_result get_result;
        vectorized_closer close;
    };

    executor _executor;

    vectorized::AggregatedDataVariants* _agg_data;
    bool _agg_data_created_without_key = false;
};

class AggSourceOperatorX : public OperatorX<AggLocalState> {
public:
    using Base = OperatorX<AggLocalState>;
    AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggSourceOperatorX() = default;
    DependencyResult wait_for_dependency(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class AggLocalState;

    bool _needs_finalize;
    bool _without_key;

    // left / full join will change the key nullable make output/input solt
    // nullable diff. so we need make nullable of it.
    std::vector<size_t> _make_nullable_keys;
};

} // namespace pipeline
} // namespace doris
