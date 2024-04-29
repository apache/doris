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

#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/common/sort/partition_sorter.h"

namespace doris::pipeline {

class PartitionSortSinkOperatorX;
class PartitionSortSinkLocalState : public PipelineXSinkLocalState<PartitionSortNodeSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionSortSinkLocalState);

public:
    PartitionSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<PartitionSortNodeSharedState>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    friend class PartitionSortSinkOperatorX;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    vectorized::VExprContextSPtrs _partition_expr_ctxs;
    int64_t child_input_rows = 0;
    std::vector<vectorized::PartitionDataPtr> _value_places;
    int _num_partition = 0;
    std::vector<const vectorized::IColumn*> _partition_columns;
    std::unique_ptr<vectorized::PartitionedHashMapVariants> _partitioned_data;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    int _partition_exprs_num = 0;
    std::shared_ptr<vectorized::PartitionSortInfo> _partition_sort_info = nullptr;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _emplace_key_timer = nullptr;
    RuntimeProfile::Counter* _selector_block_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _passthrough_rows_counter = nullptr;
    void _init_hash_method();
};

class PartitionSortSinkOperatorX final : public DataSinkOperatorX<PartitionSortSinkLocalState> {
public:
    PartitionSortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                               const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<PartitionSortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        if (_topn_phase == TPartTopNPhase::TWO_PHASE_GLOBAL) {
            return DataSinkOperatorX<PartitionSortSinkLocalState>::required_data_distribution();
        }
        return {ExchangeType::PASSTHROUGH};
    }

private:
    friend class PartitionSortSinkLocalState;
    ObjectPool* _pool = nullptr;
    const RowDescriptor _row_descriptor;
    const int64_t _limit = -1;
    const int _partition_exprs_num = 0;
    const TPartTopNPhase::type _topn_phase;
    const bool _has_global_limit = false;
    const TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    const int64_t _partition_inner_limit = 0;

    vectorized::VExprContextSPtrs _partition_expr_ctxs;
    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    Status _split_block_by_partition(vectorized::Block* input_block,
                                     PartitionSortSinkLocalState& local_state, bool eos);
    Status _emplace_into_hash_table(const vectorized::ColumnRawPtrs& key_columns,
                                    const vectorized::Block* input_block,
                                    PartitionSortSinkLocalState& local_state, bool eos);
};

} // namespace doris::pipeline
