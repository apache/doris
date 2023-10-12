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
#include "vec/exec/vpartition_sort_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class PartitionSortSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::VPartitionSortNode> {
public:
    PartitionSortSinkOperatorBuilder(int32_t id, ExecNode* sort_node)
            : OperatorBuilder(id, "PartitionSortSinkOperator", sort_node) {}

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;
};

class PartitionSortSinkOperator final : public StreamingOperator<PartitionSortSinkOperatorBuilder> {
public:
    PartitionSortSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node)
            : StreamingOperator(operator_builder, sort_node) {};

    bool can_write() override { return true; }
};

class PartitionSortSinkOperatorX;
class PartitionSortSinkLocalState : public PipelineXSinkLocalState<PartitionSortDependency> {
    ENABLE_FACTORY_CREATOR(PartitionSortSinkLocalState);

public:
    PartitionSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<PartitionSortDependency>(parent, state) {}

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
    std::vector<size_t> _hash_values;
    std::unique_ptr<vectorized::PartitionedHashMapVariants> _partitioned_data;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    std::vector<size_t> _partition_key_sz;
    int _partition_exprs_num = 0;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _emplace_key_timer;
    RuntimeProfile::Counter* _partition_sort_timer;
    RuntimeProfile::Counter* _get_sorted_timer;
    RuntimeProfile::Counter* _selector_block_timer;

    RuntimeProfile::Counter* _hash_table_size_counter;
    void _init_hash_method();
};

class PartitionSortSinkOperatorX final : public DataSinkOperatorX<PartitionSortSinkLocalState> {
public:
    PartitionSortSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                               const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<PartitionSortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

private:
    friend class PartitionSortSinkLocalState;
    ObjectPool* _pool;
    const RowDescriptor _row_descriptor;
    int64_t _limit = -1;
    int _partition_exprs_num = 0;
    vectorized::VExprContextSPtrs _partition_expr_ctxs;

    TPartTopNPhase::type _topn_phase;

    // Expressions and parameters used for build _sort_description
    vectorized::VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    bool _has_global_limit = false;
    int64_t _partition_inner_limit = 0;

    Status _split_block_by_partition(vectorized::Block* input_block, int batch_size,
                                     PartitionSortSinkLocalState& local_state);
    void _emplace_into_hash_table(const vectorized::ColumnRawPtrs& key_columns,
                                  const vectorized::Block* input_block, int batch_size,
                                  PartitionSortSinkLocalState& local_state);
    template <typename AggState, typename AggMethod>
    void _pre_serialize_key_if_need(AggState& state, AggMethod& agg_method,
                                    const vectorized::ColumnRawPtrs& key_columns,
                                    const size_t num_rows) {
        if constexpr (vectorized::ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                              AggState>::value) {
            (agg_method.serialize_keys(key_columns, num_rows));
            state.set_serialized_keys(agg_method.keys.data());
        }
    }
};

} // namespace pipeline
} // namespace doris
