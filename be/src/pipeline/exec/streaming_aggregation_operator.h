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

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class StreamingAggOperatorX;

class StreamingAggLocalState MOCK_REMOVE(final) : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = StreamingAggOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    ENABLE_FACTORY_CREATOR(StreamingAggLocalState);
    StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~StreamingAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_pre_agg(RuntimeState* state, vectorized::Block* input_block,
                      vectorized::Block* output_block);
    void make_nullable_output_key(vectorized::Block* block);

private:
    friend class StreamingAggOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    size_t _memory_usage() const;
    Status _pre_agg_with_serialized_key(doris::vectorized::Block* in_block,
                                        doris::vectorized::Block* out_block);
    bool _should_expand_preagg_hash_tables();

    MOCK_FUNCTION bool _should_not_do_pre_agg(size_t rows);

    Status _execute_with_serialized_key(vectorized::Block* block);
    void _update_memusage_with_serialized_key();
    Status _init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs);
    Status _get_results_with_serialized_key(RuntimeState* state, vectorized::Block* block,
                                            bool* eos);
    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, const uint32_t num_rows);
    Status _create_agg_status(vectorized::AggregateDataPtr data);
    size_t _get_hash_table_size();

    RuntimeProfile::Counter* _streaming_agg_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _insert_values_to_column_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;

    bool _should_expand_hash_table = true;
    int64_t _cur_num_rows_returned = 0;
    vectorized::Arena _agg_arena_pool;
    AggregatedDataVariantsUPtr _agg_data = nullptr;
    std::vector<vectorized::AggFnEvaluator*> _aggregate_evaluators;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::unique_ptr<vectorized::Arena> _agg_profile_arena = nullptr;
    std::unique_ptr<AggregateDataContainer> _aggregate_data_container = nullptr;
    bool _should_limit_output = false;
    bool _reach_limit = false;
    size_t _input_num_rows = 0;

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;

    std::unique_ptr<vectorized::Block> _child_block = nullptr;
    bool _child_eos = false;
    std::unique_ptr<vectorized::Block> _pre_aggregated_block = nullptr;
    std::vector<vectorized::AggregateDataPtr> _values;
    bool _opened = false;

    void _destroy_agg_status(vectorized::AggregateDataPtr data);

    void _close_with_serialized_key() {
        std::visit(
                vectorized::Overload {[&](std::monostate& arg) -> void {
                                          // Do nothing
                                      },
                                      [&](auto& agg_method) -> void {
                                          auto& data = *agg_method.hash_table;
                                          data.for_each_mapped([&](auto& mapped) {
                                              if (mapped) {
                                                  _destroy_agg_status(mapped);
                                                  mapped = nullptr;
                                              }
                                          });
                                          if (data.has_null_key_data()) {
                                              _destroy_agg_status(data.template get_null_key_data<
                                                                  vectorized::AggregateDataPtr>());
                                          }
                                      }},
                _agg_data->method_variant);
    }
};

class StreamingAggOperatorX MOCK_REMOVE(final) : public StatefulOperatorX<StreamingAggLocalState> {
public:
    StreamingAggOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs);
#ifdef BE_TEST
    StreamingAggOperatorX() : _is_first_phase {false} {}
#endif

    ~StreamingAggOperatorX() override = default;
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status pull(RuntimeState* state, vectorized::Block* block, bool* eos) const override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;
    void set_low_memory_mode(RuntimeState* state) override {
        _spill_streaming_agg_mem_limit = 1024 * 1024;
    }

private:
    friend class StreamingAggLocalState;

    MOCK_FUNCTION Status _init_probe_expr_ctx(RuntimeState* state);

    MOCK_FUNCTION Status _init_aggregate_evaluators(RuntimeState* state);

    MOCK_FUNCTION Status _calc_aggregate_evaluators();

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    bool _needs_finalize;
    bool _is_merge;
    const bool _is_first_phase;
    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    /// When spilling is enabled, the streaming agg should not occupy too much memory.
    size_t _spill_streaming_agg_mem_limit;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::vector<vectorized::AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;
    std::vector<size_t> _make_nullable_keys;
    bool _have_conjuncts;
    RowDescriptor _agg_fn_output_row_descriptor;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
