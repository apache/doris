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

#include "common/be_mock_util.h"
#include "common/status.h"
#include "operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {
#include "common/compile_check_begin.h"
class AggSourceOperatorX;

class AggLocalState MOCK_REMOVE(final) : public PipelineXLocalState<AggSharedState> {
public:
    using Base = PipelineXLocalState<AggSharedState>;
    ENABLE_FACTORY_CREATOR(AggLocalState);
    AggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~AggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

    void make_nullable_output_key(vectorized::Block* block);
    Status merge_with_serialized_key_helper(vectorized::Block* block);
    void do_agg_limit(vectorized::Block* block, bool* eos);

protected:
    friend class AggSourceOperatorX;

    Status _get_without_key_result(RuntimeState* state, vectorized::Block* block, bool* eos);
    Status _get_results_without_key(RuntimeState* state, vectorized::Block* block, bool* eos);
    Status _get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                           bool* eos);
    Status _get_results_with_serialized_key(RuntimeState* state, vectorized::Block* block,
                                            bool* eos);
    Status _create_agg_status(vectorized::AggregateDataPtr data);
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

    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, uint32_t num_rows);

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    vectorized::Arena _agg_arena_pool;
    std::vector<char> _deserialize_buffer;

    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _insert_values_to_column_timer = nullptr;

    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _memory_usage_container = nullptr;
    RuntimeProfile::Counter* _memory_usage_arena = nullptr;

    using vectorized_get_result =
            std::function<Status(RuntimeState* state, vectorized::Block* block, bool* eos)>;

    struct executor {
        vectorized_get_result get_result;
    };

    executor _executor;
};

class AggSourceOperatorX : public OperatorX<AggLocalState> {
public:
    using Base = OperatorX<AggLocalState>;
    AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs);
    ~AggSourceOperatorX() override = default;

#ifdef BE_TEST
    AggSourceOperatorX() = default;
#endif

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status merge_with_serialized_key_helper(RuntimeState* state, vectorized::Block* block);

    size_t get_estimated_memory_size_for_merging(RuntimeState* state, size_t rows) const;

private:
    friend class AggLocalState;

    bool _needs_finalize;
    bool _without_key;
    bool _has_empty_grouping;

    // left / full join will change the key nullable make output/input solt
    // nullable diff. so we need make nullable of it.
    std::vector<size_t> _make_nullable_keys;
};

} // namespace pipeline
} // namespace doris
#include "common/compile_check_end.h"
