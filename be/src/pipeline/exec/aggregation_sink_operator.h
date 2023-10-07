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

#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class AggSinkOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    AggSinkOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
    bool is_sink() const override { return true; }
};

class AggSinkOperator final : public StreamingOperator<AggSinkOperatorBuilder> {
public:
    AggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return true; }
};

template <typename LocalStateType>
class AggSinkOperatorX;

template <typename DependencyType, typename Derived>
class AggSinkLocalState : public PipelineXSinkLocalState<DependencyType> {
public:
    using Base = PipelineXSinkLocalState<DependencyType>;
    ~AggSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    Status try_spill_disk(bool eos = false);

protected:
    AggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);

    template <typename LocalStateType>
    friend class AggSinkOperatorX;

    Status _execute_without_key(vectorized::Block* block);
    Status _merge_without_key(vectorized::Block* block);
    void _update_memusage_without_key();
    void _init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs);
    Status _execute_with_serialized_key(vectorized::Block* block);
    Status _merge_with_serialized_key(vectorized::Block* block);
    void _update_memusage_with_serialized_key();
    template <bool limit>
    Status _execute_with_serialized_key_helper(vectorized::Block* block);
    void _find_in_hash_table(vectorized::AggregateDataPtr* places,
                             vectorized::ColumnRawPtrs& key_columns, size_t num_rows);

    template <typename AggState, typename AggMethod>
    void _pre_serialize_key_if_need(AggState& state, AggMethod& agg_method,
                                    const vectorized::ColumnRawPtrs& key_columns,
                                    const size_t num_rows) {
        if constexpr (vectorized::ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                              AggState>::value) {
            auto old_keys_memory = agg_method.keys_memory_usage;
            SCOPED_TIMER(_serialize_key_timer);
            int64_t row_size = (int64_t)(agg_method.serialize_keys(key_columns, num_rows));
            COUNTER_SET(_max_row_size_counter, std::max(_max_row_size_counter->value(), row_size));
            state.set_serialized_keys(agg_method.keys.data());

            _serialize_key_arena_memory_usage->add(agg_method.keys_memory_usage - old_keys_memory);
        }
    }
    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, const size_t num_rows);
    size_t _get_hash_table_size();

    template <bool limit, bool for_spill = false>
    Status _merge_with_serialized_key_helper(vectorized::Block* block);

    template <typename HashTableCtxType, typename HashTableType, typename KeyType>
    Status _serialize_hash_table_to_block(HashTableCtxType& context, HashTableType& hash_table,
                                          vectorized::Block& block, std::vector<KeyType>& keys_) {
        int key_size = Base::_shared_state->probe_expr_ctxs.size();
        int agg_size = Base::_shared_state->aggregate_evaluators.size();

        vectorized::MutableColumns value_columns(agg_size);
        vectorized::DataTypes value_data_types(agg_size);
        vectorized::MutableColumns key_columns;

        for (int i = 0; i < key_size; ++i) {
            key_columns.emplace_back(
                    Base::_shared_state->probe_expr_ctxs[i]->root()->data_type()->create_column());
        }

        for (size_t i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            value_data_types[i] =
                    Base::_shared_state->aggregate_evaluators[i]->function()->get_serialized_type();
            value_columns[i] = Base::_shared_state->aggregate_evaluators[i]
                                       ->function()
                                       ->create_serialize_column();
        }

        context.init_once();
        const auto size = hash_table.size();
        std::vector<KeyType> keys(size);
        if (Base::_shared_state->values.size() < size) {
            Base::_shared_state->values.resize(size);
        }

        size_t num_rows = 0;
        Base::_shared_state->aggregate_data_container->init_once();
        auto& iter = Base::_shared_state->aggregate_data_container->iterator;

        {
            while (iter != Base::_shared_state->aggregate_data_container->end()) {
                keys[num_rows] = iter.template get_key<KeyType>();
                Base::_shared_state->values[num_rows] = iter.get_aggregate_data();
                ++iter;
                ++num_rows;
            }
        }

        {
            context.insert_keys_into_columns(keys, key_columns, num_rows,
                                             Base::_shared_state->probe_key_sz);
        }

        if (hash_table.has_null_key_data()) {
            // only one key of group by support wrap null key
            // here need additional processing logic on the null key / value
            CHECK(key_columns.size() == 1);
            CHECK(key_columns[0]->is_nullable());
            key_columns[0]->insert_data(nullptr, 0);

            // Here is no need to set `keys[num_rows]`, keep it as default value.
            Base::_shared_state->values[num_rows] = hash_table.get_null_key_data();
            ++num_rows;
        }

        for (size_t i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            Base::_shared_state->aggregate_evaluators[i]->function()->serialize_to_column(
                    Base::_shared_state->values,
                    Base::_dependency->offsets_of_aggregate_states()[i], value_columns[i],
                    num_rows);
        }

        vectorized::ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(
                    std::move(key_columns[i]),
                    Base::_shared_state->probe_expr_ctxs[i]->root()->data_type(),
                    Base::_shared_state->probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(
                    std::move(value_columns[i]), value_data_types[i],
                    Base::_shared_state->aggregate_evaluators[i]->function()->get_name());
        }

        block = columns_with_schema;
        keys_.swap(keys);
        return Status::OK();
    }

    template <typename HashTableCtxType, typename HashTableType>
    Status _spill_hash_table(HashTableCtxType& agg_method, HashTableType& hash_table) {
        vectorized::Block block;
        std::vector<typename HashTableType::key_type> keys;
        RETURN_IF_ERROR(_serialize_hash_table_to_block(agg_method, hash_table, block, keys));
        CHECK_EQ(block.rows(), hash_table.size());
        CHECK_EQ(keys.size(), block.rows());

        if (!Base::_shared_state->spill_context.has_data) {
            Base::_shared_state->spill_context.has_data = true;
            Base::_shared_state->spill_context.runtime_profile =
                    Base::profile()->create_child("Spill", true, true);
        }

        vectorized::BlockSpillWriterUPtr writer;
        RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_writer(
                std::numeric_limits<int32_t>::max(), writer,
                Base::_shared_state->spill_context.runtime_profile));
        Defer defer {[&]() {
            // redundant call is ok
            static_cast<void>(writer->close());
        }};
        Base::_shared_state->spill_context.stream_ids.emplace_back(writer->get_id());

        std::vector<size_t> partitioned_indices(block.rows());
        std::vector<size_t> blocks_rows(
                Base::_shared_state->spill_partition_helper->partition_count);

        // The last row may contain a null key.
        const size_t rows = hash_table.has_null_key_data() ? block.rows() - 1 : block.rows();
        for (size_t i = 0; i < rows; ++i) {
            const auto index = Base::_shared_state->spill_partition_helper->get_index(
                    hash_table.hash(keys[i]));
            partitioned_indices[i] = index;
            blocks_rows[index]++;
        }

        if (hash_table.has_null_key_data()) {
            // Here put the row with null key at the last partition.
            const auto index = Base::_shared_state->spill_partition_helper->partition_count - 1;
            partitioned_indices[rows] = index;
            blocks_rows[index]++;
        }

        for (size_t i = 0; i < Base::_shared_state->spill_partition_helper->partition_count; ++i) {
            vectorized::Block block_to_write = block.clone_empty();
            if (blocks_rows[i] == 0) {
                /// Here write one empty block to ensure there are enough blocks in the file,
                /// blocks' count should be equal with partition_count.
                static_cast<void>(writer->write(block_to_write));
                continue;
            }

            vectorized::MutableBlock mutable_block(std::move(block_to_write));

            for (auto& column : mutable_block.mutable_columns()) {
                column->reserve(blocks_rows[i]);
            }

            size_t begin = 0;
            size_t length = 0;
            for (size_t j = 0; j < partitioned_indices.size(); ++j) {
                if (partitioned_indices[j] != i) {
                    if (length > 0) {
                        mutable_block.add_rows(&block, begin, length);
                    }
                    length = 0;
                    continue;
                }

                if (length == 0) {
                    begin = j;
                }
                length++;
            }

            if (length > 0) {
                mutable_block.add_rows(&block, begin, length);
            }

            CHECK_EQ(mutable_block.rows(), blocks_rows[i]);
            RETURN_IF_ERROR(writer->write(mutable_block.to_block()));
        }
        RETURN_IF_ERROR(writer->close());

        return Status::OK();
    }
    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    int _get_slot_column_id(const vectorized::AggFnEvaluator* evaluator);
    size_t _memory_usage() const;

    RuntimeProfile::Counter* _hash_table_compute_timer;
    RuntimeProfile::Counter* _hash_table_emplace_timer;
    RuntimeProfile::Counter* _hash_table_input_counter;
    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _expr_timer;
    RuntimeProfile::Counter* _exec_timer;
    RuntimeProfile::Counter* _build_table_convert_timer;
    RuntimeProfile::Counter* _serialize_key_timer;
    RuntimeProfile::Counter* _merge_timer;
    RuntimeProfile::Counter* _serialize_data_timer;
    RuntimeProfile::Counter* _deserialize_data_timer;
    RuntimeProfile::Counter* _max_row_size_counter;
    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage;

    bool _should_limit_output = false;
    bool _reach_limit = false;

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;

    vectorized::Block _preagg_block = vectorized::Block();

    vectorized::AggregatedDataVariants* _agg_data;
    vectorized::Arena* _agg_arena_pool;
    std::vector<size_t> _hash_values;

    using vectorized_execute = std::function<Status(vectorized::Block* block)>;
    using vectorized_update_memusage = std::function<void()>;

    struct executor {
        vectorized_execute execute;
        vectorized_update_memusage update_memusage;
    };

    executor _executor;
};

class BlockingAggSinkLocalState
        : public AggSinkLocalState<AggDependency, BlockingAggSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(BlockingAggSinkLocalState);
    using Parent = AggSinkOperatorX<BlockingAggSinkLocalState>;

    BlockingAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : AggSinkLocalState<AggDependency, BlockingAggSinkLocalState>(parent, state) {}
    ~BlockingAggSinkLocalState() = default;
};

template <typename LocalStateType = BlockingAggSinkLocalState>
class AggSinkOperatorX : public DataSinkOperatorX<LocalStateType> {
public:
    AggSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<LocalStateType>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    using DataSinkOperatorX<LocalStateType>::id;

protected:
    using LocalState = LocalStateType;
    template <typename DependencyType, typename Derived>
    friend class AggSinkLocalState;
    friend class StreamingAggSinkLocalState;
    friend class DistinctStreamingAggSinkLocalState;
    std::vector<vectorized::AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    bool _is_merge;
    bool _is_first_phase;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    size_t _external_agg_bytes_threshold;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    ObjectPool* _pool;
    std::vector<size_t> _make_nullable_keys;
    size_t _spill_partition_count_bits;
    int64_t _limit; // -1: no limit
    bool _have_conjuncts;
};

} // namespace pipeline
} // namespace doris
