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
#include "aggregation_sink_operator.h"
#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
class PartitionedAggSinkOperatorX;
class PartitionedAggSinkLocalState
        : public PipelineXSpillSinkLocalState<PartitionedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggSinkLocalState);
    using Base = PipelineXSpillSinkLocalState<PartitionedAggSharedState>;
    using Parent = PartitionedAggSinkOperatorX;

    PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~PartitionedAggSinkLocalState() override = default;

    friend class PartitionedAggSinkOperatorX;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Dependency* finishdependency() override { return _finish_dependency.get(); }

    Status revoke_memory(RuntimeState* state);

    Status setup_in_memory_agg_op(RuntimeState* state);

    void update_profile(RuntimeProfile* child_profile);

    template <typename KeyType>
    struct TmpSpillInfo {
        std::vector<KeyType> keys_;
        std::vector<vectorized::AggregateDataPtr> values_;
    };
    template <typename HashTableCtxType, typename HashTableType>
    Status _spill_hash_table(RuntimeState* state, HashTableCtxType& context,
                             HashTableType& hash_table, bool eos) {
        Status status;
        Defer defer {[&]() {
            if (!status.ok()) {
                Base::_shared_state->close();
            }
        }};

        context.init_iterator();

        Base::_shared_state->in_mem_shared_state->aggregate_data_container->init_once();

        static int spill_batch_rows = 4096;
        int row_count = 0;

        std::vector<TmpSpillInfo<typename HashTableType::key_type>> spill_infos(
                Base::_shared_state->partition_count);
        auto& iter = Base::_shared_state->in_mem_shared_state->aggregate_data_container->iterator;
        while (iter != Base::_shared_state->in_mem_shared_state->aggregate_data_container->end() &&
               !state->is_cancelled()) {
            const auto& key = iter.template get_key<typename HashTableType::key_type>();
            auto partition_index = Base::_shared_state->get_partition_index(hash_table.hash(key));
            spill_infos[partition_index].keys_.emplace_back(key);
            spill_infos[partition_index].values_.emplace_back(iter.get_aggregate_data());

            if (++row_count == spill_batch_rows) {
                row_count = 0;
                for (int i = 0; i < Base::_shared_state->partition_count && !state->is_cancelled();
                     ++i) {
                    if (spill_infos[i].keys_.size() >= spill_batch_rows) {
                        status = _spill_partition(
                                state, context, Base::_shared_state->spill_partitions[i],
                                spill_infos[i].keys_, spill_infos[i].values_, nullptr, false);
                        RETURN_IF_ERROR(status);
                    }
                }
            }

            ++iter;
        }
        auto hash_null_key_data = hash_table.has_null_key_data();
        for (int i = 0; i < Base::_shared_state->partition_count && !state->is_cancelled(); ++i) {
            auto spill_null_key_data =
                    (hash_null_key_data && i == Base::_shared_state->partition_count - 1);
            if (spill_infos[i].keys_.size() > 0 || spill_null_key_data) {
                status = _spill_partition(state, context, Base::_shared_state->spill_partitions[i],
                                          spill_infos[i].keys_, spill_infos[i].values_,
                                          spill_null_key_data
                                                  ? hash_table.template get_null_key_data<
                                                            vectorized::AggregateDataPtr>()
                                                  : nullptr,
                                          true);
                RETURN_IF_ERROR(status);
            }
        }

        for (auto& partition : Base::_shared_state->spill_partitions) {
            status = partition->finish_current_spilling(eos);
            RETURN_IF_ERROR(status);
        }
        if (eos) {
            _clear_tmp_data();
        }
        return Status::OK();
    }

    template <typename HashTableCtxType, typename KeyType>
    Status _spill_partition(RuntimeState* state, HashTableCtxType& context,
                            AggSpillPartitionSPtr& spill_partition, std::vector<KeyType>& keys,
                            std::vector<vectorized::AggregateDataPtr>& values,
                            const vectorized::AggregateDataPtr null_key_data, bool is_last) {
        vectorized::SpillStreamSPtr spill_stream;
        auto status = spill_partition->get_spill_stream(state, Base::_parent->node_id(),
                                                        Base::profile(), spill_stream);
        RETURN_IF_ERROR(status);
        spill_stream->set_write_counters(Base::_spill_serialize_block_timer,
                                         Base::_spill_block_count, Base::_spill_data_size,
                                         Base::_spill_write_disk_timer,
                                         Base::_spill_write_wait_io_timer);

        status = to_block(context, keys, values, null_key_data);
        RETURN_IF_ERROR(status);

        if (is_last) {
            std::vector<KeyType> tmp_keys;
            std::vector<vectorized::AggregateDataPtr> tmp_values;
            keys.swap(tmp_keys);
            values.swap(tmp_values);

        } else {
            keys.clear();
            values.clear();
        }
        status = spill_stream->prepare_spill();
        RETURN_IF_ERROR(status);

        {
            SCOPED_TIMER(_spill_write_disk_timer);
            status = spill_stream->spill_block(state, block_, false);
        }
        RETURN_IF_ERROR(status);
        status = spill_partition->flush_if_full();
        _reset_tmp_data();
        return status;
    }

    template <typename HashTableCtxType, typename KeyType>
    Status to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                    std::vector<vectorized::AggregateDataPtr>& values,
                    const vectorized::AggregateDataPtr null_key_data) {
        SCOPED_TIMER(_spill_serialize_hash_table_timer);
        context.insert_keys_into_columns(keys, key_columns_, keys.size());

        if (null_key_data) {
            // only one key of group by support wrap null key
            // here need additional processing logic on the null key / value
            CHECK(key_columns_.size() == 1);
            CHECK(key_columns_[0]->is_nullable());
            key_columns_[0]->insert_data(nullptr, 0);

            values.emplace_back(null_key_data);
        }

        for (size_t i = 0;
             i < Base::_shared_state->in_mem_shared_state->aggregate_evaluators.size(); ++i) {
            Base::_shared_state->in_mem_shared_state->aggregate_evaluators[i]
                    ->function()
                    ->serialize_to_column(values,
                                          Base::_shared_state->in_mem_shared_state
                                                  ->offsets_of_aggregate_states[i],
                                          value_columns_[i], values.size());
        }

        vectorized::ColumnsWithTypeAndName key_columns_with_schema;
        for (int i = 0; i < key_columns_.size(); ++i) {
            key_columns_with_schema.emplace_back(
                    std::move(key_columns_[i]),
                    Base::_shared_state->in_mem_shared_state->probe_expr_ctxs[i]
                            ->root()
                            ->data_type(),
                    Base::_shared_state->in_mem_shared_state->probe_expr_ctxs[i]
                            ->root()
                            ->expr_name());
        }
        key_block_ = key_columns_with_schema;

        vectorized::ColumnsWithTypeAndName value_columns_with_schema;
        for (int i = 0; i < value_columns_.size(); ++i) {
            value_columns_with_schema.emplace_back(
                    std::move(value_columns_[i]), value_data_types_[i],
                    Base::_shared_state->in_mem_shared_state->aggregate_evaluators[i]
                            ->function()
                            ->get_name());
        }
        value_block_ = value_columns_with_schema;

        for (const auto& column : key_block_.get_columns_with_type_and_name()) {
            block_.insert(column);
        }
        for (const auto& column : value_block_.get_columns_with_type_and_name()) {
            block_.insert(column);
        }
        return Status::OK();
    }

    void _reset_tmp_data() {
        block_.clear();
        key_columns_.clear();
        value_columns_.clear();
        key_block_.clear_column_data();
        value_block_.clear_column_data();
        key_columns_ = key_block_.mutate_columns();
        value_columns_ = value_block_.mutate_columns();
    }

    void _clear_tmp_data() {
        {
            vectorized::Block empty_block;
            block_.swap(empty_block);
        }
        {
            vectorized::Block empty_block;
            key_block_.swap(empty_block);
        }
        {
            vectorized::Block empty_block;
            value_block_.swap(empty_block);
        }
        {
            vectorized::MutableColumns cols;
            key_columns_.swap(cols);
        }
        {
            vectorized::MutableColumns cols;
            value_columns_.swap(cols);
        }

        vectorized::DataTypes tmp_value_data_types;
        value_data_types_.swap(tmp_value_data_types);
    }

    void _init_counters();

    std::unique_ptr<RuntimeState> _runtime_state;

    bool _eos = false;
    std::shared_ptr<Dependency> _finish_dependency;

    // temp structures during spilling
    vectorized::MutableColumns key_columns_;
    vectorized::MutableColumns value_columns_;
    vectorized::DataTypes value_data_types_;
    vectorized::Block block_;
    vectorized::Block key_block_;
    vectorized::Block value_block_;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _serialize_key_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _serialize_data_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _max_row_size_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage = nullptr;

    RuntimeProfile::Counter* _spill_serialize_hash_table_timer = nullptr;
};

class PartitionedAggSinkOperatorX : public DataSinkOperatorX<PartitionedAggSinkLocalState> {
public:
    PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                const DescriptorTbl& descs, bool require_bucket_distribution);
    ~PartitionedAggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<PartitionedAggSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override {
        return _agg_sink_operator->required_data_distribution();
    }

    bool require_data_distribution() const override {
        return _agg_sink_operator->require_data_distribution();
    }

    Status set_child(OperatorXPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<PartitionedAggSinkLocalState>::set_child(child));
        return _agg_sink_operator->set_child(child);
    }
    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

private:
    friend class PartitionedAggSinkLocalState;
    std::unique_ptr<AggSinkOperatorX> _agg_sink_operator;

    size_t _spill_partition_count_bits = 4;
};
} // namespace doris::pipeline