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

#include "exprs/runtime_filter_slots.h"
#include "join_build_sink_operator.h"
#include "operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
class HashJoinBuildSinkOperatorX;

class HashJoinBuildSinkLocalState final
        : public JoinBuildSinkLocalState<HashJoinSharedState, HashJoinBuildSinkLocalState> {
public:
    ENABLE_FACTORY_CREATOR(HashJoinBuildSinkLocalState);
    using Base = JoinBuildSinkLocalState<HashJoinSharedState, HashJoinBuildSinkLocalState>;
    using Parent = HashJoinBuildSinkOperatorX;
    HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~HashJoinBuildSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status process_build_block(RuntimeState* state, vectorized::Block& block);

    void init_short_circuit_for_probe();

    bool build_unique() const;
    std::shared_ptr<vectorized::Arena> arena() { return _shared_state->arena; }

    void add_hash_buckets_info(const std::string& info) const {
        _profile->add_info_string("HashTableBuckets", info);
    }
    void add_hash_buckets_filled_info(const std::string& info) const {
        _profile->add_info_string("HashTableFilledBuckets", info);
    }

    Dependency* finishdependency() override { return _finish_dependency.get(); }

    Status close(RuntimeState* state, Status exec_status) override;

protected:
    Status _hash_table_init(RuntimeState* state);
    void _set_build_side_has_external_nullmap(vectorized::Block& block,
                                              const std::vector<int>& res_col_ids);
    Status _do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);
    std::vector<uint16_t> _convert_block_to_null(vectorized::Block& block);
    Status _extract_join_column(vectorized::Block& block,
                                vectorized::ColumnUInt8::MutablePtr& null_map,
                                vectorized::ColumnRawPtrs& raw_ptrs,
                                const std::vector<int>& res_col_ids);
    friend class HashJoinBuildSinkOperatorX;
    friend class PartitionedHashJoinSinkLocalState;
    template <class HashTableContext>
    friend struct ProcessHashTableBuild;

    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;
    std::vector<vectorized::ColumnPtr> _key_columns_holder;

    bool _should_build_hash_table = true;

    size_t _build_side_rows = 0;

    vectorized::MutableBlock _build_side_mutable_block;
    std::shared_ptr<VRuntimeFilterSlots> _runtime_filter_slots;

    /*
     * The comparison result of a null value with any other value is null,
     * which means that for most join(exclude: null aware join, null equal safe join),
     * the result of an equality condition involving null should be false,
     * so null does not need to be added to the hash table.
     */
    bool _build_side_has_external_nullmap = false;
    std::vector<int> _build_col_ids;
    std::shared_ptr<CountedFinishDependency> _finish_dependency;

    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _build_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _build_table_insert_timer = nullptr;
    RuntimeProfile::Counter* _build_side_merge_block_timer = nullptr;

    RuntimeProfile::Counter* _build_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _build_arena_memory_usage = nullptr;
    RuntimeProfile::Counter* _runtime_filter_init_timer = nullptr;
};

class HashJoinBuildSinkOperatorX final
        : public JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState> {
public:
    HashJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                               const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    bool should_dry_run(RuntimeState* state) override {
        return _is_broadcast_join && !state->get_sink_local_state()
                                              ->cast<HashJoinBuildSinkLocalState>()
                                              ._should_build_hash_table;
    }

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        } else if (_is_broadcast_join) {
            return _child->is_serial_operator() ? DataDistribution(ExchangeType::PASS_TO_ONE)
                                                : DataDistribution(ExchangeType::NOOP);
        }
        return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                               _join_distribution == TJoinDistributionType::COLOCATE
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

    bool is_shuffled_operator() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }
    bool require_data_distribution() const override {
        return _join_distribution != TJoinDistributionType::BROADCAST &&
               _join_distribution != TJoinDistributionType::NONE;
    }

private:
    friend class HashJoinBuildSinkLocalState;

    const TJoinDistributionType::type _join_distribution;
    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> _serialize_null_into_key;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    bool _is_broadcast_join = false;
    std::shared_ptr<vectorized::SharedHashTableController> _shared_hashtable_controller;

    vectorized::SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    const std::vector<TExpr> _partition_exprs;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _should_keep_column_flags;
    bool _should_keep_hash_key_column = false;
};

template <class HashTableContext>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(size_t rows, vectorized::ColumnRawPtrs& build_raw_ptrs,
                          HashJoinBuildSinkLocalState* parent, int batch_size, RuntimeState* state)
            : _rows(rows),
              _build_raw_ptrs(build_raw_ptrs),
              _parent(parent),
              _batch_size(batch_size),
              _state(state) {}

    template <int JoinOpType, bool short_circuit_for_null, bool with_other_conjuncts>
    Status run(HashTableContext& hash_table_ctx, vectorized::ConstNullMapPtr null_map,
               bool* has_null_key) {
        if (null_map) {
            // first row is mocked and is null
            // TODO: Need to test the for loop. break may better
            for (uint32_t i = 1; i < _rows; i++) {
                if ((*null_map)[i]) {
                    *has_null_key = true;
                }
            }
            if (short_circuit_for_null && *has_null_key) {
                return Status::OK();
            }
        }

        SCOPED_TIMER(_parent->_build_table_insert_timer);
        hash_table_ctx.hash_table->template prepare_build<JoinOpType>(_rows, _batch_size,
                                                                      *has_null_key);

        hash_table_ctx.init_serialized_keys(_build_raw_ptrs, _rows,
                                            null_map ? null_map->data() : nullptr, true, true,
                                            hash_table_ctx.hash_table->get_bucket_size());
        // only 2 cases need to access the null value in hash table
        bool keep_null_key = false;
        if ((JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
             JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
            with_other_conjuncts) {
            //null aware join with other conjuncts
            keep_null_key = true;
        } else if (_parent->_shared_state->is_null_safe_eq_join.size() == 1 &&
                   _parent->_shared_state->is_null_safe_eq_join[0]) {
            // single null safe eq
            keep_null_key = true;
        }

        hash_table_ctx.hash_table->build(hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(),
                                         _rows, keep_null_key);
        hash_table_ctx.bucket_nums.resize(_batch_size);
        hash_table_ctx.bucket_nums.shrink_to_fit();

        COUNTER_SET(_parent->_hash_table_memory_usage,
                    (int64_t)hash_table_ctx.hash_table->get_byte_size());
        COUNTER_SET(_parent->_build_arena_memory_usage,
                    (int64_t)hash_table_ctx.serialized_keys_size(true));
        return Status::OK();
    }

private:
    const size_t _rows;
    vectorized::ColumnRawPtrs& _build_raw_ptrs;
    HashJoinBuildSinkLocalState* _parent = nullptr;
    int _batch_size;
    RuntimeState* _state = nullptr;
};

} // namespace doris::pipeline
#include "common/compile_check_end.h"
