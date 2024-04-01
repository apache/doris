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

#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "exprs/runtime_filter_slots.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/join/join_op.h" // IWYU pragma: keep
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vjoin_node_base.h"

template <typename T>
struct HashCRC32;

namespace doris {
class ObjectPool;
class DescriptorTbl;
class RuntimeState;

namespace pipeline {
class HashJoinProbeLocalState;
class HashJoinBuildSinkLocalState;
} // namespace pipeline

namespace vectorized {

constexpr size_t CHECK_FRECUENCY = 65536;

struct UInt128;
struct UInt256;
template <int JoinOpType, typename Parent>
struct ProcessHashTableProbe;
class HashJoinNode;

template <typename Parent>
Status process_runtime_filter_build(RuntimeState* state, Block* block, Parent* parent,
                                    bool is_global = false) {
    if (parent->runtime_filters().empty()) {
        return Status::OK();
    }
    uint64_t rows = block->rows();
    {
        SCOPED_TIMER(parent->_runtime_filter_init_timer);
        RETURN_IF_ERROR(parent->_runtime_filter_slots->init_filters(state, rows));
        RETURN_IF_ERROR(parent->_runtime_filter_slots->ignore_filters(state));
    }

    if (!parent->_runtime_filter_slots->empty() && rows > 1) {
        SCOPED_TIMER(parent->_runtime_filter_compute_timer);
        parent->_runtime_filter_slots->insert(block);
    }
    {
        SCOPED_TIMER(parent->_publish_runtime_filter_timer);
        RETURN_IF_ERROR(parent->_runtime_filter_slots->publish());
    }

    return Status::OK();
}

using ProfileCounter = RuntimeProfile::Counter;

template <class HashTableContext, typename Parent>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, ColumnRawPtrs& build_raw_ptrs, Parent* parent, int batch_size,
                          RuntimeState* state)
            : _rows(rows),
              _build_raw_ptrs(build_raw_ptrs),
              _parent(parent),
              _batch_size(batch_size),
              _state(state) {}

    template <int JoinOpType, bool ignore_null, bool short_circuit_for_null,
              bool with_other_conjuncts>
    Status run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map, bool* has_null_key) {
        if (short_circuit_for_null || ignore_null) {
            // first row is mocked and is null
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
        hash_table_ctx.hash_table->template build<JoinOpType, with_other_conjuncts>(
                hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), _rows);
        hash_table_ctx.bucket_nums.resize(_batch_size);
        hash_table_ctx.bucket_nums.shrink_to_fit();

        COUNTER_SET(_parent->_hash_table_memory_usage,
                    (int64_t)hash_table_ctx.hash_table->get_byte_size());
        COUNTER_SET(_parent->_build_arena_memory_usage,
                    (int64_t)hash_table_ctx.serialized_keys_size(true));
        return Status::OK();
    }

private:
    const uint32_t _rows;
    ColumnRawPtrs& _build_raw_ptrs;
    Parent* _parent = nullptr;
    int _batch_size;
    RuntimeState* _state = nullptr;
};

using I8HashTableContext = PrimaryTypeHashTableContext<UInt8>;
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16>;
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32>;
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64>;
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128>;
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256>;

template <bool has_null>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null>;

template <bool has_null>
using I136FixedKeyHashTableContext = FixedKeyHashTableContext<UInt136, has_null>;

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I128HashTableContext, I256HashTableContext, I64FixedKeyHashTableContext<true>,
                     I64FixedKeyHashTableContext<false>, I128FixedKeyHashTableContext<true>,
                     I128FixedKeyHashTableContext<false>, I256FixedKeyHashTableContext<true>,
                     I256FixedKeyHashTableContext<false>, I136FixedKeyHashTableContext<true>,
                     I136FixedKeyHashTableContext<false>>;

class VExprContext;

using HashTableCtxVariants =
        std::variant<std::monostate, ProcessHashTableProbe<TJoinOp::INNER_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::LEFT_SEMI_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::LEFT_ANTI_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::FULL_OUTER_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_OUTER_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::CROSS_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_SEMI_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_ANTI_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, HashJoinNode>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN, HashJoinNode>>;

class HashJoinNode final : public VJoinNodeBase {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void add_hash_buckets_info(const std::string& info);
    void add_hash_buckets_filled_info(const std::string& info);

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    bool need_more_input_data() const;
    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    void prepare_for_next() override;

    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool can_sink_write() const {
        if (_should_build_hash_table) {
            return true;
        }
        return _shared_hash_table_context && _shared_hash_table_context->signaled;
    }

    bool should_build_hash_table() const { return _should_build_hash_table; }

    bool have_other_join_conjunct() const { return _have_other_join_conjunct; }
    bool is_right_semi_anti() const { return _is_right_semi_anti; }
    bool is_outer_join() const { return _is_outer_join; }
    const std::shared_ptr<vectorized::Block>& build_block() const { return _build_block; }
    std::vector<bool>* left_output_slot_flags() { return &_left_output_slot_flags; }
    std::vector<bool>* right_output_slot_flags() { return &_right_output_slot_flags; }
    bool* has_null_in_build_side() { return &_has_null_in_build_side; }
    DataTypes right_table_data_types() { return _right_table_data_types; }
    DataTypes left_table_data_types() { return _left_table_data_types; }
    bool build_unique() const { return _build_unique; }
    std::shared_ptr<vectorized::Arena> arena() { return _arena; }

protected:
    void _probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) override;

private:
    template <int JoinOpType, typename Parent>
    friend struct ProcessHashTableProbe;

    void _init_short_circuit_for_probe() {
        bool empty_block = !_build_block;
        _short_circuit_for_probe =
                (_has_null_in_build_side && _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                 !_is_mark_join) ||
                (empty_block && _join_op == TJoinOp::INNER_JOIN && !_is_mark_join) ||
                (empty_block && _join_op == TJoinOp::LEFT_SEMI_JOIN && !_is_mark_join) ||
                (empty_block && _join_op == TJoinOp::RIGHT_OUTER_JOIN) ||
                (empty_block && _join_op == TJoinOp::RIGHT_SEMI_JOIN) ||
                (empty_block && _join_op == TJoinOp::RIGHT_ANTI_JOIN);

        //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        //we could get the result is probe table + null-column(if need output)
        _empty_right_table_need_probe_dispose =
                (empty_block && !_have_other_join_conjunct && !_is_mark_join) &&
                (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN ||
                 _join_op == TJoinOp::LEFT_ANTI_JOIN);
    }

    bool _enable_hash_join_early_start_probe(RuntimeState* state) const;
    bool _is_hash_join_early_start_probe_eos(RuntimeState* state) const;

    // probe expr
    VExprContextSPtrs _probe_expr_ctxs;
    // build expr
    VExprContextSPtrs _build_expr_ctxs;
    // other expr
    VExprContextSPtrs _other_join_conjuncts;

    // conjuncts for mark join, which result type is ternary boolean(true, false, null)
    VExprContextSPtrs _mark_join_conjuncts;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    std::vector<bool> _should_convert_build_side_to_nullable;
    std::vector<bool> _should_convert_probe_side_to_nullable;
    // In right anti join, if the probe side is not nullable and the build side is nullable,
    // we need to convert the probe column to nullable.
    std::vector<vectorized::ColumnPtr> _key_columns_holder;

    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;
    std::vector<std::string> _right_table_column_names;

    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _build_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _build_table_insert_timer = nullptr;
    RuntimeProfile::Counter* _probe_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _probe_next_timer = nullptr;
    RuntimeProfile::Counter* _search_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _build_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_side_output_timer = nullptr;
    RuntimeProfile::Counter* _probe_process_hashtable_timer = nullptr;
    RuntimeProfile::Counter* _build_side_compute_hash_timer = nullptr;
    RuntimeProfile::Counter* _build_side_merge_block_timer = nullptr;
    RuntimeProfile::Counter* _init_probe_side_timer = nullptr;

    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _allocate_resource_timer = nullptr;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer = nullptr;

    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _build_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage = nullptr;

    std::shared_ptr<Arena> _arena;

    // maybe share hash table with other fragment instances
    std::shared_ptr<HashTableVariants> _hash_table_variants;

    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants;

    std::shared_ptr<Block> _build_block;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_build = false;
    bool _probe_ignore_null = false;
    int _probe_index = -1;
    uint32_t _build_index = 0;
    bool _ready_probe = false;
    bool _probe_eos = false;
    int _last_probe_match;

    // For mark join, last probe index of null mark
    int _last_probe_null_mark;

    bool _build_side_ignore_null = false;

    bool _is_broadcast_join = false;
    bool _should_build_hash_table = true;
    std::shared_ptr<SharedHashTableController> _shared_hashtable_controller;
    std::shared_ptr<VRuntimeFilterSlots> _runtime_filter_slots;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    int64_t _build_side_mem_used = 0;
    int64_t _build_side_last_mem_used = 0;
    MutableBlock _build_side_mutable_block;

    SharedHashTableContextPtr _shared_hash_table_context = nullptr;

    Status _materialize_build_side(RuntimeState* state) override;

    Status _process_build_block(RuntimeState* state, Block& block);

    Status _do_evaluate(Block& block, VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    template <bool BuildSide>
    Status _extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids);

    bool _need_probe_null_map(Block& block, const std::vector<int>& res_col_ids);

    void _set_build_ignore_flag(Block& block, const std::vector<int>& res_col_ids);

    void _hash_table_init(RuntimeState* state);
    void _process_hashtable_ctx_variants_init(RuntimeState* state);

    void _prepare_probe_block();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    void _release_mem();

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block) override;

    Status _filter_data_and_build_output(RuntimeState* state, vectorized::Block* output_block,
                                         bool* eos, Block* temp_block,
                                         bool check_rows_count = true);

    template <class HashTableContext, typename Parent>
    friend struct ProcessHashTableBuild;

    template <int JoinOpType, typename Parent>
    friend struct ProcessHashTableProbe;

    template <typename Parent>
    friend Status process_runtime_filter_build(RuntimeState* state, vectorized::Block* block,
                                               Parent* parent, bool is_global);

    std::atomic_bool _probe_open_finish = false;
    std::vector<int> _build_col_ids;
};
} // namespace vectorized
} // namespace doris
