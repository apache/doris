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
#include "vec/common/aggregation_common.h"
#include "vec/common/arena.h"
#include "vec/common/columns_hashing.h"
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
class IRuntimeFilter;
class DescriptorTbl;
class RuntimeState;

namespace vectorized {

struct UInt128;
struct UInt256;
template <int JoinOpType>
struct ProcessHashTableProbe;

template <typename RowRefListType>
struct SerializedHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<StringRef, Mapped>;
    using State =
            ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped, true>;
    using Iter = typename HashTable::iterator;

    SerializedHashTableContext() { _arena.reset(new Arena()); }

    HashTable hash_table;
    Iter iter;
    bool inited = false;
    std::vector<StringRef> keys;
    size_t keys_memory_usage = 0;

    void serialize_keys(const ColumnRawPtrs& key_columns, size_t num_rows) {
        if (keys.size() < num_rows) {
            keys.resize(num_rows);
        }

        _arena->clear();
        keys_memory_usage = 0;
        size_t keys_size = key_columns.size();
        for (size_t i = 0; i < num_rows; ++i) {
            keys[i] = serialize_keys_to_pool_contiguous(i, keys_size, key_columns, *_arena);
        }
        keys_memory_usage = _arena->size();
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }

private:
    std::unique_ptr<Arena> _arena;
};

// T should be UInt32 UInt64 UInt128
template <class T, typename RowRefListType>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

// TODO: use FixedHashTable instead of HashTable
template <typename RowRefListType>
using I8HashTableContext = PrimaryTypeHashTableContext<UInt8, RowRefListType>;
template <typename RowRefListType>
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16, RowRefListType>;
template <typename RowRefListType>
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32, RowRefListType>;
template <typename RowRefListType>
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64, RowRefListType>;
template <typename RowRefListType>
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128, RowRefListType>;
template <typename RowRefListType>
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256, RowRefListType>;

template <class T, bool has_null, typename RowRefListType>
struct FixedKeyHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <bool has_null, typename RowRefListType>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null, RowRefListType>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext<RowRefList>, I8HashTableContext<RowRefList>,
        I16HashTableContext<RowRefList>, I32HashTableContext<RowRefList>,
        I64HashTableContext<RowRefList>, I128HashTableContext<RowRefList>,
        I256HashTableContext<RowRefList>, I64FixedKeyHashTableContext<true, RowRefList>,
        I64FixedKeyHashTableContext<false, RowRefList>,
        I128FixedKeyHashTableContext<true, RowRefList>,
        I128FixedKeyHashTableContext<false, RowRefList>,
        I256FixedKeyHashTableContext<true, RowRefList>,
        I256FixedKeyHashTableContext<false, RowRefList>,
        SerializedHashTableContext<RowRefListWithFlag>, I8HashTableContext<RowRefListWithFlag>,
        I16HashTableContext<RowRefListWithFlag>, I32HashTableContext<RowRefListWithFlag>,
        I64HashTableContext<RowRefListWithFlag>, I128HashTableContext<RowRefListWithFlag>,
        I256HashTableContext<RowRefListWithFlag>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlag>,
        SerializedHashTableContext<RowRefListWithFlags>, I8HashTableContext<RowRefListWithFlags>,
        I16HashTableContext<RowRefListWithFlags>, I32HashTableContext<RowRefListWithFlags>,
        I64HashTableContext<RowRefListWithFlags>, I128HashTableContext<RowRefListWithFlags>,
        I256HashTableContext<RowRefListWithFlags>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlags>>;

class VExprContext;

using HashTableCtxVariants =
        std::variant<std::monostate, ProcessHashTableProbe<TJoinOp::INNER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::FULL_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::CROSS_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>>;

using HashTableIteratorVariants =
        std::variant<std::monostate, ForwardIterator<RowRefList>,
                     ForwardIterator<RowRefListWithFlag>, ForwardIterator<RowRefListWithFlags>>;

class HashJoinNode final : public VJoinNodeBase {
public:
    // TODO: Best prefetch step is decided by machine. We should also provide a
    //  SQL hint to allow users to tune by hand.
    static constexpr int PREFETCH_STEP = 64;

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

    bool ready_for_finish() {
        if (_runtime_filter_slots == nullptr) {
            return true;
        }
        return _runtime_filter_slots->ready_finish_publish();
    }

private:
    void _init_short_circuit_for_probe() {
        _short_circuit_for_probe =
                (_has_null_in_build_side && _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
                 !_is_mark_join) ||
                (_build_blocks->empty() && _join_op == TJoinOp::INNER_JOIN && !_is_mark_join) ||
                (_build_blocks->empty() && _join_op == TJoinOp::LEFT_SEMI_JOIN && !_is_mark_join) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_OUTER_JOIN) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_SEMI_JOIN) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_ANTI_JOIN);

        //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        //we could get the result is probe table + null-column(if need output)
        _empty_right_table_need_probe_dispose =
                (_build_blocks->empty() && !_have_other_join_conjunct && !_is_mark_join) &&
                (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN ||
                 _join_op == TJoinOp::LEFT_ANTI_JOIN);
    }

    // probe expr
    VExprContextSPtrs _probe_expr_ctxs;
    // build expr
    VExprContextSPtrs _build_expr_ctxs;
    // other expr
    VExprContextSPtrs _other_join_conjuncts;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;
    std::vector<std::string> _right_table_column_names;

    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_table_convert_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _build_buckets_fill_counter;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _probe_process_hashtable_timer;
    RuntimeProfile::Counter* _build_side_compute_hash_timer;
    RuntimeProfile::Counter* _build_side_merge_block_timer;
    RuntimeProfile::Counter* _build_runtime_filter_timer;

    RuntimeProfile::Counter* _build_collisions_counter;

    RuntimeProfile::Counter* _open_timer;
    RuntimeProfile::Counter* _allocate_resource_timer;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _build_blocks_memory_usage;
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage;

    std::shared_ptr<Arena> _arena;

    // maybe share hash table with other fragment instances
    std::shared_ptr<HashTableVariants> _hash_table_variants;

    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants;

    // for full/right outer join
    HashTableIteratorVariants _outer_join_pull_visited_iter;
    HashTableIteratorVariants _probe_row_match_iter;

    std::shared_ptr<std::vector<Block>> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_build = false;
    bool _probe_ignore_null = false;
    int _probe_index = -1;
    bool _probe_eos = false;

    bool _build_side_ignore_null = false;

    Sizes _probe_key_sz;
    Sizes _build_key_sz;

    bool _is_broadcast_join = false;
    bool _should_build_hash_table = true;
    std::shared_ptr<SharedHashTableController> _shared_hashtable_controller = nullptr;
    std::shared_ptr<VRuntimeFilterSlots> _runtime_filter_slots = nullptr;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    // for cases when a probe row matches more than batch size build rows.
    bool _is_any_probe_match_row_output = false;
    uint8_t _build_block_idx = 0;
    int64_t _build_side_mem_used = 0;
    int64_t _build_side_last_mem_used = 0;
    MutableBlock _build_side_mutable_block;

    SharedHashTableContextPtr _shared_hash_table_context = nullptr;

    Status _materialize_build_side(RuntimeState* state) override;

    Status _process_build_block(RuntimeState* state, Block& block, uint8_t offset);

    Status _do_evaluate(Block& block, VExprContextSPtrs& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    template <bool BuildSide>
    Status _extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids);

    bool _need_probe_null_map(Block& block, const std::vector<int>& res_col_ids);

    void _set_build_ignore_flag(Block& block, const std::vector<int>& res_col_ids);

    void _hash_table_init(RuntimeState* state);
    void _process_hashtable_ctx_variants_init(RuntimeState* state);

    static constexpr auto _MAX_BUILD_BLOCK_COUNT = 128;

    void _prepare_probe_block();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    void _release_mem();

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block) override;

    Status _filter_data_and_build_output(RuntimeState* state, vectorized::Block* output_block,
                                         bool* eos, Block* temp_block,
                                         bool check_rows_count = true);

    template <class HashTableContext>
    friend struct ProcessHashTableBuild;

    template <int JoinOpType>
    friend struct ProcessHashTableProbe;

    template <class HashTableContext>
    friend struct ProcessRuntimeFilterBuild;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>> _inserted_rows;

    std::vector<IRuntimeFilter*> _runtime_filters;
    size_t _build_bf_cardinality = 0;
};
} // namespace vectorized
} // namespace doris
