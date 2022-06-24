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
#include <variant>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "exprs/runtime_filter_slots.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

struct SerializedHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
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

// T should be UInt32 UInt64 UInt128
template <class T>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
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
using I8HashTableContext = PrimaryTypeHashTableContext<UInt8>;
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16>;
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32>;
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64>;
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128>;
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256>;

template <class T, bool has_null>
struct FixedKeyHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
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

template <bool has_null>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null>;

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I128HashTableContext, I256HashTableContext, I64FixedKeyHashTableContext<true>,
                     I64FixedKeyHashTableContext<false>, I128FixedKeyHashTableContext<true>,
                     I128FixedKeyHashTableContext<false>, I256FixedKeyHashTableContext<true>,
                     I256FixedKeyHashTableContext<false>>;

using JoinOpVariants =
        std::variant<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>>;

#define APPLY_FOR_JOINOP_VARIANTS(M) \
    M(INNER_JOIN)                    \
    M(LEFT_SEMI_JOIN)                \
    M(LEFT_ANTI_JOIN)                \
    M(LEFT_OUTER_JOIN)               \
    M(FULL_OUTER_JOIN)               \
    M(RIGHT_OUTER_JOIN)              \
    M(CROSS_JOIN)                    \
    M(RIGHT_SEMI_JOIN)               \
    M(RIGHT_ANTI_JOIN)               \
    M(NULL_AWARE_LEFT_ANTI_JOIN)

class VExprContext;

class HashJoinNode : public ::doris::ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    HashTableVariants& get_hash_table_variants() { return _hash_table_variants; }
    void init_join_op();

    const RowDescriptor& row_desc() const override { return _output_row_desc; }

private:
    using VExprContexts = std::vector<VExprContext*>;

    TJoinOp::type _join_op;

    JoinOpVariants _join_op_variants;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    std::unique_ptr<VExprContext*> _vother_join_conjunct_ptr;
    // output expr
    VExprContexts _output_expr_ctxs;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    // mark the build hash table whether contain null column
    std::vector<bool> _build_not_ignore_null;
    // mark the probe table should dispose null column
    std::vector<bool> _probe_not_ignore_null;

    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;

    int64_t _hash_table_rows;
    int64_t _mem_used;

    Arena _arena;
    HashTableVariants _hash_table_variants;

    std::vector<Block> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _probe_ignore_null = false;
    int _probe_index = -1;
    bool _probe_eos = false;

    Sizes _probe_key_sz;
    Sizes _build_key_sz;

    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_one_build; // match at most one build row to each probe row. Left semi Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    bool _build_unique;          // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_outer_join;
    bool _have_other_join_conjunct = false;

    RowDescriptor _row_desc_for_other_join_conjunt;
    Block _join_block;

    std::vector<uint32_t> _items_counts;
    std::vector<int8_t> _build_block_offsets;
    std::vector<int> _build_block_rows;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    RowDescriptor _output_row_desc;

private:
    Status _hash_table_build(RuntimeState* state);
    Status _process_build_block(RuntimeState* state, Block& block, uint8_t offset);

    Status _extract_build_join_column(Block& block, NullMap& null_map, ColumnRawPtrs& raw_ptrs,
                                      bool& ignore_null, RuntimeProfile::Counter& expr_call_timer);

    Status _extract_probe_join_column(Block& block, NullMap& null_map, ColumnRawPtrs& raw_ptrs,
                                      bool& ignore_null, RuntimeProfile::Counter& expr_call_timer);

    void _hash_table_init();

    void _prepare_probe_block();

    void _construct_mutable_join_block();

    Status _build_output_block(Block* origin_block, Block* output_block);

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    template <class HashTableContext, bool ignore_null, bool build_unique>
    friend struct ProcessHashTableBuild;

    template <class HashTableContext, class JoinOpType, bool ignore_null>
    friend struct ProcessHashTableProbe;

    template <class HashTableContext>
    friend struct ProcessRuntimeFilterBuild;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>> _inserted_rows;
};
} // namespace vectorized
} // namespace doris
