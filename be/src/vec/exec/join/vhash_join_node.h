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

namespace pipeline {
class HashJoinProbeLocalState;
class HashJoinBuildSinkLocalState;
} // namespace pipeline

namespace vectorized {

struct UInt128;
struct UInt256;
template <int JoinOpType>
struct ProcessHashTableProbe;
class HashJoinNode;

struct RuntimeFilterContext {
    RuntimeFilterContext(HashJoinNode* join_node);
    RuntimeFilterContext(pipeline::HashJoinBuildSinkLocalState* local_state);
    std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    std::shared_ptr<VRuntimeFilterSlots>& _runtime_filter_slots;
    VExprContextSPtrs& _build_expr_ctxs;
    size_t& _build_bf_cardinality;
    std::unordered_map<const Block*, std::vector<int>>& _inserted_rows;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
};

template <class HashTableContext>
struct ProcessRuntimeFilterBuild {
    ProcessRuntimeFilterBuild(RuntimeFilterContext* context) : _context(context) {}

    Status operator()(RuntimeState* state, HashTableContext& hash_table_ctx);

private:
    RuntimeFilterContext* _context;
};

struct HashJoinBuildContext {
    HashJoinBuildContext(HashJoinNode* join_node);
    HashJoinBuildContext(pipeline::HashJoinBuildSinkLocalState* local_state);

    void add_hash_buckets_info(const std::string& info) {
        _profile->add_info_string("HashTableBuckets", info);
    }
    void add_hash_buckets_filled_info(const std::string& info) {
        _profile->add_info_string("HashTableFilledBuckets", info);
    }
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _build_collisions_counter;
    RuntimeProfile::Counter* _build_buckets_fill_counter;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_table_convert_timer;
    RuntimeProfile::Counter* _build_side_compute_hash_timer;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage;
    RuntimeProfile* _profile;

    Sizes& _build_key_sz;
    bool& _build_unique;
    std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>>& _inserted_rows;
    std::shared_ptr<Arena>& _arena;
    size_t& _build_bf_cardinality;
};

using ProfileCounter = RuntimeProfile::Counter;

// TODO: Best prefetch step is decided by machine. We should also provide a
//  SQL hint to allow users to tune by hand.
static constexpr int PREFETCH_STEP = 64;

template <class HashTableContext>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinBuildContext* join_context, int batch_size, uint8_t offset,
                          RuntimeState* state)
            : _rows(rows),
              _skip_rows(0),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_context(join_context),
              _batch_size(batch_size),
              _offset(offset),
              _state(state),
              _build_side_compute_hash_timer(join_context->_build_side_compute_hash_timer) {}

    template <bool ignore_null, bool short_circuit_for_null>
    Status run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map, bool* has_null_key) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            int64_t filled_bucket_size = hash_table_ctx.hash_table.size();
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            COUNTER_SET(_join_context->_hash_table_memory_usage, bucket_bytes);
            COUNTER_SET(_join_context->_build_buckets_counter, bucket_size);
            COUNTER_SET(_join_context->_build_collisions_counter,
                        hash_table_ctx.hash_table.get_collisions());
            COUNTER_SET(_join_context->_build_buckets_fill_counter, filled_bucket_size);

            auto hash_table_buckets = hash_table_ctx.hash_table.get_buffer_sizes_in_cells();
            std::string hash_table_buckets_info;
            for (auto bucket_count : hash_table_buckets) {
                hash_table_buckets_info += std::to_string(bucket_count) + ", ";
            }
            _join_context->add_hash_buckets_info(hash_table_buckets_info);

            auto hash_table_sizes = hash_table_ctx.hash_table.sizes();
            hash_table_buckets_info.clear();
            for (auto table_size : hash_table_sizes) {
                hash_table_buckets_info += std::to_string(table_size) + ", ";
            }
            _join_context->add_hash_buckets_filled_info(hash_table_buckets_info);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_context->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_context->_build_table_insert_timer);
        hash_table_ctx.hash_table.reset_resize_timer();

        // only not build_unique, we need expanse hash table before insert data
        // 1. There are fewer duplicate keys, reducing the number of resize hash tables
        // can improve performance to a certain extent, about 2%-5%
        // 2. There are many duplicate keys, and the hash table filled bucket is far less than
        // the hash table build bucket, which may waste a lot of memory.
        // TODO, use the NDV expansion of the key column in the optimizer statistics
        if (!_join_context->_build_unique) {
            RETURN_IF_CATCH_EXCEPTION(hash_table_ctx.hash_table.expanse_for_add_elem(
                    std::min<int>(_rows, config::hash_table_pre_expanse_max_rows)));
        }

        vector<int>& inserted_rows = _join_context->_inserted_rows[&_acquired_block];
        bool has_runtime_filter = !_join_context->_runtime_filter_descs.empty();
        if (has_runtime_filter) {
            inserted_rows.reserve(_batch_size);
        }

        _build_side_hash_values.resize(_rows);
        auto& arena = *(_join_context->_arena);
        auto old_build_arena_memory = arena.size();
        {
            SCOPED_TIMER(_build_side_compute_hash_timer);
            if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<KeyGetter>::value) {
                auto old_keys_memory = hash_table_ctx.keys_memory_usage;
                hash_table_ctx.serialize_keys(_build_raw_ptrs, _rows);
                key_getter.set_serialized_keys(hash_table_ctx.keys.data());
                _join_context->_build_arena_memory_usage->add(hash_table_ctx.keys_memory_usage -
                                                              old_keys_memory);
            }

            for (size_t k = 0; k < _rows; ++k) {
                if (k % 65536 == 0) {
                    RETURN_IF_CANCELLED(_state);
                }
                if constexpr (ignore_null) {
                    if ((*null_map)[k]) {
                        continue;
                    }
                }
                // If apply short circuit strategy for null value (e.g. join operator is
                // NULL_AWARE_LEFT_ANTI_JOIN), we build hash table until we meet a null value.
                if constexpr (short_circuit_for_null) {
                    if ((*null_map)[k]) {
                        DCHECK(has_null_key);
                        *has_null_key = true;
                        return Status::OK();
                    }
                }
                if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      KeyGetter>::value) {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena).key);
                } else {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena));
                }
            }
        }

        bool build_unique = _join_context->_build_unique;
#define EMPLACE_IMPL(stmt)                                                                  \
    for (size_t k = 0; k < _rows; ++k) {                                                    \
        if (k % 65536 == 0) {                                                               \
            RETURN_IF_CANCELLED(_state);                                                    \
        }                                                                                   \
        if constexpr (ignore_null) {                                                        \
            if ((*null_map)[k]) {                                                           \
                continue;                                                                   \
            }                                                                               \
        }                                                                                   \
        auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table,             \
                                                     _build_side_hash_values[k], k, arena); \
        if (k + PREFETCH_STEP < _rows) {                                                    \
            key_getter.template prefetch_by_hash<false>(                                    \
                    hash_table_ctx.hash_table, _build_side_hash_values[k + PREFETCH_STEP]); \
        }                                                                                   \
        stmt;                                                                               \
    }

        if (has_runtime_filter && build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                        inserted_rows.push_back(k);
                        _join_context->_build_bf_cardinality++;
                    } else { _skip_rows++; });
        } else if (has_runtime_filter && !build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                        inserted_rows.push_back(k);
                        _join_context->_build_bf_cardinality++;
                    } else {
                        emplace_result.get_mapped().insert({k, _offset}, *(_join_context->_arena));
                        inserted_rows.push_back(k);
                    });
        } else if (!has_runtime_filter && build_unique) {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                    } else { _skip_rows++; });
        } else {
            EMPLACE_IMPL(
                    if (emplace_result.is_inserted()) {
                        new (&emplace_result.get_mapped()) Mapped({k, _offset});
                    } else {
                        emplace_result.get_mapped().insert({k, _offset}, *(_join_context->_arena));
                    });
        }
#undef EMPLACE_IMPL

        _join_context->_build_arena_memory_usage->add(arena.size() - old_build_arena_memory);

        COUNTER_UPDATE(_join_context->_build_table_expanse_timer,
                       hash_table_ctx.hash_table.get_resize_timer_value());
        COUNTER_UPDATE(_join_context->_build_table_convert_timer,
                       hash_table_ctx.hash_table.get_convert_timer_value());

        return Status::OK();
    }

private:
    const int _rows;
    int _skip_rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinBuildContext* _join_context;
    int _batch_size;
    uint8_t _offset;
    RuntimeState* _state;

    ProfileCounter* _build_side_compute_hash_timer;
    std::vector<size_t> _build_side_hash_values;
};

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

static constexpr auto HASH_JOIN_MAX_BUILD_BLOCK_COUNT = 128;

struct HashJoinProbeContext {
    HashJoinProbeContext(HashJoinNode* join_node);
    HashJoinProbeContext(pipeline::HashJoinProbeLocalState* local_state);
    bool _have_other_join_conjunct;
    const bool _is_right_semi_anti;
    const bool _is_outer_join;

    MutableColumnPtr* _tuple_is_null_left_flag_column;
    MutableColumnPtr* _tuple_is_null_right_flag_column;

    // other expr
    VExprContextSPtrs* _other_join_conjuncts;

    DataTypes* _right_table_data_types;
    DataTypes* _left_table_data_types;

    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _probe_process_hashtable_timer;
    RuntimeProfile::Counter* _process_other_join_conjunct_timer;
    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage;

    std::shared_ptr<Arena> _arena;

    // for full/right outer join
    HashTableIteratorVariants* _outer_join_pull_visited_iter;
    HashTableIteratorVariants* _probe_row_match_iter;

    std::shared_ptr<std::vector<Block>> _build_blocks;
    Block* _probe_block;
    ColumnRawPtrs* _probe_columns;
    int* _probe_index;

    Sizes _probe_key_sz;

    std::vector<bool>* _left_output_slot_flags;
    std::vector<bool>* _right_output_slot_flags;

    // for cases when a probe row matches more than batch size build rows.
    bool* _is_any_probe_match_row_output;
};

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

    bool ready_for_finish() {
        if (_runtime_filter_slots == nullptr) {
            return true;
        }
        return _runtime_filter_slots->ready_finish_publish();
    }

protected:
    void _probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) override;

private:
    friend struct HashJoinProbeContext;
    friend struct HashJoinBuildContext;
    friend struct RuntimeFilterContext;

    void _init_short_circuit_for_probe() override {
        _short_circuit_for_probe =
                (_short_circuit_for_null_in_probe_side &&
                 _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) ||
                (_build_blocks->empty() && _join_op == TJoinOp::INNER_JOIN && !_is_mark_join) ||
                (_build_blocks->empty() && _join_op == TJoinOp::LEFT_SEMI_JOIN && !_is_mark_join) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_OUTER_JOIN) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_SEMI_JOIN) ||
                (_build_blocks->empty() && _join_op == TJoinOp::RIGHT_ANTI_JOIN);

        //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        //we could get the result is probe table + null-column(if need output)
        _short_circuit_for_probe_and_additional_data =
                (_build_blocks->empty() && !_have_other_join_conjunct && !_is_mark_join) &&
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

    void _prepare_probe_block();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    void _release_mem();

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block) override;

    Status _filter_data_and_build_output(RuntimeState* state, vectorized::Block* output_block,
                                         bool* eos, Block* temp_block);

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
    std::atomic_bool _probe_open_finish = false;

    std::unique_ptr<HashJoinProbeContext> _probe_context;
};
} // namespace vectorized
} // namespace doris
