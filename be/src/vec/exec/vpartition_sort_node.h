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

#include <glog/logging.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "exec/exec_node.h"
#include "vec/columns/column.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/fixed_hash_map.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/block.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace vectorized {
static constexpr size_t INITIAL_BUFFERED_BLOCK_BYTES = 64 << 20;

struct PartitionBlocks {
public:
    PartitionBlocks() = default;
    ~PartitionBlocks() = default;

    void add_row_idx(size_t row) { selector.push_back(row); }

    void append_block_by_selector(const vectorized::Block* input_block,
                                  const RowDescriptor& row_desc, bool is_limit,
                                  int64_t partition_inner_limit, int batch_size) {
        if (blocks.empty() || reach_limit()) {
            init_rows = batch_size;
            blocks.push_back(Block::create_unique(VectorizedUtils::create_empty_block(row_desc)));
        }
        auto columns = input_block->get_columns();
        auto mutable_columns = blocks.back()->mutate_columns();
        DCHECK(columns.size() == mutable_columns.size());
        for (int i = 0; i < mutable_columns.size(); ++i) {
            columns[i]->append_data_by_selector(mutable_columns[i], selector);
        }
        init_rows = init_rows - selector.size();
        total_rows = total_rows + selector.size();
        selector.clear();
    }

    void append_whole_block(vectorized::Block* input_block, const RowDescriptor& row_desc) {
        auto empty_block = Block::create_unique(VectorizedUtils::create_empty_block(row_desc));
        empty_block->swap(*input_block);
        blocks.emplace_back(std::move(empty_block));
    }

    bool reach_limit() {
        return init_rows <= 0 || blocks.back()->bytes() > INITIAL_BUFFERED_BLOCK_BYTES;
    }

    size_t get_total_rows() const { return total_rows; }

    IColumn::Selector selector;
    std::vector<std::unique_ptr<Block>> blocks;
    size_t total_rows = 0;
    int init_rows = 4096;
};

using PartitionDataPtr = PartitionBlocks*;
using PartitionDataWithStringKey = PHHashMap<StringRef, PartitionDataPtr, DefaultHash<StringRef>>;
using PartitionDataWithShortStringKey = StringHashMap<PartitionDataPtr>;
using PartitionDataWithUInt8Key =
        FixedImplicitZeroHashMapWithCalculatedSize<UInt8, PartitionDataPtr>;
using PartitionDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, PartitionDataPtr>;
using PartitionDataWithUInt32Key = PHHashMap<UInt32, PartitionDataPtr, HashCRC32<UInt32>>;
using PartitionDataWithUInt64Key = PHHashMap<UInt64, PartitionDataPtr, HashCRC32<UInt64>>;
using PartitionDataWithUInt128Key = PHHashMap<UInt128, PartitionDataPtr, HashCRC32<UInt128>>;
using PartitionDataWithUInt256Key = PHHashMap<UInt256, PartitionDataPtr, HashCRC32<UInt256>>;
template <typename TData>
struct PartitionMethodSerialized {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;
    std::vector<StringRef> keys;
    size_t keys_memory_usage = 0;
    PartitionMethodSerialized() : _serialized_key_buffer_size(0), _serialized_key_buffer(nullptr) {
        _arena.reset(new Arena());
        _serialize_key_arena.reset(new Arena());
    }

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped, true>;

    template <typename Other>
    explicit PartitionMethodSerialized(const Other& other) : data(other.data) {
        _arena.reset(new Arena());
        _serialize_key_arena.reset(new Arena());
    }

    size_t serialize_keys(const ColumnRawPtrs& key_columns, size_t num_rows) {
        if (keys.size() < num_rows) {
            keys.resize(num_rows);
        }

        size_t max_one_row_byte_size = 0;
        for (const auto& column : key_columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t total_bytes = max_one_row_byte_size * num_rows;

        if (total_bytes > config::pre_serialize_keys_limit_bytes) {
            // reach mem limit, don't serialize in batch
            // for simplicity, we just create a new arena here.
            _arena->clear();
            size_t keys_size = key_columns.size();
            for (size_t i = 0; i < num_rows; ++i) {
                keys[i] = serialize_keys_to_pool_contiguous(i, keys_size, key_columns, *_arena);
            }
            keys_memory_usage = _arena->size();
        } else {
            _arena->clear();
            if (total_bytes > _serialized_key_buffer_size) {
                _serialized_key_buffer_size = total_bytes;
                _serialize_key_arena->clear();
                _serialized_key_buffer = reinterpret_cast<uint8_t*>(
                        _serialize_key_arena->alloc(_serialized_key_buffer_size));
            }

            for (size_t i = 0; i < num_rows; ++i) {
                keys[i].data =
                        reinterpret_cast<char*>(_serialized_key_buffer + i * max_one_row_byte_size);
                keys[i].size = 0;
            }

            for (const auto& column : key_columns) {
                column->serialize_vec(keys, num_rows, max_one_row_byte_size);
            }
            keys_memory_usage = _serialized_key_buffer_size;
        }
        return max_one_row_byte_size;
    }

private:
    size_t _serialized_key_buffer_size;
    uint8_t* _serialized_key_buffer;
    std::unique_ptr<Arena> _serialize_key_arena;
    std::unique_ptr<Arena> _arena;
};

//for string
template <typename TData>
struct PartitionMethodStringNoCache {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    PartitionMethodStringNoCache() = default;

    explicit PartitionMethodStringNoCache(size_t size_hint) : data(size_hint) {}

    template <typename Other>
    explicit PartitionMethodStringNoCache(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false>;

    static const bool low_cardinality_optimization = false;
};

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData>
struct PartitionMethodOneNumber {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    PartitionMethodOneNumber() = default;

    template <typename Other>
    PartitionMethodOneNumber(const Other& other) : data(other.data) {}

    /// To use one `Method` in different threads, use different `State`.
    using State = ColumnsHashing::HashMethodOneNumber<typename Data::value_type, Mapped, FieldType,
                                                      false>;
};

template <typename Base>
struct PartitionDataWithNullKey : public Base {
    using Base::Base;

    bool& has_null_key_data() { return has_null_key; }
    PartitionDataPtr& get_null_key_data() { return null_key_data; }
    bool has_null_key_data() const { return has_null_key; }
    PartitionDataPtr get_null_key_data() const { return null_key_data; }
    size_t size() const { return Base::size() + (has_null_key ? 1 : 0); }
    bool empty() const { return Base::empty() && !has_null_key; }

    void clear() {
        Base::clear();
        has_null_key = false;
    }

    void clear_and_shrink() {
        Base::clear_and_shrink();
        has_null_key = false;
    }

private:
    bool has_null_key = false;
    PartitionDataPtr null_key_data = nullptr;
};

template <typename SingleColumnMethod>
struct PartitionMethodSingleNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;
    using BaseState = typename Base::State;

    using Data = typename Base::Data;
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;

    using Base::data;

    PartitionMethodSingleNullableColumn() = default;

    template <typename Other>
    explicit PartitionMethodSingleNullableColumn(const Other& other) : Base(other) {}

    using State = ColumnsHashing::HashMethodSingleLowNullableColumn<BaseState, Mapped, true>;
};

template <typename TData, bool has_nullable_keys_ = false>
struct PartitionMethodKeysFixed {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;
    Iterator iterator;
    PartitionMethodKeysFixed() = default;

    template <typename Other>
    PartitionMethodKeysFixed(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, Mapped,
                                                      has_nullable_keys, false>;
};

using PartitionedMethodVariants =
        std::variant<PartitionMethodSerialized<PartitionDataWithStringKey>,
                     PartitionMethodOneNumber<UInt8, PartitionDataWithUInt8Key>,
                     PartitionMethodOneNumber<UInt16, PartitionDataWithUInt16Key>,
                     PartitionMethodOneNumber<UInt32, PartitionDataWithUInt32Key>,
                     PartitionMethodOneNumber<UInt64, PartitionDataWithUInt64Key>,
                     PartitionMethodOneNumber<UInt128, PartitionDataWithUInt128Key>,
                     PartitionMethodSingleNullableColumn<PartitionMethodOneNumber<
                             UInt8, PartitionDataWithNullKey<PartitionDataWithUInt8Key>>>,
                     PartitionMethodSingleNullableColumn<PartitionMethodOneNumber<
                             UInt16, PartitionDataWithNullKey<PartitionDataWithUInt16Key>>>,
                     PartitionMethodSingleNullableColumn<PartitionMethodOneNumber<
                             UInt32, PartitionDataWithNullKey<PartitionDataWithUInt32Key>>>,
                     PartitionMethodSingleNullableColumn<PartitionMethodOneNumber<
                             UInt64, PartitionDataWithNullKey<PartitionDataWithUInt64Key>>>,
                     PartitionMethodSingleNullableColumn<PartitionMethodOneNumber<
                             UInt128, PartitionDataWithNullKey<PartitionDataWithUInt128Key>>>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt64Key, false>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt64Key, true>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt128Key, false>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt128Key, true>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt256Key, false>,
                     PartitionMethodKeysFixed<PartitionDataWithUInt256Key, true>,
                     PartitionMethodStringNoCache<PartitionDataWithShortStringKey>,
                     PartitionMethodSingleNullableColumn<PartitionMethodStringNoCache<
                             PartitionDataWithNullKey<PartitionDataWithShortStringKey>>>>;

struct PartitionedHashMapVariants
        : public DataVariants<PartitionedMethodVariants, PartitionMethodSingleNullableColumn,
                              PartitionMethodOneNumber, PartitionMethodKeysFixed,
                              PartitionDataWithNullKey> {
    template <bool nullable>
    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::serialized: {
            method_variant.emplace<PartitionMethodSerialized<PartitionDataWithStringKey>>();
            break;
        }
        case Type::int8_key: {
            emplace_single<UInt8, PartitionDataWithUInt8Key, nullable>();
            break;
        }
        case Type::int16_key: {
            emplace_single<UInt16, PartitionDataWithUInt16Key, nullable>();
            break;
        }
        case Type::int32_key: {
            emplace_single<UInt32, PartitionDataWithUInt32Key, nullable>();
            break;
        }
        case Type::int64_key: {
            emplace_single<UInt64, PartitionDataWithUInt64Key, nullable>();
            break;
        }
        case Type::int128_key: {
            emplace_single<UInt128, PartitionDataWithUInt128Key, nullable>();
            break;
        }
        case Type::int64_keys: {
            emplace_fixed<PartitionDataWithUInt64Key, nullable>();
            break;
        }
        case Type::int128_keys: {
            emplace_fixed<PartitionDataWithUInt128Key, nullable>();
            break;
        }
        case Type::int256_keys: {
            emplace_fixed<PartitionDataWithUInt256Key, nullable>();
            break;
        }
        case Type::string_key: {
            if (nullable) {
                method_variant
                        .emplace<PartitionMethodSingleNullableColumn<PartitionMethodStringNoCache<
                                PartitionDataWithNullKey<PartitionDataWithShortStringKey>>>>();
            } else {
                method_variant
                        .emplace<PartitionMethodStringNoCache<PartitionDataWithShortStringKey>>();
            }
            break;
        }
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid key type, type={}", type);
        }
    }
    void init(Type type, bool is_nullable = false) {
        if (is_nullable) {
            init<true>(type);
        } else {
            init<false>(type);
        }
    }
};

class VExprContext;

class VPartitionSortNode : public ExecNode {
public:
    VPartitionSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VPartitionSortNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

    void debug_profile();
    bool can_read();

private:
    template <typename AggState, typename AggMethod>
    void _pre_serialize_key_if_need(AggState& state, AggMethod& agg_method,
                                    const ColumnRawPtrs& key_columns, const size_t num_rows) {
        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<AggState>::value) {
            (agg_method.serialize_keys(key_columns, num_rows));
            state.set_serialized_keys(agg_method.keys.data());
        }
    }

    void _init_hash_method();
    Status _split_block_by_partition(vectorized::Block* input_block, int batch_size);
    void _emplace_into_hash_table(const ColumnRawPtrs& key_columns,
                                  const vectorized::Block* input_block, int batch_size);
    Status get_sorted_block(RuntimeState* state, Block* output_block, bool* eos);

    // hash table
    std::unique_ptr<PartitionedHashMapVariants> _partitioned_data;
    std::unique_ptr<Arena> _agg_arena_pool;
    // partition by k1,k2
    int _partition_exprs_num = 0;
    VExprContextSPtrs _partition_expr_ctxs;
    std::vector<const IColumn*> _partition_columns;
    std::vector<size_t> _partition_key_sz;
    std::vector<size_t> _hash_values;

    std::vector<std::unique_ptr<PartitionSorter>> _partition_sorts;
    std::vector<PartitionDataPtr> _value_places;
    // Expressions and parameters used for build _sort_description
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    bool _has_global_limit = false;
    int _num_partition = 0;
    int64_t _partition_inner_limit = 0;
    int _sort_idx = 0;
    std::unique_ptr<SortCursorCmp> _previous_row = nullptr;
    std::queue<Block> _blocks_buffer;
    int64_t child_input_rows = 0;
    std::mutex _buffer_mutex;
    TPartTopNPhase::type _topn_phase;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _emplace_key_timer;
    RuntimeProfile::Counter* _partition_sort_timer;
    RuntimeProfile::Counter* _get_sorted_timer;
    RuntimeProfile::Counter* _selector_block_timer;

    RuntimeProfile::Counter* _hash_table_size_counter;
    //only for profile record
    std::vector<int> partition_profile_output_rows;
};

} // namespace vectorized
} // namespace doris
