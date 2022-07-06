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

#include <functional>
#include <variant>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/fixed_hash_map.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class MemPool;

namespace vectorized {
class VExprContext;

/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    AggregationMethodSerialized() = default;

    template <typename Other>
    explicit AggregationMethodSerialized(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped>;

    static void insert_key_into_columns(const StringRef& key, MutableColumns& key_columns,
                                        const Sizes&) {
        auto pos = key.data;
        for (auto& column : key_columns) pos = column->deserialize_and_insert_from_arena(pos);
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }
};

using AggregatedDataWithoutKey = AggregateDataPtr;
using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData, bool consecutive_keys_optimization = true>
struct AggregationMethodOneNumber {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    AggregationMethodOneNumber() = default;

    template <typename Other>
    AggregationMethodOneNumber(const Other& other) : data(other.data) {}

    /// To use one `Method` in different threads, use different `State`.
    using State = ColumnsHashing::HashMethodOneNumber<typename Data::value_type, Mapped, FieldType,
                                                      consecutive_keys_optimization>;

    // Insert the key from the hash table into columns.
    static void insert_key_into_columns(const Key& key, MutableColumns& key_columns,
                                        const Sizes& /*key_sizes*/) {
        const auto* key_holder = reinterpret_cast<const char*>(&key);
        auto* column = static_cast<ColumnVectorHelper*>(key_columns[0].get());
        column->insert_raw_data<sizeof(FieldType)>(key_holder);
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }
};

template <typename Base>
struct AggregationDataWithNullKey : public Base {
    using Base::Base;

    bool& has_null_key_data() { return has_null_key; }
    AggregateDataPtr& get_null_key_data() { return null_key_data; }
    bool has_null_key_data() const { return has_null_key; }
    const AggregateDataPtr get_null_key_data() const { return null_key_data; }
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
    AggregateDataPtr null_key_data = nullptr;
};

template <typename TData, bool has_nullable_keys_ = false>
struct AggregationMethodKeysFixed {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;
    Iterator iterator;
    bool inited = false;

    AggregationMethodKeysFixed() {}

    template <typename Other>
    AggregationMethodKeysFixed(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, Mapped,
                                                      has_nullable_keys>;

    static void insert_key_into_columns(const Key& key, MutableColumns& key_columns,
                                        const Sizes& key_sizes) {
        size_t keys_size = key_columns.size();

        static constexpr auto bitmap_size =
                has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = bitmap_size;

        for (size_t i = 0; i < keys_size; ++i) {
            IColumn* observed_column;
            ColumnUInt8* null_map;

            bool column_nullable = false;
            if constexpr (has_nullable_keys) column_nullable = is_column_nullable(*key_columns[i]);

            /// If we have a nullable column, get its nested column and its null map.
            if (column_nullable) {
                ColumnNullable& nullable_col = assert_cast<ColumnNullable&>(*key_columns[i]);
                observed_column = &nullable_col.get_nested_column();
                null_map = assert_cast<ColumnUInt8*>(&nullable_col.get_null_map_column());
            } else {
                observed_column = key_columns[i].get();
                null_map = nullptr;
            }

            bool is_null = false;
            if (column_nullable) {
                /// The current column is nullable. Check if the value of the
                /// corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                UInt8 val = (reinterpret_cast<const UInt8*>(&key)[bucket] >> offset) & 1;
                null_map->insert_value(val);
                is_null = val == 1;
            }

            if (has_nullable_keys && is_null)
                observed_column->insert_default();
            else {
                size_t size = key_sizes[i];
                observed_column->insert_data(reinterpret_cast<const char*>(&key) + pos, size);
                pos += size;
            }
        }
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct AggregationMethodSingleNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;
    using BaseState = typename Base::State;

    using Data = typename Base::Data;
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;

    using Base::data;

    AggregationMethodSingleNullableColumn() = default;

    template <typename Other>
    explicit AggregationMethodSingleNullableColumn(const Other& other) : Base(other) {}

    using State = ColumnsHashing::HashMethodSingleLowNullableColumn<BaseState, Mapped, true>;

    static void insert_key_into_columns(const Key& key, MutableColumns& key_columns,
                                        const Sizes& /*key_sizes*/) {
        auto col = key_columns[0].get();

        if constexpr (std::is_same_v<Key, StringRef>) {
            col->insert_data(key.data, key.size);
        } else {
            col->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
        }
    }
};

using AggregatedDataWithUInt8Key =
        FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;
using AggregatedDataWithUInt32Key = HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithUInt128Key = HashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithUInt256Key = HashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;

using AggregatedDataWithNullableUInt8Key = AggregationDataWithNullKey<AggregatedDataWithUInt8Key>;
using AggregatedDataWithNullableUInt16Key = AggregationDataWithNullKey<AggregatedDataWithUInt16Key>;
using AggregatedDataWithNullableUInt32Key = AggregationDataWithNullKey<AggregatedDataWithUInt32Key>;
using AggregatedDataWithNullableUInt64Key = AggregationDataWithNullKey<AggregatedDataWithUInt64Key>;
using AggregatedDataWithNullableUInt128Key =
        AggregationDataWithNullKey<AggregatedDataWithUInt128Key>;

using AggregatedMethodVariants = std::variant<
        AggregationMethodSerialized<AggregatedDataWithStringKey>,
        AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>,
        AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>,
        AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32Key>,
        AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>,
        AggregationMethodOneNumber<UInt128, AggregatedDataWithUInt128Key>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt128, AggregatedDataWithNullableUInt128Key>>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, true>>;

struct AggregatedDataVariants {
    AggregatedDataVariants() = default;
    AggregatedDataVariants(const AggregatedDataVariants&) = delete;
    AggregatedDataVariants& operator=(const AggregatedDataVariants&) = delete;
    AggregatedDataWithoutKey without_key = nullptr;
    AggregatedMethodVariants _aggregated_method_variant;

    // TODO: may we should support uint256 in the future
    enum class Type {
        EMPTY = 0,
        without_key,
        serialized,
        int8_key,
        int16_key,
        int32_key,
        int64_key,
        int128_key,
        int64_keys,
        int128_keys,
        int256_keys
    };

    Type _type = Type::EMPTY;

    void init(Type type, bool is_nullable = false) {
        _type = type;
        switch (_type) {
        case Type::without_key:
            break;
        case Type::serialized:
            _aggregated_method_variant
                    .emplace<AggregationMethodSerialized<AggregatedDataWithStringKey>>();
            break;
        case Type::int8_key:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodSingleNullableColumn<AggregationMethodOneNumber<
                                UInt8, AggregatedDataWithNullableUInt8Key, false>>>();
            } else {
                _aggregated_method_variant.emplace<
                        AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>>();
            }
            break;
        case Type::int16_key:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodSingleNullableColumn<AggregationMethodOneNumber<
                                UInt16, AggregatedDataWithNullableUInt16Key, false>>>();
            } else {
                _aggregated_method_variant.emplace<
                        AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>>();
            }
            break;
        case Type::int32_key:
            if (is_nullable) {
                _aggregated_method_variant.emplace<AggregationMethodSingleNullableColumn<
                        AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key>>>();
            } else {
                _aggregated_method_variant
                        .emplace<AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32Key>>();
            }
            break;
        case Type::int64_key:
            if (is_nullable) {
                _aggregated_method_variant.emplace<AggregationMethodSingleNullableColumn<
                        AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>>();
            } else {
                _aggregated_method_variant
                        .emplace<AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>>();
            }
            break;
        case Type::int128_key:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodSingleNullableColumn<AggregationMethodOneNumber<
                                UInt128, AggregatedDataWithNullableUInt128Key>>>();
            } else {
                _aggregated_method_variant.emplace<
                        AggregationMethodOneNumber<UInt128, AggregatedDataWithUInt128Key>>();
            }
            break;
        case Type::int64_keys:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, true>>();
            } else {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, false>>();
            }
            break;
        case Type::int128_keys:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, true>>();
            } else {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, false>>();
            }
            break;
        case Type::int256_keys:
            if (is_nullable) {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, true>>();
            } else {
                _aggregated_method_variant
                        .emplace<AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, false>>();
            }
            break;
        default:
            DCHECK(false) << "Do not have a rigth agg data type";
        }
    }
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;

// not support spill
class AggregationNode : public ::doris::ExecNode {
public:
    using Sizes = std::vector<size_t>;

    AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregationNode();
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // group by k1,k2
    std::vector<VExprContext*> _probe_expr_ctxs;
    // left / full join will change the output nullable make output/input solt
    // nullable diff. so we need make nullable of it.
    std::vector<size_t> _make_nullable_output_column_pos;
    std::vector<size_t> _probe_key_sz;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::vector<bool> _aggregate_evaluators_changed_flags;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    bool _is_merge;
    bool _is_update_stage;
    std::unique_ptr<MemPool> _mem_pool;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    AggregatedDataVariants _agg_data;

    Arena _agg_arena_pool;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _exec_timer;
    RuntimeProfile::Counter* _merge_timer;
    RuntimeProfile::Counter* _expr_timer;
    RuntimeProfile::Counter* _get_results_timer;

    bool _is_streaming_preagg;
    Block _preagg_block = Block();
    bool _should_expand_hash_table = true;
    std::vector<char*> _streaming_pre_places;

private:
    /// Return true if we should keep expanding hash tables in the preagg. If false,
    /// the preagg should pass through any rows it can't fit in its tables.
    bool _should_expand_preagg_hash_tables();

    void _make_nullable_output_column(Block* block);

    Status _create_agg_status(AggregateDataPtr data);
    Status _destroy_agg_status(AggregateDataPtr data);

    Status _get_without_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_without_key(RuntimeState* state, Block* block, bool* eos);
    Status _execute_without_key(Block* block);
    Status _merge_without_key(Block* block);
    void _update_memusage_without_key();
    void _close_without_key();

    Status _get_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _pre_agg_with_serialized_key(Block* in_block, Block* out_block);
    Status _execute_with_serialized_key(Block* block);
    Status _merge_with_serialized_key(Block* block);
    void _update_memusage_with_serialized_key();
    void _close_with_serialized_key();
    void _init_hash_method(std::vector<VExprContext*>& probe_exprs);

    void release_tracker();

    using vectorized_execute = std::function<Status(Block* block)>;
    using vectorized_pre_agg = std::function<Status(Block* in_block, Block* out_block)>;
    using vectorized_get_result =
            std::function<Status(RuntimeState* state, Block* block, bool* eos)>;
    using vectorized_closer = std::function<void()>;
    using vectorized_update_memusage = std::function<void()>;

    struct executor {
        vectorized_execute execute;
        vectorized_pre_agg pre_agg;
        vectorized_get_result get_result;
        vectorized_closer close;
        vectorized_update_memusage update_memusage;
    };

    executor _executor;

    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };

    MemoryRecord _mem_usage_record;
};
} // namespace vectorized
} // namespace doris
