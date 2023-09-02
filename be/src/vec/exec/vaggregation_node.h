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

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/global_types.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector_helper.h"
#include "vec/columns/columns_number.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/allocator.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/fixed_hash_map.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/block.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TupleDescriptor;

namespace pipeline {
class AggSinkOperator;
class AggSourceOperator;
class StreamingAggSinkOperator;
class StreamingAggSourceOperator;
} // namespace pipeline

namespace vectorized {

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
    std::vector<StringRef> keys;
    size_t keys_memory_usage = 0;
    AggregationMethodSerialized()
            : _serialized_key_buffer_size(0), _serialized_key_buffer(nullptr) {
        _arena.reset(new Arena());
        _serialize_key_arena.reset(new Arena());
    }

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped, true>;

    template <typename Other>
    explicit AggregationMethodSerialized(const Other& other) : data(other.data) {}

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

    static void insert_keys_into_columns(std::vector<StringRef>& keys, MutableColumns& key_columns,
                                         const size_t num_rows, const Sizes&) {
        for (auto& column : key_columns) {
            column->deserialize_vec(keys, num_rows);
        }
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }

    void reset() {
        _arena.reset(new Arena());
        keys_memory_usage = 0;
        _serialized_key_buffer_size = 0;
    }

private:
    size_t _serialized_key_buffer_size;
    uint8_t* _serialized_key_buffer;
    std::unique_ptr<Arena> _serialize_key_arena;
    std::unique_ptr<Arena> _arena;
};

using AggregatedDataWithoutKey = AggregateDataPtr;
using AggregatedDataWithStringKey = PHHashMap<StringRef, AggregateDataPtr, DefaultHash<StringRef>>;
using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;

template <typename TData>
struct AggregationMethodStringNoCache {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    using Iterator = typename Data::iterator;

    Data data;
    Iterator iterator;
    bool inited = false;

    AggregationMethodStringNoCache() = default;

    explicit AggregationMethodStringNoCache(size_t size_hint) : data(size_hint) {}

    template <typename Other>
    explicit AggregationMethodStringNoCache(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false>;

    static const bool low_cardinality_optimization = false;

    static void insert_keys_into_columns(std::vector<StringRef>& keys, MutableColumns& key_columns,
                                         const size_t num_rows, const Sizes&) {
        key_columns[0]->reserve(num_rows);
        key_columns[0]->insert_many_strings(keys.data(), num_rows);
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iterator = data.begin();
        }
    }
};

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData>
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
                                                      false>;

    static void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                         const size_t num_rows, const Sizes&) {
        key_columns[0]->reserve(num_rows);
        auto* column = static_cast<ColumnVectorHelper*>(key_columns[0].get());
        for (size_t i = 0; i != num_rows; ++i) {
            const auto* key_holder = reinterpret_cast<const char*>(&keys[i]);
            column->insert_raw_data<sizeof(FieldType)>(key_holder);
        }
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
    AggregateDataPtr get_null_key_data() const { return null_key_data; }
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

    AggregationMethodKeysFixed() = default;

    template <typename Other>
    AggregationMethodKeysFixed(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, Mapped,
                                                      has_nullable_keys, false>;

    static void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                         const size_t num_rows, const Sizes& key_sizes) {
        // In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = has_nullable_keys ? get_bitmap_size(key_columns.size()) : 0;

        for (size_t i = 0; i < key_columns.size(); ++i) {
            size_t size = key_sizes[i];
            char* data = nullptr;
            key_columns[i]->resize(num_rows);
            // If we have a nullable column, get its nested column and its null map.
            if (is_column_nullable(*key_columns[i])) {
                ColumnNullable& nullable_col = assert_cast<ColumnNullable&>(*key_columns[i]);

                data = const_cast<char*>(nullable_col.get_nested_column().get_raw_data().data);
                UInt8* nullmap = assert_cast<ColumnUInt8*>(&nullable_col.get_null_map_column())
                                         ->get_data()
                                         .data();

                // The current column is nullable. Check if the value of the
                // corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                for (size_t j = 0; j < num_rows; j++) {
                    nullmap[j] = (reinterpret_cast<const UInt8*>(&keys[j])[bucket] >> offset) & 1;
                }
            } else {
                data = const_cast<char*>(key_columns[i]->get_raw_data().data);
            }

            auto foo = [&]<typename Fixed>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), size);
                for (size_t j = 0; j < num_rows; j++) {
                    memcpy_fixed<Fixed>(data + j * sizeof(Fixed), (char*)(&keys[j]) + pos);
                }
            };

            if (size == 1) {
                foo(int8_t());
            } else if (size == 2) {
                foo(int16_t());
            } else if (size == 4) {
                foo(int32_t());
            } else if (size == 8) {
                foo(int64_t());
            } else if (size == 16) {
                foo(UInt128());
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "pack_fixeds input invalid key size, key_size={}", size);
            }

            pos += size;
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

    static void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                         const size_t num_rows, const Sizes&) {
        auto col = key_columns[0].get();
        col->reserve(num_rows);
        if constexpr (std::is_same_v<Key, StringRef>) {
            col->insert_many_strings(keys.data(), num_rows);
        } else {
            col->insert_many_raw_data(reinterpret_cast<char*>(keys.data()), num_rows);
        }
    }
};

using AggregatedDataWithUInt8Key =
        FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;
using AggregatedDataWithUInt32Key = PHHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64Key = PHHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithUInt128Key = PHHashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithUInt256Key = PHHashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;
using AggregatedDataWithUInt136Key = PHHashMap<UInt136, AggregateDataPtr, HashCRC32<UInt136>>;

using AggregatedDataWithUInt32KeyPhase2 =
        PHHashMap<UInt32, AggregateDataPtr, HashMixWrapper<UInt32>>;
using AggregatedDataWithUInt64KeyPhase2 =
        PHHashMap<UInt64, AggregateDataPtr, HashMixWrapper<UInt64>>;
using AggregatedDataWithUInt128KeyPhase2 =
        PHHashMap<UInt128, AggregateDataPtr, HashMixWrapper<UInt128>>;
using AggregatedDataWithUInt256KeyPhase2 =
        PHHashMap<UInt256, AggregateDataPtr, HashMixWrapper<UInt256>>;

using AggregatedDataWithUInt136KeyPhase2 =
        PHHashMap<UInt136, AggregateDataPtr, HashMixWrapper<UInt136>>;

using AggregatedDataWithNullableUInt8Key = AggregationDataWithNullKey<AggregatedDataWithUInt8Key>;
using AggregatedDataWithNullableUInt16Key = AggregationDataWithNullKey<AggregatedDataWithUInt16Key>;
using AggregatedDataWithNullableUInt32Key = AggregationDataWithNullKey<AggregatedDataWithUInt32Key>;
using AggregatedDataWithNullableUInt64Key = AggregationDataWithNullKey<AggregatedDataWithUInt64Key>;
using AggregatedDataWithNullableUInt32KeyPhase2 =
        AggregationDataWithNullKey<AggregatedDataWithUInt32KeyPhase2>;
using AggregatedDataWithNullableUInt64KeyPhase2 =
        AggregationDataWithNullKey<AggregatedDataWithUInt64KeyPhase2>;
using AggregatedDataWithNullableShortStringKey =
        AggregationDataWithNullKey<AggregatedDataWithShortStringKey>;
using AggregatedDataWithNullableUInt128Key =
        AggregationDataWithNullKey<AggregatedDataWithUInt128Key>;
using AggregatedDataWithNullableUInt128KeyPhase2 =
        AggregationDataWithNullKey<AggregatedDataWithUInt128KeyPhase2>;

using AggregatedMethodVariants = std::variant<
        AggregationMethodSerialized<AggregatedDataWithStringKey>,
        AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key>,
        AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key>,
        AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32Key>,
        AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>,
        AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>,
        AggregationMethodOneNumber<UInt128, AggregatedDataWithUInt128Key>,
        AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32KeyPhase2>,
        AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyPhase2>,
        AggregationMethodOneNumber<UInt128, AggregatedDataWithUInt128KeyPhase2>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyPhase2>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyPhase2>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt128, AggregatedDataWithNullableUInt128Key>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodOneNumber<UInt128, AggregatedDataWithNullableUInt128KeyPhase2>>,
        AggregationMethodSingleNullableColumn<
                AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKey>>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt136Key, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt136Key, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyPhase2, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyPhase2, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128KeyPhase2, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt128KeyPhase2, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256KeyPhase2, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt256KeyPhase2, true>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt136KeyPhase2, false>,
        AggregationMethodKeysFixed<AggregatedDataWithUInt136KeyPhase2, true>>;

struct AggregatedDataVariants
        : public DataVariants<AggregatedMethodVariants, AggregationMethodSingleNullableColumn,
                              AggregationMethodOneNumber, AggregationMethodKeysFixed,
                              AggregationDataWithNullKey> {
    AggregatedDataWithoutKey without_key = nullptr;

    template <bool nullable>
    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::without_key:
            break;
        case Type::serialized:
            method_variant.emplace<AggregationMethodSerialized<AggregatedDataWithStringKey>>();
            break;
        case Type::int8_key:
            emplace_single<UInt8, AggregatedDataWithUInt8Key, nullable>();
            break;
        case Type::int16_key:
            emplace_single<UInt16, AggregatedDataWithUInt16Key, nullable>();
            break;
        case Type::int32_key:
            emplace_single<UInt32, AggregatedDataWithUInt32Key, nullable>();
            break;
        case Type::int32_key_phase2:
            emplace_single<UInt32, AggregatedDataWithUInt32KeyPhase2, nullable>();
            break;
        case Type::int64_key:
            emplace_single<UInt64, AggregatedDataWithUInt64Key, nullable>();
            break;
        case Type::int64_key_phase2:
            emplace_single<UInt64, AggregatedDataWithUInt64KeyPhase2, nullable>();
            break;
        case Type::int128_key:
            emplace_single<UInt128, AggregatedDataWithUInt128Key, nullable>();
            break;
        case Type::int128_key_phase2:
            emplace_single<UInt128, AggregatedDataWithUInt128KeyPhase2, nullable>();
            break;
        case Type::int64_keys:
            emplace_fixed<AggregatedDataWithUInt64Key, nullable>();
            break;
        case Type::int64_keys_phase2:
            emplace_fixed<AggregatedDataWithUInt64KeyPhase2, nullable>();
            break;
        case Type::int128_keys:
            emplace_fixed<AggregatedDataWithUInt128Key, nullable>();
            break;
        case Type::int128_keys_phase2:
            emplace_fixed<AggregatedDataWithUInt128KeyPhase2, nullable>();
            break;
        case Type::int136_keys:
            emplace_fixed<AggregatedDataWithUInt136Key, nullable>();
            break;
        case Type::int136_keys_phase2:
            emplace_fixed<AggregatedDataWithUInt136KeyPhase2, nullable>();
            break;
        case Type::int256_keys:
            emplace_fixed<AggregatedDataWithUInt256Key, nullable>();
            break;
        case Type::int256_keys_phase2:
            emplace_fixed<AggregatedDataWithUInt256KeyPhase2, nullable>();
            break;
        case Type::string_key:
            if (nullable) {
                method_variant.emplace<
                        AggregationMethodSingleNullableColumn<AggregationMethodStringNoCache<
                                AggregatedDataWithNullableShortStringKey>>>();
            } else {
                method_variant.emplace<
                        AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>>();
            }
            break;
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

using AggregatedDataVariantsUPtr = std::unique_ptr<AggregatedDataVariants>;
using ArenaUPtr = std::unique_ptr<Arena>;

struct AggregateDataContainer {
public:
    AggregateDataContainer(size_t size_of_key, size_t size_of_aggregate_states)
            : _size_of_key(size_of_key), _size_of_aggregate_states(size_of_aggregate_states) {
        _expand();
    }

    int64_t memory_usage() const { return _arena_pool.size(); }

    template <typename KeyType>
    AggregateDataPtr append_data(const KeyType& key) {
        assert(sizeof(KeyType) == _size_of_key);
        if (UNLIKELY(_index_in_sub_container == SUB_CONTAINER_CAPACITY)) {
            _expand();
        }

        *reinterpret_cast<KeyType*>(_current_keys) = key;
        auto aggregate_data = _current_agg_data;
        ++_total_count;
        ++_index_in_sub_container;
        _current_agg_data += _size_of_aggregate_states;
        _current_keys += _size_of_key;
        return aggregate_data;
    }

    template <typename Derived, bool IsConst>
    class IteratorBase {
        using Container =
                std::conditional_t<IsConst, const AggregateDataContainer, AggregateDataContainer>;

        Container* container;
        uint32_t index;
        uint32_t sub_container_index;
        uint32_t index_in_sub_container;

        friend class HashTable;

    public:
        IteratorBase() = default;
        IteratorBase(Container* container_, uint32_t index_)
                : container(container_), index(index_) {
            sub_container_index = index / SUB_CONTAINER_CAPACITY;
            index_in_sub_container = index - sub_container_index * SUB_CONTAINER_CAPACITY;
        }

        bool operator==(const IteratorBase& rhs) const { return index == rhs.index; }
        bool operator!=(const IteratorBase& rhs) const { return index != rhs.index; }

        Derived& operator++() {
            index++;
            index_in_sub_container++;
            if (index_in_sub_container == SUB_CONTAINER_CAPACITY) {
                index_in_sub_container = 0;
                sub_container_index++;
            }
            return static_cast<Derived&>(*this);
        }

        template <typename KeyType>
        KeyType get_key() {
            assert(sizeof(KeyType) == container->_size_of_key);
            return ((KeyType*)(container->_key_containers[sub_container_index]))
                    [index_in_sub_container];
        }

        AggregateDataPtr get_aggregate_data() {
            return &(container->_value_containers[sub_container_index]
                                                 [container->_size_of_aggregate_states *
                                                  index_in_sub_container]);
        }
    };

    class Iterator : public IteratorBase<Iterator, false> {
    public:
        using IteratorBase<Iterator, false>::IteratorBase;
    };

    class ConstIterator : public IteratorBase<ConstIterator, true> {
    public:
        using IteratorBase<ConstIterator, true>::IteratorBase;
    };

    ConstIterator begin() const { return ConstIterator(this, 0); }

    ConstIterator cbegin() const { return begin(); }

    Iterator begin() { return Iterator(this, 0); }

    ConstIterator end() const { return ConstIterator(this, _total_count); }
    ConstIterator cend() const { return end(); }
    Iterator end() { return Iterator(this, _total_count); }

    void init_once() {
        if (_inited) {
            return;
        }
        _inited = true;
        iterator = begin();
    }
    Iterator iterator;

private:
    void _expand() {
        _index_in_sub_container = 0;
        _current_keys = _arena_pool.alloc(_size_of_key * SUB_CONTAINER_CAPACITY);
        _key_containers.emplace_back(_current_keys);

        _current_agg_data = (AggregateDataPtr)_arena_pool.alloc(_size_of_aggregate_states *
                                                                SUB_CONTAINER_CAPACITY);
        _value_containers.emplace_back(_current_agg_data);
    }

    static constexpr uint32_t SUB_CONTAINER_CAPACITY = 8192;
    Arena _arena_pool;
    std::vector<char*> _key_containers;
    std::vector<AggregateDataPtr> _value_containers;
    AggregateDataPtr _current_agg_data;
    char* _current_keys;
    size_t _size_of_key {};
    size_t _size_of_aggregate_states {};
    uint32_t _index_in_sub_container {};
    uint32_t _total_count {};
    bool _inited = false;
};

struct AggSpillContext {
    bool has_data = false;
    bool readers_prepared = false;

    /// stream ids of writers/readers
    std::vector<int64_t> stream_ids;
    std::vector<BlockSpillReaderUPtr> readers;
    RuntimeProfile* runtime_profile;

    size_t read_cursor {};

    Status prepare_for_reading();

    ~AggSpillContext() {
        for (auto& reader : readers) {
            if (reader) {
                reader->close();
                reader.reset();
            }
        }
    }
};

struct SpillPartitionHelper {
    const size_t partition_count_bits;
    const size_t partition_count;
    const size_t max_partition_index;

    SpillPartitionHelper(const size_t partition_count_bits_)
            : partition_count_bits(partition_count_bits_),
              partition_count(1 << partition_count_bits),
              max_partition_index(partition_count - 1) {}

    size_t get_index(size_t hash_value) const {
        return (hash_value >> (32 - partition_count_bits)) & max_partition_index;
    }
};

// not support spill
class AggregationNode : public ::doris::ExecNode {
public:
    using Sizes = std::vector<size_t>;

    AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregationNode() override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare_profile(RuntimeState* state);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    Status do_pre_agg(vectorized::Block* input_block, vectorized::Block* output_block);
    bool is_streaming_preagg() const { return _is_streaming_preagg; }
    bool is_aggregate_evaluators_empty() const { return _aggregate_evaluators.empty(); }
    void _make_nullable_output_key(Block* block);

protected:
    bool _is_streaming_preagg;
    bool _child_eos = false;
    Block _preagg_block = Block();
    ArenaUPtr _agg_arena_pool;
    // group by k1,k2
    VExprContextSPtrs _probe_expr_ctxs;
    AggregatedDataVariantsUPtr _agg_data;

    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _hash_values;
    // left / full join will change the key nullable make output/input solt
    // nullable diff. so we need make nullable of it.
    std::vector<size_t> _make_nullable_keys;
    RuntimeProfile::Counter* _hash_table_compute_timer;
    RuntimeProfile::Counter* _hash_table_emplace_timer;
    RuntimeProfile::Counter* _hash_table_input_counter;
    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _expr_timer;
    RuntimeProfile::Counter* _exec_timer;

private:
    friend class pipeline::AggSinkOperator;
    friend class pipeline::StreamingAggSinkOperator;
    friend class pipeline::AggSourceOperator;
    friend class pipeline::StreamingAggSourceOperator;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    bool _is_merge;
    bool _is_first_phase;
    std::unique_ptr<Arena> _agg_profile_arena;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    size_t _external_agg_bytes_threshold;
    size_t _partitioned_threshold = 0;

    AggSpillContext _spill_context;
    std::unique_ptr<SpillPartitionHelper> _spill_partition_helper;

    RuntimeProfile::Counter* _build_table_convert_timer;
    RuntimeProfile::Counter* _serialize_key_timer;
    RuntimeProfile::Counter* _merge_timer;
    RuntimeProfile::Counter* _get_results_timer;
    RuntimeProfile::Counter* _serialize_data_timer;
    RuntimeProfile::Counter* _serialize_result_timer;
    RuntimeProfile::Counter* _deserialize_data_timer;
    RuntimeProfile::Counter* _hash_table_iterate_timer;
    RuntimeProfile::Counter* _insert_keys_to_column_timer;
    RuntimeProfile::Counter* _streaming_agg_timer;
    RuntimeProfile::Counter* _hash_table_size_counter;
    RuntimeProfile::Counter* _max_row_size_counter;
    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage;

    bool _should_expand_hash_table = true;
    bool _should_limit_output = false;
    bool _reach_limit = false;
    bool _agg_data_created_without_key = false;

    PODArray<AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;
    std::vector<AggregateDataPtr> _values;
    std::unique_ptr<AggregateDataContainer> _aggregate_data_container;

    void _release_self_resource(RuntimeState* state);
    /// Return true if we should keep expanding hash tables in the preagg. If false,
    /// the preagg should pass through any rows it can't fit in its tables.
    bool _should_expand_preagg_hash_tables();

    size_t _get_hash_table_size();

    Status _create_agg_status(AggregateDataPtr data);
    Status _destroy_agg_status(AggregateDataPtr data);

    Status _get_without_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_without_key(RuntimeState* state, Block* block, bool* eos);
    Status _execute_without_key(Block* block);
    Status _merge_without_key(Block* block);
    void _update_memusage_without_key();
    void _close_without_key();

    Status _get_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _get_result_with_serialized_key_non_spill(RuntimeState* state, Block* block, bool* eos);

    Status _merge_spilt_data();

    Status _get_result_with_spilt_data(RuntimeState* state, Block* block, bool* eos);

    Status _serialize_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _serialize_with_serialized_key_result_non_spill(RuntimeState* state, Block* block,
                                                           bool* eos);
    Status _serialize_with_serialized_key_result_with_spilt_data(RuntimeState* state, Block* block,
                                                                 bool* eos);

    Status _pre_agg_with_serialized_key(Block* in_block, Block* out_block);
    Status _execute_with_serialized_key(Block* block);
    Status _merge_with_serialized_key(Block* block);
    void _update_memusage_with_serialized_key();
    void _close_with_serialized_key();
    void _init_hash_method(const VExprContextSPtrs& probe_exprs);

protected:
    template <typename AggState, typename AggMethod>
    void _pre_serialize_key_if_need(AggState& state, AggMethod& agg_method,
                                    const ColumnRawPtrs& key_columns, const size_t num_rows) {
        if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<AggState>::value) {
            auto old_keys_memory = agg_method.keys_memory_usage;
            SCOPED_TIMER(_serialize_key_timer);
            int64_t row_size = (int64_t)(agg_method.serialize_keys(key_columns, num_rows));
            COUNTER_SET(_max_row_size_counter, std::max(_max_row_size_counter->value(), row_size));
            state.set_serialized_keys(agg_method.keys.data());

            _serialize_key_arena_memory_usage->add(agg_method.keys_memory_usage - old_keys_memory);
        }
    }

private:
    template <bool limit>
    Status _execute_with_serialized_key_helper(Block* block) {
        SCOPED_TIMER(_build_timer);
        DCHECK(!_probe_expr_ctxs.empty());

        size_t key_size = _probe_expr_ctxs.size();
        ColumnRawPtrs key_columns(key_size);
        {
            SCOPED_TIMER(_expr_timer);
            for (size_t i = 0; i < key_size; ++i) {
                int result_column_id = -1;
                RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
                block->get_by_position(result_column_id).column =
                        block->get_by_position(result_column_id)
                                .column->convert_to_full_column_if_const();
                key_columns[i] = block->get_by_position(result_column_id).column.get();
            }
        }

        int rows = block->rows();
        if (_places.size() < rows) {
            _places.resize(rows);
        }

        if constexpr (limit) {
            _find_in_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add_selected(
                        block, _offsets_of_aggregate_states[i], _places.data(),
                        _agg_arena_pool.get()));
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                        block, _offsets_of_aggregate_states[i], _places.data(),
                        _agg_arena_pool.get()));
            }

            if (_should_limit_output) {
                _reach_limit = _get_hash_table_size() >= _limit;
                if (_reach_limit && _can_short_circuit) {
                    _can_read = true;
                    return Status::Error<ErrorCode::END_OF_FILE>("");
                }
            }
        }

        return Status::OK();
    }

    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    int _get_slot_column_id(const AggFnEvaluator* evaluator) {
        auto ctxs = evaluator->input_exprs_ctxs();
        CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
                << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
                << ctxs[0]->root()->debug_string();
        return ((VSlotRef*)ctxs[0]->root().get())->column_id();
    }

    template <bool limit, bool for_spill = false>
    Status _merge_with_serialized_key_helper(Block* block) {
        SCOPED_TIMER(_merge_timer);

        size_t key_size = _probe_expr_ctxs.size();
        ColumnRawPtrs key_columns(key_size);

        for (size_t i = 0; i < key_size; ++i) {
            if constexpr (for_spill) {
                key_columns[i] = block->get_by_position(i).column.get();
            } else {
                int result_column_id = -1;
                RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
                block->replace_by_position_if_const(result_column_id);
                key_columns[i] = block->get_by_position(result_column_id).column.get();
            }
        }

        int rows = block->rows();
        if (_places.size() < rows) {
            _places.resize(rows);
        }

        if constexpr (limit) {
            _find_in_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                if (_aggregate_evaluators[i]->is_merge()) {
                    int col_id = _get_slot_column_id(_aggregate_evaluators[i]);
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((ColumnNullable*)column.get())->get_nested_column_ptr();
                    }

                    size_t buffer_size =
                            _aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        SCOPED_TIMER(_deserialize_data_timer);
                        _aggregate_evaluators[i]->function()->deserialize_and_merge_vec_selected(
                                _places.data(), _offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(), (ColumnString*)(column.get()),
                                _agg_arena_pool.get(), rows);
                    }
                } else {
                    RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add_selected(
                            block, _offsets_of_aggregate_states[i], _places.data(),
                            _agg_arena_pool.get()));
                }
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                if (_aggregate_evaluators[i]->is_merge() || for_spill) {
                    int col_id;
                    if constexpr (for_spill) {
                        col_id = _probe_expr_ctxs.size() + i;
                    } else {
                        col_id = _get_slot_column_id(_aggregate_evaluators[i]);
                    }
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((ColumnNullable*)column.get())->get_nested_column_ptr();
                    }

                    size_t buffer_size =
                            _aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        SCOPED_TIMER(_deserialize_data_timer);
                        _aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                                _places.data(), _offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(), (ColumnString*)(column.get()),
                                _agg_arena_pool.get(), rows);
                    }
                } else {
                    RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                            block, _offsets_of_aggregate_states[i], _places.data(),
                            _agg_arena_pool.get()));
                }
            }

            if (_should_limit_output) {
                _reach_limit = _get_hash_table_size() >= _limit;
            }
        }

        return Status::OK();
    }

    void _emplace_into_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                  const size_t num_rows);

    size_t _memory_usage() const;

    Status _reset_hash_table();

    Status _try_spill_disk(bool eos = false);

    template <typename HashTableCtxType, typename HashTableType, typename KeyType>
    Status _serialize_hash_table_to_block(HashTableCtxType& context, HashTableType& hash_table,
                                          Block& block, std::vector<KeyType>& keys);

    template <typename HashTableCtxType, typename HashTableType>
    Status _spill_hash_table(HashTableCtxType& agg_method, HashTableType& hash_table);

    void _find_in_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns, size_t num_rows);

    void release_tracker();

    void _release_mem();

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
