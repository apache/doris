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

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/global_types.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/allocator.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
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

using AggregatedDataWithoutKey = AggregateDataPtr;
using AggregatedDataWithStringKey = PHHashMap<StringRef, AggregateDataPtr>;
using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;
using AggregatedDataWithUInt8Key = PHHashMap<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = PHHashMap<UInt16, AggregateDataPtr>;
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

using AggregatedDataWithNullableUInt8Key = DataWithNullKey<AggregatedDataWithUInt8Key>;
using AggregatedDataWithNullableUInt16Key = DataWithNullKey<AggregatedDataWithUInt16Key>;
using AggregatedDataWithNullableUInt32Key = DataWithNullKey<AggregatedDataWithUInt32Key>;
using AggregatedDataWithNullableUInt64Key = DataWithNullKey<AggregatedDataWithUInt64Key>;
using AggregatedDataWithNullableUInt32KeyPhase2 =
        DataWithNullKey<AggregatedDataWithUInt32KeyPhase2>;
using AggregatedDataWithNullableUInt64KeyPhase2 =
        DataWithNullKey<AggregatedDataWithUInt64KeyPhase2>;
using AggregatedDataWithNullableShortStringKey = DataWithNullKey<AggregatedDataWithShortStringKey>;
using AggregatedDataWithNullableUInt128Key = DataWithNullKey<AggregatedDataWithUInt128Key>;
using AggregatedDataWithNullableUInt128KeyPhase2 =
        DataWithNullKey<AggregatedDataWithUInt128KeyPhase2>;

using AggregatedMethodVariants = std::variant<
        std::monostate, MethodSerialized<AggregatedDataWithStringKey>,
        MethodOneNumber<UInt8, AggregatedDataWithUInt8Key>,
        MethodOneNumber<UInt16, AggregatedDataWithUInt16Key>,
        MethodOneNumber<UInt32, AggregatedDataWithUInt32Key>,
        MethodOneNumber<UInt64, AggregatedDataWithUInt64Key>,
        MethodStringNoCache<AggregatedDataWithShortStringKey>,
        MethodOneNumber<UInt128, AggregatedDataWithUInt128Key>,
        MethodOneNumber<UInt32, AggregatedDataWithUInt32KeyPhase2>,
        MethodOneNumber<UInt64, AggregatedDataWithUInt64KeyPhase2>,
        MethodOneNumber<UInt128, AggregatedDataWithUInt128KeyPhase2>,
        MethodSingleNullableColumn<MethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyPhase2>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyPhase2>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt128, AggregatedDataWithNullableUInt128Key>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt128, AggregatedDataWithNullableUInt128KeyPhase2>>,
        MethodSingleNullableColumn<MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>,
        MethodKeysFixed<AggregatedDataWithUInt64Key, false>,
        MethodKeysFixed<AggregatedDataWithUInt64Key, true>,
        MethodKeysFixed<AggregatedDataWithUInt128Key, false>,
        MethodKeysFixed<AggregatedDataWithUInt128Key, true>,
        MethodKeysFixed<AggregatedDataWithUInt256Key, false>,
        MethodKeysFixed<AggregatedDataWithUInt256Key, true>,
        MethodKeysFixed<AggregatedDataWithUInt136Key, false>,
        MethodKeysFixed<AggregatedDataWithUInt136Key, true>,
        MethodKeysFixed<AggregatedDataWithUInt64KeyPhase2, false>,
        MethodKeysFixed<AggregatedDataWithUInt64KeyPhase2, true>,
        MethodKeysFixed<AggregatedDataWithUInt128KeyPhase2, false>,
        MethodKeysFixed<AggregatedDataWithUInt128KeyPhase2, true>,
        MethodKeysFixed<AggregatedDataWithUInt256KeyPhase2, false>,
        MethodKeysFixed<AggregatedDataWithUInt256KeyPhase2, true>,
        MethodKeysFixed<AggregatedDataWithUInt136KeyPhase2, false>,
        MethodKeysFixed<AggregatedDataWithUInt136KeyPhase2, true>>;

struct AggregatedDataVariants
        : public DataVariants<AggregatedMethodVariants, MethodSingleNullableColumn, MethodOneNumber,
                              MethodKeysFixed, DataWithNullKey> {
    AggregatedDataWithoutKey without_key = nullptr;

    template <bool nullable>
    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::without_key:
            break;
        case Type::serialized:
            method_variant.emplace<MethodSerialized<AggregatedDataWithStringKey>>();
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
        case Type::string_key:
            if (nullable) {
                method_variant.emplace<MethodSingleNullableColumn<
                        MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>>();
            } else {
                method_variant.emplace<MethodStringNoCache<AggregatedDataWithShortStringKey>>();
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
            : _size_of_key(size_of_key), _size_of_aggregate_states(size_of_aggregate_states) {}

    int64_t memory_usage() const { return _arena_pool.size(); }

    template <typename KeyType>
    AggregateDataPtr append_data(const KeyType& key) {
        DCHECK_EQ(sizeof(KeyType), _size_of_key);
        // SUB_CONTAINER_CAPACITY should add a new sub container, and also expand when it is zero
        if (UNLIKELY(_index_in_sub_container % SUB_CONTAINER_CAPACITY == 0)) {
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

        Container* container = nullptr;
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
            DCHECK_EQ(sizeof(KeyType), container->_size_of_key);
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
        _current_keys = nullptr;
        _current_agg_data = nullptr;
        try {
            _current_keys = _arena_pool.alloc(_size_of_key * SUB_CONTAINER_CAPACITY);
            _key_containers.emplace_back(_current_keys);

            _current_agg_data = (AggregateDataPtr)_arena_pool.alloc(_size_of_aggregate_states *
                                                                    SUB_CONTAINER_CAPACITY);
            _value_containers.emplace_back(_current_agg_data);
        } catch (...) {
            if (_current_keys) {
                _key_containers.pop_back();
                _current_keys = nullptr;
            }
            if (_current_agg_data) {
                _value_containers.pop_back();
                _current_agg_data = nullptr;
            }
            throw;
        }
    }

    static constexpr uint32_t SUB_CONTAINER_CAPACITY = 8192;
    Arena _arena_pool;
    std::vector<char*> _key_containers;
    std::vector<AggregateDataPtr> _value_containers;
    AggregateDataPtr _current_agg_data = nullptr;
    char* _current_keys = nullptr;
    size_t _size_of_key {};
    size_t _size_of_aggregate_states {};
    uint32_t _index_in_sub_container {};
    uint32_t _total_count {};
    bool _inited = false;
};

} // namespace vectorized

constexpr auto init_agg_hash_method =
        init_hash_method<vectorized::AggregatedDataVariants, vectorized::AggregateDataPtr>;

} // namespace doris
