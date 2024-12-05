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
#include <vector>

#include "vec/common/arena.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"

namespace doris {

template <typename T>
using AggData = PHHashMap<T, vectorized::AggregateDataPtr, HashCRC32<T>>;
template <typename T>
using AggDataNullable = vectorized::DataWithNullKey<AggData<T>>;

using AggregatedDataWithoutKey = vectorized::AggregateDataPtr;
using AggregatedDataWithStringKey = PHHashMap<StringRef, vectorized::AggregateDataPtr>;
using AggregatedDataWithShortStringKey = StringHashMap<vectorized::AggregateDataPtr>;

using AggregatedDataWithUInt32KeyPhase2 =
        PHHashMap<vectorized::UInt32, vectorized::AggregateDataPtr,
                  HashMixWrapper<vectorized::UInt32>>;
using AggregatedDataWithUInt64KeyPhase2 =
        PHHashMap<vectorized::UInt64, vectorized::AggregateDataPtr,
                  HashMixWrapper<vectorized::UInt64>>;

using AggregatedDataWithNullableUInt32KeyPhase2 =
        vectorized::DataWithNullKey<AggregatedDataWithUInt32KeyPhase2>;
using AggregatedDataWithNullableUInt64KeyPhase2 =
        vectorized::DataWithNullKey<AggregatedDataWithUInt64KeyPhase2>;
using AggregatedDataWithNullableShortStringKey =
        vectorized::DataWithNullKey<AggregatedDataWithShortStringKey>;

using AggregatedMethodVariants = std::variant<
        std::monostate, vectorized::MethodSerialized<AggregatedDataWithStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt8, AggData<vectorized::UInt8>>,
        vectorized::MethodOneNumber<vectorized::UInt16, AggData<vectorized::UInt16>>,
        vectorized::MethodOneNumber<vectorized::UInt32, AggData<vectorized::UInt32>>,
        vectorized::MethodOneNumber<vectorized::UInt64, AggData<vectorized::UInt64>>,
        vectorized::MethodStringNoCache<AggregatedDataWithShortStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt128, AggData<vectorized::UInt128>>,
        vectorized::MethodOneNumber<vectorized::UInt256, AggData<vectorized::UInt256>>,
        vectorized::MethodOneNumber<vectorized::UInt32, AggregatedDataWithUInt32KeyPhase2>,
        vectorized::MethodOneNumber<vectorized::UInt64, AggregatedDataWithUInt64KeyPhase2>,
        vectorized::MethodSingleNullableColumn<
                vectorized::MethodOneNumber<vectorized::UInt8, AggDataNullable<vectorized::UInt8>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt16, AggDataNullable<vectorized::UInt16>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32, AggDataNullable<vectorized::UInt32>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt64, AggDataNullable<vectorized::UInt64>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32, AggregatedDataWithNullableUInt32KeyPhase2>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt64, AggregatedDataWithNullableUInt64KeyPhase2>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt128, AggDataNullable<vectorized::UInt128>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt256, AggDataNullable<vectorized::UInt256>>>,
        vectorized::MethodSingleNullableColumn<
                vectorized::MethodStringNoCache<AggregatedDataWithNullableShortStringKey>>,
        vectorized::MethodKeysFixed<AggData<vectorized::UInt64>>,
        vectorized::MethodKeysFixed<AggData<vectorized::UInt128>>,
        vectorized::MethodKeysFixed<AggData<vectorized::UInt256>>,
        vectorized::MethodKeysFixed<AggData<vectorized::UInt136>>>;

struct AggregatedDataVariants
        : public DataVariants<AggregatedMethodVariants, vectorized::MethodSingleNullableColumn,
                              vectorized::MethodOneNumber, vectorized::DataWithNullKey> {
    AggregatedDataWithoutKey without_key = nullptr;

    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        bool nullable = data_types.size() == 1 && data_types[0]->is_nullable();

        switch (type) {
        case HashKeyType::without_key:
            break;
        case HashKeyType::serialized:
            method_variant.emplace<vectorized::MethodSerialized<AggregatedDataWithStringKey>>();
            break;
        case HashKeyType::int8_key:
            emplace_single<vectorized::UInt8, AggData<vectorized::UInt8>>(nullable);
            break;
        case HashKeyType::int16_key:
            emplace_single<vectorized::UInt16, AggData<vectorized::UInt16>>(nullable);
            break;
        case HashKeyType::int32_key:
            emplace_single<vectorized::UInt32, AggData<vectorized::UInt32>>(nullable);
            break;
        case HashKeyType::int32_key_phase2:
            emplace_single<vectorized::UInt32, AggregatedDataWithUInt32KeyPhase2>(nullable);
            break;
        case HashKeyType::int64_key:
            emplace_single<vectorized::UInt64, AggData<vectorized::UInt64>>(nullable);
            break;
        case HashKeyType::int64_key_phase2:
            emplace_single<vectorized::UInt64, AggregatedDataWithUInt64KeyPhase2>(nullable);
            break;
        case HashKeyType::int128_key:
            emplace_single<vectorized::UInt128, AggData<vectorized::UInt128>>(nullable);
            break;
        case HashKeyType::int256_key:
            emplace_single<vectorized::UInt256, AggData<vectorized::UInt256>>(nullable);
            break;
        case HashKeyType::string_key:
            if (nullable) {
                method_variant.emplace<
                        vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                                AggregatedDataWithNullableShortStringKey>>>();
            } else {
                method_variant.emplace<
                        vectorized::MethodStringNoCache<AggregatedDataWithShortStringKey>>();
            }
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<vectorized::MethodKeysFixed<AggData<vectorized::UInt64>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<vectorized::MethodKeysFixed<AggData<vectorized::UInt128>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<vectorized::MethodKeysFixed<AggData<vectorized::UInt136>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<vectorized::MethodKeysFixed<AggData<vectorized::UInt256>>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "AggregatedDataVariants meet invalid key type, type={}", type);
        }
    }
};

using AggregatedDataVariantsUPtr = std::unique_ptr<AggregatedDataVariants>;
using ArenaUPtr = std::unique_ptr<vectorized::Arena>;

struct AggregateDataContainer {
public:
    AggregateDataContainer(size_t size_of_key, size_t size_of_aggregate_states)
            : _size_of_key(size_of_key), _size_of_aggregate_states(size_of_aggregate_states) {}

    int64_t memory_usage() const { return _arena_pool.size(); }

    template <typename KeyType>
    vectorized::AggregateDataPtr append_data(const KeyType& key) {
        DCHECK_EQ(sizeof(KeyType), _size_of_key);
        // SUB_CONTAINER_CAPACITY should add a new sub container, and also expand when it is zero
        if (UNLIKELY(_index_in_sub_container % SUB_CONTAINER_CAPACITY == 0)) {
            _expand();
        }

        *reinterpret_cast<KeyType*>(_current_keys) = key;
        auto* aggregate_data = _current_agg_data;
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

        vectorized::AggregateDataPtr get_aggregate_data() {
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

    ConstIterator begin() const { return {this, 0}; }

    ConstIterator cbegin() const { return begin(); }

    Iterator begin() { return {this, 0}; }

    ConstIterator end() const { return {this, _total_count}; }
    ConstIterator cend() const { return end(); }
    Iterator end() { return {this, _total_count}; }

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

            _current_agg_data = (vectorized::AggregateDataPtr)_arena_pool.alloc(
                    _size_of_aggregate_states * SUB_CONTAINER_CAPACITY);
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
    vectorized::Arena _arena_pool;
    std::vector<char*> _key_containers;
    std::vector<vectorized::AggregateDataPtr> _value_containers;
    vectorized::AggregateDataPtr _current_agg_data = nullptr;
    char* _current_keys = nullptr;
    size_t _size_of_key {};
    size_t _size_of_aggregate_states {};
    uint32_t _index_in_sub_container {};
    uint32_t _total_count {};
    bool _inited = false;
};
} // namespace doris
