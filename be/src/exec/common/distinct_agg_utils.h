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

#include "core/arena.h"
#include "core/types.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/hash_table/ph_hash_map.h"
#include "exec/common/hash_table/ph_hash_set.h"
#include "exec/common/hash_table/string_hash_map.h"

namespace doris {

template <typename T>
struct DistinctHashSetType {
    using HashSet = PHHashSet<T, HashCRC32<T>>;
};

template <>
struct DistinctHashSetType<UInt8> {
    using HashSet = SmallFixedSizeHashSet<UInt8>;
};

template <>
struct DistinctHashSetType<Int8> {
    using HashSet = SmallFixedSizeHashSet<Int8>;
};

template <typename T>
struct DistinctPhase2HashSetType {
    using HashSet = PHHashSet<T, HashMixWrapper<T>>;
};

template <>
struct DistinctPhase2HashSetType<UInt8> {
    using HashSet = SmallFixedSizeHashSet<UInt8>;
};

template <>
struct DistinctPhase2HashSetType<Int8> {
    using HashSet = SmallFixedSizeHashSet<Int8>;
};

template <typename T>
using DistinctData = typename DistinctHashSetType<T>::HashSet;

template <typename T>
using DistinctDataPhase2 = typename DistinctPhase2HashSetType<T>::HashSet;

using DistinctDataWithStringKey = PHHashSet<StringRef>;

// todo: Need to implement StringHashSet like StringHashMap
using DistinctDataWithShortStringKey = PHHashSet<StringRef>;

using DistinctMethodVariants = std::variant<
        std::monostate, MethodSerialized<DistinctDataWithStringKey>,
        MethodOneNumber<UInt8, DistinctData<UInt8>>, MethodOneNumber<UInt16, DistinctData<UInt16>>,
        MethodOneNumber<UInt32, DistinctData<UInt32>>,
        MethodOneNumber<UInt64, DistinctData<UInt64>>,
        MethodStringNoCache<DistinctDataWithShortStringKey>,
        MethodOneNumber<UInt128, DistinctData<UInt128>>,
        MethodOneNumber<UInt256, DistinctData<UInt256>>,
        MethodOneNumber<UInt32, DistinctDataPhase2<UInt32>>,
        MethodOneNumber<UInt64, DistinctDataPhase2<UInt64>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt8, DataWithNullKey<DistinctData<UInt8>>>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt16, DataWithNullKey<DistinctData<UInt16>>>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt32, DataWithNullKey<DistinctData<UInt32>>>>,
        MethodSingleNullableColumn<MethodOneNumber<UInt64, DataWithNullKey<DistinctData<UInt64>>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt32, DataWithNullKey<DistinctDataPhase2<UInt32>>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt64, DataWithNullKey<DistinctDataPhase2<UInt64>>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt128, DataWithNullKey<DistinctData<UInt128>>>>,
        MethodSingleNullableColumn<
                MethodOneNumber<UInt256, DataWithNullKey<DistinctData<UInt256>>>>,
        MethodSingleNullableColumn<
                MethodStringNoCache<DataWithNullKey<DistinctDataWithShortStringKey>>>,
        MethodKeysFixed<DistinctData<UInt64>>, MethodKeysFixed<DistinctData<UInt72>>,
        MethodKeysFixed<DistinctData<UInt96>>, MethodKeysFixed<DistinctData<UInt104>>,
        MethodKeysFixed<DistinctData<UInt128>>, MethodKeysFixed<DistinctData<UInt136>>,
        MethodKeysFixed<DistinctData<UInt256>>>;

struct DistinctDataVariants
        : public DataVariants<DistinctMethodVariants, MethodSingleNullableColumn, MethodOneNumber,
                              DataWithNullKey> {
    void init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
        bool nullable = data_types.size() == 1 && data_types[0]->is_nullable();
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<MethodSerialized<DistinctDataWithStringKey>>();
            break;
        case HashKeyType::int8_key:
            emplace_single<UInt8, DistinctData<UInt8>>(nullable);
            break;
        case HashKeyType::int16_key:
            emplace_single<UInt16, DistinctData<UInt16>>(nullable);
            break;
        case HashKeyType::int32_key:
            emplace_single<UInt32, DistinctData<UInt32>>(nullable);
            break;
        case HashKeyType::int32_key_phase2:
            emplace_single<UInt32, DistinctDataPhase2<UInt32>>(nullable);
            break;
        case HashKeyType::int64_key:
            emplace_single<UInt64, DistinctData<UInt64>>(nullable);
            break;
        case HashKeyType::int64_key_phase2:
            emplace_single<UInt64, DistinctDataPhase2<UInt64>>(nullable);
            break;
        case HashKeyType::int128_key:
            emplace_single<UInt128, DistinctData<UInt128>>(nullable);
            break;
        case HashKeyType::int256_key:
            emplace_single<UInt256, DistinctData<UInt256>>(nullable);
            break;
        case HashKeyType::string_key:
            if (nullable) {
                method_variant.emplace<MethodSingleNullableColumn<
                        MethodStringNoCache<DataWithNullKey<DistinctDataWithShortStringKey>>>>();
            } else {
                method_variant.emplace<MethodStringNoCache<DistinctDataWithShortStringKey>>();
            }
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt64>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt72>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt96>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt104>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt128>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt136>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<MethodKeysFixed<DistinctData<UInt256>>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "AggregatedDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
