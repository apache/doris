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
#include "vec/common/hash_table/ph_hash_set.h"
#include "vec/common/hash_table/string_hash_map.h"

namespace doris {
template <typename T>
using DistinctData = PHHashSet<T, HashCRC32<T>>;

template <typename T>
using DistinctDataPhase2 = PHHashSet<T, HashMixWrapper<T>>;

using DistinctDataWithStringKey = PHHashSet<StringRef>;

// todo: Need to implement StringHashSet like StringHashMap
using DistinctDataWithShortStringKey = PHHashSet<StringRef>;

using DistinctMethodVariants = std::variant<
        std::monostate, vectorized::MethodSerialized<DistinctDataWithStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt8, DistinctData<vectorized::UInt8>>,
        vectorized::MethodOneNumber<vectorized::UInt16, DistinctData<vectorized::UInt16>>,
        vectorized::MethodOneNumber<vectorized::UInt32, DistinctData<vectorized::UInt32>>,
        vectorized::MethodOneNumber<vectorized::UInt64, DistinctData<vectorized::UInt64>>,
        vectorized::MethodStringNoCache<DistinctDataWithShortStringKey>,
        vectorized::MethodOneNumber<vectorized::UInt128, DistinctData<vectorized::UInt128>>,
        vectorized::MethodOneNumber<vectorized::UInt256, DistinctData<vectorized::UInt256>>,
        vectorized::MethodOneNumber<vectorized::UInt32, DistinctDataPhase2<vectorized::UInt32>>,
        vectorized::MethodOneNumber<vectorized::UInt64, DistinctDataPhase2<vectorized::UInt64>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt8, vectorized::DataWithNullKey<DistinctData<vectorized::UInt8>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt16, vectorized::DataWithNullKey<DistinctData<vectorized::UInt16>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32, vectorized::DataWithNullKey<DistinctData<vectorized::UInt32>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt64, vectorized::DataWithNullKey<DistinctData<vectorized::UInt64>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt32,
                vectorized::DataWithNullKey<DistinctDataPhase2<vectorized::UInt32>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt64,
                vectorized::DataWithNullKey<DistinctDataPhase2<vectorized::UInt64>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt128,
                vectorized::DataWithNullKey<DistinctData<vectorized::UInt128>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                vectorized::UInt256,
                vectorized::DataWithNullKey<DistinctData<vectorized::UInt256>>>>,
        vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                vectorized::DataWithNullKey<DistinctDataWithShortStringKey>>>,
        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt64>>,
        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt128>>,
        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt256>>,
        vectorized::MethodKeysFixed<DistinctData<vectorized::UInt136>>>;

struct DistinctDataVariants
        : public DataVariants<DistinctMethodVariants, vectorized::MethodSingleNullableColumn,
                              vectorized::MethodOneNumber, vectorized::DataWithNullKey> {
    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        bool nullable = data_types.size() == 1 && data_types[0]->is_nullable();
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<vectorized::MethodSerialized<DistinctDataWithStringKey>>();
            break;
        case HashKeyType::int8_key:
            emplace_single<vectorized::UInt8, DistinctData<vectorized::UInt8>>(nullable);
            break;
        case HashKeyType::int16_key:
            emplace_single<vectorized::UInt16, DistinctData<vectorized::UInt16>>(nullable);
            break;
        case HashKeyType::int32_key:
            emplace_single<vectorized::UInt32, DistinctData<vectorized::UInt32>>(nullable);
            break;
        case HashKeyType::int32_key_phase2:
            emplace_single<vectorized::UInt32, DistinctDataPhase2<vectorized::UInt32>>(nullable);
            break;
        case HashKeyType::int64_key:
            emplace_single<vectorized::UInt64, DistinctData<vectorized::UInt64>>(nullable);
            break;
        case HashKeyType::int64_key_phase2:
            emplace_single<vectorized::UInt64, DistinctDataPhase2<vectorized::UInt64>>(nullable);
            break;
        case HashKeyType::int128_key:
            emplace_single<vectorized::UInt128, DistinctData<vectorized::UInt128>>(nullable);
            break;
        case HashKeyType::int256_key:
            emplace_single<vectorized::UInt256, DistinctData<vectorized::UInt256>>(nullable);
            break;
        case HashKeyType::string_key:
            if (nullable) {
                method_variant.emplace<
                        vectorized::MethodSingleNullableColumn<vectorized::MethodStringNoCache<
                                vectorized::DataWithNullKey<DistinctDataWithShortStringKey>>>>();
            } else {
                method_variant
                        .emplace<vectorized::MethodStringNoCache<DistinctDataWithShortStringKey>>();
            }
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<vectorized::MethodKeysFixed<DistinctData<vectorized::UInt64>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<vectorized::MethodKeysFixed<DistinctData<vectorized::UInt128>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<vectorized::MethodKeysFixed<DistinctData<vectorized::UInt136>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<vectorized::MethodKeysFixed<DistinctData<vectorized::UInt256>>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "AggregatedDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
