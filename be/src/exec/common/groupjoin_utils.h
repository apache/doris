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

#include <memory>
#include <variant>
#include <vector>

#include "common/exception.h"
#include "exec/common/columns_hashing.h"
#include "exec/common/hash_table/hash_crc32_return32.h"
#include "exec/common/hash_table/hash_key_type.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/hash_table/ph_hash_map.h"
#include "exec/common/hash_table/string_hash_map.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

struct GroupJoinEntry {
    uint64_t build_count = 0;
    uint64_t probe_count = 0;
    AggregateDataPtr agg_states = nullptr;
};

template <typename T>
using GroupJoinData = PHHashMap<T, GroupJoinEntry*, HashCRC32<T>>;

using GroupJoinDataWithStringKey = PHHashMap<StringRef, GroupJoinEntry*>;
using GroupJoinDataWithShortStringKey = StringHashMap<GroupJoinEntry*>;

template <class T>
using GroupJoinPrimaryHashTableContext = MethodOneNumber<T, GroupJoinData<T>>;

template <class Key>
using GroupJoinFixedKeyHashTableContext = MethodKeysFixed<GroupJoinData<Key>>;

using GroupJoinSerializedHashTableContext = MethodSerialized<GroupJoinDataWithStringKey>;
using GroupJoinMethodOneString = MethodStringNoCache<GroupJoinDataWithShortStringKey>;

using GroupJoinMethodVariants = std::variant<
        std::monostate, GroupJoinSerializedHashTableContext,
        GroupJoinPrimaryHashTableContext<UInt8>, GroupJoinPrimaryHashTableContext<UInt16>,
        GroupJoinPrimaryHashTableContext<UInt32>, GroupJoinPrimaryHashTableContext<UInt64>,
        GroupJoinPrimaryHashTableContext<UInt128>, GroupJoinPrimaryHashTableContext<UInt256>,
        GroupJoinFixedKeyHashTableContext<UInt64>, GroupJoinFixedKeyHashTableContext<UInt72>,
        GroupJoinFixedKeyHashTableContext<UInt96>, GroupJoinFixedKeyHashTableContext<UInt104>,
        GroupJoinFixedKeyHashTableContext<UInt128>, GroupJoinFixedKeyHashTableContext<UInt136>,
        GroupJoinFixedKeyHashTableContext<UInt256>, GroupJoinMethodOneString>;

struct GroupJoinDataVariants
        : public DataVariants<GroupJoinMethodVariants, MethodSingleNullableColumn, MethodOneNumber,
                              DataWithNullKey> {
    void init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<GroupJoinSerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt8>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt16>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt32>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt64>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt128>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<GroupJoinPrimaryHashTableContext<UInt256>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<GroupJoinMethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt64>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt72>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt96>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt104>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt128>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt136>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<GroupJoinFixedKeyHashTableContext<UInt256>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "GroupJoinDataVariants meet invalid key type, type={}", type);
        }
    }
};

using GroupJoinDataVariantsUPtr = std::unique_ptr<GroupJoinDataVariants>;

} // namespace doris
