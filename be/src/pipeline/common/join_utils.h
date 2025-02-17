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

#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/join_hash_table.h"

namespace doris {
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
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>>;

template <class T>
using PrimaryTypeHashTableContext = vectorized::MethodOneNumber<T, JoinHashMap<T, HashCRC32<T>>>;

template <class Key>
using FixedKeyHashTableContext = vectorized::MethodKeysFixed<JoinHashMap<Key, HashCRC32<Key>>>;

using SerializedHashTableContext = vectorized::MethodSerialized<JoinHashMap<StringRef>>;
using MethodOneString = vectorized::MethodStringNoCache<JoinHashMap<StringRef>>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext, PrimaryTypeHashTableContext<vectorized::UInt8>,
        PrimaryTypeHashTableContext<vectorized::UInt16>,
        PrimaryTypeHashTableContext<vectorized::UInt32>,
        PrimaryTypeHashTableContext<vectorized::UInt64>,
        PrimaryTypeHashTableContext<vectorized::UInt128>,
        PrimaryTypeHashTableContext<vectorized::UInt256>,
        FixedKeyHashTableContext<vectorized::UInt64>, FixedKeyHashTableContext<vectorized::UInt128>,
        FixedKeyHashTableContext<vectorized::UInt136>,
        FixedKeyHashTableContext<vectorized::UInt256>, MethodOneString>;

struct JoinDataVariants {
    HashTableVariants method_variant;

    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        // todo: support single column nullable context
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt8>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt16>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt32>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt64>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt128>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt256>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<MethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt64>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt128>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt136>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt256>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "JoinDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
