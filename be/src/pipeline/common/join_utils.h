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

using SerializedHashTableContext = vectorized::MethodSerialized<JoinHashMap<StringRef>>;
using I8HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt8>;
using I16HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt16>;
using I32HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt32>;
using I64HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt64>;
using I128HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt128>;
using I256HashTableContext = vectorized::PrimaryTypeHashTableContext<vectorized::UInt256>;
using MethodOneString = vectorized::MethodStringNoCache<JoinHashMap<StringRef>>;
template <bool has_null>
using I64FixedKeyHashTableContext =
        vectorized::FixedKeyHashTableContext<vectorized::UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContext =
        vectorized::FixedKeyHashTableContext<vectorized::UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContext =
        vectorized::FixedKeyHashTableContext<vectorized::UInt256, has_null>;

template <bool has_null>
using I136FixedKeyHashTableContext =
        vectorized::FixedKeyHashTableContext<vectorized::UInt136, has_null>;

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I8HashTableContext,
                     I16HashTableContext, I32HashTableContext, I64HashTableContext,
                     I128HashTableContext, I256HashTableContext, I64FixedKeyHashTableContext<true>,
                     I64FixedKeyHashTableContext<false>, I128FixedKeyHashTableContext<true>,
                     I128FixedKeyHashTableContext<false>, I256FixedKeyHashTableContext<true>,
                     I256FixedKeyHashTableContext<false>, I136FixedKeyHashTableContext<true>,
                     I136FixedKeyHashTableContext<false>, MethodOneString>;

struct JoinDataVariants {
    HashTableVariants method_variant;

    template <bool nullable>
    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        // todo: support single column nullable context
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            if (nullable) {
                method_variant.emplace<I64FixedKeyHashTableContext<nullable>>(
                        get_key_sizes(data_types));
            } else {
                method_variant.emplace<I8HashTableContext>();
            }
            break;
        case HashKeyType::int16_key:
            if (nullable) {
                method_variant.emplace<I64FixedKeyHashTableContext<nullable>>(
                        get_key_sizes(data_types));
            } else {
                method_variant.emplace<I16HashTableContext>();
            }
            break;
        case HashKeyType::int32_key:
            if (nullable) {
                method_variant.emplace<I64FixedKeyHashTableContext<nullable>>(
                        get_key_sizes(data_types));
            } else {
                method_variant.emplace<I32HashTableContext>();
            }
            break;
        case HashKeyType::int64_key:
            if (nullable) {
                method_variant.emplace<I128FixedKeyHashTableContext<nullable>>(
                        get_key_sizes(data_types));
            } else {
                method_variant.emplace<I64HashTableContext>();
            }
            break;
        case HashKeyType::int128_key:
            if (nullable) {
                method_variant.emplace<I136FixedKeyHashTableContext<nullable>>(
                        get_key_sizes(data_types));
            } else {
                method_variant.emplace<I128HashTableContext>();
            }
            break;
        case HashKeyType::int256_key:
            if (nullable) {
                method_variant.emplace<SerializedHashTableContext>();
            } else {
                method_variant.emplace<I256HashTableContext>();
            }
            break;
        case HashKeyType::string_key:
            method_variant.emplace<MethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<I64FixedKeyHashTableContext<nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<I128FixedKeyHashTableContext<nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<I136FixedKeyHashTableContext<nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<I256FixedKeyHashTableContext<nullable>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "JoinDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
