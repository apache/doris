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

#include "exec/common/hash_table/hash_key_type.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/join_hash_table.h"

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
using PrimaryTypeHashTableContext =
        MethodOneNumber<T, JoinHashMap<T, HashCRC32<T>, false>>;

template <class T>
using DirectPrimaryTypeHashTableContext =
        MethodOneNumberDirect<T, JoinHashMap<T, HashCRC32<T>, true>>;

template <class Key>
using FixedKeyHashTableContext =
        MethodKeysFixed<JoinHashMap<Key, HashCRC32<Key>, false>>;

using SerializedHashTableContext =
        MethodSerialized<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>;
using MethodOneString =
        MethodStringNoCache<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext, PrimaryTypeHashTableContext<UInt8>,
        PrimaryTypeHashTableContext<UInt16>,
        PrimaryTypeHashTableContext<UInt32>,
        PrimaryTypeHashTableContext<UInt64>,
        PrimaryTypeHashTableContext<UInt128>,
        PrimaryTypeHashTableContext<UInt256>,
        DirectPrimaryTypeHashTableContext<UInt8>,
        DirectPrimaryTypeHashTableContext<UInt16>,
        DirectPrimaryTypeHashTableContext<UInt32>,
        DirectPrimaryTypeHashTableContext<UInt64>,
        DirectPrimaryTypeHashTableContext<UInt128>,
        FixedKeyHashTableContext<UInt64>, FixedKeyHashTableContext<UInt72>,
        FixedKeyHashTableContext<UInt96>, FixedKeyHashTableContext<UInt104>,
        FixedKeyHashTableContext<UInt128>,
        FixedKeyHashTableContext<UInt136>,
        FixedKeyHashTableContext<UInt256>, MethodOneString>;

struct JoinDataVariants {
    HashTableVariants method_variant;

    void init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt8>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt16>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt32>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt64>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt128>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<PrimaryTypeHashTableContext<UInt256>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<MethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<FixedKeyHashTableContext<UInt64>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<FixedKeyHashTableContext<UInt72>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<FixedKeyHashTableContext<UInt96>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<FixedKeyHashTableContext<UInt104>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<FixedKeyHashTableContext<UInt128>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<FixedKeyHashTableContext<UInt136>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<FixedKeyHashTableContext<UInt256>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "JoinDataVariants meet invalid key type, type={}", type);
        }
    }
};

template <typename Method>
void primary_to_direct_mapping(Method* context, const ColumnRawPtrs& key_columns,
                               const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    using FieldType = typename Method::Base::Key;
    FieldType max_key = std::numeric_limits<FieldType>::min();
    FieldType min_key = std::numeric_limits<FieldType>::max();

    size_t num_rows = key_columns[0]->size();
    if (key_columns[0]->is_nullable()) {
        const FieldType* input_keys =
                (FieldType*)assert_cast<const ColumnNullable*>(key_columns[0])
                        ->get_nested_column_ptr()
                        ->get_raw_data()
                        .data;
        const NullMap& null_map =
                assert_cast<const ColumnNullable*>(key_columns[0])->get_null_map_data();
        // skip first mocked row
        for (size_t i = 1; i < num_rows; i++) {
            if (null_map[i]) {
                continue;
            }
            max_key = std::max(max_key, input_keys[i]);
            min_key = std::min(min_key, input_keys[i]);
        }
    } else {
        const FieldType* input_keys = (FieldType*)key_columns[0]->get_raw_data().data;
        // skip first mocked row
        for (size_t i = 1; i < num_rows; i++) {
            max_key = std::max(max_key, input_keys[i]);
            min_key = std::min(min_key, input_keys[i]);
        }
    }

    constexpr auto MAX_MAPPING_RANGE = 1 << 23;
    bool allow_direct_mapping = (max_key >= min_key && max_key - min_key < MAX_MAPPING_RANGE - 1);
    if (allow_direct_mapping) {
        for (const auto& variant_ptr : variant_ptrs) {
            variant_ptr->method_variant.emplace<DirectPrimaryTypeHashTableContext<FieldType>>(
                    max_key, min_key);
        }
    }
}

template <typename Method>
void try_convert_to_direct_mapping(
        Method* method, const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<UInt8>* context,
        const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<UInt16>* context,
        const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<UInt32>* context,
        const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<UInt64>* context,
        const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<UInt128>* context,
        const ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

} // namespace doris
