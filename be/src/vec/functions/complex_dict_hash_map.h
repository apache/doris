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

#include "vec/columns/column.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

// key -> column index
template <typename KeyType>
using DictHashMap = PHHashMap<KeyType, IColumn::ColumnIndex, HashCRC32<KeyType>>;

using DictHashMapVariants = std::variant<
        std::monostate,

        MethodSerialized<StringHashMap<IColumn::ColumnIndex>>,
        MethodStringNoCache<StringHashMap<IColumn::ColumnIndex>>,

        MethodOneNumber<UInt8, DictHashMap<UInt8>>, MethodOneNumber<UInt16, DictHashMap<UInt16>>,
        MethodOneNumber<UInt32, DictHashMap<UInt32>>, MethodOneNumber<UInt64, DictHashMap<UInt64>>,
        MethodOneNumber<UInt128, DictHashMap<UInt128>>,
        MethodOneNumber<UInt256, DictHashMap<UInt256>>,

        MethodKeysFixed<DictHashMap<UInt64>>, MethodKeysFixed<DictHashMap<UInt128>>,
        MethodKeysFixed<DictHashMap<UInt256>>, MethodKeysFixed<DictHashMap<UInt136>>>;

struct DictionaryHashMapMethod
        : public DataVariants<DictHashMapVariants, vectorized::MethodSingleNullableColumn,
                              vectorized::MethodOneNumber, vectorized::DataWithNullKey> {
    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<MethodSerialized<StringHashMap<IColumn::ColumnIndex>>>();
            break;
        // Here we do not call emplace_single because we do not have a corresponding nullable type
        case HashKeyType::int8_key:
            method_variant.emplace<MethodOneNumber<UInt8, DictHashMap<UInt8>>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<MethodOneNumber<UInt16, DictHashMap<UInt16>>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<MethodOneNumber<UInt32, DictHashMap<UInt32>>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<MethodOneNumber<UInt64, DictHashMap<UInt64>>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<MethodOneNumber<UInt128, DictHashMap<UInt128>>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<MethodOneNumber<UInt256, DictHashMap<UInt256>>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<MethodStringNoCache<StringHashMap<IColumn::ColumnIndex>>>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<MethodKeysFixed<DictHashMap<UInt64>>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<MethodKeysFixed<DictHashMap<UInt128>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<MethodKeysFixed<DictHashMap<UInt136>>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<MethodKeysFixed<DictHashMap<UInt256>>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "DictionaryHashMapMethod meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris::vectorized