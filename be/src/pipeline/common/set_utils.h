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

#include "pipeline/exec/join/join_op.h"
#include "vec/common/hash_table/hash_map_util.h"

namespace doris {

template <class Key, bool has_null>
using SetFixedKeyHashTableContext =
        vectorized::MethodKeysFixed<HashMap<Key, pipeline::RowRefListWithFlags, HashCRC32<Key>>,
                                    has_null>;

template <class T>
using SetPrimaryTypeHashTableContext =
        vectorized::MethodOneNumber<T, HashMap<T, pipeline::RowRefListWithFlags, HashCRC32<T>>>;

using SetSerializedHashTableContext =
        vectorized::MethodSerialized<HashMap<StringRef, pipeline::RowRefListWithFlags>>;
using SetMethodOneString =
        vectorized::MethodStringNoCache<HashMap<StringRef, pipeline::RowRefListWithFlags>>;

using SetHashTableVariants =
        std::variant<std::monostate, SetSerializedHashTableContext, SetMethodOneString,
                     SetPrimaryTypeHashTableContext<vectorized::UInt8>,
                     SetPrimaryTypeHashTableContext<vectorized::UInt16>,
                     SetPrimaryTypeHashTableContext<vectorized::UInt32>,
                     SetPrimaryTypeHashTableContext<vectorized::UInt64>,
                     SetPrimaryTypeHashTableContext<vectorized::UInt128>,
                     SetPrimaryTypeHashTableContext<vectorized::UInt256>,
                     SetFixedKeyHashTableContext<vectorized::UInt64, true>,
                     SetFixedKeyHashTableContext<vectorized::UInt64, false>,
                     SetFixedKeyHashTableContext<vectorized::UInt128, true>,
                     SetFixedKeyHashTableContext<vectorized::UInt128, false>,
                     SetFixedKeyHashTableContext<vectorized::UInt256, true>,
                     SetFixedKeyHashTableContext<vectorized::UInt256, false>,
                     SetFixedKeyHashTableContext<vectorized::UInt136, true>,
                     SetFixedKeyHashTableContext<vectorized::UInt136, false>>;

struct SetDataVariants {
    SetHashTableVariants method_variant;

    template <bool nullable>
    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SetSerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt8>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt16>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt32>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt64>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt128>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<SetPrimaryTypeHashTableContext<vectorized::UInt256>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<SetMethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<SetFixedKeyHashTableContext<vectorized::UInt64, nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<SetFixedKeyHashTableContext<vectorized::UInt128, nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<SetFixedKeyHashTableContext<vectorized::UInt136, nullable>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<SetFixedKeyHashTableContext<vectorized::UInt256, nullable>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "SetDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
