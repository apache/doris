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

#include "exec/common/hash_table/hash_map_util.h"

namespace doris {

struct RowRefWithFlag {
    bool visited;
    uint32_t row_num = 0;
    RowRefWithFlag() = default;
#include "common/compile_check_avoid_begin.h"
    //To consider performance, checks are avoided here.
    RowRefWithFlag(size_t row_num_count, bool is_visited = false)
            : visited(is_visited), row_num(row_num_count) {}
#include "common/compile_check_avoid_end.h"
};

template <typename T>
using SetData = PHHashMap<T, RowRefWithFlag, HashCRC32<T>>;

template <typename T>
using SetFixedKeyHashTableContext = MethodKeysFixed<SetData<T>>;

template <typename T>
using SetPrimaryTypeHashTableContext = MethodOneNumber<T, SetData<T>>;

template <typename T>
using SetPrimaryTypeHashTableContextNullable =
        MethodSingleNullableColumn<MethodOneNumber<T, DataWithNullKey<SetData<T>>>>;

using SetSerializedHashTableContext = MethodSerialized<PHHashMap<StringRef, RowRefWithFlag>>;
using SetMethodOneString = MethodStringNoCache<PHHashMap<StringRef, RowRefWithFlag>>;
using SetMethodOneStringNullable = MethodSingleNullableColumn<
        MethodStringNoCache<DataWithNullKey<PHHashMap<StringRef, RowRefWithFlag>>>>;

using SetHashTableVariants =
        std::variant<std::monostate, SetSerializedHashTableContext, SetMethodOneString,
                     SetMethodOneStringNullable, SetPrimaryTypeHashTableContextNullable<UInt8>,
                     SetPrimaryTypeHashTableContextNullable<UInt16>,
                     SetPrimaryTypeHashTableContextNullable<UInt32>,
                     SetPrimaryTypeHashTableContextNullable<UInt64>,
                     SetPrimaryTypeHashTableContextNullable<UInt128>,
                     SetPrimaryTypeHashTableContextNullable<UInt256>,
                     SetPrimaryTypeHashTableContext<UInt8>, SetPrimaryTypeHashTableContext<UInt16>,
                     SetPrimaryTypeHashTableContext<UInt32>, SetPrimaryTypeHashTableContext<UInt64>,
                     SetPrimaryTypeHashTableContext<UInt128>,
                     SetPrimaryTypeHashTableContext<UInt256>, SetFixedKeyHashTableContext<UInt64>,
                     SetFixedKeyHashTableContext<UInt72>, SetFixedKeyHashTableContext<UInt96>,
                     SetFixedKeyHashTableContext<UInt104>, SetFixedKeyHashTableContext<UInt128>,
                     SetFixedKeyHashTableContext<UInt256>, SetFixedKeyHashTableContext<UInt136>>;

struct SetDataVariants : public DataVariants<SetHashTableVariants, MethodSingleNullableColumn,
                                             MethodOneNumber, DataWithNullKey> {
    void init(const std::vector<DataTypePtr>& data_types, HashKeyType type) {
        bool nullable = data_types.size() == 1 && data_types[0]->is_nullable();
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SetSerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            emplace_single<UInt8, SetData<UInt8>>(nullable);
            break;
        case HashKeyType::int16_key:
            emplace_single<UInt16, SetData<UInt16>>(nullable);
            break;
        case HashKeyType::int32_key:
            emplace_single<UInt32, SetData<UInt32>>(nullable);
            break;
        case HashKeyType::int64_key:
            emplace_single<UInt64, SetData<UInt64>>(nullable);
            break;
        case HashKeyType::int128_key:
            emplace_single<UInt128, SetData<UInt128>>(nullable);
            break;
        case HashKeyType::int256_key:
            emplace_single<UInt256, SetData<UInt256>>(nullable);
            break;
        case HashKeyType::string_key:
            if (nullable) {
                method_variant.emplace<SetMethodOneStringNullable>();
            } else {
                method_variant.emplace<SetMethodOneString>();
            }
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt64>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt72>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt96>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt104>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt128>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt136>>(get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<SetFixedKeyHashTableContext<UInt256>>(get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "SetDataVariants meet invalid key type, type={}", type);
        }
    }
};

} // namespace doris
