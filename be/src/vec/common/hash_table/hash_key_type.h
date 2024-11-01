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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/core/types.h"

namespace doris {

enum class HashKeyType {
    EMPTY = 0,
    without_key,
    serialized,
    int8_key,
    int16_key,
    int32_key,
    int32_key_phase2,
    int64_key,
    int64_key_phase2,
    int128_key,
    int256_key,
    string_key,
    fixed64,
    fixed128,
    fixed136,
    fixed256
};

inline HashKeyType get_hash_key_type_with_phase(HashKeyType t, bool phase2) {
    if (!phase2) {
        return t;
    }
    if (t == HashKeyType::int32_key) {
        return HashKeyType::int32_key_phase2;
    }
    if (t == HashKeyType::int64_key) {
        return HashKeyType::int64_key_phase2;
    }
    return t;
}

inline HashKeyType get_hash_key_type_with_fixed(size_t size) {
    using namespace vectorized;
    if (size <= sizeof(UInt64)) {
        return HashKeyType::fixed64;
    } else if (size <= sizeof(UInt128)) {
        return HashKeyType::fixed128;
    } else if (size <= sizeof(UInt136)) {
        return HashKeyType::fixed136;
    } else if (size <= sizeof(UInt256)) {
        return HashKeyType::fixed256;
    } else {
        return HashKeyType::serialized;
    }
}

inline HashKeyType get_hash_key_type_fixed(const std::vector<vectorized::DataTypePtr>& data_types) {
    bool has_null = false;
    size_t key_byte_size = 0;

    for (const auto& data_type : data_types) {
        if (!data_type->have_maximum_size_of_value()) {
            return HashKeyType::serialized;
        }
        key_byte_size += data_type->get_maximum_size_of_value_in_memory();
        if (data_type->is_nullable()) {
            has_null = true;
            key_byte_size--;
        }
    }

    size_t bitmap_size = has_null ? vectorized::get_bitmap_size(data_types.size()) : 0;
    return get_hash_key_type_with_fixed(bitmap_size + key_byte_size);
}

inline HashKeyType get_hash_key_type(const std::vector<vectorized::DataTypePtr>& data_types) {
    if (data_types.size() > 1) {
        return get_hash_key_type_fixed(data_types);
    }
    if (data_types.empty()) {
        return HashKeyType::without_key;
    }

    auto t = remove_nullable(data_types[0]);
    // serialized cannot be used in the case of single column, because the join operator will have some processing of column nullable, resulting in incorrect serialized results.
    if (!t->have_maximum_size_of_value()) {
        if (is_string(t)) {
            return HashKeyType::string_key;
        }
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type, type={}", t->get_name());
    }

    size_t size = t->get_maximum_size_of_value_in_memory();
    if (size == sizeof(vectorized::UInt8)) {
        return HashKeyType::int8_key;
    } else if (size == sizeof(vectorized::UInt16)) {
        return HashKeyType::int16_key;
    } else if (size == sizeof(vectorized::UInt32)) {
        return HashKeyType::int32_key;
    } else if (size == sizeof(vectorized::UInt64)) {
        return HashKeyType::int64_key;
    } else if (size == sizeof(vectorized::UInt128)) {
        return HashKeyType::int128_key;
    } else if (size == sizeof(vectorized::UInt256)) {
        return HashKeyType::int256_key;
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type size, size={}, type={}", size,
                        t->get_name());
    }
}

} // namespace doris