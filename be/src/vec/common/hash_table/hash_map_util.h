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

namespace doris::vectorized {

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
    int128_key_phase2,
    string_key,
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

template <typename MethodVariants, template <typename> typename MethodNullable,
          template <typename, typename> typename MethodOneNumber,
          template <typename, bool> typename MethodFixed, template <typename> typename DataNullable>
struct DataVariants {
    DataVariants() = default;
    DataVariants(const DataVariants&) = delete;
    DataVariants& operator=(const DataVariants&) = delete;
    MethodVariants method_variant;

    using Type = HashKeyType;

    Type _type = Type::EMPTY;

    template <typename T, typename TT, bool nullable>
    void emplace_single() {
        if (nullable) {
            method_variant.template emplace<MethodNullable<MethodOneNumber<T, DataNullable<TT>>>>();
        } else {
            method_variant.template emplace<MethodOneNumber<T, TT>>();
        }
    }
};
} // namespace doris::vectorized