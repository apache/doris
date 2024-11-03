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

#include "vec/common/hash_table/hash_key_type.h"

namespace doris {

inline std::vector<vectorized::DataTypePtr> get_data_types(
        const vectorized::VExprContextSPtrs& expr_contexts) {
    std::vector<vectorized::DataTypePtr> data_types;
    for (const auto& ctx : expr_contexts) {
        data_types.emplace_back(ctx->root()->data_type());
    }
    return data_types;
}

template <typename DataVariants>
Status init_hash_method(DataVariants* data, const std::vector<vectorized::DataTypePtr>& data_types,
                        bool is_first_phase) {
    auto type = HashKeyType::EMPTY;
    try {
        type = get_hash_key_type_with_phase(get_hash_key_type(data_types), !is_first_phase);
        data->init(data_types, type);
    } catch (const Exception& e) {
        // method_variant may meet valueless_by_exception, so we set it to monostate
        data->method_variant.template emplace<std::monostate>();
        return e.to_status();
    }

    CHECK(!data->method_variant.valueless_by_exception());

    if (type != HashKeyType::without_key && type != HashKeyType::EMPTY &&
        data->method_variant.index() == 0) { // index is 0 means variant is monostate
        return Status::InternalError("method_variant init failed");
    }
    return Status::OK();
}

template <typename MethodVariants, template <typename> typename MethodNullable,
          template <typename, typename> typename MethodOneNumber,
          template <typename> typename DataNullable>
struct DataVariants {
    DataVariants() = default;
    DataVariants(const DataVariants&) = delete;
    DataVariants& operator=(const DataVariants&) = delete;
    MethodVariants method_variant;

    template <typename T, typename TT>
    void emplace_single(bool nullable) {
        if (nullable) {
            method_variant.template emplace<MethodNullable<MethodOneNumber<T, DataNullable<TT>>>>();
        } else {
            method_variant.template emplace<MethodOneNumber<T, TT>>();
        }
    }
};

} // namespace doris