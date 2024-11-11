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

#include "vec/aggregate_functions/aggregate_function_approx_top_k.h"

#include "common/exception.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

int32_t is_valid_const_columns(const std::vector<bool>& is_const_columns) {
    int32_t true_count = 0;
    bool found_false_after_true = false;
    for (int32_t i = is_const_columns.size() - 1; i >= 0; --i) {
        if (is_const_columns[i]) {
            true_count++;
            if (found_false_after_true) {
                return false;
            }
        } else {
            if (true_count > 2) {
                return false;
            }
            found_false_after_true = true;
        }
    }
    if (true_count > 2) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid is_const_columns configuration");
    }
    return true_count;
}

AggregateFunctionPtr create_aggregate_function_approx_top_k(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr) {
    if (argument_types.empty()) {
        return nullptr;
    }

    std::vector<bool> is_const_columns;
    std::vector<std::string> column_names;
    for (const auto& [name, is_const] : attr.column_infos) {
        is_const_columns.push_back(is_const);
        if (!is_const) {
            column_names.push_back(name);
        }
    }

    int32_t true_count = is_valid_const_columns(is_const_columns);
    if (true_count == 0) {
        return creator_without_type::create<AggregateFunctionApproxTopK<0>>(
                argument_types, result_is_nullable, column_names);
    } else if (true_count == 1) {
        return creator_without_type::create<AggregateFunctionApproxTopK<1>>(
                argument_types, result_is_nullable, column_names);
    } else if (true_count == 2) {
        return creator_without_type::create<AggregateFunctionApproxTopK<2>>(
                argument_types, result_is_nullable, column_names);
    } else {
        return nullptr;
    }
}

void register_aggregate_function_approx_top_k(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("approx_top_k", create_aggregate_function_approx_top_k);
}

} // namespace doris::vectorized