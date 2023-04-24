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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionUniq.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_uniq.h"

#include <string>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/hash_table/hash.h" // IWYU pragma: keep
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_uniq(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        const IDataType& argument_type = *remove_nullable(argument_types[0]);
        WhichDataType which(argument_type);

        AggregateFunctionPtr res(creator_with_numeric_type::create<AggregateFunctionUniq, Data>(
                argument_types, result_is_nullable));
        if (res) {
            return res;
        } else if (which.is_decimal32()) {
            return creator_without_type::create<AggregateFunctionUniq<Decimal32, Data<Int32>>>(
                    argument_types, result_is_nullable);
        } else if (which.is_decimal64()) {
            return creator_without_type::create<AggregateFunctionUniq<Decimal64, Data<Int64>>>(
                    argument_types, result_is_nullable);
        } else if (which.is_decimal128() || which.is_decimal128i()) {
            return creator_without_type::create<AggregateFunctionUniq<Decimal128, Data<Int128>>>(
                    argument_types, result_is_nullable);
        } else if (which.is_string_or_fixed_string()) {
            return creator_without_type::create<AggregateFunctionUniq<String, Data<String>>>(
                    argument_types, result_is_nullable);
        }
    }

    return nullptr;
}

void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            create_aggregate_function_uniq<AggregateFunctionUniqExactData>;
    factory.register_function_both("multi_distinct_count", creator);
}

} // namespace doris::vectorized
