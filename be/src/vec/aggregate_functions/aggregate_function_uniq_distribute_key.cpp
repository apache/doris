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

#include "vec/aggregate_functions/aggregate_function_uniq_distribute_key.h"

#include <string>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_uniq(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        const IDataType& argument_type = *remove_nullable(argument_types[0]);
        WhichDataType which(argument_type);

        AggregateFunctionPtr res(
                creator_with_numeric_type::create<AggregateFunctionUniqDistributeKey, Data>(
                        argument_types, result_is_nullable));
        if (res) {
            return res;
        } else if (which.is_decimal32()) {
            return creator_without_type::create<
                    AggregateFunctionUniqDistributeKey<Decimal32, Data<Int32>>>(argument_types,
                                                                                result_is_nullable);
        } else if (which.is_decimal64()) {
            return creator_without_type::create<
                    AggregateFunctionUniqDistributeKey<Decimal64, Data<Int64>>>(argument_types,
                                                                                result_is_nullable);
        } else if (which.is_decimal128v3()) {
            return creator_without_type::create<
                    AggregateFunctionUniqDistributeKey<Decimal128V3, Data<Int128>>>(
                    argument_types, result_is_nullable);
        } else if (which.is_decimal128v2() || which.is_decimal128v3()) {
            return creator_without_type::create<
                    AggregateFunctionUniqDistributeKey<Decimal128V2, Data<Int128>>>(
                    argument_types, result_is_nullable);
        } else if (which.is_string_or_fixed_string()) {
            return creator_without_type::create<
                    AggregateFunctionUniqDistributeKey<String, Data<String>>>(argument_types,
                                                                              result_is_nullable);
        }
    }

    return nullptr;
}

void register_aggregate_function_uniq_distribute_key(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            create_aggregate_function_uniq<AggregateFunctionUniqDistributeKeyData>;
    factory.register_function_both("multi_distinct_count_distribute_key", creator);
}

} // namespace doris::vectorized
