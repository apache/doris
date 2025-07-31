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
#include "vec/core/extended_types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <template <PrimitiveType> class Data>
AggregateFunctionPtr create_aggregate_function_uniq(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    if (argument_types.size() == 1) {
        AggregateFunctionPtr res(creator_with_numeric_type::create<AggregateFunctionUniq, Data>(
                argument_types, result_is_nullable, attr));
        if (res) {
            return res;
        } else {
            switch (argument_types[0]->get_primitive_type()) {
            case TYPE_DECIMAL32:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_DECIMAL32, Data<TYPE_DECIMAL32>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_DECIMAL64:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_DECIMAL64, Data<TYPE_DECIMAL64>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_DECIMAL128I:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_DECIMAL128I, Data<TYPE_DECIMAL128I>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_DECIMAL256:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_DECIMAL256, Data<TYPE_DECIMAL256>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_DECIMALV2:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_DECIMALV2, Data<TYPE_DECIMALV2>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_STRING:
            case TYPE_CHAR:
            case TYPE_VARCHAR:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_STRING, Data<TYPE_STRING>>>(
                        argument_types, result_is_nullable, attr);
            case TYPE_ARRAY:
                return creator_without_type::create<
                        AggregateFunctionUniq<TYPE_ARRAY, Data<TYPE_ARRAY>>>(
                        argument_types, result_is_nullable, attr);
            default:
                break;
            }
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
