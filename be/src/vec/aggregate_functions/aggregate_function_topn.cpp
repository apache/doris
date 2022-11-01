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

#include <vec/aggregate_functions/aggregate_function_topn.h>

#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_topn(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    if (argument_types.size() == 2) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNImplInt<StringDataImplTopN>>(
                        argument_types));
    } else if (argument_types.size() == 3) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNImplIntInt<StringDataImplTopN>>(
                        argument_types));
    }

    LOG(WARNING) << fmt::format("Illegal number {} of argument for aggregate function {}",
                                argument_types.size(), name);
    return nullptr;
}

template <bool has_default_param>
AggregateFunctionPtr create_topn_array(const DataTypes& argument_types) {
    auto type = argument_types[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }

    WhichDataType which(*type);

#define DISPATCH(TYPE)                                                                  \
    if (which.idx == TypeIndex::TYPE)                                                   \
        return AggregateFunctionPtr(                                                    \
                new AggregateFunctionTopNArray<                                         \
                        AggregateFunctionTopNImplArray<TYPE, has_default_param>, TYPE>( \
                        argument_types));
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.is_string_or_fixed_string()) {
        return AggregateFunctionPtr(new AggregateFunctionTopNArray<
                                    AggregateFunctionTopNImplArray<std::string, has_default_param>,
                                    std::string>(argument_types));
    }
    if (which.is_decimal()) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopNArray<
                        AggregateFunctionTopNImplArray<Decimal128, has_default_param>, Decimal128>(
                        argument_types));
    }
    if (which.is_date_or_datetime() || which.is_date_time_v2()) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopNArray<
                        AggregateFunctionTopNImplArray<Int64, has_default_param>, Int64>(
                        argument_types));
    }
    if (which.is_date_v2()) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopNArray<
                        AggregateFunctionTopNImplArray<UInt32, has_default_param>, UInt32>(
                        argument_types));
    }

    LOG(WARNING) << fmt::format("Illegal argument  type for aggregate function topn_array is: {}",
                                type->get_name());
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_topn_array(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable) {
    bool has_default_param = (argument_types.size() == 3);
    if (has_default_param) {
        return create_topn_array<true>(argument_types);
    } else {
        return create_topn_array<false>(argument_types);
    }
}

void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("topn", create_aggregate_function_topn);
    factory.register_function("topn_array", create_aggregate_function_topn_array);
}

} // namespace doris::vectorized