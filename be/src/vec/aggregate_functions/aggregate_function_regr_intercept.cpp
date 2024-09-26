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
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_regr_intercept.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename TX, typename TY>
AggregateFunctionPtr type_dispatch_for_aggregate_function_regr_intercept(const DataTypes& argument_types,
                                                                         const bool& result_is_nullable,
                                                                         bool nullable_input) {
    using StatFunctionTemplate = RegrInterceptFuncTwoArg<TX, TY>;
    if (nullable_input) {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionRegrInterceptSimple<StatFunctionTemplate, true>>(
                argument_types, result_is_nullable);
    } else {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionRegrInterceptSimple<StatFunctionTemplate, false>>(
                argument_types, result_is_nullable);
    }
}

AggregateFunctionPtr create_aggregate_function_regr_intercept(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable) {
    if (argument_types.size() != 2) {
        LOG(WARNING) << "aggregate function " << name << " requires exactly 2 arguments";
        return nullptr;
    }
    if (!result_is_nullable) {
        LOG(WARNING) << "aggregate function " << name << " requires nullable result type";
        return nullptr;
    }
    const bool nullable_input = argument_types[0]->is_nullable() || argument_types[1]->is_nullable();
    WhichDataType x_type(remove_nullable(argument_types[0]));
    WhichDataType y_type(remove_nullable(argument_types[1]));

#define DISPATCH(TX, TY)                                                                                   \
    if (x_type.idx == TypeIndex::TX && y_type.idx == TypeIndex::TY)                                        \
        return type_dispatch_for_aggregate_function_regr_intercept<TX, TY>(argument_types, result_is_nullable, \
                                                                           nullable_input);
#define FOR_ALL_NUMERIC_TYPE_PAIRS(M) \
    M(UInt8, UInt8)   M(UInt8, Int8)   M(UInt8, Int16)   M(UInt8, Int32)   M(UInt8, Int64)   M(UInt8, Int128)   M(UInt8, Float32)   M(UInt8, Float64) \
    M(Int8, UInt8)    M(Int8, Int8)    M(Int8, Int16)    M(Int8, Int32)    M(Int8, Int64)    M(Int8, Int128)    M(Int8, Float32)    M(Int8, Float64)  \
    M(Int16, UInt8)   M(Int16, Int8)   M(Int16, Int16)   M(Int16, Int32)   M(Int16, Int64)   M(Int16, Int128)   M(Int16, Float32)   M(Int16, Float64) \
    M(Int32, UInt8)   M(Int32, Int8)   M(Int32, Int16)   M(Int32, Int32)   M(Int32, Int64)   M(Int32, Int128)   M(Int32, Float32)   M(Int32, Float64) \
    M(Int64, UInt8)   M(Int64, Int8)   M(Int64, Int16)   M(Int64, Int32)   M(Int64, Int64)   M(Int64, Int128)   M(Int64, Float32)   M(Int64, Float64) \
    M(Int128, UInt8)  M(Int128, Int8)  M(Int128, Int16)  M(Int128, Int32)  M(Int128, Int64)  M(Int128, Int128)  M(Int128, Float32)  M(Int128, Float64)\
    M(Float32, UInt8) M(Float32, Int8) M(Float32, Int16) M(Float32, Int32) M(Float32, Int64) M(Float32, Int128) M(Float32, Float32) M(Float32, Float64)\
    M(Float64, UInt8) M(Float64, Int8) M(Float64, Int16) M(Float64, Int32) M(Float64, Int64) M(Float64, Int128) M(Float64, Float32) M(Float64, Float64)

    FOR_ALL_NUMERIC_TYPE_PAIRS(DISPATCH)
#undef DISPATCH
#undef FOR_ALL_NUMERIC_TYPE_PAIRS

    LOG(WARNING) << "Unsupported input types " << argument_types[0]->get_name()
                 << " and " << argument_types[1]->get_name()
                 << " for aggregate function " << name;
    return nullptr;
}

void register_aggregate_function_regr_intercept(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_intercept", create_aggregate_function_regr_intercept);
}

} // namespace doris::vectorized