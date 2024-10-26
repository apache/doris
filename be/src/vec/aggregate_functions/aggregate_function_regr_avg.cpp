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

#include "vec/aggregate_functions/aggregate_function_regr_avg.h"

#include "io/io_common.h"
#include "vec/aggregate_functions/aggregate_function_avg.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/field.h"

namespace doris::vectorized {

template <typename T>
struct RegrAvg {
    using FieldType = typename AvgNearestFieldTypeTrait<T>::Type;
    using Data = AggregateFunctionAvgData<FieldType>;
};

template <typename T>
using AggregateFuncRegrAvgData = typename RegrAvg<T>::Data;

template <bool is_regr_avgy, typename T>
AggregateFunctionPtr type_dispatch_for_aggregate_function_regr_avg(const DataTypes& argument_types,
                                                                   const bool result_is_nullable,
                                                                   bool y_is_nullable,
                                                                   bool x_is_nullable) {
    if (y_is_nullable) {
        if (x_is_nullable) {
            return creator_without_type::create_ignore_nullable<AggregateFunctionRegrAvg<
                    is_regr_avgy, T, AggregateFuncRegrAvgData<T>, true, true>>(argument_types,
                                                                               result_is_nullable);
        } else {
            return creator_without_type::create_ignore_nullable<AggregateFunctionRegrAvg<
                    is_regr_avgy, T, AggregateFuncRegrAvgData<T>, true, false>>(argument_types,
                                                                                result_is_nullable);
        }
    } else {
        if (x_is_nullable) {
            return creator_without_type::create_ignore_nullable<AggregateFunctionRegrAvg<
                    is_regr_avgy, T, AggregateFuncRegrAvgData<T>, false, true>>(argument_types,
                                                                                result_is_nullable);
        } else {
            return creator_without_type::create_ignore_nullable<AggregateFunctionRegrAvg<
                    is_regr_avgy, T, AggregateFuncRegrAvgData<T>, false, false>>(
                    argument_types, result_is_nullable);
        }
    }
}

template <bool is_regr_avgy>
AggregateFunctionPtr create_aggregate_function_regr_avg(const std::string& name,
                                                        const DataTypes& argument_types,
                                                        const bool result_is_nullable) {
    bool y_is_nullable = argument_types[0]->is_nullable();
    bool x_is_nullable = argument_types[1]->is_nullable();
    WhichDataType y_type(remove_nullable(argument_types[0]));
    WhichDataType x_type(remove_nullable(argument_types[1]));

#define DISPATCH(TYPE)                                                            \
    if (x_type.idx == TypeIndex::TYPE && y_type.idx == TypeIndex::TYPE)           \
        return type_dispatch_for_aggregate_function_regr_avg<is_regr_avgy, TYPE>( \
                argument_types, result_is_nullable, y_is_nullable, x_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << "unsupported input types " << argument_types[0]->get_name() << " and "
                 << argument_types[1]->get_name() << " for aggregate function " << name;
    return nullptr;
}

void register_aggregate_function_regr_avg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_avgy", create_aggregate_function_regr_avg<true>);
    factory.register_function_both("regr_avgx", create_aggregate_function_regr_avg<false>);
}
} // namespace doris::vectorized
