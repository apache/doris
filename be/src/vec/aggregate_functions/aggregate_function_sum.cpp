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

#include "vec/aggregate_functions/aggregate_function_sum.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

namespace {

template <typename T>
struct SumSimple {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;
    // using ResultType = NearestFieldType<T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
struct SumSameType {
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
struct SumKahan {
    using ResultType = Float64;
    using AggregateDataType = AggregateFunctionSumKahanData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimple = typename SumSimple<T>::Function;
template <typename T>
using AggregateFunctionSumWithOverflow = typename SumSameType<T>::Function;
template <typename T>
using AggregateFunctionSumKahan =
        std::conditional_t<IsDecimalNumber<T>, typename SumSimple<T>::Function,
                           typename SumKahan<T>::Function>;

template <template <typename> class Function>
AggregateFunctionPtr create_aggregate_function_sum(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable) {
    // assert_no_parameters(name, parameters);
    // assert_unary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (is_decimal(data_type))
        res.reset(create_with_decimal_type<Function>(*data_type, *data_type, argument_types));
    else
        res.reset(create_with_numeric_type<Function>(*data_type, argument_types));

    if (!res) {
        LOG(WARNING) << fmt::format("Illegal type {} of argument for aggregate function {}",
                                    argument_types[0]->get_name(), name);
    }
    return res;
}

} // namespace

void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("sum", create_aggregate_function_sum<AggregateFunctionSumSimple>);
}

} // namespace doris::vectorized
