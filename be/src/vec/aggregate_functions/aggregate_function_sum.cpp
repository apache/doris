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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_sum.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename T>
struct SumSimple {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType = DisposeDecimal<T, NearestFieldType<T>>;
    // using ResultType = NearestFieldType<T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimple = typename SumSimple<T>::Function;

template <template <typename> class Function>
AggregateFunctionPtr create_aggregate_function_sum(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const bool result_is_nullable) {
    AggregateFunctionPtr res(
            creator_with_type::create<Function>(result_is_nullable, argument_types));
    if (!res) {
        LOG(WARNING) << fmt::format("Illegal type {} of argument for aggregate function {}",
                                    argument_types[0]->get_name(), name);
    }
    return res;
}

// do not level up return type for agg reader
template <typename T>
struct SumSimpleReader {
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimpleReader = typename SumSimpleReader<T>::Function;

AggregateFunctionPtr create_aggregate_function_sum_reader(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    return create_aggregate_function_sum<AggregateFunctionSumSimpleReader>(name, argument_types,
                                                                           result_is_nullable);
}

void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("sum",
                                   create_aggregate_function_sum<AggregateFunctionSumSimple>);
}

} // namespace doris::vectorized
