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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionAvg.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_avg.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/field.h"

namespace doris::vectorized {

template <typename T>
struct Avg {
    using FieldType = typename AvgNearestFieldTypeTrait<T>::Type;
    using Function = AggregateFunctionAvg<T, AggregateFunctionAvgData<FieldType>>;
};

template <typename T>
using AggregateFuncAvg = typename Avg<T>::Function;

template <typename T>
struct AvgDecimal256 {
    using FieldType = typename AvgNearestFieldTypeTrait256<T>::Type;
    using Function = AggregateFunctionAvg<T, AggregateFunctionAvgData<FieldType>>;
};

template <typename T>
using AggregateFuncAvgDecimal256 = typename AvgDecimal256<T>::Function;

void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("avg", creator_with_type::creator<AggregateFuncAvg>);
    factory.register_function_both("avg_decimal256",
                                   creator_with_type::creator<AggregateFuncAvgDecimal256>);
}
} // namespace doris::vectorized
