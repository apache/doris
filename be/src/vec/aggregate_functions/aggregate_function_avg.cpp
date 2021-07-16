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

#include "vec/aggregate_functions/aggregate_function_avg.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

namespace {

template <typename T>
struct Avg {
    using FieldType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;
    using Function = AggregateFunctionAvg<T, AggregateFunctionAvgData<FieldType>>;
};

template <typename T>
using AggregateFuncAvg = typename Avg<T>::Function;

AggregateFunctionPtr create_aggregate_function_avg(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable) {
    assert_no_parameters(name, parameters);
    assert_unary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (is_decimal(data_type))
        res.reset(
                create_with_decimal_type<AggregateFuncAvg>(*data_type, *data_type, argument_types));
    else
        res.reset(create_with_numeric_type<AggregateFuncAvg>(*data_type, argument_types));

    if (!res) {
        LOG(WARNING) << fmt::format("Illegal type {} of argument for aggregate function {}",
                                    argument_types[0]->get_name(), name);
    }
    return res;
}

} // namespace

void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("avg", create_aggregate_function_avg);
}
} // namespace doris::vectorized
