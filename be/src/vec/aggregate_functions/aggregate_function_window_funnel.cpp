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

#include "vec/aggregate_functions/aggregate_function_window_funnel.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_window_funnel(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const Array& parameters,
                                                             const bool result_is_nullable) {
    if (argument_types.size() < 3) {
        LOG(WARNING) << "window_funnel's argument less than 3.";
        return nullptr;
    }
    if (WhichDataType(remove_nullable(argument_types[2])).is_date_time_v2()) {
        return std::make_shared<
                AggregateFunctionWindowFunnel<DateV2Value<DateTimeV2ValueType>, UInt64>>(
                argument_types);
    } else if (WhichDataType(remove_nullable(argument_types[2])).is_date_time()) {
        return std::make_shared<AggregateFunctionWindowFunnel<VecDateTimeValue, Int64>>(
                argument_types);
    } else {
        LOG(FATAL) << "Only support DateTime type as window argument!";
    }
}

void register_aggregate_function_window_funnel(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("window_funnel", create_aggregate_function_window_funnel, false);
}
} // namespace doris::vectorized
