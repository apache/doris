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

#include <algorithm>
#include <ostream>
#include <string>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_window_funnel(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const bool result_is_nullable) {
    if (argument_types.size() < 3) {
        LOG(WARNING) << "window_funnel's argument less than 3.";
        return nullptr;
    }
    if (WhichDataType(remove_nullable(argument_types[2])).is_date_time_v2()) {
        return creator_without_type::create<
                AggregateFunctionWindowFunnel<TypeIndex::DateTimeV2, UInt64>>(argument_types,
                                                                              result_is_nullable);
    } else if (WhichDataType(remove_nullable(argument_types[2])).is_date_time()) {
        return creator_without_type::create<
                AggregateFunctionWindowFunnel<TypeIndex::DateTime, Int64>>(argument_types,
                                                                           result_is_nullable);
    } else {
        LOG(WARNING) << "Only support DateTime type as window argument!";
        return nullptr;
    }
}

AggregateFunctionPtr create_aggregate_function_window_funnel_old(const std::string& name,
                                                                 const DataTypes& argument_types,
                                                                 const bool result_is_nullable) {
    if (argument_types.size() < 3) {
        LOG(WARNING) << "window_funnel's argument less than 3.";
        return nullptr;
    }
    if (WhichDataType(remove_nullable(argument_types[2])).is_date_time_v2()) {
        return creator_without_type::create<
                AggregateFunctionWindowFunnelOld<DateV2Value<DateTimeV2ValueType>, UInt64>>(
                argument_types, result_is_nullable);
    } else if (WhichDataType(remove_nullable(argument_types[2])).is_date_time()) {
        return creator_without_type::create<
                AggregateFunctionWindowFunnelOld<VecDateTimeValue, Int64>>(argument_types,
                                                                           result_is_nullable);
    } else {
        LOG(WARNING) << "Only support DateTime type as window argument!";
        return nullptr;
    }
}

void register_aggregate_function_window_funnel(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("window_funnel", create_aggregate_function_window_funnel);
}
void register_aggregate_function_window_funnel_old(AggregateFunctionSimpleFactory& factory) {
    factory.register_alternative_function("window_funnel",
                                          create_aggregate_function_window_funnel_old, true);
    factory.register_alternative_function("window_funnel",
                                          create_aggregate_function_window_funnel_old, false);
}
} // namespace doris::vectorized
