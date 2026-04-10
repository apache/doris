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

#include "exprs/aggregate/aggregate_function_window_funnel_v2.h"

#include <string>

#include "common/logging.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

AggregateFunctionPtr create_aggregate_function_window_funnel_v2(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const DataTypePtr& result_type,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr) {
    if (argument_types.size() < 4) {
        LOG(WARNING) << "window_funnel_v2 requires at least 4 arguments (window, mode, timestamp, "
                        "and at least 1 boolean condition), but got "
                     << argument_types.size() << ".";
        return nullptr;
    }
    if (argument_types[2]->get_primitive_type() == TYPE_DATETIMEV2) {
        return creator_without_type::create<AggregateFunctionWindowFunnelV2>(
                argument_types, result_is_nullable, attr);
    } else {
        LOG(WARNING) << "Only support DateTime type as window argument!";
        return nullptr;
    }
}

void register_aggregate_function_window_funnel_v2(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("window_funnel_v2", create_aggregate_function_window_funnel_v2);
}

} // namespace doris
