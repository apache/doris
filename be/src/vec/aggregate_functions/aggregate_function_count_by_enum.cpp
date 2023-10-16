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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionCount.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_count_by_enum.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_count_by_enum(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const Array& parameters,
                                                             const bool result_is_nullable) {
    if (argument_types.size() < 1) {
        LOG(WARNING) << fmt::format("Illegal number {} of argument for aggregate function {}",
                                    argument_types.size(), name);
        return nullptr;
    }

    auto type = argument_types[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }

    WhichDataType which(*type);

    if (which.is_string()) {
        return std::make_shared<AggregateFunctionCountByEnum<AggregateFunctionCountByEnumData>>(argument_types);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_count_by_enum(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("count_by_enum", create_aggregate_function_count_by_enum, true);
}

} // namespace doris::vectorized