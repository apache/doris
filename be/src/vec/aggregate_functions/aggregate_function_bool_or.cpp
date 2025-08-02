// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/aggregate_functions/aggregate_function_bool_or.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_bool_or(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& att) {
    if (argument_types.size() != 1) {
        LOG(WARNING) << "Aggregate function " << name << " requires exactly 1 argument";
        return nullptr;
    }

    if (!result_is_nullable) {
        LOG(WARNING) << "Aggregate funtion " << name << " requires nullable result type";
        return nullptr;
    }

    WhichDataType which(remove_nullable(argument_types[0]));
    if (!which.is_uint8()) {
        LOG(WARNING) << "unsupported input types " << argument_types[0]->get_name()
                     << " for aggregate function " << name;
    }

    if (argument_types[0]->is_nullable()) {
        return creator_without_type::create_ignore_nullable<AggregateFuntionBoolOr<true>>(
                argument_types, result_is_nullable);
    } else {
        return creator_without_type::create_ignore_nullable<AggregateFuntionBoolOr<false>>(
                argument_types, result_is_nullable);
    }
}

void register_aggregate_function_bool_or(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("bool_or", create_aggregate_function_bool_or);
}
} // namespace doris::vectorized