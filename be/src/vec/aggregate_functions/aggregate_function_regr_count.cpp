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

#include "vec/aggregate_functions/aggregate_function_regr_count.h"

#include <string>

#include "vec/aggregate_functions/aggregate_function_count.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

AggregateFunctionPtr type_dispatch_for_aggregate_function_regr_count(
        const DataTypes& argument_types, const bool result_is_nullable, bool y_is_nullable,
        bool x_is_nullable) {
    if (y_is_nullable) {
        if (x_is_nullable) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrCount<true, true>>(argument_types, result_is_nullable);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrCount<true, false>>(argument_types, result_is_nullable);
        }
    } else {
        if (x_is_nullable) {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrCount<false, true>>(argument_types, result_is_nullable);
        } else {
            return creator_without_type::create_ignore_nullable<
                    AggregateFunctionRegrCount<false, false>>(argument_types, result_is_nullable);
        }
    }
}

AggregateFunctionPtr create_aggregate_function_regr_count(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    bool y_is_nullable = argument_types[0]->is_nullable();
    bool x_is_nullable = argument_types[1]->is_nullable();
    WhichDataType y_type(remove_nullable(argument_types[0]));
    WhichDataType x_type(remove_nullable(argument_types[1]));

#define DISPATCH(TYPE)                                                                             \
    if (x_type.idx == TypeIndex::TYPE && y_type.idx == TypeIndex::TYPE)                            \
        return type_dispatch_for_aggregate_function_regr_count(argument_types, result_is_nullable, \
                                                               y_is_nullable, x_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << "unsupported input types " << argument_types[0]->get_name() << " and "
                 << argument_types[1]->get_name() << " for aggregate function " << name;
    return nullptr;
}

void register_aggregate_function_regr_count(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("regr_count", create_aggregate_function_regr_count);
}

} // namespace doris::vectorized
