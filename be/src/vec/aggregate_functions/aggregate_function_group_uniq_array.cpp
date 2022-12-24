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

#include "vec/aggregate_functions/aggregate_function_group_uniq_array.h"

#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

template <typename TYPE, typename HasLimit, typename... TArgs>
AggregateFunctionPtr do_create_aggregate_function_group_uniq_array(const DataTypePtr& argument_type,
                                                                   TArgs... args) {
    return AggregateFunctionPtr(new AggregateFunctionGroupUniqArray<TYPE, HasLimit>(
            argument_type, std::forward<TArgs>(args)...));
}

template <typename HasLimit, typename... TArgs>
AggregateFunctionPtr create_aggregate_function_group_uniq_array_impl(
        const std::string& name, const DataTypePtr& argument_type, TArgs... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE)                                                        \
    if (which.idx == TypeIndex::TYPE)                                         \
        return do_create_aggregate_function_group_uniq_array<TYPE, HasLimit>( \
                argument_type, std::forward<TArgs>(args)...) FOR_NUMERIC_TYPES(DISPATCH);
#undef DISPATCH

    if (which.is_date() || which.is_date_time()) {
        return do_create_aggregate_function_group_uniq_array<StringRef, HasLimit>(
                argument_type, std::forward<TArgs>(args)...);
    } else if (which.is_string()) {
        return do_create_aggregate_function_group_uniq_array<StringRef, HasLimit>(
                argument_type, std::forward<TArgs>(args)...);
    }
    //TODO:support extra types
    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_type->get_name(), name);
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_group_uniq_array(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const Array& parameters,
                                                                const bool result_is_nullable) {
    assert_unary(name, argument_types);
    if (parameters.empty()) {
        return create_aggregate_function_group_uniq_array_impl<std::false_type>(
                name, argument_types[0], parameters);
    }
    if (parameters.size() == 1) {
        auto type = parameters[0].get_type();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64) {
            LOG(WARNING) << fmt::format(
                    "parameter type error for aggregate function {},should be Int64 or UInt64",
                    name);
            return nullptr;
        }
        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0)) {
            LOG(WARNING) << fmt::format(
                    "parameter invalid for aggregate function {},should be positive", name);
            return nullptr;
        }
        UInt64 max_size = parameters[0].get<UInt64>();
        return create_aggregate_function_group_uniq_array_impl<std::true_type>(
                name, argument_types[0], parameters, max_size);
    }

    LOG(WARNING) << fmt::format("number of parameters for aggregate function {}, should be 0 or 1",
                                name);
    return nullptr;
}

void register_aggregate_function_group_uniq_array(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("group_uniq_array", create_aggregate_function_group_uniq_array);
}

} // namespace doris::vectorized