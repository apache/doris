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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_window.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_dense_rank(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable) {
    assert_no_parameters(name, parameters);

    return std::make_shared<WindowFunctionDenseRank>(argument_types);
}

AggregateFunctionPtr create_aggregate_function_rank(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    assert_no_parameters(name, parameters);

    return std::make_shared<WindowFunctionRank>(argument_types);
}

AggregateFunctionPtr create_aggregate_function_row_number(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const Array& parameters,
                                                          const bool result_is_nullable) {
    assert_no_parameters(name, parameters);

    return std::make_shared<WindowFunctionRowNumber>(argument_types);
}

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_lag(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionLagData, is_nullable>(
                    name, argument_types, parameters));
}

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_lead(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<WindowFunctionData, WindowFunctionLeadData, is_nullable>(
                    name, argument_types, parameters));
}

void register_aggregate_function_window_rank(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("dense_rank", create_aggregate_function_dense_rank);
    factory.register_function("rank", create_aggregate_function_rank);
    factory.register_function("row_number", create_aggregate_function_row_number);
}

void register_aggregate_function_window_lead_lag(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("lead", create_aggregate_function_lead<false>);
    factory.register_function("lead", create_aggregate_function_lead<true>, true);
    factory.register_function("lag", create_aggregate_function_lag<false>);
    factory.register_function("lag", create_aggregate_function_lag<true>, true);
    factory.register_function("first_value", create_aggregate_function_first<false, false>);
    factory.register_function("first_value", create_aggregate_function_first<true, false>, true);
    factory.register_function("last_value", create_aggregate_function_last<false, false>);
    factory.register_function("last_value", create_aggregate_function_last<true, false>, true);
}

} // namespace doris::vectorized