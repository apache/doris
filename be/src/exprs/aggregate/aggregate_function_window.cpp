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

#include "exprs/aggregate/aggregate_function_window.h"

#include <string>

#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

// Defined in separate translation units to reduce per-file template instantiation cost:
//   aggregate_function_window_lag.cpp
//   aggregate_function_window_lead.cpp
//   aggregate_function_window_first.cpp
//   aggregate_function_window_last.cpp
//   aggregate_function_window_nth_value.cpp
AggregateFunctionPtr create_aggregate_function_window_lag(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const DataTypePtr& result_type,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_lead(const std::string& name,
                                                           const DataTypes& argument_types,
                                                           const DataTypePtr& result_type,
                                                           const bool result_is_nullable,
                                                           const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_first(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const DataTypePtr& result_type,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_first_ignore_null(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_last(const std::string& name,
                                                           const DataTypes& argument_types,
                                                           const DataTypePtr& result_type,
                                                           const bool result_is_nullable,
                                                           const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_last_ignore_null(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);
AggregateFunctionPtr create_aggregate_function_window_nth_value(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const DataTypePtr& result_type,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr);

template <typename AggregateFunctionTemplate>
AggregateFunctionPtr create_empty_arg_window(const std::string& name,
                                             const DataTypes& argument_types,
                                             const DataTypePtr& result_type,
                                             const bool result_is_nullable,
                                             const AggregateFunctionAttr& attr) {
    if (!argument_types.empty()) {
        throw doris::Exception(
                Status::InternalError("create_window: argument_types must be empty"));
    }
    std::unique_ptr<IAggregateFunction> result =
            std::make_unique<AggregateFunctionTemplate>(argument_types);
    CHECK_AGG_FUNCTION_SERIALIZED_TYPE(AggregateFunctionTemplate);
    return AggregateFunctionPtr(result.release());
}

void register_aggregate_function_window_rank(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("dense_rank", create_empty_arg_window<WindowFunctionDenseRank>);
    factory.register_function("rank", create_empty_arg_window<WindowFunctionRank>);
    factory.register_function("percent_rank", create_empty_arg_window<WindowFunctionPercentRank>);
    factory.register_function("row_number", create_empty_arg_window<WindowFunctionRowNumber>);
    factory.register_function("ntile", creator_without_type::creator<WindowFunctionNTile>);
    factory.register_function("cume_dist", create_empty_arg_window<WindowFunctionCumeDist>);
}

void register_aggregate_function_window_lead_lag_first_last(
        AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("lead", create_aggregate_function_window_lead);
    factory.register_function_both("lag", create_aggregate_function_window_lag);
    // FE rewrites first_value(k1, false) → first_value(k1), so argument_types.size() == 2
    // means arg_ignore_null = true. Dispatch at registration to avoid runtime branching
    // that would double template instantiations.
    factory.register_function_both(
            "first_value",
            [](const std::string& name, const DataTypes& argument_types,
               const DataTypePtr& result_type, const bool result_is_nullable,
               const AggregateFunctionAttr& attr) -> AggregateFunctionPtr {
                if (argument_types.size() == 2) {
                    return create_aggregate_function_window_first_ignore_null(
                            name, argument_types, result_type, result_is_nullable, attr);
                }
                return create_aggregate_function_window_first(name, argument_types, result_type,
                                                              result_is_nullable, attr);
            });
    factory.register_function_both(
            "last_value",
            [](const std::string& name, const DataTypes& argument_types,
               const DataTypePtr& result_type, const bool result_is_nullable,
               const AggregateFunctionAttr& attr) -> AggregateFunctionPtr {
                if (argument_types.size() == 2) {
                    return create_aggregate_function_window_last_ignore_null(
                            name, argument_types, result_type, result_is_nullable, attr);
                }
                return create_aggregate_function_window_last(name, argument_types, result_type,
                                                             result_is_nullable, attr);
            });
    // nth_value always has 2 args (column, N) from FE.
    // WindowFunctionNthValueImpl does not implement ignore-null logic,
    // so register directly without dispatch.
    factory.register_function_both("nth_value", create_aggregate_function_window_nth_value);
}

} // namespace doris
