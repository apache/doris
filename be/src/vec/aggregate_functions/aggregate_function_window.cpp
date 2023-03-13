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
#include "vec/aggregate_functions/helpers.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_dense_rank(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    return AggregateFunctionPtr(creator_without_type::create<WindowFunctionDenseRank>(
            result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_rank(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable) {
    return AggregateFunctionPtr(
            creator_without_type::create<WindowFunctionRank>(result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_row_number(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    return AggregateFunctionPtr(creator_without_type::create<WindowFunctionRowNumber>(
            result_is_nullable, argument_types));
}

AggregateFunctionPtr create_aggregate_function_ntile(const std::string& name,
                                                     const DataTypes& argument_types,
                                                     const bool result_is_nullable) {
    assert_unary(name, argument_types);
    return AggregateFunctionPtr(
            creator_without_type::create<WindowFunctionNTile>(result_is_nullable, argument_types));
}

template <template <typename> class AggregateFunctionTemplate,
          template <typename ColVecType, bool, bool> class Data, template <typename> class Impl,
          bool result_is_nullable, bool arg_is_nullable>
IAggregateFunction* create_function_lead_lag_first_last(const String& name,
                                                        const DataTypes& argument_types) {
    auto type = remove_nullable(argument_types[0]);
    WhichDataType which(*type);

#define DISPATCH(TYPE, COLUMN_TYPE)           \
    if (which.idx == TypeIndex::TYPE)         \
        return new AggregateFunctionTemplate< \
                Impl<Data<COLUMN_TYPE, result_is_nullable, arg_is_nullable>>>(argument_types);
    TYPE_TO_BASIC_COLUMN_TYPE(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << "with unknowed type, failed in  create_aggregate_function_" << name
                 << " and type is: " << argument_types[0]->get_name();
    return nullptr;
}

#define CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(CREATE_FUNCTION_NAME, FUNCTION_DATA,             \
                                                  FUNCTION_IMPL)                                   \
    AggregateFunctionPtr CREATE_FUNCTION_NAME(const std::string& name,                             \
                                              const DataTypes& argument_types,                     \
                                              const bool result_is_nullable) {                     \
        const bool arg_is_nullable = argument_types[0]->is_nullable();                             \
        AggregateFunctionPtr res = nullptr;                                                        \
                                                                                                   \
        std::visit(                                                                                \
                [&](auto result_is_nullable, auto arg_is_nullable) {                               \
                    res = AggregateFunctionPtr(                                                    \
                            create_function_lead_lag_first_last<WindowFunctionData, FUNCTION_DATA, \
                                                                FUNCTION_IMPL, result_is_nullable, \
                                                                arg_is_nullable>(name,             \
                                                                                 argument_types)); \
                },                                                                                 \
                make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));        \
        if (!res) {                                                                                \
            LOG(WARNING) << " failed in  create_aggregate_function_" << name                       \
                         << " and type is: " << argument_types[0]->get_name();                     \
        }                                                                                          \
        return res;                                                                                \
    }

CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_lag, LeadLagData,
                                          WindowFunctionLagImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_lead, LeadLagData,
                                          WindowFunctionLeadImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_first, FirstLastData,
                                          WindowFunctionFirstImpl);
CREATE_WINDOW_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_window_last, FirstLastData,
                                          WindowFunctionLastImpl);

void register_aggregate_function_window_rank(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("dense_rank", create_aggregate_function_dense_rank);
    factory.register_function("rank", create_aggregate_function_rank);
    factory.register_function("row_number", create_aggregate_function_row_number);
    factory.register_function("ntile", create_aggregate_function_ntile);
}

void register_aggregate_function_window_lead_lag_first_last(
        AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("lead", create_aggregate_function_window_lead);
    factory.register_function_both("lag", create_aggregate_function_window_lag);
    factory.register_function_both("first_value", create_aggregate_function_window_first);
    factory.register_function_both("last_value", create_aggregate_function_window_last);
}

} // namespace doris::vectorized