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

#include "vec/aggregate_functions/aggregate_function_covar.h"

#include <fmt/format.h>

#include <string>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <template <typename, bool> class AggregateFunctionTemplate,
          template <typename> class NameData, template <typename, typename> class Data,
          bool is_nullable = false>
AggregateFunctionPtr create_function_single_value(const String& name,
                                                  const DataTypes& argument_types,
                                                  const bool result_is_nullable,
                                                  bool custom_nullable) {
    WhichDataType which(remove_nullable(argument_types[0]));
#define DISPATCH(TYPE)                                                                         \
    if (which.idx == TypeIndex::TYPE)                                                          \
        return creator_without_type::create<                                                   \
                AggregateFunctionTemplate<NameData<Data<TYPE, BaseData<TYPE>>>, is_nullable>>( \
                custom_nullable ? remove_nullable(argument_types) : argument_types,            \
                result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE)                                                              \
    if (which.idx == TypeIndex::TYPE)                                               \
        return creator_without_type::create<AggregateFunctionTemplate<              \
                NameData<Data<TYPE, BaseDatadecimal<TYPE>>>, is_nullable>>(         \
                custom_nullable ? remove_nullable(argument_types) : argument_types, \
                result_is_nullable);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << fmt::format("create_function_single_value with unknowed type {}",
                                argument_types[0]->get_name());
    return nullptr;
}

template <bool is_nullable>
AggregateFunctionPtr create_aggregate_function_covariance_samp_old(const std::string& name,
                                                                   const DataTypes& argument_types,
                                                                   const bool result_is_nullable) {
    return create_function_single_value<AggregateFunctionSamp_OLDER, CovarSampName, SampData_OLDER,
                                        is_nullable>(name, argument_types, result_is_nullable,
                                                     NULLABLE);
}

AggregateFunctionPtr create_aggregate_function_covariance_samp(const std::string& name,
                                                               const DataTypes& argument_types,
                                                               const bool result_is_nullable) {
    return create_function_single_value<AggregateFunctionSamp, CovarSampName, SampData>(
            name, argument_types, result_is_nullable, NOTNULLABLE);
}

AggregateFunctionPtr create_aggregate_function_covariance_pop(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable) {
    return create_function_single_value<AggregateFunctionPop, CovarName, PopData>(
            name, argument_types, result_is_nullable, NOTNULLABLE);
}

void register_aggregate_function_covar_pop(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("covar", create_aggregate_function_covariance_pop);
    factory.register_alias("covar", "covar_pop");
}

void register_aggregate_function_covar_samp_old(AggregateFunctionSimpleFactory& factory) {
    factory.register_alternative_function(
            "covar_samp", create_aggregate_function_covariance_samp_old<NOTNULLABLE>);
    factory.register_alternative_function(
            "covar_samp", create_aggregate_function_covariance_samp_old<NULLABLE>, NULLABLE);
}

void register_aggregate_function_covar_samp(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("covar_samp", create_aggregate_function_covariance_samp);
    register_aggregate_function_covar_samp_old(factory);
}
} // namespace doris::vectorized
