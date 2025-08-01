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

#include "vec/aggregate_functions/aggregate_function_stddev.h"

#include <fmt/format.h>

#include <string>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <template <typename> class Function, typename Name,
          template <PrimitiveType, typename, bool> class Data, bool is_stddev>
AggregateFunctionPtr create_function_single_value(const String& name,
                                                  const DataTypes& argument_types,
                                                  const bool result_is_nullable,
                                                  const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create<Function<Data<TYPE_BOOLEAN, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<Function<Data<TYPE_TINYINT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<Function<Data<TYPE_SMALLINT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<Function<Data<TYPE_INT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<Function<Data<TYPE_BIGINT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create<Function<Data<TYPE_LARGEINT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<Function<Data<TYPE_FLOAT, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<Function<Data<TYPE_DOUBLE, Name, is_stddev>>>(
                argument_types, result_is_nullable, attr);
    default:
        LOG(WARNING) << fmt::format("create_function_single_value with unknowed type {}",
                                    argument_types[0]->get_name());
        return nullptr;
    }
}

AggregateFunctionPtr create_aggregate_function_variance_samp(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const bool result_is_nullable,
                                                             const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampVariance, VarianceSampName, SampData,
                                        false>(name, argument_types, result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_variance_pop(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampVariance, VarianceName, PopData,
                                        false>(name, argument_types, result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_stddev_pop(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampVariance, StddevName, PopData, true>(
            name, argument_types, result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_stddev_samp(const std::string& name,
                                                           const DataTypes& argument_types,
                                                           const bool result_is_nullable,
                                                           const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampVariance, StddevSampName, SampData,
                                        true>(name, argument_types, result_is_nullable, attr);
}

void register_aggregate_function_stddev_variance_pop(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("variance", create_aggregate_function_variance_pop);
    factory.register_alias("variance", "var_pop");
    factory.register_alias("variance", "variance_pop");
    factory.register_function_both("stddev", create_aggregate_function_stddev_pop);
    factory.register_alias("stddev", "stddev_pop");
    factory.register_alias("stddev", "std");
}

void register_aggregate_function_stddev_variance_samp_old(AggregateFunctionSimpleFactory& factory) {
    BeExecVersionManager::registe_restrict_function_compatibility("variance_samp");
    BeExecVersionManager::registe_restrict_function_compatibility("stddev_samp");
}

void register_aggregate_function_stddev_variance_samp(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("variance_samp", create_aggregate_function_variance_samp);
    factory.register_alias("variance_samp", "var_samp");
    factory.register_function_both("stddev_samp", create_aggregate_function_stddev_samp);
    register_aggregate_function_stddev_variance_samp_old(factory);
}
} // namespace doris::vectorized
