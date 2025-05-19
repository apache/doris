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
#include "common/compile_check_begin.h"

template <template <typename> class Function, template <typename> class Data>
AggregateFunctionPtr create_function_single_value(const String& name,
                                                  const DataTypes& argument_types,
                                                  const bool result_is_nullable) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create<Function<Data<UInt8>>>(argument_types,
                                                                   result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<Function<Data<Int8>>>(argument_types,
                                                                  result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<Function<Data<Int16>>>(argument_types,
                                                                   result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<Function<Data<Int32>>>(argument_types,
                                                                   result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<Function<Data<Int64>>>(argument_types,
                                                                   result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create<Function<Data<Int128>>>(argument_types,
                                                                    result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<Function<Data<Float32>>>(argument_types,
                                                                     result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<Function<Data<Float64>>>(argument_types,
                                                                     result_is_nullable);
    default:
        LOG(WARNING) << fmt::format("create_function_single_value with unknowed type {}",
                                    argument_types[0]->get_name());
    }

    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_covariance_samp(const std::string& name,
                                                               const DataTypes& argument_types,
                                                               const bool result_is_nullable,
                                                               const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampCovariance, SampData>(
            name, argument_types, result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_covariance_pop(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    return create_function_single_value<AggregateFunctionSampCovariance, PopData>(
            name, argument_types, result_is_nullable);
}

void register_aggregate_function_covar_pop(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("covar", create_aggregate_function_covariance_pop);
    factory.register_alias("covar", "covar_pop");
}

void register_aggregate_function_covar_samp_old(AggregateFunctionSimpleFactory& factory) {
    BeExecVersionManager::registe_restrict_function_compatibility("covar_samp");
}

void register_aggregate_function_covar_samp(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("covar_samp", create_aggregate_function_covariance_samp);
    register_aggregate_function_covar_samp_old(factory);
}
} // namespace doris::vectorized
