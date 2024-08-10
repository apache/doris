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

#include <fmt/format.h>

#include <string>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_regr_sxx_.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
template <template <typename> class NameData, template <typename, typename> class Data,
          template <typename> class AggregateFunctionRegrData>
AggregateFunctionPtr create_function_regr(const String& name, const DataTypes& argument_types,
                                          const bool result_is_nullable) {
    WhichDataType which(remove_nullable(argument_types[0]));
#define DISPATCH(TYPE)                                                                        \
    if (which.idx == TypeIndex::TYPE)                                                         \
        return creator_without_type::create<AggregateFunctionRegrFunc<                        \
                TYPE, NameData<Data<TYPE, AggregateFunctionRegrData<TYPE>>>>>(argument_types, \
                                                                              result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    LOG(WARNING) << fmt::format("function {} with unknowed type {}", name,
                                argument_types[0]->get_name());
    return nullptr;
}

AggregateFunctionPtr create_aggregate_function_regr_sxx(const std::string& name,
                                                        const DataTypes& argument_types,
                                                        const bool result_is_nullable) {
    return create_function_regr<RegrSxxName, RegrSxxData, AggregateFunctionRegrSxxData>(
            name, argument_types, result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_regr_sxy(const std::string& name,
                                                        const DataTypes& argument_types,
                                                        const bool result_is_nullable) {
    return create_function_regr<RegrSxyName, RegrSxyData, AggregateFunctionRegrSxyData>(
            name, argument_types, result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_regr_syy(const std::string& name,
                                                        const DataTypes& argument_types,
                                                        const bool result_is_nullable) {
    return create_function_regr<RegrSyyName, RegrSyyData, AggregateFunctionRegrSyyData>(
            name, argument_types, result_is_nullable);
}

void register_aggregate_function_regr_mixed(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("regr_sxx", create_aggregate_function_regr_sxx);
    factory.register_function_both("regr_sxy", create_aggregate_function_regr_sxy);
    factory.register_function_both("regr_syy", create_aggregate_function_regr_syy);
}
} // namespace doris::vectorized
