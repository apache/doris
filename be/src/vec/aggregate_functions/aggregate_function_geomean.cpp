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

#include "vec/aggregate_functions/aggregate_function_geomean.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
AggregateFunctionPtr create_agg_function_geomean(const DataTypes& argument_types,
                                                 const bool result_is_nullable) {
    return creator_without_type::create<AggregateFunctionGeomean<T>>(argument_types,
                                                                     result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_geomean(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    if (argument_types[0]->get_primitive_type() != PrimitiveType::TYPE_DOUBLE) {
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }

    return create_agg_function_geomean<TYPE_DOUBLE>(argument_types, result_is_nullable);
}

void register_aggregate_function_geomean(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("geomean", create_aggregate_function_geomean);
}

} // namespace doris::vectorized
