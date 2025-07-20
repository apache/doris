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
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_agg_function_geomean<TYPE_BOOLEAN>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return create_agg_function_geomean<TYPE_TINYINT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return create_agg_function_geomean<TYPE_SMALLINT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return create_agg_function_geomean<TYPE_INT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return create_agg_function_geomean<TYPE_BIGINT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return create_agg_function_geomean<TYPE_LARGEINT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return create_agg_function_geomean<TYPE_FLOAT>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return create_agg_function_geomean<TYPE_DOUBLE>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL32:
        return create_agg_function_geomean<TYPE_DECIMAL32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL64:
        return create_agg_function_geomean<TYPE_DECIMAL64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL128I:
        return create_agg_function_geomean<TYPE_DECIMAL128I>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMALV2:
        return create_agg_function_geomean<TYPE_DECIMALV2>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL256:
        return create_agg_function_geomean<TYPE_DECIMAL256>(argument_types, result_is_nullable);
    default:
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_geomean(AggregateFunctionSimpleFactory& factory) {
	factory.register_function_both("geomean", create_aggregate_function_geomean);
}

} // namespace doris::vectorized
