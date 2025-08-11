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

#include "vec/aggregate_functions/aggregate_function_map_v2.h"

#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_agg_function_map_agg_v2(const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    return creator_without_type::create_ignore_nullable<
            AggregateFunctionMapAggV2<AggregateFunctionMapAggDataV2>>(argument_types,
                                                                      result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_map_agg_v2(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_LARGEINT:
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DOUBLE:
    case PrimitiveType::TYPE_DECIMAL32:
    case PrimitiveType::TYPE_DECIMAL64:
    case PrimitiveType::TYPE_DECIMAL128I:
    case PrimitiveType::TYPE_DECIMALV2:
    case PrimitiveType::TYPE_DECIMAL256:
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATEV2:
    case PrimitiveType::TYPE_DATETIMEV2:
    case PrimitiveType::TYPE_TIMEV2:
        return create_agg_function_map_agg_v2(argument_types, result_is_nullable, attr);
    default:
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_map_agg_v2(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("map_agg_v2", create_aggregate_function_map_agg_v2);
}

} // namespace doris::vectorized