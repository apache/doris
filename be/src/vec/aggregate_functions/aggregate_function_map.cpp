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

#include "vec/aggregate_functions/aggregate_function_map.h"

#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType K>
AggregateFunctionPtr create_agg_function_map_agg(const DataTypes& argument_types,
                                                 const bool result_is_nullable,
                                                 const AggregateFunctionAttr& attr) {
    return creator_without_type::create_ignore_nullable<
            AggregateFunctionMapAgg<AggregateFunctionMapAggData<K>, K>>(argument_types,
                                                                        result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_map_agg(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_agg_function_map_agg<TYPE_BOOLEAN>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return create_agg_function_map_agg<TYPE_TINYINT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return create_agg_function_map_agg<TYPE_SMALLINT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return create_agg_function_map_agg<TYPE_INT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return create_agg_function_map_agg<TYPE_BIGINT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_LARGEINT:
        return create_agg_function_map_agg<TYPE_LARGEINT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_FLOAT:
        return create_agg_function_map_agg<TYPE_FLOAT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return create_agg_function_map_agg<TYPE_DOUBLE>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL32:
        return create_agg_function_map_agg<TYPE_DECIMAL32>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return create_agg_function_map_agg<TYPE_DECIMAL64>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return create_agg_function_map_agg<TYPE_DECIMAL128I>(argument_types, result_is_nullable,
                                                             attr);
    case PrimitiveType::TYPE_DECIMALV2:
        return create_agg_function_map_agg<TYPE_DECIMALV2>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return create_agg_function_map_agg<TYPE_DECIMAL256>(argument_types, result_is_nullable,
                                                            attr);
    case PrimitiveType::TYPE_STRING:
        return create_agg_function_map_agg<TYPE_STRING>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_CHAR:
        return create_agg_function_map_agg<TYPE_CHAR>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_VARCHAR:
        return create_agg_function_map_agg<TYPE_VARCHAR>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATE:
        return create_agg_function_map_agg<TYPE_DATE>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIME:
        return create_agg_function_map_agg<TYPE_DATETIME>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATEV2:
        return create_agg_function_map_agg<TYPE_DATEV2>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIMEV2:
        return create_agg_function_map_agg<TYPE_DATETIMEV2>(argument_types, result_is_nullable,
                                                            attr);
    default:
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_map_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("map_agg_v1", create_aggregate_function_map_agg);
    factory.register_alias("map_agg_v1", "map_agg");
}

} // namespace doris::vectorized