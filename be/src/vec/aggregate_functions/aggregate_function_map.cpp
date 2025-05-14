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

#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename K>
AggregateFunctionPtr create_agg_function_map_agg(const DataTypes& argument_types,
                                                 const bool result_is_nullable) {
    return creator_without_type::create_ignore_nullable<
            AggregateFunctionMapAgg<AggregateFunctionMapAggData<K>, K>>(argument_types,
                                                                        result_is_nullable);
}

AggregateFunctionPtr create_aggregate_function_map_agg(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_agg_function_map_agg<UInt8>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return create_agg_function_map_agg<Int8>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return create_agg_function_map_agg<Int16>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return create_agg_function_map_agg<Int32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return create_agg_function_map_agg<Int64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return create_agg_function_map_agg<Int128>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return create_agg_function_map_agg<Float32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return create_agg_function_map_agg<Float64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL32:
        return create_agg_function_map_agg<Decimal32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL64:
        return create_agg_function_map_agg<Decimal64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL128I:
        return create_agg_function_map_agg<Decimal128V3>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMALV2:
        return create_agg_function_map_agg<Decimal128V2>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL256:
        return create_agg_function_map_agg<Decimal256>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return create_agg_function_map_agg<String>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATETIME:
        return create_agg_function_map_agg<Int64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATEV2:
        return create_agg_function_map_agg<UInt32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATETIMEV2:
        return create_agg_function_map_agg<UInt64>(argument_types, result_is_nullable);
    default:
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_map_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("map_agg", create_aggregate_function_map_agg);
}

} // namespace doris::vectorized