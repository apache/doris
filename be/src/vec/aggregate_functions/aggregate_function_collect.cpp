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

#include "vec/aggregate_functions/aggregate_function_collect.h"

#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function_collect_creator.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename HasLimit>
AggregateFunctionPtr create_aggregate_function_collect_impl(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable) {
    bool distinct = name == "collect_set";

    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return AggregateFunctionCollectCreator<TYPE_BOOLEAN, HasLimit>()(distinct, argument_types,
                                                                         result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return AggregateFunctionCollectCreator<TYPE_TINYINT, HasLimit>()(distinct, argument_types,
                                                                         result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return AggregateFunctionCollectCreator<TYPE_SMALLINT, HasLimit>()(distinct, argument_types,
                                                                          result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return AggregateFunctionCollectCreator<TYPE_INT, HasLimit>()(distinct, argument_types,
                                                                     result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return AggregateFunctionCollectCreator<TYPE_BIGINT, HasLimit>()(distinct, argument_types,
                                                                        result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return AggregateFunctionCollectCreator<TYPE_LARGEINT, HasLimit>()(distinct, argument_types,
                                                                          result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return AggregateFunctionCollectCreator<TYPE_FLOAT, HasLimit>()(distinct, argument_types,
                                                                       result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return AggregateFunctionCollectCreator<TYPE_DOUBLE, HasLimit>()(distinct, argument_types,
                                                                        result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL32:
        return AggregateFunctionCollectCreator<TYPE_DECIMAL32, HasLimit>()(distinct, argument_types,
                                                                           result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL64:
        return AggregateFunctionCollectCreator<TYPE_DECIMAL64, HasLimit>()(distinct, argument_types,
                                                                           result_is_nullable);
    case PrimitiveType::TYPE_DECIMALV2:
        return AggregateFunctionCollectCreator<TYPE_DECIMALV2, HasLimit>()(distinct, argument_types,
                                                                           result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL128I:
        return AggregateFunctionCollectCreator<TYPE_DECIMAL128I, HasLimit>()(
                distinct, argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL256:
        return AggregateFunctionCollectCreator<TYPE_DECIMAL256, HasLimit>()(
                distinct, argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATE:
        return AggregateFunctionCollectCreator<TYPE_DATE, HasLimit>()(distinct, argument_types,
                                                                      result_is_nullable);
    case PrimitiveType::TYPE_DATETIME:
        return AggregateFunctionCollectCreator<TYPE_DATETIME, HasLimit>()(distinct, argument_types,
                                                                          result_is_nullable);
    case PrimitiveType::TYPE_DATEV2:
        return AggregateFunctionCollectCreator<TYPE_DATEV2, HasLimit>()(distinct, argument_types,
                                                                        result_is_nullable);
    case PrimitiveType::TYPE_DATETIMEV2:
        return AggregateFunctionCollectCreator<TYPE_DATETIMEV2, HasLimit>()(
                distinct, argument_types, result_is_nullable);
    case PrimitiveType::TYPE_IPV6:
        return AggregateFunctionCollectCreator<TYPE_IPV6, HasLimit>()(distinct, argument_types,
                                                                      result_is_nullable);
    case PrimitiveType::TYPE_IPV4:
        return AggregateFunctionCollectCreator<TYPE_IPV4, HasLimit>()(distinct, argument_types,
                                                                      result_is_nullable);
    case PrimitiveType::TYPE_STRING:
        return AggregateFunctionCollectCreator<TYPE_STRING, HasLimit>()(distinct, argument_types,
                                                                        result_is_nullable);
    case PrimitiveType::TYPE_CHAR:
        return AggregateFunctionCollectCreator<TYPE_CHAR, HasLimit>()(distinct, argument_types,
                                                                      result_is_nullable);
    case PrimitiveType::TYPE_VARCHAR:
        return AggregateFunctionCollectCreator<TYPE_VARCHAR, HasLimit>()(distinct, argument_types,
                                                                         result_is_nullable);
    default:
        // We do not care what the real type is.
        return AggregateFunctionCollectCreator<INVALID_TYPE, HasLimit>()(distinct, argument_types,
                                                                         result_is_nullable);
    }
}

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    if (argument_types.size() == 1) {
        return create_aggregate_function_collect_impl<std::false_type>(name, argument_types,
                                                                       result_is_nullable);
    }
    if (argument_types.size() == 2) {
        return create_aggregate_function_collect_impl<std::true_type>(name, argument_types,
                                                                      result_is_nullable);
    }
    throw Exception(ErrorCode::INTERNAL_ERROR,
                    "unexpected type for collect, please check the input");
}

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory) {
    // notice: array_agg only differs from collect_list in that array_agg will show null elements in array
    factory.register_function_both("collect_list", create_aggregate_function_collect);
    factory.register_function_both("collect_set", create_aggregate_function_collect);
    factory.register_alias("collect_list", "group_array");
    factory.register_alias("collect_set", "group_uniq_array");
}
} // namespace doris::vectorized