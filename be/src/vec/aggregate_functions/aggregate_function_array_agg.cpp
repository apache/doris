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

#include "vec/aggregate_functions/aggregate_function_array_agg.h"

#include "vec/aggregate_functions/aggregate_function_collect.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
AggregateFunctionPtr do_create_agg_function_collect(const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    if (argument_types[0]->is_nullable()) {
        return creator_without_type::create_ignore_nullable<
                AggregateFunctionArrayAgg<AggregateFunctionArrayAggData<T>>>(
                argument_types, result_is_nullable, attr);
    } else {
        return creator_without_type::create<
                AggregateFunctionCollect<AggregateFunctionCollectListData<T, false>, false>>(
                argument_types, result_is_nullable, attr);
    }
}

AggregateFunctionPtr create_aggregate_function_array_agg(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const bool result_is_nullable,
                                                         const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return do_create_agg_function_collect<TYPE_BOOLEAN>(argument_types, result_is_nullable,
                                                            attr);
    case PrimitiveType::TYPE_TINYINT:
        return do_create_agg_function_collect<TYPE_TINYINT>(argument_types, result_is_nullable,
                                                            attr);
    case PrimitiveType::TYPE_SMALLINT:
        return do_create_agg_function_collect<TYPE_SMALLINT>(argument_types, result_is_nullable,
                                                             attr);
    case PrimitiveType::TYPE_INT:
        return do_create_agg_function_collect<TYPE_INT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return do_create_agg_function_collect<TYPE_BIGINT>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_LARGEINT:
        return do_create_agg_function_collect<TYPE_LARGEINT>(argument_types, result_is_nullable,
                                                             attr);
    case PrimitiveType::TYPE_FLOAT:
        return do_create_agg_function_collect<TYPE_FLOAT>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return do_create_agg_function_collect<TYPE_DOUBLE>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_DECIMAL32:
        return do_create_agg_function_collect<TYPE_DECIMAL32>(argument_types, result_is_nullable,
                                                              attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return do_create_agg_function_collect<TYPE_DECIMAL64>(argument_types, result_is_nullable,
                                                              attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return do_create_agg_function_collect<TYPE_DECIMAL128I>(argument_types, result_is_nullable,
                                                                attr);
    case PrimitiveType::TYPE_DECIMALV2:
        return do_create_agg_function_collect<TYPE_DECIMALV2>(argument_types, result_is_nullable,
                                                              attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return do_create_agg_function_collect<TYPE_DECIMAL256>(argument_types, result_is_nullable,
                                                               attr);
    case PrimitiveType::TYPE_DATE:
        return do_create_agg_function_collect<TYPE_DATE>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIME:
        return do_create_agg_function_collect<TYPE_DATETIME>(argument_types, result_is_nullable,
                                                             attr);
    case PrimitiveType::TYPE_DATEV2:
        return do_create_agg_function_collect<TYPE_DATEV2>(argument_types, result_is_nullable,
                                                           attr);
    case PrimitiveType::TYPE_DATETIMEV2:
        return do_create_agg_function_collect<TYPE_DATETIMEV2>(argument_types, result_is_nullable,
                                                               attr);
    case PrimitiveType::TYPE_IPV4:
        return do_create_agg_function_collect<TYPE_IPV4>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV6:
        return do_create_agg_function_collect<TYPE_IPV6>(argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return do_create_agg_function_collect<TYPE_VARCHAR>(argument_types, result_is_nullable,
                                                            attr);
    default:
        // We do not care what the real type is.
        return do_create_agg_function_collect<INVALID_TYPE>(argument_types, result_is_nullable,
                                                            attr);
    }
}

void register_aggregate_function_array_agg(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("array_agg", create_aggregate_function_array_agg);
}
} // namespace doris::vectorized