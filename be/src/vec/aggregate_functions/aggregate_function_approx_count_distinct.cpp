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

#include "vec/aggregate_functions/aggregate_function_approx_count_distinct.h"

#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_variant.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_approx_count_distinct(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_BOOLEAN>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_TINYINT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_SMALLINT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_INT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_BIGINT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_LARGEINT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_FLOAT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DOUBLE>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL32:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DECIMAL32>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL64:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DECIMAL64>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL128I:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DECIMAL128I>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMALV2:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DECIMALV2>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL256:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DECIMAL256>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_JSONB:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_STRING>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATE:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DATE>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATETIME:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DATETIME>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATEV2:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DATEV2>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATETIMEV2:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_DATETIMEV2>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_IPV4:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_IPV4>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_IPV6:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_IPV6>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_ARRAY:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_ARRAY>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_MAP:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_MAP>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_STRUCT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_STRUCT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_VARIANT:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_VARIANT>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BITMAP:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_BITMAP>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_HLL:
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<TYPE_HLL>,
                                            ArgReturnJudge::UnaryArgumentsResultNotNullable>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return creator_without_type::create<
                AggregateFunctionApproxCountDistinct<TYPE_QUANTILE_STATE>,
                ArgReturnJudge::UnaryArgumentsResultNotNullable>(argument_types,
                                                                 result_is_nullable);
    default:
        return nullptr;
    }
}

void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("approx_count_distinct",
                                   create_aggregate_function_approx_count_distinct);
    factory.register_alias("approx_count_distinct", "ndv");
}

} // namespace doris::vectorized
