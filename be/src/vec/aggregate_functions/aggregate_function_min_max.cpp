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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionMinMaxAny.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_min_max.h"

#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/// min, max
template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value(const String& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr) {
    assert_unary(name, argument_types);
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_JSONB:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataString>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATE:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_DATE>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIME:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_DATETIME>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATEV2:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_DATEV2>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIMEV2:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_DATETIMEV2>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_TIME:
    case PrimitiveType::TYPE_TIMEV2:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_TIMEV2>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV4:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_IPV4>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV6:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_IPV6>>>>(
                argument_types, result_is_nullable, attr);
    // For boolean, tinyint, smallint, int, bigint, largeint
    case PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_BOOLEAN>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_TINYINT>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_SMALLINT>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_INT>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_BIGINT>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_LARGEINT>>>>(
                argument_types, result_is_nullable, attr);
    // For float, double
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_FLOAT>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataFixed<TYPE_DOUBLE>>>>(
                argument_types, result_is_nullable, attr);
    // For decimal
    case PrimitiveType::TYPE_DECIMAL32:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataDecimal<TYPE_DECIMAL32>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataDecimal<TYPE_DECIMAL64>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMALV2:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataDecimal<TYPE_DECIMALV2>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataDecimal<TYPE_DECIMAL128I>>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<Data<SingleValueDataDecimal<TYPE_DECIMAL256>>>>(
                argument_types, result_is_nullable, attr);
    default:
        return nullptr;
    }
}

// any_value
template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value_any_value_function(
        const String& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    AggregateFunctionPtr res = create_aggregate_function_single_value<Data>(
            name, argument_types, result_is_nullable, attr);
    if (res) {
        return res;
    }
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    if (argument_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_MAP ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_AGG_STATE ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_BITMAP ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_HLL ||
        argument_type->get_primitive_type() == PrimitiveType::TYPE_QUANTILE_STATE) {
        return creator_without_type::create_unary_arguments<
                AggregateFunctionsSingleValue<SingleValueDataComplexType>>(
                argument_types, result_is_nullable, attr);
    }

    return nullptr;
}

void register_aggregate_function_minmax(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "max", create_aggregate_function_single_value<AggregateFunctionMaxData>);
    factory.register_function_both(
            "min", create_aggregate_function_single_value<AggregateFunctionMinData>);
    factory.register_function_both(
            "any",
            create_aggregate_function_single_value_any_value_function<AggregateFunctionAnyData>);
    factory.register_alias("any", "any_value");
}

} // namespace doris::vectorized
