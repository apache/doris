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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupArrayIntersect.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_group_array_set_op.h"

#include <glog/logging.h>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <template <PrimitiveType> class ImplNumericData, typename ImplStringData>
inline AggregateFunctionPtr create_aggregate_function_group_array_impl(
        const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    const auto& nested_type = remove_nullable(
            assert_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type());

    switch (nested_type->get_primitive_type()) {
    case doris::PrimitiveType::TYPE_BOOLEAN:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_BOOLEAN>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_TINYINT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_SMALLINT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_INT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_BIGINT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_LARGEINT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_LARGEINT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATEV2:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DATEV2>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DATETIMEV2:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DATETIMEV2>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DOUBLE>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_FLOAT>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL32:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DECIMAL32>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DECIMAL64>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DECIMAL128I>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_DECIMAL256>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV4:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_IPV4>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_IPV6:
        return creator_without_type::create<
                AggregateFunctionGroupArraySetOp<ImplNumericData<TYPE_IPV6>>>(
                argument_types, result_is_nullable, attr);
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_CHAR:
        return creator_without_type::create<AggregateFunctionGroupArraySetOp<ImplStringData>>(
                argument_types, result_is_nullable, attr);
    default:
        LOG(WARNING) << " got invalid of nested type: " << nested_type->get_name();
        return nullptr;
    }
}

AggregateFunctionPtr create_aggregate_function_group_array_intersect(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);

    if (argument_type->get_primitive_type() != TYPE_ARRAY) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "Aggregate function group_array_intersect accepts only array type argument. "
                "Provided argument type: " +
                        argument_type->get_name());
    }
    return create_aggregate_function_group_array_impl<GroupArrayNumericIntersectData,
                                                      GroupArrayStringIntersectData>(
            {argument_type}, result_is_nullable, attr);
}

AggregateFunctionPtr create_aggregate_function_group_array_union(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);

    if (argument_type->get_primitive_type() != TYPE_ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Aggregate function group_array_union accepts only array type argument. "
                        "Provided argument type: " +
                                argument_type->get_name());
    }
    return create_aggregate_function_group_array_impl<GroupArrayNumericUnionData,
                                                      GroupArrayStringUnionData>(
            {argument_type}, result_is_nullable, attr);
}

void register_aggregate_function_group_array_set_op(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("group_array_intersect",
                                   create_aggregate_function_group_array_intersect);
    factory.register_function_both("group_array_union",
                                   create_aggregate_function_group_array_union);
}
} // namespace doris::vectorized
