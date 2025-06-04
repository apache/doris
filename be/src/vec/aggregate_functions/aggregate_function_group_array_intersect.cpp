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

#include "vec/aggregate_functions/aggregate_function_group_array_intersect.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

IAggregateFunction* create_with_extra_types(const DataTypePtr& nested_type,
                                            const DataTypes& argument_types) {
    if (nested_type->get_primitive_type() == PrimitiveType::TYPE_DATE ||
        nested_type->get_primitive_type() == PrimitiveType::TYPE_DATETIME) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "We don't support array<date> or array<datetime> for "
                        "group_array_intersect(), please use array<datev2> or array<datetimev2>.");
    } else if (nested_type->get_primitive_type() == PrimitiveType::TYPE_DATEV2) {
        return new AggregateFunctionGroupArrayIntersect<DateV2>(argument_types);
    } else if (nested_type->get_primitive_type() == PrimitiveType::TYPE_DATETIMEV2) {
        return new AggregateFunctionGroupArrayIntersect<DateTimeV2>(argument_types);
    } else {
        /// Check that we can use plain version of AggregateFunctionGroupArrayIntersectGeneric
        if (nested_type->is_value_unambiguously_represented_in_contiguous_memory_region())
            return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_types);
        else
            return new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_types);
    }
}

inline AggregateFunctionPtr create_aggregate_function_group_array_intersect_impl(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable) {
    const auto& nested_type = remove_nullable(
            dynamic_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type());
    AggregateFunctionPtr res = nullptr;

    switch (nested_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<UInt8>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_TINYINT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Int8>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_SMALLINT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Int16>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_INT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Int32>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_BIGINT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Int64>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_LARGEINT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Int128>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_FLOAT:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Float32>>(
                argument_types, result_is_nullable);
        break;
    case PrimitiveType::TYPE_DOUBLE:
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<Float64>>(
                argument_types, result_is_nullable);
        break;
    default:
        res = AggregateFunctionPtr(create_with_extra_types(nested_type, argument_types));
    }

    if (!res) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->get_name(), name);
    }

    return res;
}

AggregateFunctionPtr create_aggregate_function_group_array_intersect(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    assert_unary(name, argument_types);
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);

    if (!WhichDataType(argument_type).is_array())
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Aggregate function groupArrayIntersect accepts only array type argument. "
                        "Provided argument type: " +
                                argument_type->get_name());
    return create_aggregate_function_group_array_intersect_impl(name, {argument_type},
                                                                result_is_nullable);
}

void register_aggregate_function_group_array_intersect(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("group_array_intersect",
                                   create_aggregate_function_group_array_intersect);
}
} // namespace doris::vectorized
