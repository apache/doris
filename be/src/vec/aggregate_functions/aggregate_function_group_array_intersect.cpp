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

#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
using ImplArrayIntersect = AggregateFunctionGroupArrayImplData<T, false>;
template <PrimitiveType T>
using ImplArrayUnion = AggregateFunctionGroupArrayImplData<T, true>;

inline AggregateFunctionPtr create_aggregate_function_group_array_impl(
        bool is_array_union, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    const auto& nested_type = remove_nullable(
            assert_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type());
    using creator = creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                           TYPE_LARGEINT, TYPE_DATEV2, TYPE_DATETIMEV2>;
    AggregateFunctionPtr res = nullptr;
    if (is_array_union) {
        res = creator::create<AggregateFunctionGroupArrayImpl, ImplArrayUnion>(
                argument_types, result_is_nullable, attr);
    } else {
        res = creator::create<AggregateFunctionGroupArrayImpl, ImplArrayIntersect>(
                argument_types, result_is_nullable, attr);
    }

    if (!res) {
        if (is_array_union) {
            res = AggregateFunctionPtr(
                    new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_types));
        } else {
            res = AggregateFunctionPtr(
                    new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_types));
        }
    }
    return res;
}

AggregateFunctionPtr create_aggregate_function_group_array_intersect(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);

    if (argument_type->get_primitive_type() != TYPE_ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Aggregate function groupArrayIntersect accepts only array type argument. "
                        "Provided argument type: " +
                                argument_type->get_name());
    }
    return create_aggregate_function_group_array_impl(false, {argument_type}, result_is_nullable,
                                                      attr);
}

AggregateFunctionPtr create_aggregate_function_group_array_union(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);
    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);

    if (argument_type->get_primitive_type() != TYPE_ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Aggregate function groupArrayIntersect accepts only array type argument. "
                        "Provided argument type: " +
                                argument_type->get_name());
    }
    return create_aggregate_function_group_array_impl(true, {argument_type}, result_is_nullable,
                                                      attr);
}

void register_aggregate_function_group_array_intersect(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("group_array_intersect",
                                   create_aggregate_function_group_array_intersect);
    factory.register_function_both("group_array_union",
                                   create_aggregate_function_group_array_union);
}
} // namespace doris::vectorized
