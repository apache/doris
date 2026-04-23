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

#include "common/exception.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/aggregate/aggregate_function_group_array_set_op_impl.h"

namespace doris {

AggregateFunctionPtr create_aggregate_function_group_array_intersect(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr) {
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

} // namespace doris
