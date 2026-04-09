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

#include <string>

#include "core/data_type/data_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

AggregateFunctionPtr create_aggregate_function_orth_bitmap_intersect(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_orth_bitmap_intersect_count(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_orth_bitmap_union_count(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_orth_intersect_count(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_orth_bitmap_expr_cal(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

AggregateFunctionPtr create_aggregate_function_orth_bitmap_expr_cal_count(
        const std::string& name, const DataTypes& argument_types, const DataTypePtr& result_type,
        const bool result_is_nullable, const AggregateFunctionAttr& attr);

void register_aggregate_function_orthogonal_bitmap(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("orthogonal_bitmap_intersect",
                                   create_aggregate_function_orth_bitmap_intersect);
    factory.register_function_both("orthogonal_bitmap_intersect_count",
                                   create_aggregate_function_orth_bitmap_intersect_count);
    factory.register_function_both("orthogonal_bitmap_union_count",
                                   create_aggregate_function_orth_bitmap_union_count);
    factory.register_function_both("intersect_count",
                                   create_aggregate_function_orth_intersect_count);
    factory.register_function_both("orthogonal_bitmap_expr_calculate",
                                   create_aggregate_function_orth_bitmap_expr_cal);
    factory.register_function_both("orthogonal_bitmap_expr_calculate_count",
                                   create_aggregate_function_orth_bitmap_expr_cal_count);
}

} // namespace doris
