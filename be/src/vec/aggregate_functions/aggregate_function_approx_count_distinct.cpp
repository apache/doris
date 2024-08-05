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
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_approx_count_distinct(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable) {
    WhichDataType which(remove_nullable(argument_types[0]));

#define DISPATCH(TYPE, COLUMN_TYPE)                                                             \
    if (which.idx == TypeIndex::TYPE)                                                           \
        return creator_without_type::create<AggregateFunctionApproxCountDistinct<COLUMN_TYPE>>( \
                argument_types, result_is_nullable);
    TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH

    return nullptr;
}

void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("approx_count_distinct",
                                   create_aggregate_function_approx_count_distinct);
    factory.register_alias("approx_count_distinct", "ndv");
}

} // namespace doris::vectorized
