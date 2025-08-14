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

#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_approx_count_distinct(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    return creator_with_type_list<
            TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
            TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
            TYPE_DECIMAL256, TYPE_VARCHAR, TYPE_DATEV2, TYPE_DATETIMEV2, TYPE_IPV4,
            TYPE_IPV6>::create<AggregateFunctionApproxCountDistinct>(argument_types,
                                                                     result_is_nullable, attr);
}

void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("approx_count_distinct",
                                   create_aggregate_function_approx_count_distinct);
    factory.register_alias("approx_count_distinct", "ndv");
}

} // namespace doris::vectorized
