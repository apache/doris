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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionSum.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_sum.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const bool result_is_nullable,
                                           const AggregateFunctionAttr& attr) {
        if (attr.enable_decimal256 && is_decimal(types[0]->get_primitive_type())) {
            return creator_with_type_list<TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
                                          TYPE_DECIMAL256>::
                    creator<AggregateFunctionSumSimpleDecimal256>(name, types, result_is_nullable,
                                                                  attr);
        } else {
            return creator_with_type_list<
                    TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT,
                    TYPE_DOUBLE, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I,
                    TYPE_DECIMALV2>::creator<AggregateFunctionSumSimple>(name, types,
                                                                         result_is_nullable, attr);
        }
    };
    factory.register_function_both("sum", creator);
    factory.register_alias("sum", "sum0");
}

} // namespace doris::vectorized
