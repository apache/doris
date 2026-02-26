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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionEntropy.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_entropy.h"

#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

AggregateFunctionPtr create_aggregate_function_entropy(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const DataTypePtr& result_type,
                                                       const bool result_is_nullable,
                                                       const AggregateFunctionAttr& attr) {
    if (argument_types.size() == 1) {
        auto res = creator_with_type_list<
                TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
                TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I, TYPE_DECIMAL256, TYPE_DECIMALV2,
                TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE, TYPE_DATETIME, TYPE_DATEV2, TYPE_DATETIMEV2,
                TYPE_TIME, TYPE_TIMEV2, TYPE_TIMESTAMPTZ>::
                create<AggregateFunctionEntropy, AggregateFunctionEntropySingleNumericData>(
                        argument_types, result_is_nullable, attr);
        if (res) {
            return res;
        }

        auto type = argument_types[0]->get_primitive_type();
        if (is_string_type(type) || is_varbinary(type) || type == TYPE_JSONB) {
            res = creator_without_type::create<
                    AggregateFunctionEntropy<AggregateFunctionEntropySingleStringData>>(
                    argument_types, result_is_nullable, attr);
            return res;
        }
    }

    return creator_without_type::create<
            AggregateFunctionEntropy<AggregateFunctionEntropyGenericData>>(
            argument_types, result_is_nullable, attr);
}

void register_aggregate_function_entropy(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("entropy", create_aggregate_function_entropy);
}

} // namespace doris::vectorized
