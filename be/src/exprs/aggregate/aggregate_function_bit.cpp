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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionBitwise.cpp
// and modified by Doris

#include "exprs/aggregate/aggregate_function_bit.h"

#include "core/data_type/define_primitive_type.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

void register_aggregate_function_bit(AggregateFunctionSimpleFactory& factory) {
    using creator = creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                           TYPE_LARGEINT>;
    factory.register_function_both(
            "group_bit_or",
            creator::creator<AggregateFunctionBitwise, AggregateFunctionGroupBitOrData>);
    factory.register_function_both(
            "group_bit_and",
            creator::creator<AggregateFunctionBitwise, AggregateFunctionGroupBitAndData>);
    factory.register_function_both(
            "group_bit_xor",
            creator::creator<AggregateFunctionBitwise, AggregateFunctionGroupBitXorData>);
}

} // namespace doris