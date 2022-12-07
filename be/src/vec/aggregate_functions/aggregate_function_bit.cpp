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

#include "vec/aggregate_functions/aggregate_function_bit.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitwise(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    if (!argument_types[0]->can_be_used_in_bit_operations()) {
        LOG(WARNING) << fmt::format("The type " + argument_types[0]->get_name() +
                                    " of argument for aggregate function " + name +
                                    " is illegal, because it cannot be used in bitwise operations");
    }

    auto type = argument_types[0].get();
    if (type->is_nullable()) {
        type = assert_cast<const DataTypeNullable*>(type)->get_nested_type().get();
    }

    WhichDataType which(*type);
    if (which.is_int8()) {
        return AggregateFunctionPtr(new AggregateFunctionBitwise<Int8, Data<Int8>>(argument_types));
    } else if (which.is_int16()) {
        return AggregateFunctionPtr(
                new AggregateFunctionBitwise<Int16, Data<Int16>>(argument_types));
    } else if (which.is_int32()) {
        return AggregateFunctionPtr(
                new AggregateFunctionBitwise<Int32, Data<Int32>>(argument_types));
    } else if (which.is_int64()) {
        return AggregateFunctionPtr(
                new AggregateFunctionBitwise<Int64, Data<Int64>>(argument_types));
    } else if (which.is_int128()) {
        return AggregateFunctionPtr(
                new AggregateFunctionBitwise<Int128, Data<Int128>>(argument_types));
    }

    LOG(WARNING) << fmt::format("Illegal type " + argument_types[0]->get_name() +
                                " of argument for aggregate function " + name);
    return nullptr;
}

void register_aggregate_function_bit(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("group_bit_or",
                              createAggregateFunctionBitwise<AggregateFunctionGroupBitOrData>);
    factory.register_function("group_bit_and",
                              createAggregateFunctionBitwise<AggregateFunctionGroupBitAndData>);
    factory.register_function("group_bit_xor",
                              createAggregateFunctionBitwise<AggregateFunctionGroupBitXorData>);
}

} // namespace doris::vectorized