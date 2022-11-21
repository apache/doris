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

#include "vec/aggregate_functions/aggregate_function_collect.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {

template <typename T>
AggregateFunctionPtr create_agg_function_collect(bool distinct, const DataTypes& argument_types) {
    if (distinct) {
        return AggregateFunctionPtr(
                new AggregateFunctionCollect<AggregateFunctionCollectSetData<T>>(argument_types));
    } else {
        return AggregateFunctionPtr(
                new AggregateFunctionCollect<AggregateFunctionCollectListData<T>>(argument_types));
    }
}

AggregateFunctionPtr create_aggregate_function_collect(const std::string& name,
                                                       const DataTypes& argument_types,
                                                       const Array& parameters,
                                                       const bool result_is_nullable) {
    if (argument_types.size() != 1) {
        LOG(WARNING) << fmt::format("Illegal number {} of argument for aggregate function {}",
                                    argument_types.size(), name);
        return nullptr;
    }

    bool distinct = false;
    if (name == "collect_set") {
        distinct = true;
    }

    WhichDataType type(argument_types[0]);
    if (type.is_uint8()) {
        return create_agg_function_collect<UInt8>(distinct, argument_types);
    } else if (type.is_int8()) {
        return create_agg_function_collect<Int8>(distinct, argument_types);
    } else if (type.is_int16()) {
        return create_agg_function_collect<Int16>(distinct, argument_types);
    } else if (type.is_int32()) {
        return create_agg_function_collect<Int32>(distinct, argument_types);
    } else if (type.is_int64()) {
        return create_agg_function_collect<Int64>(distinct, argument_types);
    } else if (type.is_int128()) {
        return create_agg_function_collect<Int128>(distinct, argument_types);
    } else if (type.is_float32()) {
        return create_agg_function_collect<Float32>(distinct, argument_types);
    } else if (type.is_float64()) {
        return create_agg_function_collect<Float64>(distinct, argument_types);
    } else if (type.is_decimal32()) {
        return create_agg_function_collect<Decimal32>(distinct, argument_types);
    } else if (type.is_decimal64()) {
        return create_agg_function_collect<Decimal64>(distinct, argument_types);
    } else if (type.is_decimal128()) {
        return create_agg_function_collect<Decimal128>(distinct, argument_types);
    } else if (type.is_decimal128i()) {
        return create_agg_function_collect<Decimal128I>(distinct, argument_types);
    } else if (type.is_date()) {
        return create_agg_function_collect<Int64>(distinct, argument_types);
    } else if (type.is_date_time()) {
        return create_agg_function_collect<Int64>(distinct, argument_types);
    } else if (type.is_string()) {
        return create_agg_function_collect<StringRef>(distinct, argument_types);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_collect_list(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("collect_list", create_aggregate_function_collect);
    factory.register_function("collect_set", create_aggregate_function_collect);
}
} // namespace doris::vectorized
