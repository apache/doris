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

#include "vec/aggregate_functions/aggregate_function_histogram.h"

namespace doris::vectorized {

template <typename T>
AggregateFunctionPtr create_agg_function_histogram(const DataTypes& argument_types) {
    return AggregateFunctionPtr(
            new AggregateFunctionHistogram<AggregateFunctionHistogramData<T>, T>(argument_types));
}

AggregateFunctionPtr create_aggregate_function_histogram(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable) {
    WhichDataType type(argument_types[0]);

    if (type.is_uint8()) {
        return create_agg_function_histogram<UInt8>(argument_types);
    } else if (type.is_int8()) {
        return create_agg_function_histogram<Int8>(argument_types);
    } else if (type.is_int16()) {
        return create_agg_function_histogram<Int16>(argument_types);
    } else if (type.is_int32()) {
        return create_agg_function_histogram<Int32>(argument_types);
    } else if (type.is_int64()) {
        return create_agg_function_histogram<Int64>(argument_types);
    } else if (type.is_int128()) {
        return create_agg_function_histogram<Int128>(argument_types);
    } else if (type.is_float32()) {
        return create_agg_function_histogram<Float32>(argument_types);
    } else if (type.is_float64()) {
        return create_agg_function_histogram<Float64>(argument_types);
    } else if (type.is_decimal32()) {
        return create_agg_function_histogram<Decimal32>(argument_types);
    } else if (type.is_decimal64()) {
        return create_agg_function_histogram<Decimal64>(argument_types);
    } else if (type.is_decimal128()) {
        return create_agg_function_histogram<Decimal128>(argument_types);
    } else if (type.is_date()) {
        return create_agg_function_histogram<Int64>(argument_types);
    } else if (type.is_date_time()) {
        return create_agg_function_histogram<Int64>(argument_types);
    } else if (type.is_string()) {
        return create_agg_function_histogram<StringRef>(argument_types);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("histogram", create_aggregate_function_histogram);
}

} // namespace doris::vectorized