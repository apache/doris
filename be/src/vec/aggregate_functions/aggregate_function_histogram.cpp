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

#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <typename T>
AggregateFunctionPtr create_agg_function_histogram(const DataTypes& argument_types) {
    bool has_input_param = (argument_types.size() == 3);

    if (has_input_param) {
        return AggregateFunctionPtr(
                new AggregateFunctionHistogram<AggregateFunctionHistogramData<T>, T, true>(
                        argument_types));
    } else {
        return AggregateFunctionPtr(
                new AggregateFunctionHistogram<AggregateFunctionHistogramData<T>, T, false>(
                        argument_types));
    }
}

AggregateFunctionPtr create_aggregate_function_histogram(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable) {
    WhichDataType type(argument_types[0]);

    LOG(INFO) << fmt::format("supported input type {} for aggregate function {}",
                             argument_types[0]->get_name(), name);

#define DISPATCH(TYPE) \
    if (type.idx == TypeIndex::TYPE) return create_agg_function_histogram<TYPE>(argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (type.idx == TypeIndex::String) {
        return create_agg_function_histogram<String>(argument_types);
    }
    if (type.idx == TypeIndex::DateTime || type.idx == TypeIndex::Date) {
        return create_agg_function_histogram<Int64>(argument_types);
    }
    if (type.idx == TypeIndex::DateV2) {
        return create_agg_function_histogram<UInt32>(argument_types);
    }
    if (type.idx == TypeIndex::DateTimeV2) {
        return create_agg_function_histogram<UInt64>(argument_types);
    }
    if (type.idx == TypeIndex::Decimal32) {
        return create_agg_function_histogram<Decimal32>(argument_types);
    }
    if (type.idx == TypeIndex::Decimal64) {
        return create_agg_function_histogram<Decimal64>(argument_types);
    }
    if (type.idx == TypeIndex::Decimal128) {
        return create_agg_function_histogram<Decimal128>(argument_types);
    }
    if (type.idx == TypeIndex::Decimal128I) {
        return create_agg_function_histogram<Decimal128I>(argument_types);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("histogram", create_aggregate_function_histogram);
    factory.register_alias("histogram", "hist");
}

} // namespace doris::vectorized