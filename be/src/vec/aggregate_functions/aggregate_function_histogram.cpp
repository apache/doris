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

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>

#include "vec/aggregate_functions/helpers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename T>
AggregateFunctionPtr create_agg_function_histogram(const DataTypes& argument_types,
                                                   const bool result_is_nullable) {
    bool has_input_param = (argument_types.size() == 2);

    if (has_input_param) {
        return creator_without_type::create<
                AggregateFunctionHistogram<AggregateFunctionHistogramData<T>, T, true>>(
                argument_types, result_is_nullable);
    } else {
        return creator_without_type::create<
                AggregateFunctionHistogram<AggregateFunctionHistogramData<T>, T, false>>(
                argument_types, result_is_nullable);
    }
}

AggregateFunctionPtr create_aggregate_function_histogram(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const bool result_is_nullable,
                                                         const AggregateFunctionAttr& attr) {
    WhichDataType type(remove_nullable(argument_types[0]));

#define DISPATCH(TYPE)               \
    if (type.idx == TypeIndex::TYPE) \
        return create_agg_function_histogram<TYPE>(argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (type.idx == TypeIndex::String) {
        return create_agg_function_histogram<String>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateTime || type.idx == TypeIndex::Date) {
        return create_agg_function_histogram<Int64>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateV2) {
        return create_agg_function_histogram<UInt32>(argument_types, result_is_nullable);
    }
    if (type.idx == TypeIndex::DateTimeV2) {
        return create_agg_function_histogram<UInt64>(argument_types, result_is_nullable);
    }

    LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                argument_types[0]->get_name(), name);
    return nullptr;
}

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("histogram", create_aggregate_function_histogram);
    factory.register_alias("histogram", "hist");
}

} // namespace doris::vectorized