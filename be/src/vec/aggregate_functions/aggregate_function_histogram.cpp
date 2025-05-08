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
#include "common/compile_check_begin.h"

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
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_agg_function_histogram<UInt8>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_TINYINT:
        return create_agg_function_histogram<Int8>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return create_agg_function_histogram<Int16>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return create_agg_function_histogram<Int32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return create_agg_function_histogram<Int64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_LARGEINT:
        return create_agg_function_histogram<Int128>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return create_agg_function_histogram<Float32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return create_agg_function_histogram<Float64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL32:
        return create_agg_function_histogram<Decimal32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL64:
        return create_agg_function_histogram<Decimal64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL128I:
        return create_agg_function_histogram<Decimal128V3>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMALV2:
        return create_agg_function_histogram<Decimal128V2>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DECIMAL256:
        return create_agg_function_histogram<Decimal256>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        return create_agg_function_histogram<String>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATETIME:
        return create_agg_function_histogram<Int64>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATEV2:
        return create_agg_function_histogram<UInt32>(argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DATETIMEV2:
        return create_agg_function_histogram<UInt64>(argument_types, result_is_nullable);
    default:
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("histogram", create_aggregate_function_histogram);
    factory.register_alias("histogram", "hist");
}

} // namespace doris::vectorized