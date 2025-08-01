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

#include "vec/aggregate_functions/aggregate_function_linear_histogram.h"

#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

const std::string AggregateFunctionLinearHistogramConsts::NAME = "linear_histogram";

template <PrimitiveType T>
AggregateFunctionPtr create_agg_function_linear_histogram(const DataTypes& argument_types,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    bool has_offset = (argument_types.size() == 3);

    if (has_offset) {
        return creator_without_type::create<
                AggregateFunctionLinearHistogram<T, AggregateFunctionLinearHistogramData<T>, true>>(
                argument_types, result_is_nullable, attr);
    } else {
        return creator_without_type::create<AggregateFunctionLinearHistogram<
                T, AggregateFunctionLinearHistogramData<T>, false>>(argument_types,
                                                                    result_is_nullable, attr);
    }
}

AggregateFunctionPtr create_aggregate_function_linear_histogram(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr) {
    switch (argument_types[0]->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_agg_function_linear_histogram<TYPE_BOOLEAN>(argument_types,
                                                                  result_is_nullable, attr);
    case PrimitiveType::TYPE_TINYINT:
        return create_agg_function_linear_histogram<TYPE_TINYINT>(argument_types,
                                                                  result_is_nullable, attr);
    case PrimitiveType::TYPE_SMALLINT:
        return create_agg_function_linear_histogram<TYPE_SMALLINT>(argument_types,
                                                                   result_is_nullable, attr);
    case PrimitiveType::TYPE_INT:
        return create_agg_function_linear_histogram<TYPE_INT>(argument_types, result_is_nullable,
                                                              attr);
    case PrimitiveType::TYPE_BIGINT:
        return create_agg_function_linear_histogram<TYPE_BIGINT>(argument_types, result_is_nullable,
                                                                 attr);
    case PrimitiveType::TYPE_LARGEINT:
        return create_agg_function_linear_histogram<TYPE_LARGEINT>(argument_types,
                                                                   result_is_nullable, attr);
    case PrimitiveType::TYPE_FLOAT:
        return create_agg_function_linear_histogram<TYPE_FLOAT>(argument_types, result_is_nullable,
                                                                attr);
    case PrimitiveType::TYPE_DOUBLE:
        return create_agg_function_linear_histogram<TYPE_DOUBLE>(argument_types, result_is_nullable,
                                                                 attr);
    case PrimitiveType::TYPE_DECIMAL32:
        return create_agg_function_linear_histogram<TYPE_DECIMAL32>(argument_types,
                                                                    result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL64:
        return create_agg_function_linear_histogram<TYPE_DECIMAL64>(argument_types,
                                                                    result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL128I:
        return create_agg_function_linear_histogram<TYPE_DECIMAL128I>(argument_types,
                                                                      result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMALV2:
        return create_agg_function_linear_histogram<TYPE_DECIMALV2>(argument_types,
                                                                    result_is_nullable, attr);
    case PrimitiveType::TYPE_DECIMAL256:
        return create_agg_function_linear_histogram<TYPE_DECIMAL256>(argument_types,
                                                                     result_is_nullable, attr);
    default:

        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }
}

void register_aggregate_function_linear_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(AggregateFunctionLinearHistogramConsts::NAME,
                                   create_aggregate_function_linear_histogram);
}

} // namespace doris::vectorized
