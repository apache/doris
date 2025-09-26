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

template <PrimitiveType T, typename Data>
using HistogramWithInputParam = AggregateFunctionLinearHistogram<T, Data, true>;

template <PrimitiveType T, typename Data>
using HistogramNormal = AggregateFunctionLinearHistogram<T, Data, false>;

AggregateFunctionPtr create_aggregate_function_linear_histogram(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr) {
    using creator = creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                           TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL32,
                                           TYPE_DECIMAL64, TYPE_DECIMAL128I, TYPE_DECIMAL256>;
    bool has_offset = (argument_types.size() == 3);
    if (has_offset) {
        return creator::create<HistogramWithInputParam, AggregateFunctionLinearHistogramData>(
                argument_types, result_is_nullable, attr);
    } else {
        return creator::create<HistogramNormal, AggregateFunctionLinearHistogramData>(
                argument_types, result_is_nullable, attr);
    }
}

void register_aggregate_function_linear_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(AggregateFunctionLinearHistogramConsts::NAME,
                                   create_aggregate_function_linear_histogram);
}

} // namespace doris::vectorized
