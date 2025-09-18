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

#include "runtime/define_primitive_type.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename Data>
using HistogramWithInputParam = AggregateFunctionHistogram<Data, true>;

template <typename Data>
using HistogramNormal = AggregateFunctionHistogram<Data, false>;

AggregateFunctionPtr create_aggregate_function_histogram(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const bool result_is_nullable,
                                                         const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 2);
    using creator = creator_with_type_list<TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                           TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMAL32,
                                           TYPE_DECIMAL64, TYPE_DECIMAL128I, TYPE_DECIMAL256,
                                           TYPE_VARCHAR, TYPE_DATEV2, TYPE_DATETIMEV2>;
    if (argument_types.size() == 2) {
        return creator::create<HistogramWithInputParam, AggregateFunctionHistogramData>(
                argument_types, result_is_nullable, attr);
    } else {
        return creator::create<HistogramNormal, AggregateFunctionHistogramData>(
                argument_types, result_is_nullable, attr);
    }
}

void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("histogram", create_aggregate_function_histogram);
    factory.register_alias("histogram", "hist");
}

} // namespace doris::vectorized