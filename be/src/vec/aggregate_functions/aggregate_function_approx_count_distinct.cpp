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

#include "vec/aggregate_functions/aggregate_function_approx_count_distinct.h"

#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_approx_count_distinct(
        const std::string& name, const DataTypes& argument_types, const Array& parameters,
        const bool result_is_nullable) {
    AggregateFunctionPtr res = nullptr;
    WhichDataType which(argument_types[0]->is_nullable()
                                ? reinterpret_cast<const DataTypeNullable*>(argument_types[0].get())
                                          ->get_nested_type()
                                : argument_types[0]);

    // TODO: use template traits here.
    if (which.is_uint8()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnUInt8>(argument_types));
    } else if (which.is_int8()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnInt8>(argument_types));
    } else if (which.is_int16()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnInt16>(argument_types));
    } else if (which.is_int32()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnInt32>(argument_types));
    } else if (which.is_int64()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnInt64>(argument_types));
    } else if (which.is_date_or_datetime()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnVector<DateTime>>(argument_types));
    } else if (which.is_float32()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnFloat32>(argument_types));
    } else if (which.is_float64()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnFloat64>(argument_types));
    } else if (which.is_decimal()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnDecimal<Decimal128>>(
                argument_types));
    } else if (which.is_string()) {
        res.reset(new AggregateFunctionApproxCountDistinct<ColumnString>(argument_types));
    }

    if (!res) {
        LOG(WARNING) << fmt::format("Illegal type {} of argument for aggregate function {}",
                                    argument_types[0]->get_name(), name);
    }

    return res;
}

void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("approx_count_distinct",
                              create_aggregate_function_approx_count_distinct);
    factory.register_alias("approx_count_distinct", "ndv");
}

} // namespace doris::vectorized
