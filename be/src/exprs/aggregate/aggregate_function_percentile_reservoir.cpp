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

#include "exprs/aggregate/aggregate_function_percentile_reservoir.h"

#include "core/data_type/define_primitive_type.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

AggregateFunctionPtr createAggregateFunctionPercentileReservoir(const std::string& name,
                                                                const DataTypes& argument_types,
                                                                const DataTypePtr& result_type,
                                                                const bool result_is_nullable,
                                                                const AggregateFunctionAttr& attr) {
    if (argument_types.empty()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Aggregate function {} requires at least one argument", name);
    }

    const DataTypePtr& argument_type = remove_nullable(argument_types[0]);
    if (argument_types[0]->get_primitive_type() == TYPE_DOUBLE) {
        return creator_without_type::create<
                AggregateFunctionPercentileReservoir<QuantileReservoirSampler>>(
                argument_types, result_is_nullable, attr);
    }
    return nullptr;
}

void register_aggregate_function_percentile_reservoir(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("percentile_reservoir",
                                   createAggregateFunctionPercentileReservoir);
}

} // namespace doris