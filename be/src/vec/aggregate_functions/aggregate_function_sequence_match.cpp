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

#include "vec/aggregate_functions/aggregate_function_sequence_match.h"

#include <boost/iterator/iterator_facade.hpp>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <template <typename, typename> typename AggregateFunction>
AggregateFunctionPtr create_aggregate_function_sequence_base(const std::string& name,
                                                             const DataTypes& argument_types,
                                                             const bool result_is_nullable) {
    const auto arg_count = argument_types.size();

    if (arg_count < 4) {
        LOG(WARNING) << "Aggregate function " + name + " requires at least 4 arguments.";
        return nullptr;
    }
    if (arg_count - 2 > MAX_EVENTS) {
        LOG(WARNING) << "Aggregate function " + name + " supports up to " +
                                std::to_string(MAX_EVENTS) + " event arguments.";
        return nullptr;
    }

    if (WhichDataType(remove_nullable(argument_types[1])).is_date_time_v2()) {
        return creator_without_type::create<
                AggregateFunction<DateV2Value<DateTimeV2ValueType>, UInt64>>(argument_types,
                                                                             result_is_nullable);
    } else if (WhichDataType(remove_nullable(argument_types[1])).is_date_time()) {
        return creator_without_type::create<AggregateFunction<VecDateTimeValue, Int64>>(
                argument_types, result_is_nullable);
    } else if (WhichDataType(remove_nullable(argument_types[1])).is_date_v2()) {
        return creator_without_type::create<
                AggregateFunction<DateV2Value<DateV2ValueType>, UInt32>>(argument_types,
                                                                         result_is_nullable);
    }
    return nullptr;
}

void register_aggregate_function_sequence_match(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both(
            "sequence_match",
            create_aggregate_function_sequence_base<AggregateFunctionSequenceMatch>);
    factory.register_function_both(
            "sequence_count",
            create_aggregate_function_sequence_base<AggregateFunctionSequenceCount>);
}
} // namespace doris::vectorized
