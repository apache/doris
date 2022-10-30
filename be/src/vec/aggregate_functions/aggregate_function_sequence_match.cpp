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

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized{

template <template <typename, typename> typename AggregateFunction>
AggregateFunctionPtr create_aggregate_function_sequence_base(const std::string & name,
                                                            const DataTypes & argument_types,
                                                            const Array & parameters,
                                                            const bool result_is_nullable){
    const auto arg_count = argument_types.size();

    if (parameters.size() != 1){
        LOG(WARNING) << "Aggregate function " + name + " requires exactly one parameter.";
        return nullptr;
    }

    if (arg_count < 3){
        LOG(WARNING) << "Aggregate function " + name + " requires at least 3 arguments.";
        return nullptr;
    }
    if (arg_count - 1 > max_events){
        LOG(WARNING) << "Aggregate function " + name + " supports up to "
            + std::to_string(max_events) + " event arguments.";
        return nullptr;
    }

    String pattern = parameters.front().safe_get<std::string>();

    if (WhichDataType(remove_nullable(argument_types[0])).is_date_time_v2()) {
        return std::make_shared<
                AggregateFunction<DateV2Value<DateTimeV2ValueType>, UInt64>>(
                argument_types, pattern);
    } else if (WhichDataType(remove_nullable(argument_types[0])).is_date_time()) {
        return std::make_shared<
                AggregateFunction<VecDateTimeValue, Int64>>(
                argument_types, pattern);
    } else if (WhichDataType(remove_nullable(argument_types[0])).is_date_v2()) {
        return std::make_shared<
                AggregateFunction<DateV2Value<DateV2ValueType>, UInt64>>(
                argument_types, pattern);
    } else if(WhichDataType(remove_nullable(argument_types[0])).is_date()){
        return std::make_shared<
                AggregateFunction<Date, Int64>>(
                argument_types, pattern);
    } else {
        LOG(FATAL) << "Only support Date and DateTime type as timestamp argument!";
    }
}


void register_aggregate_function_sequence_match(AggregateFunctionSimpleFactory & factory)
{
    factory.register_function("sequence_match", create_aggregate_function_sequence_base<AggregateFunctionSequenceMatch>);
    factory.register_function("sequence_count", create_aggregate_function_sequence_base<AggregateFunctionSequenceCount>);
}
} // namespce doris::vectorized
