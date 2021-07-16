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

#include "vec/aggregate_functions/aggregate_function_uniq.h"

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

template <template <typename> class Data, typename DataForVariadic>
AggregateFunctionPtr create_aggregate_function_uniq(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& params,
                                                    const bool result_is_nullable) {
    assert_no_parameters(name, params);

    if (argument_types.empty()) {
        LOG(WARNING) << "Incorrect number of arguments for aggregate function " << name;
        return nullptr;
    }

    if (argument_types.size() == 1) {
        const IDataType& argument_type = *argument_types[0];

        AggregateFunctionPtr res(create_with_numeric_type<AggregateFunctionUniq, Data>(
                *argument_types[0], argument_types));

        WhichDataType which(argument_type);
        // TODO: DateType
        if (res)
            return res;
        else if (which.is_string_or_fixed_string())
            return std::make_shared<AggregateFunctionUniq<String, Data<String>>>(argument_types);
    }

    return nullptr;
}

void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            create_aggregate_function_uniq<AggregateFunctionUniqExactData,
                                           AggregateFunctionUniqExactData<String>>;
    factory.register_function("multi_distinct_count", creator);
}

} // namespace doris::vectorized
