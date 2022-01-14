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

#include <vec/aggregate_functions/aggregate_function_topn.h>

namespace doris::vectorized {

template <template <typename DataHelper> typename Impl>
struct currying_function_topn {
    template <typename T>
    using Function = AggregateFunctionTopN<AggregateFunctionTopNData, Impl<T>>;

    template <typename T>
    using FunctionNumric = Function<NumricDataImplTopN<T>>;

    AggregateFunctionPtr operator()(const std::string& name, const DataTypes& argument_types) {
        AggregateFunctionPtr res = nullptr;
        DataTypePtr data_type = argument_types[0];

        if (is_decimal(data_type)) {
            res.reset(new Function<DecimalDataImplTopN>(argument_types));
        } else if (is_date_or_datetime(data_type)) {
            res.reset(new Function<DatetimeDataImplTopN>(argument_types));
        } else if (is_string(data_type)) {
            res.reset(new Function<StringDataImplTopN>(argument_types));
        } else {
            res.reset(create_with_numeric_type<FunctionNumric>(*data_type, argument_types));
        }

        if (!res) {
            LOG(WARNING) << fmt::format("Illegal type {} of argument for aggregate function {}",
                                        argument_types[0]->get_name(), name);
        }
        return res;
    }
};

AggregateFunctionPtr create_aggregate_function_topn(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNData,
                                          AggregateFunctionTopNImplMerge>(argument_types));
    } else if (argument_types.size() == 2) {
        return currying_function_topn<AggregateFunctionTopNImplInt>()(name, argument_types);
    } else if (argument_types.size() == 3) {
        return currying_function_topn<AggregateFunctionTopNImplIntInt>()(name, argument_types);
    }

    LOG(WARNING) << fmt::format("Illegal number {} of argument for aggregate function {}",
                                argument_types.size(), name);
    return nullptr;
}

void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("topn", create_aggregate_function_topn);
}

} // namespace doris::vectorized