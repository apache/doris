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

AggregateFunctionPtr create_aggregate_function_topn(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    const bool result_is_nullable) {
    if (argument_types.size() == 1) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNImplEmpty>(argument_types));
    } else if (argument_types.size() == 2) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNImplInt<StringDataImplTopN>>(
                        argument_types));
    } else if (argument_types.size() == 3) {
        return AggregateFunctionPtr(
                new AggregateFunctionTopN<AggregateFunctionTopNImplIntInt<StringDataImplTopN>>(
                        argument_types));
    }

    LOG(WARNING) << fmt::format("Illegal number {} of argument for aggregate function {}",
                                argument_types.size(), name);
    return nullptr;
}

void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("topn", create_aggregate_function_topn);
}

} // namespace doris::vectorized