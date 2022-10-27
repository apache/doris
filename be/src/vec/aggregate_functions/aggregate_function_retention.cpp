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

#include "vec/aggregate_functions/aggregate_function_retention.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/factory_helpers.h"
#include "vec/aggregate_functions/helpers.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_retention(const std::string& name,
                                                         const DataTypes& argument_types,
                                                         const Array& parameters,
                                                         const bool result_is_nullable) {
    return std::make_shared<AggregateFunctionRetention>(argument_types);
}

void register_aggregate_function_retention(AggregateFunctionSimpleFactory& factory) {
    factory.register_function("retention", create_aggregate_function_retention, false);
}
} // namespace doris::vectorized