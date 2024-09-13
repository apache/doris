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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/Combinators/AggregateFunctionForEach.cpp
// and modified by Doris

#include "vec/aggregate_functions/aggregate_function_foreach.h"

#include <memory>
#include <ostream>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

void register_aggregate_function_combinator_foreach(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const bool result_is_nullable) -> AggregateFunctionPtr {
        const std::string& suffix = AggregateFunctionForEach::AGG_FOREACH_SUFFIX;
        DataTypes transform_arguments;
        for (const auto& t : types) {
            auto item_type =
                    assert_cast<const DataTypeArray*>(remove_nullable(t).get())->get_nested_type();
            transform_arguments.push_back((item_type));
        }
        auto nested_function_name = name.substr(0, name.size() - suffix.size());
        auto nested_function =
                factory.get(nested_function_name, transform_arguments, result_is_nullable,
                            BeExecVersionManager::get_newest_version(), false);
        if (!nested_function) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "The combiner did not find a foreach combiner function. nested function "
                    "name {} , args {}",
                    nested_function_name, types_name(types));
        }
        return creator_without_type::create<AggregateFunctionForEach>(types, true, nested_function);
    };
    factory.register_foreach_function_combinator(
            creator, AggregateFunctionForEach::AGG_FOREACH_SUFFIX, true);
    factory.register_foreach_function_combinator(
            creator, AggregateFunctionForEach::AGG_FOREACH_SUFFIX, false);
}
} // namespace doris::vectorized
