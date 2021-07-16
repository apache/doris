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

#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "vec/aggregate_functions/aggregate_function_combinator.h"
#include "vec/aggregate_functions/aggregate_function_distinct.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

class AggregateFunctionCombinatorDistinct final : public IAggregateFunctionCombinator {
public:
    String get_name() const override { return "Distinct"; }

    DataTypes transform_arguments(const DataTypes& arguments) const override {
        if (arguments.empty()) {
            LOG(FATAL)
                    << "Incorrect number of arguments for aggregate function with Distinct suffix";
        }
        return arguments;
    }

    AggregateFunctionPtr transform_aggregate_function(const AggregateFunctionPtr& nested_function,
                                                      const DataTypes& arguments,
                                                      const Array& params,
                                                      const bool result_is_nullable) const override {
        AggregateFunctionPtr res;
        if (arguments.size() == 1) {
            res.reset(create_with_numeric_type<AggregateFunctionDistinct,
                                               AggregateFunctionDistinctSingleNumericData>(
                    *arguments[0], nested_function, arguments));

            if (res) return res;

            if (arguments[0]->is_value_unambiguously_represented_in_contiguous_memory_region())
                return std::make_shared<AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<true>>>(nested_function,
                                                                           arguments);
            else
                return std::make_shared<AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<false>>>(nested_function,
                                                                            arguments);
        }

        return std::make_shared<
                AggregateFunctionDistinct<AggregateFunctionDistinctMultipleGenericData>>(
                nested_function, arguments);
    }
};

const std::string DISTINCT_FUNCTION_PREFIX = "multi_distinct_";

void register_aggregate_function_combinator_distinct(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const Array& params, const bool result_is_nullable) {
        // 1. we should get not nullable types;
        DataTypes nested_types(types.size());
        std::transform(types.begin(), types.end(), nested_types.begin(),
                       [](const auto& e) { return remove_nullable(e); });
        auto function_combinator = std::make_shared<AggregateFunctionCombinatorDistinct>();
        auto transform_arguments = function_combinator->transform_arguments(nested_types);
        if (!boost::algorithm::starts_with(name, DISTINCT_FUNCTION_PREFIX)) {
            return AggregateFunctionPtr();
        }
        auto nested_function_name = name.substr(DISTINCT_FUNCTION_PREFIX.size());
        auto nested_function = factory.get(nested_function_name, transform_arguments, params);
        return function_combinator->transform_aggregate_function(nested_function, types, params, result_is_nullable);
    };
    factory.register_distinct_function_combinator(creator, DISTINCT_FUNCTION_PREFIX);
}
} // namespace doris::vectorized
