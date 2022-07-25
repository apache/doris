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

#include "vec/aggregate_functions/aggregate_function_sort.h"

#include "vec/aggregate_functions/aggregate_function_combinator.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {

class AggregateFunctionCombinatorSort final : public IAggregateFunctionCombinator {
private:
    int _sort_column_number;

public:
    AggregateFunctionCombinatorSort(int sort_column_number)
            : _sort_column_number(sort_column_number) {}

    String get_name() const override { return "Sort"; }

    DataTypes transform_arguments(const DataTypes& arguments) const override {
        if (arguments.size() < _sort_column_number + 2) {
            LOG(FATAL) << "Incorrect number of arguments for aggregate function with Sort, "
                       << arguments.size() << " less than " << _sort_column_number + 2;
        }

        DataTypes nested_types;
        nested_types.assign(arguments.begin(), arguments.end() - 1 - _sort_column_number);
        return nested_types;
    }

    template <int sort_column_number>
    struct Reducer {
        static void run(AggregateFunctionPtr& function, const AggregateFunctionPtr& nested_function,
                        const DataTypes& arguments) {
            function = std::make_shared<
                    AggregateFunctionSort<sort_column_number, AggregateFunctionSortData>>(
                    nested_function, arguments);
        }
    };

    AggregateFunctionPtr transform_aggregate_function(
            const AggregateFunctionPtr& nested_function, const DataTypes& arguments,
            const Array& params, const bool result_is_nullable) const override {
        DCHECK(nested_function != nullptr);
        if (nested_function == nullptr) {
            return nullptr;
        }

        AggregateFunctionPtr function = nullptr;
        constexpr_int_match<1, 3, Reducer>::run(_sort_column_number, function, nested_function,
                                                arguments);

        return function;
    }
};

const std::string SORT_FUNCTION_PREFIX = "sort_";

void register_aggregate_function_combinator_sort(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const Array& params, const bool result_is_nullable) {
        int sort_column_number = std::stoi(name.substr(SORT_FUNCTION_PREFIX.size(), 2));
        auto nested_function_name = name.substr(SORT_FUNCTION_PREFIX.size() + 2);

        auto function_combinator =
                std::make_shared<AggregateFunctionCombinatorSort>(sort_column_number);

        auto transform_arguments = function_combinator->transform_arguments(types);

        auto nested_function =
                factory.get(nested_function_name, transform_arguments, params, result_is_nullable);
        return function_combinator->transform_aggregate_function(nested_function, types, params,
                                                                 result_is_nullable);
    };

    for (char c = '1'; c <= '3'; c++) {
        factory.register_distinct_function_combinator(creator, SORT_FUNCTION_PREFIX + c + "_",
                                                      false);
        factory.register_distinct_function_combinator(creator, SORT_FUNCTION_PREFIX + c + "_",
                                                      true);
    }
}
} // namespace doris::vectorized
