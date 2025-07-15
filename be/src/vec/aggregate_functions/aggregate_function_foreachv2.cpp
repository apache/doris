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

#include <memory>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_foreach.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// The difference between AggregateFunctionForEachV2 and AggregateFunctionForEach is that its return value array is always an Array<Nullable<T>>.
// For example, AggregateFunctionForEach's count_foreach([1,2,3]) returns Array<Int64>, which is not ideal
// because we may have already assumed that the array's elements are always nullable types, and many places have such checks.
// V1 code is kept to ensure compatibility during upgrades and downgrades.
// V2 code differs from V1 only in the return type and insert_into logic; all other logic is exactly the same.
class AggregateFunctionForEachV2 : public AggregateFunctionForEach {
public:
    constexpr static auto AGG_FOREACH_SUFFIX = "_foreachv2";
    AggregateFunctionForEachV2(AggregateFunctionPtr nested_function_, const DataTypes& arguments)
            : AggregateFunctionForEach(nested_function_, arguments) {}

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(nested_function->get_return_type()));
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const AggregateFunctionForEachData& state = data(place);

        auto& arr_to = assert_cast<ColumnArray&>(to);
        auto& offsets_to = arr_to.get_offsets();
        IColumn& elems_nullable = arr_to.get_data();

        DCHECK(elems_nullable.is_nullable());
        auto& elems_to = assert_cast<ColumnNullable&>(elems_nullable).get_nested_column();
        auto& elements_null_map =
                assert_cast<ColumnNullable&>(elems_nullable).get_null_map_column();

        if (nested_function->get_return_type()->is_nullable()) {
            char* nested_state = state.array_of_aggregate_datas;
            for (size_t i = 0; i < state.dynamic_array_size; ++i) {
                nested_function->insert_result_into(nested_state, elems_nullable);
                nested_state += nested_size_of_data;
            }
        } else {
            char* nested_state = state.array_of_aggregate_datas;
            for (size_t i = 0; i < state.dynamic_array_size; ++i) {
                nested_function->insert_result_into(nested_state, elems_to);
                elements_null_map.insert_default(); // not null
                nested_state += nested_size_of_data;
            }
        }
        offsets_to.push_back(offsets_to.back() + state.dynamic_array_size);
    }
};

void register_aggregate_function_combinator_foreachv2(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            [&](const std::string& name, const DataTypes& types, const bool result_is_nullable,
                const AggregateFunctionAttr& attr) -> AggregateFunctionPtr {
        const std::string& suffix = AggregateFunctionForEachV2::AGG_FOREACH_SUFFIX;
        DataTypes transform_arguments;
        for (const auto& t : types) {
            auto item_type =
                    assert_cast<const DataTypeArray*>(remove_nullable(t).get())->get_nested_type();
            transform_arguments.push_back((item_type));
        }
        auto nested_function_name = name.substr(0, name.size() - suffix.size());
        auto nested_function = factory.get(nested_function_name, transform_arguments, true,
                                           BeExecVersionManager::get_newest_version(), attr);
        if (!nested_function) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "The combiner did not find a foreach combiner function. nested function "
                    "name {} , args {}",
                    nested_function_name, types_name(types));
        }
        return creator_without_type::create<AggregateFunctionForEachV2>(types, result_is_nullable,
                                                                        attr, nested_function);
    };
    factory.register_foreach_function_combinator(
            creator, AggregateFunctionForEachV2::AGG_FOREACH_SUFFIX, true);
    factory.register_foreach_function_combinator(
            creator, AggregateFunctionForEachV2::AGG_FOREACH_SUFFIX, false);
}
} // namespace doris::vectorized
