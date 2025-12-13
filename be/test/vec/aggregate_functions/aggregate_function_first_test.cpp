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

#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(AggregateFunctionFirstTest, test_factory_get_first_reader_and_load) {
    auto& factory = AggregateFunctionSimpleFactory::instance();
    DataTypes argument_types;
    argument_types.emplace_back(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeInt32>();
    auto fn_load = factory.get("first_load", argument_types, result_type, false,
                               BeExecVersionManager::get_newest_version());
    auto fn_reader = factory.get("first_reader", argument_types, result_type, false,
                                 BeExecVersionManager::get_newest_version());
    ASSERT_NE(fn_load, nullptr);
    ASSERT_NE(fn_reader, nullptr);
}

TEST(AggregateFunctionFirstTest, test_first_load_semantics_non_null) {
    auto& factory = AggregateFunctionSimpleFactory::instance();
    DataTypes argument_types;
    argument_types.emplace_back(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeInt32>();
    auto fn = factory.get("first_load", argument_types, result_type, false,
                          BeExecVersionManager::get_newest_version());
    ASSERT_NE(fn, nullptr);

    auto col = ColumnVector<TYPE_INT>::create();
    col->insert_value(1);
    col->insert_value(99);
    const IColumn* cols[] = {col.get()};

    AggregateDataPtr place = reinterpret_cast<AggregateDataPtr>(malloc(fn->size_of_data()));
    fn->create(place);
    Arena arena;
    fn->add(place, cols, 0, arena);
    fn->add(place, cols, 1, arena);

    auto res_col = ColumnVector<TYPE_INT>::create();
    fn->insert_result_into(place, *res_col);
    ASSERT_EQ(1, res_col->get_data()[0]);

    fn->destroy(place);
    free(place);
}
} // namespace doris::vectorized
