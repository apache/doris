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

#include <memory>
#include <string>

#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory);

class AggTopNTest : public testing::Test {
public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_topn(factory);
    }

protected:
    Arena _agg_arena_pool;
};

// String keys are binary, so an embedded NUL must not terminate the JSON key.
TEST_F(AggTopNTest, test_string_key_with_embedded_nul) {
    DataTypes data_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
    auto agg_function =
            AggregateFunctionSimpleFactory::instance().get("topn", data_types, nullptr, false, -1);
    ASSERT_NE(agg_function, nullptr);

    const std::string frequent("a\0b", 3);
    const std::string rare("a\0c", 3);

    auto value_column = ColumnString::create();
    value_column->insert_data(frequent.data(), frequent.size());
    value_column->insert_data(frequent.data(), frequent.size());
    value_column->insert_data(rare.data(), rare.size());

    auto top_num_column = ColumnInt32::create();
    for (size_t i = 0; i < value_column->size(); ++i) {
        top_num_column->insert_value(2);
    }

    const IColumn* columns[2] = {value_column.get(), top_num_column.get()};

    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    for (size_t i = 0; i < value_column->size(); ++i) {
        agg_function->add(place, columns, i, _agg_arena_pool);
    }

    auto result_column = ColumnString::create();
    agg_function->insert_result_into(place, *result_column);
    agg_function->destroy(place);

    ASSERT_EQ(result_column->size(), 1);
    EXPECT_EQ(result_column->get_data_at(0).to_string(), R"({"a\u0000b":2,"a\u0000c":1})");
}

} // namespace doris
