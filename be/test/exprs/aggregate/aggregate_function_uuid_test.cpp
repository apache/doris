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
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_uuid.h"
#include "core/string_buffer.hpp"
#include "core/value/uuid_value.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory);

TEST(AggregateFunctionUUIDTest, ExactDistinctFactoryMergeAndSerialization) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_uniq(factory);
    auto type = std::make_shared<DataTypeUUID>();
    auto function = factory.get("multi_distinct_count", {type}, nullptr, false,
                                BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    auto column = ColumnUUID::create();
    for (const std::string text :
         {"00000000-0000-0000-0000-000000000000", "00000000-0000-0001-0000-000000000000",
          "80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
          "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"}) {
        UUIDValueType value;
        ASSERT_TRUE(UUIDValue::from_string(value, text.data(), text.size()));
        column->insert_value(value);
    }
    Arena arena;
    auto* first = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    auto* second = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    function->create(first);
    function->create(second);
    const IColumn* columns[] = {column.get()};
    for (size_t row = 0; row < column->size(); ++row) {
        function->add(row < 3 ? first : second, columns, row, arena);
    }
    function->merge(first, second, arena);
    auto result = ColumnInt64::create();
    function->insert_result_into(first, *result);
    EXPECT_EQ(result->get_element(0), 4);

    ColumnString serialized;
    VectorBufferWriter writer(serialized);
    function->serialize(first, writer);
    writer.commit();
    function->reset(second);
    VectorBufferReader reader(serialized.get_data_at(0));
    function->deserialize(second, reader, arena);
    function->insert_result_into(second, *result);
    EXPECT_EQ(result->get_element(1), 4);
    function->reset(second);
    function->insert_result_into(second, *result);
    EXPECT_EQ(result->get_element(2), 0);
    function->destroy(second);
    function->destroy(first);
}

} // namespace doris
