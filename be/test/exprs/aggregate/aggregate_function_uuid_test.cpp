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
#include <rapidjson/document.h>

#include <algorithm>
#include <functional>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_uuid.h"
#include "core/string_buffer.hpp"
#include "core/value/uuid_value.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_uniq(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_histogram(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_map_agg_v2(AggregateFunctionSimpleFactory& factory);

namespace {

void check_uuid_aggregate_roundtrip(const AggregateFunctionPtr& function, const IColumn** columns,
                                    size_t rows,
                                    const std::function<void(const IColumn&, bool)>& check) {
    Arena arena;
    auto* first = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    auto* second = arena.aligned_alloc(function->size_of_data(), function->align_of_data());
    function->create(first);
    function->create(second);
    for (size_t row = 0; row < rows; ++row) {
        function->add(row % 2 == 0 ? first : second, columns, row, arena);
    }
    function->merge(first, second, arena);
    auto result = function->get_return_type()->create_column();
    function->insert_result_into(first, *result);
    check(*result, false);
    ColumnString serialized;
    VectorBufferWriter writer(serialized);
    function->serialize(first, writer);
    writer.commit();
    function->reset(second);
    VectorBufferReader reader(serialized.get_data_at(0));
    function->deserialize(second, reader, arena);
    result->clear();
    function->insert_result_into(second, *result);
    check(*result, false);
    function->reset(second);
    result->clear();
    function->insert_result_into(second, *result);
    check(*result, true);
    function->destroy(second);
    function->destroy(first);
}

void check_uuid_histogram_result(const IColumn& result, bool empty,
                                 const std::vector<std::string>& texts) {
    const auto json = assert_cast<const ColumnString&>(result).get_data_at(0);
    rapidjson::Document doc;
    doc.Parse(json.data, json.size);
    ASSERT_FALSE(doc.HasParseError());
    const auto& histogram = doc["buckets"];
    ASSERT_EQ(histogram.Size(), empty ? 0 : texts.size());
    for (rapidjson::SizeType i = 0; i < histogram.Size(); ++i) {
        EXPECT_EQ(histogram[i]["lower"].GetString(), texts[i]);
        EXPECT_EQ(histogram[i]["upper"].GetString(), texts[i]);
        EXPECT_EQ(histogram[i]["count"].GetUint64(), i == 0 ? 2 : 1);
    }
}

} // namespace

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

TEST(AggregateFunctionUUIDTest, HistogramFactoryPreservesUuidBoundariesAndCounts) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_histogram(factory);
    const auto type = make_nullable(std::make_shared<DataTypeUUID>());
    const std::vector<std::string> texts {
            "00000000-0000-0000-0000-000000000000", "550e8400-e29b-41d4-a716-446655440000",
            "7fffffff-ffff-ffff-ffff-ffffffffffff", "80000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff"};
    auto input = type->create_column();
    input->insert_default();
    for (const auto& text : texts) {
        UUIDValueType value;
        ASSERT_TRUE(UUIDValue::from_string(value, text.data(), text.size()));
        input->insert(Field::create_field<TYPE_UUID>(value));
    }
    input->insert(Field::create_field<TYPE_UUID>(UUIDValueType {0}));
    auto buckets = ColumnInt32::create();
    buckets->get_data().resize_fill(input->size(), 16);
    const IColumn* columns[] = {input.get(), buckets.get()};
    for (bool parameter : {false, true}) {
        DataTypes types {type};
        if (parameter) {
            types.push_back(std::make_shared<DataTypeInt32>());
        }
        auto function = factory.get("histogram", types, nullptr, false,
                                    BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        check_uuid_aggregate_roundtrip(function, columns, input->size(),
                                       [&](const IColumn& result, bool empty) {
                                           check_uuid_histogram_result(result, empty, texts);
                                       });
    }
}

TEST(AggregateFunctionUUIDTest, MapFactoryPreservesUuidKeysValuesAndNulls) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_map_agg_v2(factory);
    const auto type = make_nullable(std::make_shared<DataTypeUUID>());
    auto function = factory.get("map_agg_v2", {type, type}, nullptr, false,
                                BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    const UUIDValueType boundary = static_cast<UUIDValueType>(1) << 127;
    Array keys {Field(), Field::create_field<TYPE_UUID>(UUIDValueType {0}),
                Field::create_field<TYPE_UUID>(boundary - 1),
                Field::create_field<TYPE_UUID>(boundary),
                Field::create_field<TYPE_UUID>(static_cast<UUIDValueType>(-1))};
    Array values {Field(), keys[4], keys[3], keys[2], keys[1]};
    auto key_column = type->create_column();
    auto value_column = type->create_column();
    for (size_t i = 0; i < keys.size(); ++i) {
        key_column->insert(keys[i]);
        value_column->insert(values[i]);
    }
    key_column->insert(keys[1]);
    value_column->insert(values[1]);
    const IColumn* columns[] = {key_column.get(), value_column.get()};
    check_uuid_aggregate_roundtrip(
            function, columns, key_column->size(), [&](const IColumn& result, bool empty) {
                const Field field = result[0];
                const auto& map = field.get<TYPE_MAP>();
                const auto& actual_keys = map[0].get<TYPE_ARRAY>();
                const auto& actual_values = map[1].get<TYPE_ARRAY>();
                ASSERT_EQ(actual_keys.size(), empty ? 0 : keys.size());
                for (size_t i = 0; i < actual_keys.size(); ++i) {
                    const auto position = std::ranges::find(keys, actual_keys[i]);
                    ASSERT_NE(position, keys.end());
                    EXPECT_EQ(actual_values[i], values[position - keys.begin()]);
                }
            });
}

} // namespace doris
