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

#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "core/value/file_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {
namespace {

Field file_aggregate_value(int id) {
    static const std::string bytes = std::string(4096, '\xff') + std::string("\0end", 4);
    return Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>("urn:aggregate:" + std::to_string(id)), Field(),
                  Field::create_field<TYPE_BIGINT>(Int64 {id}), Field(),
                  Field::create_field<TYPE_STRING>("ETAG:opaque-" + std::to_string(id)),
                  id == 1 ? Field::create_field<TYPE_VARBINARY>(StringView(bytes))
                          : Field::create_field<TYPE_VARBINARY>(StringView(""))});
}

void expect_file_payload(const Field& value, int id) {
    ASSERT_EQ(value.get_type(), TYPE_FILE);
    const auto& file = value.get<TYPE_FILE>();
    ASSERT_EQ(file.size(), 6);
    EXPECT_EQ(file[0].get<TYPE_STRING>(), "urn:aggregate:" + std::to_string(id));
    EXPECT_TRUE(file[1].is_null());
    EXPECT_EQ(file[2].get<TYPE_BIGINT>(), id);
    EXPECT_TRUE(file[3].is_null());
    EXPECT_EQ(file[4].get<TYPE_STRING>(), "ETAG:opaque-" + std::to_string(id));
    ASSERT_EQ(file[5].get_type(), TYPE_VARBINARY);
    const auto bytes = file[5].get<TYPE_VARBINARY>();
    EXPECT_EQ(std::string(bytes.data(), bytes.size()),
              id == 1 ? std::string(4096, '\xff') + std::string("\0end", 4) : std::string());
}

void expect_opaque_file_bytes(const Field& value, const Field& expected) {
    ASSERT_EQ(value.get_type(), expected.get_type());
    if (value.is_null()) {
        return;
    }
    switch (value.get_type()) {
    case TYPE_FILE: {
        const auto& file = value.get<TYPE_FILE>();
        ASSERT_EQ(file.size(), 6);
        EXPECT_EQ(file[0].get<TYPE_STRING>(), std::string("urn:\xff\0x", 7));
        EXPECT_TRUE(file[1].is_null());
        EXPECT_EQ(file[2].get<TYPE_BIGINT>(), 7);
        EXPECT_TRUE(file[3].is_null());
        EXPECT_EQ(file[4].get<TYPE_STRING>(), std::string("ETAG:\xff\0x", 8));
        const auto bytes = file[5].get<TYPE_VARBINARY>();
        EXPECT_EQ(std::string(bytes.data(), bytes.size()), std::string("\xff\0x", 3));
        break;
    }
    case TYPE_ARRAY: {
        const auto& children = value.get<TYPE_ARRAY>();
        const auto& expected_children = expected.get<TYPE_ARRAY>();
        ASSERT_EQ(children.size(), expected_children.size());
        for (size_t i = 0; i < children.size(); ++i) {
            expect_opaque_file_bytes(children[i], expected_children[i]);
        }
        break;
    }
    case TYPE_STRUCT: {
        const auto& children = value.get<TYPE_STRUCT>();
        const auto& expected_children = expected.get<TYPE_STRUCT>();
        ASSERT_EQ(children.size(), expected_children.size());
        for (size_t i = 0; i < children.size(); ++i) {
            expect_opaque_file_bytes(children[i], expected_children[i]);
        }
        break;
    }
    case TYPE_MAP: {
        const auto& children = value.get<TYPE_MAP>();
        const auto& expected_children = expected.get<TYPE_MAP>();
        ASSERT_EQ(children.size(), expected_children.size());
        for (size_t i = 0; i < children.size(); ++i) {
            expect_opaque_file_bytes(children[i], expected_children[i]);
        }
        break;
    }
    default:
        ASSERT_EQ(value.get_type(), TYPE_STRING);
        EXPECT_EQ(value.get<TYPE_STRING>(), "key");
    }
}

} // namespace

TEST(FileAggregateTest, CollectListPartialStatesPreserveOpaqueBytes) {
    const auto file_type = std::make_shared<DataTypeFile>();
    const auto nullable_file = make_nullable(file_type);
    const auto file = Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>(std::string("urn:\xff\0x", 7)), Field(),
                  Field::create_field<TYPE_BIGINT>(Int64 {7}), Field(),
                  Field::create_field<TYPE_STRING>(std::string("ETAG:\xff\0x", 8)),
                  Field::create_field<TYPE_VARBINARY>(StringView("\xff\0x", 3))});
    ASSERT_TRUE(validate_file(file.get<TYPE_FILE>()).ok());
    const std::vector<std::pair<DataTypePtr, Field>> cases = {
            {file_type, file},
            {std::make_shared<DataTypeArray>(nullable_file),
             Field::create_field<TYPE_ARRAY>(Array {file, Field()})},
            {std::make_shared<DataTypeStruct>(DataTypes {nullable_file}, Strings {"f"}),
             Field::create_field<TYPE_STRUCT>(Struct {file})},
            {std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeString>()),
                                           nullable_file),
             Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {
                                                        Field::create_field<TYPE_STRING>("key")}),
                                                Field::create_field<TYPE_ARRAY>(Array {file})})}};
    for (const auto& [type, value] : cases) {
        SCOPED_TRACE(type->get_name());
        for (const auto [nullable, has_limit] : {std::pair {false, false}, std::pair {true, false},
                                                 std::pair {false, true}, std::pair {true, true}}) {
            SCOPED_TRACE(nullable);
            SCOPED_TRACE(has_limit);
            auto source_type = nullable ? make_nullable(type) : type;
            DataTypes argument_types {source_type};
            if (has_limit) {
                argument_types.push_back(std::make_shared<DataTypeInt32>());
            }
            auto function = AggregateFunctionSimpleFactory::instance().get(
                    "collect_list", argument_types, nullptr, false,
                    BeExecVersionManager::get_newest_version());
            ASSERT_NE(function, nullptr);
            AggregateFunctionGuard partial(function.get());
            AggregateFunctionGuard restored(function.get());
            AggregateFunctionGuard global(function.get());
            Arena arena;

            ColumnString empty_buffer;
            VectorBufferWriter empty_writer(empty_buffer);
            function->serialize(partial.data(), empty_writer);
            empty_writer.commit();
            VectorBufferReader empty_reader(empty_buffer.get_data_at(0));
            ASSERT_NO_THROW(function->deserialize(restored.data(), empty_reader, arena));
            EXPECT_FALSE(empty_reader.has_remaining());
            auto empty_result = function->get_return_type()->create_column();
            function->insert_result_into(restored.data(), *empty_result);
            EXPECT_TRUE((*empty_result)[0].get<TYPE_ARRAY>().empty());

            auto source = source_type->create_column();
            source->insert(value);
            if (nullable) {
                source->insert_default();
            }
            auto limits = ColumnInt32::create();
            limits->insert_value(1);
            if (nullable) {
                limits->insert_value(1);
            }
            const IColumn* columns[] = {source.get(), limits.get()};
            function->add_batch_single_place(source->size(), partial.data(), columns, arena);

            ColumnString buffer;
            VectorBufferWriter writer(buffer);
            function->serialize(partial.data(), writer);
            writer.commit();
            source->clear();
            function->reset(partial.data());
            VectorBufferReader reader(buffer.get_data_at(0));
            ASSERT_NO_THROW(function->deserialize(restored.data(), reader, arena));
            EXPECT_FALSE(reader.has_remaining());
            function->merge(global.data(), restored.data(), arena);
            function->merge(global.data(), restored.data(), arena);

            auto result = function->get_return_type()->create_column();
            function->insert_result_into(global.data(), *result);
            const auto result_value = (*result)[0];
            const auto& collected = result_value.get<TYPE_ARRAY>();
            ASSERT_EQ(collected.size(), has_limit ? 1 : 2);
            for (const auto& item : collected) {
                ASSERT_FALSE(item.is_null());
                expect_opaque_file_bytes(item, value);
            }
        }
    }
}

TEST(FileAggregateTest, ReaderReplaceRetainsInlineAfterSourceColumnReuse) {
    const auto file = std::make_shared<DataTypeFile>();
    for (const auto& name : {"replace_reader", "replace_if_not_null_reader"}) {
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, {file}, nullptr, false, BeExecVersionManager::get_newest_version());
        ASSERT_NE(function.get(), nullptr);
        AggregateFunctionGuard state(function.get());
        Arena arena;
        auto source = file->create_column();
        source->insert(file_aggregate_value(1));
        const IColumn* columns[] = {source.get()};
        function->add(state.data(), columns, 0, arena);
        source->clear();
        source->insert(file_aggregate_value(2));
        function->add(state.data(), columns, 0, arena);
        auto result = function->get_return_type()->create_column();
        function->insert_result_into(state.data(), *result);
        expect_file_payload((*result)[0], 1);
    }
}

TEST(FileAggregateTest, CountReadsOnlyTheOuterNullMask) {
    const auto type = make_nullable(std::make_shared<DataTypeFile>());
    auto source = type->create_column();
    source->insert(file_aggregate_value(1));
    source->insert_default();
    source->insert(file_aggregate_value(2));
    auto function = AggregateFunctionSimpleFactory::instance().get(
            "count", {type}, nullptr, false, BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    AggregateFunctionGuard state(function.get());
    Arena arena;
    const IColumn* columns[] = {source.get()};
    function->add_batch_single_place(source->size(), state.data(), columns, arena);
    auto result = function->get_return_type()->create_column();
    function->insert_result_into(state.data(), *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0].get<TYPE_BIGINT>(), 2);
}

} // namespace doris
