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
#include "core/arena.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
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

} // namespace

TEST(FileAggregateTest, ReaderReplaceRetainsInlineAfterSourceColumnReuse) {
    const auto file = std::make_shared<DataTypeFile>();
    for (const auto& name : {"replace_reader", "replace_if_not_null_reader"}) {
        auto function = AggregateFunctionSimpleFactory::instance().get(name, {file}, nullptr, false,
                                                                       SUPPORT_FILE_VERSION);
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
    auto function = AggregateFunctionSimpleFactory::instance().get("count", {type}, nullptr, false,
                                                                   SUPPORT_FILE_VERSION);
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
