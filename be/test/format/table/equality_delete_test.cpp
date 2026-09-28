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

#include "format/table/equality_delete.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"

namespace doris {

namespace {

ColumnWithTypeAndName nullable_varbinary_column(
        const std::string& name, const std::vector<std::optional<std::string>>& values) {
    auto data = ColumnVarbinary::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        const std::string bytes = value.value_or("");
        data->insert_data(bytes.data(), bytes.size());
        null_map->insert_value(!value.has_value());
    }
    return {ColumnNullable::create(std::move(data), std::move(null_map)),
            make_nullable(std::make_shared<DataTypeVarbinary>()), name};
}

ColumnWithTypeAndName varbinary_column(const std::string& name,
                                       const std::vector<std::string>& values) {
    auto data = ColumnVarbinary::create();
    for (const auto& value : values) {
        data->insert_data(value.data(), value.size());
    }
    return {std::move(data), std::make_shared<DataTypeVarbinary>(), name};
}

ColumnWithTypeAndName nullable_string_column(
        const std::string& name, const std::vector<std::optional<std::string>>& values) {
    auto data = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (const auto& value : values) {
        const std::string bytes = value.value_or("");
        data->insert_data(bytes.data(), bytes.size());
        null_map->insert_value(!value.has_value());
    }
    return {ColumnNullable::create(std::move(data), std::move(null_map)),
            make_nullable(std::make_shared<DataTypeString>()), name};
}

ColumnWithTypeAndName string_column(const std::string& name,
                                    const std::vector<std::string>& values) {
    auto data = ColumnString::create();
    for (const auto& value : values) {
        data->insert_data(value.data(), value.size());
    }
    return {std::move(data), std::make_shared<DataTypeString>(), name};
}

ColumnWithTypeAndName int_column(const std::string& name, const std::vector<int32_t>& values) {
    auto data = ColumnInt32::create();
    data->get_data().assign(values.begin(), values.end());
    return {std::move(data), std::make_shared<DataTypeInt32>(), name};
}

std::vector<UInt8> apply_equality_delete(
        const Block& delete_block, const std::vector<int>& field_ids, Block* data_block,
        const std::unordered_map<std::string, uint32_t>& column_indexes,
        const std::unordered_map<int, std::string>& field_names) {
    RuntimeProfile profile("equality_delete_varbinary");
    auto equality_delete = EqualityDeleteBase::get_delete_impl(&delete_block, field_ids);
    EXPECT_TRUE(equality_delete->init(&profile).ok());
    IColumn::Filter filter(data_block->rows(), 1);
    EXPECT_TRUE(equality_delete->filter_data_block(data_block, &column_indexes, field_names, filter)
                        .ok());
    return {filter.begin(), filter.end()};
}

} // namespace

TEST(EqualityDeleteTest, NullableSingleVarbinaryKeyUsesByteHashing) {
    Block delete_block;
    delete_block.insert(nullable_varbinary_column(
            "binary_key", {std::nullopt, std::string("delete\0value", 12)}));
    Block data_block;
    data_block.insert(nullable_varbinary_column(
            "binary_key",
            {std::nullopt, std::string("keep\0value", 10), std::string("delete\0value", 12)}));

    EXPECT_EQ((std::vector<UInt8> {0, 1, 0}),
              apply_equality_delete(delete_block, {7}, &data_block, {{"binary_key", 0}},
                                    {{7, "binary_key"}}));
}

TEST(EqualityDeleteTest, NullableStringDeleteMatchesVarbinaryData) {
    Block delete_block;
    delete_block.insert(
            nullable_string_column("binary_key", {std::nullopt, std::string("delete\0value", 12)}));
    Block data_block;
    data_block.insert(nullable_varbinary_column(
            "binary_key",
            {std::nullopt, std::string("keep\0value", 10), std::string("delete\0value", 12)}));

    EXPECT_EQ((std::vector<UInt8> {0, 1, 0}),
              apply_equality_delete(delete_block, {7}, &data_block, {{"binary_key", 0}},
                                    {{7, "binary_key"}}));
}

TEST(EqualityDeleteTest, NullableVarbinaryDeleteMatchesStringData) {
    Block delete_block;
    delete_block.insert(nullable_varbinary_column(
            "binary_key", {std::nullopt, std::string("delete\0value", 12)}));
    Block data_block;
    data_block.insert(nullable_string_column(
            "binary_key",
            {std::nullopt, std::string("keep\0value", 10), std::string("delete\0value", 12)}));

    EXPECT_EQ((std::vector<UInt8> {0, 1, 0}),
              apply_equality_delete(delete_block, {7}, &data_block, {{"binary_key", 0}},
                                    {{7, "binary_key"}}));
}

TEST(EqualityDeleteTest, CompositeVarbinaryKeyUsesByteHashing) {
    Block delete_block;
    delete_block.insert(varbinary_column("binary_key", {"same", "other"}));
    delete_block.insert(int_column("version", {1, 2}));
    Block data_block;
    data_block.insert(varbinary_column("binary_key", {"same", "other", "same"}));
    data_block.insert(int_column("version", {2, 2, 3}));

    EXPECT_EQ((std::vector<UInt8> {1, 0, 1}),
              apply_equality_delete(delete_block, {7, 8}, &data_block,
                                    {{"binary_key", 0}, {"version", 1}},
                                    {{7, "binary_key"}, {8, "version"}}));
}

TEST(EqualityDeleteTest, CompositeStringDeleteMatchesVarbinaryData) {
    Block delete_block;
    delete_block.insert(string_column("binary_key", {std::string("same\0bytes", 10), "other"}));
    delete_block.insert(int_column("version", {1, 2}));
    Block data_block;
    data_block.insert(
            varbinary_column("binary_key", {std::string("same\0bytes", 10), "other", "other"}));
    data_block.insert(int_column("version", {2, 2, 3}));

    EXPECT_EQ((std::vector<UInt8> {1, 0, 1}),
              apply_equality_delete(delete_block, {7, 8}, &data_block,
                                    {{"binary_key", 0}, {"version", 1}},
                                    {{7, "binary_key"}, {8, "version"}}));
}

} // namespace doris
