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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/types.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/reader/column_reader.h"
#include "format/new_parquet/selection_vector.h"
#include "format/reader/file_reader.h"

namespace doris::parquet {
namespace {

constexpr int64_t ROW_COUNT = 5;

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

class ParquetColumnReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_parquet_column_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.parquet").string();
        write_parquet_file();
        _file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
        auto metadata = _file_reader->metadata();
        ASSERT_EQ(metadata->num_row_groups(), 1);
        _row_group = _file_reader->RowGroup(0);
        ASSERT_NE(_row_group, nullptr);
        auto schema_descriptor = _file_reader->metadata()->schema();
        ASSERT_NE(schema_descriptor, nullptr);
        auto st = build_parquet_column_schema(*schema_descriptor, &_fields);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(_fields.size(), _expected_by_field.size());
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    template <typename Builder, typename Value>
    std::shared_ptr<arrow::Array> build_required_array(const std::vector<Value>& values) {
        Builder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
        arrow::StringBuilder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_binary_array(const std::vector<std::string>& values) {
        arrow::BinaryBuilder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data()),
                                       static_cast<int32_t>(value.size()))
                                .ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_fixed_binary_array(
            const std::shared_ptr<arrow::DataType>& type, const std::vector<std::string>& values) {
        arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data())).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int32_array() {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.Append(1).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(3).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(5).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_required_struct_array() {
        auto struct_type = arrow::struct_({arrow::field("a", arrow::int32(), false),
                                           arrow::field("b", arrow::utf8(), false)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto b_array_builder = std::make_unique<arrow::StringBuilder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
        auto* b_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));
        const std::vector<int32_t> a_values = {101, 102, 103, 104, 105};
        const std::vector<std::string> b_values = {"sa", "sb", "sc", "sd", "se"};
        for (size_t row = 0; row < a_values.size(); ++row) {
            EXPECT_TRUE(builder.Append().ok());
            EXPECT_TRUE(a_builder->Append(a_values[row]).ok());
            EXPECT_TRUE(b_builder->Append(b_values[row]).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_struct_array() {
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::utf8(), true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto b_array_builder = std::make_unique<arrow::StringBuilder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
        auto* b_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(201).ok());
        EXPECT_TRUE(b_builder->Append("nsa").ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(203).ok());
        EXPECT_TRUE(b_builder->AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(204).ok());
        EXPECT_TRUE(b_builder->Append("nsd").ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_struct_with_list_array() {
        auto list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("xs", list_type, true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                                 value_builder, list_type);
        field_builders.push_back(list_builder);
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(301).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(1).ok());
        EXPECT_TRUE(value_builder->Append(2).ok());

        EXPECT_TRUE(builder.AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(303).ok());
        EXPECT_TRUE(list_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(304).ok());
        EXPECT_TRUE(list_builder->AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(305).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(value_builder->Append(5).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_struct_with_map_array() {
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("kv", map_type, true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::StringBuilder>();
        auto map_builder = std::make_shared<arrow::MapBuilder>(
                arrow::default_memory_pool(), key_builder, value_builder, map_type);
        field_builders.push_back(map_builder);
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(401).ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(1).ok());
        EXPECT_TRUE(value_builder->Append("one").ok());
        EXPECT_TRUE(key_builder->Append(2).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());

        EXPECT_TRUE(builder.AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(403).ok());
        EXPECT_TRUE(map_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(404).ok());
        EXPECT_TRUE(map_builder->AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(405).ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(5).ok());
        EXPECT_TRUE(value_builder->Append("five").ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_required_int_list_array() {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder,
                                   arrow::list(arrow::field("element", arrow::int32(), false)));
        const std::vector<std::vector<int32_t>> values = {
                {1, 2}, {3}, {4, 5, 6}, {7}, {8, 9},
        };
        for (const auto& row : values) {
            EXPECT_TRUE(builder.Append().ok());
            for (const auto value : row) {
                EXPECT_TRUE(value_builder->Append(value).ok());
            }
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int_list_array() {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder,
                                   arrow::list(arrow::field("element", arrow::int32(), true)));
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->Append(10).ok());
        EXPECT_TRUE(value_builder->Append(20).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(value_builder->Append(30).ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->Append(40).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_required_nullable_int_list_array() {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder,
                                   arrow::list(arrow::field("element", arrow::int32(), true)));
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(value_builder->Append(110).ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->Append(120).ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(value_builder->Append(130).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_struct_list_array() {
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::utf8(), true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto b_array_builder = std::make_unique<arrow::StringBuilder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
        auto struct_builder = std::make_shared<arrow::StructBuilder>(
                struct_type, arrow::default_memory_pool(), std::move(field_builders));
        arrow::ListBuilder builder(arrow::default_memory_pool(), struct_builder,
                                   arrow::list(arrow::field("element", struct_type, true)));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(struct_builder->field_builder(0));
        auto* b_builder = assert_cast<arrow::StringBuilder*>(struct_builder->field_builder(1));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(11).ok());
        EXPECT_TRUE(b_builder->Append("la").ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(12).ok());
        EXPECT_TRUE(b_builder->AppendNull().ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->AppendNull().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(13).ok());
        EXPECT_TRUE(b_builder->Append("ld").ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(14).ok());
        EXPECT_TRUE(b_builder->Append("le").ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_list_list_int_array() {
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        auto inner_list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto inner_list_builder = std::make_shared<arrow::ListBuilder>(
                arrow::default_memory_pool(), value_builder, inner_list_type);
        arrow::ListBuilder builder(arrow::default_memory_pool(), inner_list_builder,
                                   arrow::list(arrow::field("element", inner_list_type, true)));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(inner_list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(1).ok());
        EXPECT_TRUE(value_builder->Append(2).ok());
        EXPECT_TRUE(inner_list_builder->AppendEmptyValue().ok());
        EXPECT_TRUE(inner_list_builder->AppendNull().ok());
        EXPECT_TRUE(inner_list_builder->Append().ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(value_builder->Append(3).ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(inner_list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(4).ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(inner_list_builder->AppendEmptyValue().ok());
        EXPECT_TRUE(inner_list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(5).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_required_int_string_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::StringBuilder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), false));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder,
                                  map_type);
        const std::vector<std::vector<std::pair<int32_t, std::string>>> values = {
                {{1, "a"}, {2, "b"}}, {{3, "c"}},           {{4, "d"}, {5, "e"}, {6, "f"}},
                {{7, "g"}},           {{8, "h"}, {9, "i"}},
        };
        for (const auto& row : values) {
            EXPECT_TRUE(builder.Append().ok());
            for (const auto& [key, value] : row) {
                EXPECT_TRUE(key_builder->Append(key).ok());
                EXPECT_TRUE(value_builder->Append(value).ok());
            }
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int_string_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::StringBuilder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder,
                                  map_type);
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(10).ok());
        EXPECT_TRUE(value_builder->Append("aa").ok());
        EXPECT_TRUE(key_builder->Append(20).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(30).ok());
        EXPECT_TRUE(value_builder->Append("cc").ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(40).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_required_nullable_string_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::StringBuilder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder,
                                  map_type);
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(101).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(102).ok());
        EXPECT_TRUE(value_builder->Append("bb").ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(103).ok());
        EXPECT_TRUE(value_builder->Append("cc").ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(104).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int_struct_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::utf8(), true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto b_array_builder = std::make_unique<arrow::StringBuilder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
        auto value_builder = std::make_shared<arrow::StructBuilder>(
                struct_type, arrow::default_memory_pool(), std::move(field_builders));
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", struct_type, true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder,
                                  map_type);
        auto* a_builder = assert_cast<arrow::Int32Builder*>(value_builder->field_builder(0));
        auto* b_builder = assert_cast<arrow::StringBuilder*>(value_builder->field_builder(1));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(101).ok());
        EXPECT_TRUE(value_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(21).ok());
        EXPECT_TRUE(b_builder->Append("ma").ok());
        EXPECT_TRUE(key_builder->Append(102).ok());
        EXPECT_TRUE(value_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(22).ok());
        EXPECT_TRUE(b_builder->AppendNull().ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(103).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(104).ok());
        EXPECT_TRUE(value_builder->Append().ok());
        EXPECT_TRUE(a_builder->Append(24).ok());
        EXPECT_TRUE(b_builder->Append("me").ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int_list_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::Int32Builder>();
        auto list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                                 value_builder, list_type);
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", list_type, true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, list_builder,
                                  map_type);

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(201).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(1).ok());
        EXPECT_TRUE(value_builder->Append(2).ok());
        EXPECT_TRUE(key_builder->Append(202).ok());
        EXPECT_TRUE(list_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(203).ok());
        EXPECT_TRUE(list_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(204).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(value_builder->Append(3).ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(205).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(4).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_time32_array(const std::shared_ptr<arrow::DataType>& type,
                                                     const std::vector<int32_t>& values) {
        arrow::Time32Builder builder(type, arrow::default_memory_pool());
        for (const auto value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_time64_array(const std::shared_ptr<arrow::DataType>& type,
                                                     const std::vector<int64_t>& values) {
        arrow::Time64Builder builder(type, arrow::default_memory_pool());
        for (const auto value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_timestamp_array(
            const std::shared_ptr<arrow::DataType>& type, const std::vector<int64_t>& values) {
        arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
        for (const auto value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_decimal_array(const std::shared_ptr<arrow::DataType>& type,
                                                      const std::vector<int64_t>& values) {
        arrow::Decimal128Builder builder(type, arrow::default_memory_pool());
        for (const auto value : values) {
            EXPECT_TRUE(builder.Append(arrow::Decimal128(value)).ok());
        }
        return finish_array(&builder);
    }

    void add_field(const std::shared_ptr<arrow::Field>& field, std::shared_ptr<arrow::Array> array,
                   std::function<void(const ParquetColumnSchema&, const IColumn&)> validator) {
        _arrow_fields.push_back(field);
        _arrays.push_back(std::move(array));
        _expected_by_field.push_back(std::move(validator));
    }

    void write_parquet_file() {
        add_field(
                arrow::field("bool_col", arrow::boolean(), false),
                build_required_array<arrow::BooleanBuilder, bool>({true, false, true, false, true}),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::BOOLEAN);
                    const auto& values = assert_cast<const ColumnBool&>(column);
                    EXPECT_EQ(values.get_element(0), 1);
                    EXPECT_EQ(values.get_element(1), 0);
                    EXPECT_EQ(values.get_element(4), 1);
                });
        add_field(arrow::field("int32_col", arrow::int32(), false),
                  build_required_array<arrow::Int32Builder, int32_t>({10, 20, 30, 40, 50}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
                      const auto& values = assert_cast<const ColumnInt32&>(column);
                      EXPECT_EQ(values.get_element(0), 10);
                      EXPECT_EQ(values.get_element(4), 50);
                  });
        add_field(arrow::field("int64_col", arrow::int64(), false),
                  build_required_array<arrow::Int64Builder, int64_t>(
                          {10000000000L, -9L, 42L, 77L, 123L}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      const auto& values = assert_cast<const ColumnInt64&>(column);
                      EXPECT_EQ(values.get_element(0), 10000000000L);
                      EXPECT_EQ(values.get_element(1), -9L);
                  });
        add_field(
                arrow::field("float_col", arrow::float32(), false),
                build_required_array<arrow::FloatBuilder, float>({1.5F, -2.25F, 3.0F, 4.5F, 5.75F}),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FLOAT);
                    const auto& values = assert_cast<const ColumnFloat32&>(column);
                    EXPECT_FLOAT_EQ(values.get_element(0), 1.5F);
                    EXPECT_FLOAT_EQ(values.get_element(1), -2.25F);
                });
        add_field(arrow::field("double_col", arrow::float64(), false),
                  build_required_array<arrow::DoubleBuilder, double>({3.5, -4.75, 6.0, 7.25, 8.5}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::DOUBLE);
                      const auto& values = assert_cast<const ColumnFloat64&>(column);
                      EXPECT_DOUBLE_EQ(values.get_element(0), 3.5);
                      EXPECT_DOUBLE_EQ(values.get_element(1), -4.75);
                  });
        add_field(arrow::field("binary_col", arrow::binary(), false),
                  build_binary_array({"bin_a", "bin_b", "bin_c", "bin_d", "bin_e"}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::BYTE_ARRAY);
                      const auto& values = assert_cast<const ColumnString&>(column);
                      EXPECT_EQ(values.get_data_at(0).to_string(), "bin_a");
                      EXPECT_EQ(values.get_data_at(3).to_string(), "bin_d");
                  });
        add_field(arrow::field("string_col", arrow::utf8(), false),
                  build_string_array({"alpha", "beta", "gamma", "delta", "epsilon"}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type_descriptor.is_string_like);
                      const auto& values = assert_cast<const ColumnString&>(column);
                      EXPECT_EQ(values.get_data_at(0).to_string(), "alpha");
                      EXPECT_EQ(values.get_data_at(4).to_string(), "epsilon");
                  });
        add_field(arrow::field("fixed_binary_col", arrow::fixed_size_binary(4), false),
                  build_fixed_binary_array(arrow::fixed_size_binary(4),
                                           {"aaaa", "bbbb", "cccc", "dddd", "eeee"}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type,
                                ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
                      EXPECT_EQ(schema.type_descriptor.fixed_length, 4);
                      const auto& values = assert_cast<const ColumnString&>(column);
                      EXPECT_EQ(values.get_data_at(0).to_string(), "aaaa");
                      EXPECT_EQ(values.get_data_at(2).to_string(), "cccc");
                  });
        add_field(arrow::field("date_col", arrow::date32(), false),
                  build_required_array<arrow::Date32Builder, int32_t>({0, 1, 18628, 18629, 18630}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DATEV2);
                      EXPECT_EQ(schema.type->to_string(column, 0), "1970-01-01");
                      EXPECT_EQ(schema.type->to_string(column, 2), "2021-01-01");
                  });
        add_field(arrow::field("time_millis_col", arrow::time32(arrow::TimeUnit::MILLI), false),
                  build_time32_array(arrow::time32(arrow::TimeUnit::MILLI),
                                     {0, 1000, 3723004, 43200000, 86399000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_TIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1), "00:00:01.000");
                      EXPECT_EQ(schema.type->to_string(column, 2), "01:02:03.004");
                  });
        add_field(arrow::field("time_micros_col", arrow::time64(arrow::TimeUnit::MICRO), false),
                  build_time64_array(arrow::time64(arrow::TimeUnit::MICRO),
                                     {0, 1000000, 3723004567, 43200000000, 86399000000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_TIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1), "00:00:01.000000");
                      EXPECT_EQ(schema.type->to_string(column, 2), "01:02:03.004567");
                  });
        add_field(arrow::field("timestamp_millis_col", arrow::timestamp(arrow::TimeUnit::MILLI),
                               false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MILLI),
                                        {0, 1234, 1609459200000, 1609459201000, -1}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(),
                                TYPE_DATETIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1), "1970-01-01 00:00:01.234");
                      EXPECT_EQ(schema.type->to_string(column, 4), "1969-12-31 23:59:59.999");
                  });
        add_field(arrow::field("timestamp_micros_col", arrow::timestamp(arrow::TimeUnit::MICRO),
                               false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO),
                                        {0, 1234567, 1609459200000000, 1609459201000000, -1}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(),
                                TYPE_DATETIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1), "1970-01-01 00:00:01.234567");
                      EXPECT_EQ(schema.type->to_string(column, 4), "1969-12-31 23:59:59.999999");
                  });
        add_field(arrow::field("decimal_fixed_binary_9_2_col", arrow::decimal128(9, 2), false),
                  build_decimal_array(arrow::decimal128(9, 2), {12345, -67, 0, 987, 1000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type,
                                ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
                      EXPECT_TRUE(schema.type_descriptor.is_decimal);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DECIMAL32);
                      const auto& values = assert_cast<const ColumnDecimal32&>(column);
                      EXPECT_EQ(values.get_element(0), Decimal32(12345));
                      EXPECT_EQ(schema.type->to_string(column, 0), "123.45");
                  });
        add_field(arrow::field("decimal_fixed_binary_18_6_col", arrow::decimal128(18, 6), false),
                  build_decimal_array(arrow::decimal128(18, 6),
                                      {1234567, -670000, 0, 9870000, 1000000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type,
                                ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
                      EXPECT_TRUE(schema.type_descriptor.is_decimal);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DECIMAL64);
                      const auto& values = assert_cast<const ColumnDecimal64&>(column);
                      EXPECT_EQ(values.get_element(0), Decimal64(1234567));
                      EXPECT_EQ(schema.type->to_string(column, 0), "1.234567");
                  });
        add_field(arrow::field("nullable_int_col", arrow::int32(), true),
                  build_nullable_int32_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      const auto& nested_column =
                              assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_TRUE(nullable_column.is_null_at(3));
                      EXPECT_EQ(nested_column.get_element(0), 1);
                      EXPECT_EQ(nested_column.get_element(2), 3);
                  });
        add_field(arrow::field("struct_col",
                               arrow::struct_({
                                       arrow::field("a", arrow::int32(), false),
                                       arrow::field("b", arrow::utf8(), false),
                               }),
                               false),
                  build_required_struct_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_STRUCT);
                      const auto& struct_column = assert_cast<const ColumnStruct&>(column);
                      ASSERT_EQ(struct_column.get_columns().size(), 2);
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      const auto& b_values =
                              assert_cast<const ColumnString&>(struct_column.get_column(1));
                      EXPECT_EQ(a_values.get_element(0), 101);
                      EXPECT_EQ(a_values.get_element(4), 105);
                      EXPECT_EQ(b_values.get_data_at(1).to_string(), "sb");
                      EXPECT_EQ(b_values.get_data_at(4).to_string(), "se");
                  });
        add_field(arrow::field("nullable_struct_col",
                               arrow::struct_({
                                       arrow::field("a", arrow::int32(), false),
                                       arrow::field("b", arrow::utf8(), true),
                               }),
                               true),
                  build_nullable_struct_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_TRUE(nullable_column.is_null_at(4));

                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
                      ASSERT_EQ(struct_column.get_columns().size(), 2);
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      const auto& b_values =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      const auto& b_nested =
                              assert_cast<const ColumnString&>(b_values.get_nested_column());
                      EXPECT_EQ(a_values.get_element(0), 201);
                      EXPECT_EQ(a_values.get_element(2), 203);
                      EXPECT_EQ(a_values.get_element(3), 204);
                      EXPECT_FALSE(b_values.is_null_at(0));
                      EXPECT_TRUE(b_values.is_null_at(2));
                      EXPECT_FALSE(b_values.is_null_at(3));
                      EXPECT_EQ(b_nested.get_data_at(0).to_string(), "nsa");
                      EXPECT_EQ(b_nested.get_data_at(3).to_string(), "nsd");
                  });
        auto struct_list_type = arrow::struct_({
                arrow::field("a", arrow::int32(), false),
                arrow::field("xs", arrow::list(arrow::field("element", arrow::int32(), true)),
                             true),
        });
        add_field(arrow::field("nullable_struct_list_col", struct_list_type, true),
                  build_nullable_struct_with_list_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
                      ASSERT_EQ(struct_column.get_columns().size(), 2);
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      EXPECT_EQ(a_values.get_element(0), 301);
                      EXPECT_EQ(a_values.get_element(2), 303);
                      EXPECT_EQ(a_values.get_element(3), 304);
                      EXPECT_EQ(a_values.get_element(4), 305);

                      const auto& xs_nullable =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      ASSERT_EQ(xs_nullable.size(), ROW_COUNT);
                      EXPECT_FALSE(xs_nullable.is_null_at(0));
                      EXPECT_FALSE(xs_nullable.is_null_at(2));
                      EXPECT_TRUE(xs_nullable.is_null_at(3));
                      EXPECT_FALSE(xs_nullable.is_null_at(4));
                      const auto& xs_array =
                              assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
                      const auto& offsets = xs_array.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 2);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 2);
                      EXPECT_EQ(offsets[3], 2);
                      EXPECT_EQ(offsets[4], 4);
                      const auto& elements =
                              assert_cast<const ColumnNullable&>(xs_array.get_data());
                      ASSERT_EQ(elements.size(), 4);
                      EXPECT_FALSE(elements.is_null_at(0));
                      EXPECT_FALSE(elements.is_null_at(1));
                      EXPECT_TRUE(elements.is_null_at(2));
                      EXPECT_FALSE(elements.is_null_at(3));
                      const auto& values =
                              assert_cast<const ColumnInt32&>(elements.get_nested_column());
                      EXPECT_EQ(values.get_element(0), 1);
                      EXPECT_EQ(values.get_element(1), 2);
                      EXPECT_EQ(values.get_element(3), 5);
                  });
        auto struct_map_type = arrow::struct_({
                arrow::field("a", arrow::int32(), false),
                arrow::field("kv",
                             arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true)),
                             true),
        });
        add_field(arrow::field("nullable_struct_map_col", struct_map_type, true),
                  build_nullable_struct_with_map_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
                      ASSERT_EQ(struct_column.get_columns().size(), 2);
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      EXPECT_EQ(a_values.get_element(0), 401);
                      EXPECT_EQ(a_values.get_element(2), 403);
                      EXPECT_EQ(a_values.get_element(3), 404);
                      EXPECT_EQ(a_values.get_element(4), 405);

                      const auto& kv_nullable =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      ASSERT_EQ(kv_nullable.size(), ROW_COUNT);
                      EXPECT_FALSE(kv_nullable.is_null_at(0));
                      EXPECT_FALSE(kv_nullable.is_null_at(2));
                      EXPECT_TRUE(kv_nullable.is_null_at(3));
                      EXPECT_FALSE(kv_nullable.is_null_at(4));
                      const auto& kv_map =
                              assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
                      const auto& offsets = kv_map.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 2);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 2);
                      EXPECT_EQ(offsets[3], 2);
                      EXPECT_EQ(offsets[4], 3);
                      const auto& keys = assert_cast<const ColumnInt32&>(kv_map.get_keys());
                      const auto& values = assert_cast<const ColumnNullable&>(kv_map.get_values());
                      const auto& value_data =
                              assert_cast<const ColumnString&>(values.get_nested_column());
                      ASSERT_EQ(keys.size(), 3);
                      ASSERT_EQ(values.size(), 3);
                      EXPECT_EQ(keys.get_element(0), 1);
                      EXPECT_EQ(keys.get_element(1), 2);
                      EXPECT_EQ(keys.get_element(2), 5);
                      EXPECT_EQ(value_data.get_data_at(0).to_string(), "one");
                      EXPECT_TRUE(values.is_null_at(1));
                      EXPECT_EQ(value_data.get_data_at(2).to_string(), "five");
                  });
        add_field(arrow::field("list_int_col",
                               arrow::list(arrow::field("element", arrow::int32(), false)), false),
                  build_required_int_list_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_ARRAY);
                      const auto* array_type =
                              assert_cast<const DataTypeArray*>(remove_nullable(schema.type).get());
                      EXPECT_EQ(
                              remove_nullable(array_type->get_nested_type())->get_primitive_type(),
                              TYPE_INT);
                      const auto& array_column = assert_cast<const ColumnArray&>(column);
                      ASSERT_EQ(array_column.size(), ROW_COUNT);
                      const auto array_size_at = [&array_column](size_t row_idx) {
                          return array_column.get_offsets()[row_idx] -
                                 (row_idx == 0 ? 0 : array_column.get_offsets()[row_idx - 1]);
                      };
                      EXPECT_EQ(array_size_at(0), 2);
                      EXPECT_EQ(array_size_at(1), 1);
                      EXPECT_EQ(array_size_at(2), 3);
                      EXPECT_EQ(array_size_at(4), 2);
                      const auto& values = assert_cast<const ColumnInt32&>(array_column.get_data());
                      ASSERT_EQ(values.size(), 9);
                      EXPECT_EQ(values.get_element(0), 1);
                      EXPECT_EQ(values.get_element(5), 6);
                      EXPECT_EQ(values.get_element(8), 9);
                  });
        add_field(arrow::field("nullable_list_int_col",
                               arrow::list(arrow::field("element", arrow::int32(), true)), true),
                  build_nullable_int_list_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      const auto& array_column =
                              assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
                      const auto& offsets = array_column.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 2);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 2);
                      EXPECT_EQ(offsets[3], 4);
                      EXPECT_EQ(offsets[4], 5);
                      const auto& elements =
                              assert_cast<const ColumnNullable&>(array_column.get_data());
                      const auto& values =
                              assert_cast<const ColumnInt32&>(elements.get_nested_column());
                      ASSERT_EQ(elements.size(), 5);
                      EXPECT_EQ(values.get_element(0), 10);
                      EXPECT_EQ(values.get_element(1), 20);
                      EXPECT_TRUE(elements.is_null_at(2));
                      EXPECT_EQ(values.get_element(3), 30);
                      EXPECT_EQ(values.get_element(4), 40);
                  });
        add_field(arrow::field("required_nullable_list_int_col",
                               arrow::list(arrow::field("element", arrow::int32(), true)), false),
                  build_required_nullable_int_list_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_FALSE(schema.type->is_nullable());
                      const auto& array_column = assert_cast<const ColumnArray&>(column);
                      const auto& offsets = array_column.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 0);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 3);
                      EXPECT_EQ(offsets[3], 5);
                      EXPECT_EQ(offsets[4], 5);
                      const auto& elements =
                              assert_cast<const ColumnNullable&>(array_column.get_data());
                      ASSERT_EQ(elements.size(), 5);
                      EXPECT_TRUE(elements.is_null_at(0));
                      EXPECT_FALSE(elements.is_null_at(1));
                      EXPECT_TRUE(elements.is_null_at(4));
                  });
        auto list_struct_type = arrow::struct_({
                arrow::field("a", arrow::int32(), false),
                arrow::field("b", arrow::utf8(), true),
        });
        add_field(arrow::field("nullable_list_struct_col",
                               arrow::list(arrow::field("element", list_struct_type, true)), true),
                  build_nullable_struct_list_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& array_column =
                              assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
                      const auto& offsets = array_column.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 2);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 2);
                      EXPECT_EQ(offsets[3], 4);
                      EXPECT_EQ(offsets[4], 5);

                      const auto& elements =
                              assert_cast<const ColumnNullable&>(array_column.get_data());
                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(elements.get_nested_column());
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      const auto& b_values =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      const auto& b_data =
                              assert_cast<const ColumnString&>(b_values.get_nested_column());
                      ASSERT_EQ(elements.size(), 5);
                      EXPECT_FALSE(elements.is_null_at(0));
                      EXPECT_FALSE(elements.is_null_at(1));
                      EXPECT_TRUE(elements.is_null_at(2));
                      EXPECT_FALSE(elements.is_null_at(3));
                      EXPECT_EQ(a_values.get_element(0), 11);
                      EXPECT_EQ(a_values.get_element(1), 12);
                      EXPECT_EQ(a_values.get_element(3), 13);
                      EXPECT_EQ(a_values.get_element(4), 14);
                      EXPECT_EQ(b_data.get_data_at(0).to_string(), "la");
                      EXPECT_TRUE(b_values.is_null_at(1));
                      EXPECT_EQ(b_data.get_data_at(3).to_string(), "ld");
                      EXPECT_EQ(b_data.get_data_at(4).to_string(), "le");
                  });
        auto nested_list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        add_field(arrow::field("nullable_list_list_int_col",
                               arrow::list(arrow::field("element", nested_list_type, true)), true),
                  build_nullable_list_list_int_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& outer_array =
                              assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
                      const auto& outer_offsets = outer_array.get_offsets();
                      ASSERT_EQ(outer_offsets.size(), ROW_COUNT);
                      EXPECT_EQ(outer_offsets[0], 4);
                      EXPECT_EQ(outer_offsets[1], 4);
                      EXPECT_EQ(outer_offsets[2], 4);
                      EXPECT_EQ(outer_offsets[3], 5);
                      EXPECT_EQ(outer_offsets[4], 7);

                      const auto& inner_nullable =
                              assert_cast<const ColumnNullable&>(outer_array.get_data());
                      ASSERT_EQ(inner_nullable.size(), 7);
                      EXPECT_FALSE(inner_nullable.is_null_at(0));
                      EXPECT_FALSE(inner_nullable.is_null_at(1));
                      EXPECT_TRUE(inner_nullable.is_null_at(2));
                      EXPECT_FALSE(inner_nullable.is_null_at(3));
                      EXPECT_FALSE(inner_nullable.is_null_at(6));

                      const auto& inner_array =
                              assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
                      const auto& inner_offsets = inner_array.get_offsets();
                      ASSERT_EQ(inner_offsets.size(), 7);
                      EXPECT_EQ(inner_offsets[0], 2);
                      EXPECT_EQ(inner_offsets[1], 2);
                      EXPECT_EQ(inner_offsets[2], 2);
                      EXPECT_EQ(inner_offsets[3], 4);
                      EXPECT_EQ(inner_offsets[4], 5);
                      EXPECT_EQ(inner_offsets[5], 5);
                      EXPECT_EQ(inner_offsets[6], 7);

                      const auto& elements =
                              assert_cast<const ColumnNullable&>(inner_array.get_data());
                      const auto& values =
                              assert_cast<const ColumnInt32&>(elements.get_nested_column());
                      ASSERT_EQ(elements.size(), 7);
                      EXPECT_EQ(values.get_element(0), 1);
                      EXPECT_EQ(values.get_element(1), 2);
                      EXPECT_TRUE(elements.is_null_at(2));
                      EXPECT_EQ(values.get_element(3), 3);
                      EXPECT_EQ(values.get_element(4), 4);
                      EXPECT_EQ(values.get_element(5), 5);
                      EXPECT_TRUE(elements.is_null_at(6));
                  });
        add_field(arrow::field(
                          "map_int_string_col",
                          arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), false)),
                          false),
                  build_required_int_string_map_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_MAP);
                      const auto* map_type =
                              assert_cast<const DataTypeMap*>(remove_nullable(schema.type).get());
                      EXPECT_EQ(remove_nullable(map_type->get_key_type())->get_primitive_type(),
                                TYPE_INT);
                      EXPECT_EQ(remove_nullable(map_type->get_value_type())->get_primitive_type(),
                                TYPE_STRING);
                      const auto& map_column = assert_cast<const ColumnMap&>(column);
                      ASSERT_EQ(map_column.size(), ROW_COUNT);
                      const auto map_size_at = [&map_column](size_t row_idx) {
                          return map_column.get_offsets()[row_idx] -
                                 (row_idx == 0 ? 0 : map_column.get_offsets()[row_idx - 1]);
                      };
                      EXPECT_EQ(map_size_at(0), 2);
                      EXPECT_EQ(map_size_at(1), 1);
                      EXPECT_EQ(map_size_at(2), 3);
                      EXPECT_EQ(map_size_at(4), 2);
                      const auto& keys = assert_cast<const ColumnInt32&>(map_column.get_keys());
                      const auto& values =
                              assert_cast<const ColumnString&>(map_column.get_values());
                      ASSERT_EQ(keys.size(), 9);
                      ASSERT_EQ(values.size(), 9);
                      EXPECT_EQ(keys.get_element(0), 1);
                      EXPECT_EQ(keys.get_element(5), 6);
                      EXPECT_EQ(keys.get_element(8), 9);
                      EXPECT_EQ(values.get_data_at(0).to_string(), "a");
                      EXPECT_EQ(values.get_data_at(5).to_string(), "f");
                      EXPECT_EQ(values.get_data_at(8).to_string(), "i");
                  });
        add_field(
                arrow::field("nullable_map_int_string_col",
                             arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true)),
                             true),
                build_nullable_int_string_map_array(),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_TRUE(schema.type->is_nullable());
                    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                    EXPECT_FALSE(nullable_column.is_null_at(0));
                    EXPECT_TRUE(nullable_column.is_null_at(1));
                    EXPECT_FALSE(nullable_column.is_null_at(2));
                    const auto& map_column =
                            assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
                    const auto& offsets = map_column.get_offsets();
                    ASSERT_EQ(offsets.size(), ROW_COUNT);
                    EXPECT_EQ(offsets[0], 2);
                    EXPECT_EQ(offsets[1], 2);
                    EXPECT_EQ(offsets[2], 2);
                    EXPECT_EQ(offsets[3], 3);
                    EXPECT_EQ(offsets[4], 4);
                    const auto& keys = assert_cast<const ColumnInt32&>(map_column.get_keys());
                    const auto& values =
                            assert_cast<const ColumnNullable&>(map_column.get_values());
                    const auto& value_data =
                            assert_cast<const ColumnString&>(values.get_nested_column());
                    ASSERT_EQ(keys.size(), 4);
                    EXPECT_EQ(keys.get_element(0), 10);
                    EXPECT_EQ(keys.get_element(1), 20);
                    EXPECT_EQ(keys.get_element(3), 40);
                    EXPECT_EQ(value_data.get_data_at(0).to_string(), "aa");
                    EXPECT_TRUE(values.is_null_at(1));
                    EXPECT_EQ(value_data.get_data_at(2).to_string(), "cc");
                    EXPECT_TRUE(values.is_null_at(3));
                });
        add_field(
                arrow::field("required_nullable_map_int_string_col",
                             arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true)),
                             false),
                build_required_nullable_string_map_array(),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_FALSE(schema.type->is_nullable());
                    const auto& map_column = assert_cast<const ColumnMap&>(column);
                    const auto& offsets = map_column.get_offsets();
                    ASSERT_EQ(offsets.size(), ROW_COUNT);
                    EXPECT_EQ(offsets[0], 0);
                    EXPECT_EQ(offsets[1], 2);
                    EXPECT_EQ(offsets[2], 3);
                    EXPECT_EQ(offsets[3], 3);
                    EXPECT_EQ(offsets[4], 4);
                    const auto& values =
                            assert_cast<const ColumnNullable&>(map_column.get_values());
                    ASSERT_EQ(values.size(), 4);
                    EXPECT_TRUE(values.is_null_at(0));
                    EXPECT_FALSE(values.is_null_at(1));
                    EXPECT_TRUE(values.is_null_at(3));
                });
        auto map_struct_type = arrow::struct_({
                arrow::field("a", arrow::int32(), false),
                arrow::field("b", arrow::utf8(), true),
        });
        add_field(arrow::field(
                          "nullable_map_int_struct_col",
                          arrow::map(arrow::int32(), arrow::field("value", map_struct_type, true)),
                          true),
                  build_nullable_int_struct_map_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& map_column =
                              assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
                      const auto& offsets = map_column.get_offsets();
                      ASSERT_EQ(offsets.size(), ROW_COUNT);
                      EXPECT_EQ(offsets[0], 2);
                      EXPECT_EQ(offsets[1], 2);
                      EXPECT_EQ(offsets[2], 2);
                      EXPECT_EQ(offsets[3], 3);
                      EXPECT_EQ(offsets[4], 4);

                      const auto& keys = assert_cast<const ColumnInt32&>(map_column.get_keys());
                      const auto& values =
                              assert_cast<const ColumnNullable&>(map_column.get_values());
                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(values.get_nested_column());
                      const auto& a_values =
                              assert_cast<const ColumnInt32&>(struct_column.get_column(0));
                      const auto& b_values =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      const auto& b_data =
                              assert_cast<const ColumnString&>(b_values.get_nested_column());
                      ASSERT_EQ(keys.size(), 4);
                      ASSERT_EQ(values.size(), 4);
                      EXPECT_EQ(keys.get_element(0), 101);
                      EXPECT_EQ(keys.get_element(1), 102);
                      EXPECT_EQ(keys.get_element(3), 104);
                      EXPECT_FALSE(values.is_null_at(0));
                      EXPECT_FALSE(values.is_null_at(1));
                      EXPECT_TRUE(values.is_null_at(2));
                      EXPECT_FALSE(values.is_null_at(3));
                      EXPECT_EQ(a_values.get_element(0), 21);
                      EXPECT_EQ(a_values.get_element(1), 22);
                      EXPECT_EQ(a_values.get_element(3), 24);
                      EXPECT_EQ(b_data.get_data_at(0).to_string(), "ma");
                      EXPECT_TRUE(b_values.is_null_at(1));
                      EXPECT_EQ(b_data.get_data_at(3).to_string(), "me");
                  });
        auto map_list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        add_field(
                arrow::field("nullable_map_int_list_col",
                             arrow::map(arrow::int32(), arrow::field("value", map_list_type, true)),
                             true),
                build_nullable_int_list_map_array(),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_TRUE(schema.type->is_nullable());
                    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                    EXPECT_FALSE(nullable_column.is_null_at(0));
                    EXPECT_TRUE(nullable_column.is_null_at(1));
                    EXPECT_FALSE(nullable_column.is_null_at(2));
                    EXPECT_FALSE(nullable_column.is_null_at(3));
                    EXPECT_FALSE(nullable_column.is_null_at(4));

                    const auto& map_column =
                            assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
                    const auto& map_offsets = map_column.get_offsets();
                    ASSERT_EQ(map_offsets.size(), ROW_COUNT);
                    EXPECT_EQ(map_offsets[0], 2);
                    EXPECT_EQ(map_offsets[1], 2);
                    EXPECT_EQ(map_offsets[2], 2);
                    EXPECT_EQ(map_offsets[3], 4);
                    EXPECT_EQ(map_offsets[4], 5);

                    const auto& keys = assert_cast<const ColumnInt32&>(map_column.get_keys());
                    ASSERT_EQ(keys.size(), 5);
                    EXPECT_EQ(keys.get_element(0), 201);
                    EXPECT_EQ(keys.get_element(1), 202);
                    EXPECT_EQ(keys.get_element(2), 203);
                    EXPECT_EQ(keys.get_element(3), 204);
                    EXPECT_EQ(keys.get_element(4), 205);

                    const auto& values =
                            assert_cast<const ColumnNullable&>(map_column.get_values());
                    ASSERT_EQ(values.size(), 5);
                    EXPECT_FALSE(values.is_null_at(0));
                    EXPECT_FALSE(values.is_null_at(1));
                    EXPECT_TRUE(values.is_null_at(2));
                    EXPECT_FALSE(values.is_null_at(3));
                    EXPECT_FALSE(values.is_null_at(4));

                    const auto& list_column =
                            assert_cast<const ColumnArray&>(values.get_nested_column());
                    const auto& list_offsets = list_column.get_offsets();
                    ASSERT_EQ(list_offsets.size(), 5);
                    EXPECT_EQ(list_offsets[0], 2);
                    EXPECT_EQ(list_offsets[1], 2);
                    EXPECT_EQ(list_offsets[2], 2);
                    EXPECT_EQ(list_offsets[3], 4);
                    EXPECT_EQ(list_offsets[4], 5);

                    const auto& elements =
                            assert_cast<const ColumnNullable&>(list_column.get_data());
                    const auto& element_values =
                            assert_cast<const ColumnInt32&>(elements.get_nested_column());
                    ASSERT_EQ(elements.size(), 5);
                    EXPECT_EQ(element_values.get_element(0), 1);
                    EXPECT_EQ(element_values.get_element(1), 2);
                    EXPECT_TRUE(elements.is_null_at(2));
                    EXPECT_EQ(element_values.get_element(3), 3);
                    EXPECT_EQ(element_values.get_element(4), 4);
                });

        auto schema = arrow::schema(_arrow_fields);
        auto table = arrow::Table::Make(schema, _arrays);

        auto file_result = arrow::io::FileOutputStream::Open(_file_path);
        ASSERT_TRUE(file_result.ok()) << file_result.status();
        std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

        ::parquet::WriterProperties::Builder builder;
        builder.version(::parquet::ParquetVersion::PARQUET_2_6);
        builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
        builder.compression(::parquet::Compression::UNCOMPRESSED);
        PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                          ROW_COUNT, builder.build()));
    }

    std::unique_ptr<ParquetColumnReader> create_reader(size_t field_idx) const {
        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(*_fields[field_idx], &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    std::unique_ptr<ParquetColumnReader> create_projected_child_reader(size_t field_idx,
                                                                       size_t child_idx) const {
        const auto& struct_schema = *_fields[field_idx];
        EXPECT_LT(child_idx, struct_schema.children.size());

        reader::LocalColumnIndex projection;
        projection.index = struct_schema.field_id;
        projection.project_all_children = false;
        reader::LocalColumnIndex child_projection;
        child_projection.index = struct_schema.children[child_idx]->field_id;
        projection.children.push_back(std::move(child_projection));

        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(struct_schema, &projection, &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    void read_and_validate(size_t field_idx) const {
        auto reader = create_reader(field_idx);
        ASSERT_NE(reader, nullptr);
        MutableColumnPtr column = reader->type()->create_column();
        int64_t rows_read = 0;
        auto st = reader->read(ROW_COUNT, column, &rows_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(rows_read, ROW_COUNT);
        ASSERT_EQ(column->size(), ROW_COUNT);
        _expected_by_field[field_idx](*_fields[field_idx], *column);
    }

    size_t find_field_idx(const std::string& name) const {
        for (size_t field_idx = 0; field_idx < _fields.size(); ++field_idx) {
            if (_fields[field_idx]->name == name) {
                return field_idx;
            }
        }
        ADD_FAILURE() << "Cannot find parquet test field " << name;
        return _fields.size();
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
    std::unique_ptr<::parquet::ParquetFileReader> _file_reader;
    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    std::vector<std::unique_ptr<ParquetColumnSchema>> _fields;
    std::vector<std::shared_ptr<arrow::Field>> _arrow_fields;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    std::vector<std::function<void(const ParquetColumnSchema&, const IColumn&)>> _expected_by_field;
};

TEST_F(ParquetColumnReaderTest, ReadAllSupportedPhysicalAndLogicalTypes) {
    for (size_t field_idx = 0; field_idx < _fields.size(); ++field_idx) {
        SCOPED_TRACE(_fields[field_idx]->name);
        if (_fields[field_idx]->kind != ParquetColumnSchemaKind::PRIMITIVE) {
            continue;
        }
        ASSERT_TRUE(supports_record_reader(_fields[field_idx]->type_descriptor));
        read_and_validate(field_idx);
    }
}

TEST_F(ParquetColumnReaderTest, ReadSupportedComplexTypes) {
    read_and_validate(find_field_idx("struct_col"));
    read_and_validate(find_field_idx("nullable_struct_col"));
    read_and_validate(find_field_idx("list_int_col"));
    read_and_validate(find_field_idx("nullable_list_int_col"));
    read_and_validate(find_field_idx("required_nullable_list_int_col"));
    read_and_validate(find_field_idx("nullable_list_struct_col"));
    read_and_validate(find_field_idx("nullable_list_list_int_col"));
    read_and_validate(find_field_idx("map_int_string_col"));
    read_and_validate(find_field_idx("nullable_map_int_string_col"));
    read_and_validate(find_field_idx("required_nullable_map_int_string_col"));
    read_and_validate(find_field_idx("nullable_map_int_struct_col"));
    read_and_validate(find_field_idx("nullable_map_int_list_col"));
}

TEST_F(ParquetColumnReaderTest, SkipThenRead) {
    auto reader = create_reader(1);
    auto st = reader->skip(2);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);

    const auto& int_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(int_values.size(), 2);
    EXPECT_EQ(int_values.get_element(0), 30);
    EXPECT_EQ(int_values.get_element(1), 40);
}

TEST_F(ParquetColumnReaderTest, SelectReadsOnlySelectedRanges) {
    auto reader = create_reader(1);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& int_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(int_values.size(), 3);
    EXPECT_EQ(int_values.get_element(0), 10);
    EXPECT_EQ(int_values.get_element(1), 30);
    EXPECT_EQ(int_values.get_element(2), 50);
}

TEST_F(ParquetColumnReaderTest, ReadProjectedStructChildren) {
    const auto field_idx = find_field_idx("struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "struct_col");
    ASSERT_EQ(struct_schema.children.size(), 2);

    reader::LocalColumnIndex projection;
    projection.index = struct_schema.field_id;
    projection.project_all_children = false;
    reader::LocalColumnIndex child_projection;
    child_projection.index = struct_schema.children[1]->field_id;
    projection.children.push_back(std::move(child_projection));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    auto st = factory.create(struct_schema, &projection, &reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(remove_nullable(reader->type())->get_primitive_type(), TYPE_STRUCT);
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(reader->type()).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);
    const auto& struct_column = assert_cast<const ColumnStruct&>(*column);
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& values = assert_cast<const ColumnString&>(struct_column.get_column(0));
    EXPECT_EQ(values.get_data_at(0).to_string(), "sa");
    EXPECT_EQ(values.get_data_at(4).to_string(), "se");
}

TEST_F(ParquetColumnReaderTest, ReadProjectedNullableStructChildren) {
    const auto field_idx = find_field_idx("nullable_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "nullable_struct_col");
    ASSERT_EQ(struct_schema.children.size(), 2);

    reader::LocalColumnIndex projection;
    projection.index = struct_schema.field_id;
    projection.project_all_children = false;
    reader::LocalColumnIndex child_projection;
    child_projection.index = struct_schema.children[1]->field_id;
    projection.children.push_back(std::move(child_projection));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    auto st = factory.create(struct_schema, &projection, &reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(reader->type()->is_nullable());
    ASSERT_EQ(remove_nullable(reader->type())->get_primitive_type(), TYPE_STRUCT);
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(reader->type()).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_TRUE(nullable_column.is_null_at(4));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& values = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    const auto& nested_values = assert_cast<const ColumnString&>(values.get_nested_column());
    EXPECT_FALSE(values.is_null_at(0));
    EXPECT_TRUE(values.is_null_at(2));
    EXPECT_FALSE(values.is_null_at(3));
    EXPECT_EQ(nested_values.get_data_at(0).to_string(), "nsa");
    EXPECT_EQ(nested_values.get_data_at(3).to_string(), "nsd");
}

TEST_F(ParquetColumnReaderTest, ReadProjectedListStructElementChildren) {
    const auto field_idx = find_field_idx("nullable_list_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& list_schema = *_fields[field_idx];
    ASSERT_EQ(list_schema.name, "nullable_list_struct_col");
    ASSERT_EQ(list_schema.children.size(), 1);
    const auto& element_schema = *list_schema.children[0];
    ASSERT_EQ(element_schema.children.size(), 2);

    reader::LocalColumnIndex projection;
    projection.index = list_schema.field_id;
    projection.project_all_children = false;
    reader::LocalColumnIndex element_projection;
    element_projection.index = element_schema.field_id;
    element_projection.project_all_children = false;
    reader::LocalColumnIndex child_projection;
    child_projection.index = element_schema.children[1]->field_id;
    element_projection.children.push_back(std::move(child_projection));
    projection.children.push_back(std::move(element_projection));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    auto st = factory.create(list_schema, &projection, &reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(reader->type()->is_nullable());
    const auto* array_type =
            assert_cast<const DataTypeArray*>(remove_nullable(reader->type()).get());
    const auto* element_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(array_type->get_nested_type()).get());
    ASSERT_EQ(element_type->get_elements().size(), 1);
    EXPECT_EQ(element_type->get_element_name(0), "b");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& elements = assert_cast<const ColumnNullable&>(array_column.get_data());
    const auto& struct_column = assert_cast<const ColumnStruct&>(elements.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& b_values = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    const auto& b_data = assert_cast<const ColumnString&>(b_values.get_nested_column());
    ASSERT_EQ(elements.size(), 5);
    EXPECT_EQ(b_data.get_data_at(0).to_string(), "la");
    EXPECT_TRUE(b_values.is_null_at(1));
    EXPECT_TRUE(elements.is_null_at(2));
    EXPECT_EQ(b_data.get_data_at(3).to_string(), "ld");
    EXPECT_EQ(b_data.get_data_at(4).to_string(), "le");
}

TEST_F(ParquetColumnReaderTest, ReadProjectedMapStructValueChildren) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& map_schema = *_fields[field_idx];
    ASSERT_EQ(map_schema.name, "nullable_map_int_struct_col");
    ASSERT_EQ(map_schema.children.size(), 1);
    const auto& key_value_schema = *map_schema.children[0];
    ASSERT_EQ(key_value_schema.children.size(), 2);
    const auto& value_schema = *key_value_schema.children[1];
    ASSERT_EQ(value_schema.children.size(), 2);

    reader::LocalColumnIndex projection;
    projection.index = map_schema.field_id;
    projection.project_all_children = false;
    reader::LocalColumnIndex entry_projection;
    entry_projection.index = key_value_schema.field_id;
    entry_projection.project_all_children = false;
    reader::LocalColumnIndex value_projection;
    value_projection.index = value_schema.field_id;
    value_projection.project_all_children = false;
    reader::LocalColumnIndex child_projection;
    child_projection.index = value_schema.children[1]->field_id;
    value_projection.children.push_back(std::move(child_projection));
    entry_projection.children.push_back(std::move(value_projection));
    projection.children.push_back(std::move(entry_projection));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    auto st = factory.create(map_schema, &projection, &reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(reader->type()->is_nullable());
    const auto* map_type = assert_cast<const DataTypeMap*>(remove_nullable(reader->type()).get());
    EXPECT_EQ(remove_nullable(map_type->get_key_type())->get_primitive_type(), TYPE_INT);
    const auto* value_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(map_type->get_value_type()).get());
    ASSERT_EQ(value_type->get_elements().size(), 1);
    EXPECT_EQ(value_type->get_element_name(0), "b");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& keys = assert_cast<const ColumnInt32&>(map_column.get_keys());
    const auto& values = assert_cast<const ColumnNullable&>(map_column.get_values());
    const auto& struct_column = assert_cast<const ColumnStruct&>(values.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& b_values = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    const auto& b_data = assert_cast<const ColumnString&>(b_values.get_nested_column());
    ASSERT_EQ(keys.size(), 4);
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(keys.get_element(0), 101);
    EXPECT_EQ(keys.get_element(1), 102);
    EXPECT_EQ(keys.get_element(3), 104);
    EXPECT_EQ(b_data.get_data_at(0).to_string(), "ma");
    EXPECT_TRUE(b_values.is_null_at(1));
    EXPECT_TRUE(values.is_null_at(2));
    EXPECT_EQ(b_data.get_data_at(3).to_string(), "me");
}

TEST_F(ParquetColumnReaderTest, ReadProjectedStructListChildOnly) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "nullable_struct_list_col");
    ASSERT_EQ(struct_schema.children.size(), 2);

    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    ASSERT_TRUE(reader->type()->is_nullable());
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(reader->type()).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "xs");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& xs_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(xs_nullable.size(), ROW_COUNT);
    EXPECT_FALSE(xs_nullable.is_null_at(0));
    EXPECT_FALSE(xs_nullable.is_null_at(2));
    EXPECT_TRUE(xs_nullable.is_null_at(3));
    EXPECT_FALSE(xs_nullable.is_null_at(4));
    const auto& xs_array = assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
    const auto& offsets = xs_array.get_offsets();
    ASSERT_EQ(offsets.size(), ROW_COUNT);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 2);
    EXPECT_EQ(offsets[4], 4);
    const auto& elements = assert_cast<const ColumnNullable&>(xs_array.get_data());
    const auto& values = assert_cast<const ColumnInt32&>(elements.get_nested_column());
    ASSERT_EQ(elements.size(), 4);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(1), 2);
    EXPECT_TRUE(elements.is_null_at(2));
    EXPECT_EQ(values.get_element(3), 5);
}

TEST_F(ParquetColumnReaderTest, SkipProjectedStructListChildOnlyThenRead) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& xs_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(xs_nullable.size(), 3);
    EXPECT_FALSE(xs_nullable.is_null_at(1));
    EXPECT_TRUE(xs_nullable.is_null_at(2));
    const auto& xs_array = assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
    const auto& offsets = xs_array.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 0);
}

TEST_F(ParquetColumnReaderTest, SelectProjectedStructListChildOnly) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& xs_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(xs_nullable.size(), 3);
    EXPECT_FALSE(xs_nullable.is_null_at(0));
    EXPECT_TRUE(xs_nullable.is_null_at(1));
    EXPECT_FALSE(xs_nullable.is_null_at(2));
    const auto& xs_array = assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
    const auto& offsets = xs_array.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 4);
}

TEST_F(ParquetColumnReaderTest, ReadProjectedStructMapChildOnly) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "nullable_struct_map_col");
    ASSERT_EQ(struct_schema.children.size(), 2);

    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    ASSERT_TRUE(reader->type()->is_nullable());
    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(reader->type()).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "kv");

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& kv_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(kv_nullable.size(), ROW_COUNT);
    EXPECT_FALSE(kv_nullable.is_null_at(0));
    EXPECT_FALSE(kv_nullable.is_null_at(2));
    EXPECT_TRUE(kv_nullable.is_null_at(3));
    EXPECT_FALSE(kv_nullable.is_null_at(4));
    const auto& kv_map = assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
    const auto& offsets = kv_map.get_offsets();
    ASSERT_EQ(offsets.size(), ROW_COUNT);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 2);
    EXPECT_EQ(offsets[4], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(kv_map.get_keys());
    const auto& values = assert_cast<const ColumnNullable&>(kv_map.get_values());
    const auto& value_data = assert_cast<const ColumnString&>(values.get_nested_column());
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 5);
    EXPECT_EQ(value_data.get_data_at(0).to_string(), "one");
    EXPECT_TRUE(values.is_null_at(1));
    EXPECT_EQ(value_data.get_data_at(2).to_string(), "five");
}

TEST_F(ParquetColumnReaderTest, SkipProjectedStructMapChildOnlyThenRead) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& kv_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(kv_nullable.size(), 3);
    EXPECT_FALSE(kv_nullable.is_null_at(1));
    EXPECT_TRUE(kv_nullable.is_null_at(2));
    const auto& kv_map = assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
    const auto& offsets = kv_map.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 0);
}

TEST_F(ParquetColumnReaderTest, SelectProjectedStructMapChildOnly) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    ASSERT_EQ(struct_column.get_columns().size(), 1);
    const auto& kv_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(kv_nullable.size(), 3);
    EXPECT_FALSE(kv_nullable.is_null_at(0));
    EXPECT_TRUE(kv_nullable.is_null_at(1));
    EXPECT_FALSE(kv_nullable.is_null_at(2));
    const auto& kv_map = assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
    const auto& offsets = kv_map.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(kv_map.get_keys());
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 5);
}

TEST_F(ParquetColumnReaderTest, ReadListWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_list_int_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipListWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_list_int_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& offsets = array_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 2);
}

TEST_F(ParquetColumnReaderTest, SelectListWithOverflow) {
    const auto field_idx = find_field_idx("nullable_list_int_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& offsets = array_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 4);
    EXPECT_EQ(offsets[2], 5);
}

TEST_F(ParquetColumnReaderTest, ReadStructListWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipStructListWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& xs_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    ASSERT_EQ(xs_nullable.size(), 3);
    EXPECT_FALSE(xs_nullable.is_null_at(1));
    EXPECT_TRUE(xs_nullable.is_null_at(2));
    const auto& xs_array = assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
    const auto& offsets = xs_array.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 0);
}

TEST_F(ParquetColumnReaderTest, SelectStructListWithOverflow) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& a_values = assert_cast<const ColumnInt32&>(struct_column.get_column(0));
    EXPECT_EQ(a_values.get_element(0), 301);
    EXPECT_EQ(a_values.get_element(1), 304);
    EXPECT_EQ(a_values.get_element(2), 305);
    const auto& xs_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    ASSERT_EQ(xs_nullable.size(), 3);
    EXPECT_FALSE(xs_nullable.is_null_at(0));
    EXPECT_TRUE(xs_nullable.is_null_at(1));
    EXPECT_FALSE(xs_nullable.is_null_at(2));
    const auto& xs_array = assert_cast<const ColumnArray&>(xs_nullable.get_nested_column());
    const auto& offsets = xs_array.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 4);
}

TEST_F(ParquetColumnReaderTest, ReadStructMapWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipStructMapWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& kv_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    ASSERT_EQ(kv_nullable.size(), 3);
    EXPECT_FALSE(kv_nullable.is_null_at(1));
    EXPECT_TRUE(kv_nullable.is_null_at(2));
    const auto& kv_map = assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
    const auto& offsets = kv_map.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 0);
}

TEST_F(ParquetColumnReaderTest, SelectStructMapWithOverflow) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& a_values = assert_cast<const ColumnInt32&>(struct_column.get_column(0));
    EXPECT_EQ(a_values.get_element(0), 401);
    EXPECT_EQ(a_values.get_element(1), 404);
    EXPECT_EQ(a_values.get_element(2), 405);
    const auto& kv_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    ASSERT_EQ(kv_nullable.size(), 3);
    EXPECT_FALSE(kv_nullable.is_null_at(0));
    EXPECT_TRUE(kv_nullable.is_null_at(1));
    EXPECT_FALSE(kv_nullable.is_null_at(2));
    const auto& kv_map = assert_cast<const ColumnMap&>(kv_nullable.get_nested_column());
    const auto& offsets = kv_map.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(kv_map.get_keys());
    const auto& values = assert_cast<const ColumnNullable&>(kv_map.get_values());
    const auto& value_data = assert_cast<const ColumnString&>(values.get_nested_column());
    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 5);
    EXPECT_EQ(value_data.get_data_at(0).to_string(), "one");
    EXPECT_TRUE(values.is_null_at(1));
    EXPECT_EQ(value_data.get_data_at(2).to_string(), "five");
}

TEST_F(ParquetColumnReaderTest, ReadListStructWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_list_struct_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipListStructWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_list_struct_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& offsets = array_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 2);
}

TEST_F(ParquetColumnReaderTest, SelectListStructWithOverflow) {
    const auto field_idx = find_field_idx("nullable_list_struct_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& offsets = array_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 4);
    EXPECT_EQ(offsets[2], 5);
}

TEST_F(ParquetColumnReaderTest, ReadListListWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_list_list_int_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipListListWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_list_list_int_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& outer_array = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_array.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 3);
    EXPECT_EQ(outer_offsets[0], 0);
    EXPECT_EQ(outer_offsets[1], 0);
    EXPECT_EQ(outer_offsets[2], 1);

    const auto& inner_nullable = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(inner_nullable.size(), 1);
    EXPECT_FALSE(inner_nullable.is_null_at(0));
    const auto& inner_array = assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
    const auto& inner_offsets = inner_array.get_offsets();
    ASSERT_EQ(inner_offsets.size(), 1);
    EXPECT_EQ(inner_offsets[0], 1);
}

TEST_F(ParquetColumnReaderTest, SelectListListWithOverflow) {
    const auto field_idx = find_field_idx("nullable_list_list_int_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& outer_array = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_array.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 3);
    EXPECT_EQ(outer_offsets[0], 4);
    EXPECT_EQ(outer_offsets[1], 5);
    EXPECT_EQ(outer_offsets[2], 7);

    const auto& inner_nullable = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(inner_nullable.size(), 7);
    EXPECT_TRUE(inner_nullable.is_null_at(2));
    const auto& inner_array = assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
    const auto& inner_offsets = inner_array.get_offsets();
    ASSERT_EQ(inner_offsets.size(), 7);
    EXPECT_EQ(inner_offsets[0], 2);
    EXPECT_EQ(inner_offsets[3], 4);
    EXPECT_EQ(inner_offsets[4], 5);
    EXPECT_EQ(inner_offsets[6], 7);
}

TEST_F(ParquetColumnReaderTest, ReadMapWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_map_int_string_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipMapWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_map_int_string_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& offsets = map_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 1);
}

TEST_F(ParquetColumnReaderTest, SelectMapWithOverflow) {
    const auto field_idx = find_field_idx("nullable_map_int_string_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& offsets = map_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 3);
    EXPECT_EQ(offsets[2], 4);
}

TEST_F(ParquetColumnReaderTest, ReadMapStructWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipMapStructWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& offsets = map_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 1);
}

TEST_F(ParquetColumnReaderTest, SelectMapStructWithOverflow) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& offsets = map_column.get_offsets();
    ASSERT_EQ(offsets.size(), 3);
    EXPECT_EQ(offsets[0], 2);
    EXPECT_EQ(offsets[1], 3);
    EXPECT_EQ(offsets[2], 4);
}

TEST_F(ParquetColumnReaderTest, ReadMapListWithOverflowAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_map_int_list_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipMapListWithOverflowThenRead) {
    const auto field_idx = find_field_idx("nullable_map_int_list_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(3, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 3);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& map_offsets = map_column.get_offsets();
    ASSERT_EQ(map_offsets.size(), 3);
    EXPECT_EQ(map_offsets[0], 0);
    EXPECT_EQ(map_offsets[1], 0);
    EXPECT_EQ(map_offsets[2], 2);

    const auto& values = assert_cast<const ColumnNullable&>(map_column.get_values());
    ASSERT_EQ(values.size(), 2);
    EXPECT_TRUE(values.is_null_at(0));
    EXPECT_FALSE(values.is_null_at(1));
    const auto& list_column = assert_cast<const ColumnArray&>(values.get_nested_column());
    const auto& list_offsets = list_column.get_offsets();
    ASSERT_EQ(list_offsets.size(), 2);
    EXPECT_EQ(list_offsets[0], 0);
    EXPECT_EQ(list_offsets[1], 2);
}

TEST_F(ParquetColumnReaderTest, SelectMapListWithOverflow) {
    const auto field_idx = find_field_idx("nullable_map_int_list_col");
    auto reader = create_reader(field_idx);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 3);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, ROW_COUNT, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    const auto& map_column = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& map_offsets = map_column.get_offsets();
    ASSERT_EQ(map_offsets.size(), 3);
    EXPECT_EQ(map_offsets[0], 2);
    EXPECT_EQ(map_offsets[1], 4);
    EXPECT_EQ(map_offsets[2], 5);

    const auto& values = assert_cast<const ColumnNullable&>(map_column.get_values());
    ASSERT_EQ(values.size(), 5);
    EXPECT_FALSE(values.is_null_at(0));
    EXPECT_TRUE(values.is_null_at(2));
    EXPECT_FALSE(values.is_null_at(4));
    const auto& list_column = assert_cast<const ColumnArray&>(values.get_nested_column());
    const auto& list_offsets = list_column.get_offsets();
    ASSERT_EQ(list_offsets.size(), 5);
    EXPECT_EQ(list_offsets[0], 2);
    EXPECT_EQ(list_offsets[1], 2);
    EXPECT_EQ(list_offsets[2], 2);
    EXPECT_EQ(list_offsets[3], 4);
    EXPECT_EQ(list_offsets[4], 5);
}

TEST_F(ParquetColumnReaderTest, BuildComplexSchemaNodeMetadata) {
    const auto field_idx = find_field_idx("struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "struct_col");
    ASSERT_EQ(struct_schema.children.size(), 2);
    EXPECT_EQ(struct_schema.field_id, static_cast<int32_t>(field_idx));
    EXPECT_EQ(struct_schema.parent_schema_node_id, -1);
    EXPECT_EQ(struct_schema.children[0]->field_id, 0);
    EXPECT_EQ(struct_schema.children[1]->field_id, 1);
    EXPECT_EQ(struct_schema.children[0]->parent_schema_node_id, struct_schema.schema_node_id);
    EXPECT_EQ(struct_schema.children[1]->parent_schema_node_id, struct_schema.schema_node_id);
    EXPECT_EQ(struct_schema.children[0]->name, "a");
    EXPECT_EQ(struct_schema.children[1]->name, "b");
    EXPECT_EQ(struct_schema.max_definition_level, 0);
    EXPECT_EQ(struct_schema.max_repetition_level, 0);
}

TEST_F(ParquetColumnReaderTest, ResolveSupportedPhysicalAndLogicalSchemas) {
    std::vector<::parquet::schema::NodePtr> nodes = {
            ::parquet::schema::PrimitiveNode::Make("required_bool", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BOOLEAN),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_int64", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_float", ::parquet::Repetition::REQUIRED, ::parquet::Type::FLOAT),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_double", ::parquet::Repetition::REQUIRED, ::parquet::Type::DOUBLE),
            ::parquet::schema::PrimitiveNode::Make("required_binary",
                                                   ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BYTE_ARRAY),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_fixed_binary", ::parquet::Repetition::REQUIRED,
                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::NONE, 4),
            ::parquet::schema::PrimitiveNode::Make(
                    "optional_int32", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32),
            ::parquet::schema::PrimitiveNode::Make("utf8_binary", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BYTE_ARRAY,
                                                   ::parquet::ConvertedType::UTF8),
            ::parquet::schema::PrimitiveNode::Make("enum_binary", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BYTE_ARRAY,
                                                   ::parquet::ConvertedType::ENUM),
            ::parquet::schema::PrimitiveNode::Make("json_binary", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BYTE_ARRAY,
                                                   ::parquet::ConvertedType::JSON),
            ::parquet::schema::PrimitiveNode::Make("bson_binary", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::BYTE_ARRAY,
                                                   ::parquet::ConvertedType::BSON),
            ::parquet::schema::PrimitiveNode::Make("decimal_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::DECIMAL, -1, 9, 2),
            ::parquet::schema::PrimitiveNode::Make("decimal_int64", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT64,
                                                   ::parquet::ConvertedType::DECIMAL, -1, 18, 6),
            ::parquet::schema::PrimitiveNode::Make(
                    "decimal_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::DECIMAL, -1, 18, 6),
            ::parquet::schema::PrimitiveNode::Make("decimal_fixed_binary",
                                                   ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                   ::parquet::ConvertedType::DECIMAL, 8, 18, 6),
            ::parquet::schema::PrimitiveNode::Make("date_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::DATE),
            ::parquet::schema::PrimitiveNode::Make(
                    "time_millis_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::TIME_MILLIS),
            ::parquet::schema::PrimitiveNode::Make(
                    "time_micros_int64", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
                    ::parquet::ConvertedType::TIME_MICROS),
            ::parquet::schema::PrimitiveNode::Make(
                    "timestamp_millis_int64", ::parquet::Repetition::REQUIRED,
                    ::parquet::Type::INT64, ::parquet::ConvertedType::TIMESTAMP_MILLIS),
            ::parquet::schema::PrimitiveNode::Make(
                    "timestamp_micros_int64", ::parquet::Repetition::REQUIRED,
                    ::parquet::Type::INT64, ::parquet::ConvertedType::TIMESTAMP_MICROS),
            ::parquet::schema::PrimitiveNode::Make("int8_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::INT_8),
            ::parquet::schema::PrimitiveNode::Make("uint8_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::UINT_8),
            ::parquet::schema::PrimitiveNode::Make("int16_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::INT_16),
            ::parquet::schema::PrimitiveNode::Make("uint16_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::UINT_16),
            ::parquet::schema::PrimitiveNode::Make("int32_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::INT_32),
            ::parquet::schema::PrimitiveNode::Make("uint32_int32", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT32,
                                                   ::parquet::ConvertedType::UINT_32),
            ::parquet::schema::PrimitiveNode::Make("int64_int64", ::parquet::Repetition::REQUIRED,
                                                   ::parquet::Type::INT64,
                                                   ::parquet::ConvertedType::INT_64),
    };

    auto schema =
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, nodes);
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(schema);

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    auto st = build_parquet_column_schema(descriptor, &fields);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(fields.size(), nodes.size());

    for (const auto& field : fields) {
        SCOPED_TRACE(field->name);
        ASSERT_TRUE(supports_record_reader(field->type_descriptor));
        ASSERT_NE(field->type, nullptr);
    }
}

TEST_F(ParquetColumnReaderTest, RejectUnsupportedPhysicalAndLogicalTypes) {
    auto schema = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {
                    ::parquet::schema::PrimitiveNode::Make(
                            "int96_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT96),
                    ::parquet::schema::PrimitiveNode::Make("repeated_int32_col",
                                                           ::parquet::Repetition::REPEATED,
                                                           ::parquet::Type::INT32),
                    ::parquet::schema::PrimitiveNode::Make(
                            "decimal256_fixed_col", ::parquet::Repetition::REQUIRED,
                            ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
                            ::parquet::ConvertedType::DECIMAL, 20, 39, 6),
                    ::parquet::schema::PrimitiveNode::Make(
                            "uint64_col", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
                            ::parquet::ConvertedType::UINT_64),
                    ::parquet::schema::PrimitiveNode::Make(
                            "time_nanos_col", ::parquet::Repetition::REQUIRED,
                            ::parquet::LogicalType::Time(false,
                                                         ::parquet::LogicalType::TimeUnit::NANOS),
                            ::parquet::Type::INT64),
                    ::parquet::schema::PrimitiveNode::Make(
                            "timestamp_nanos_col", ::parquet::Repetition::REQUIRED,
                            ::parquet::LogicalType::Timestamp(
                                    false, ::parquet::LogicalType::TimeUnit::NANOS),
                            ::parquet::Type::INT64),
            });
    ::parquet::SchemaDescriptor descriptor;
    descriptor.Init(schema);

    std::vector<std::unique_ptr<ParquetColumnSchema>> fields;
    auto st = build_parquet_column_schema(descriptor, &fields);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(fields.size(), 6);

    for (const auto& field : fields) {
        SCOPED_TRACE(field->name);
        ASSERT_FALSE(supports_record_reader(field->type_descriptor));
    }
}

} // namespace
} // namespace doris::parquet
