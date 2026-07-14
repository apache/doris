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

#include <array>
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
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/types.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/selection_vector.h"

namespace doris::format::parquet {
namespace {

constexpr int64_t ROW_COUNT = 5;

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

template <typename ColumnType>
const ColumnType& get_nullable_nested_column(const IColumn& column) {
    // File-local schema exposed by the parquet reader follows Doris external-table semantics:
    // nested STRUCT fields, LIST elements, and MAP keys/values are nullable even when the parquet
    // field is required.
    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
    return assert_cast<const ColumnType&>(nullable_column.get_nested_column());
}

ParquetColumnSchema mock_column_schema() {
    ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "mock";
    schema.type = std::make_shared<DataTypeInt32>();
    return schema;
}

class BaseUnsupportedReader final : public ParquetColumnReader {
public:
    BaseUnsupportedReader()
            : ParquetColumnReader(mock_column_schema(), mock_column_schema().type) {}

    Status read(int64_t, MutableColumnPtr&, int64_t*) override { return Status::OK(); }
};

class DefaultSelectReader final : public ParquetColumnReader {
public:
    DefaultSelectReader() : ParquetColumnReader(mock_column_schema(), mock_column_schema().type) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        auto& values = assert_cast<ColumnInt32&>(*column);
        for (int64_t row = 0; row < rows; ++row) {
            values.insert_value(static_cast<int32_t>(_cursor + row));
        }
        _cursor += rows;
        *rows_read = rows;
        _read_ranges.push_back(rows);
        return Status::OK();
    }

    Status skip(int64_t rows) override {
        _cursor += rows;
        _skip_ranges.push_back(rows);
        return Status::OK();
    }

    const std::vector<int64_t>& read_ranges() const { return _read_ranges; }
    const std::vector<int64_t>& skip_ranges() const { return _skip_ranges; }

private:
    int64_t _cursor = 0;
    std::vector<int64_t> _read_ranges;
    std::vector<int64_t> _skip_ranges;
};

class NestedSkipReader final : public ParquetColumnReader {
public:
    NestedSkipReader() : ParquetColumnReader(mock_column_schema(), mock_column_schema().type) {}

    Status read(int64_t, MutableColumnPtr&, int64_t*) override { return Status::OK(); }

    Status consume_nested_column(int64_t length_upper_bound, int64_t* values_consumed) override {
        *values_consumed = length_upper_bound;
        return Status::OK();
    }
};

class ParquetColumnReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_parquet_column_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.parquet").string();
        _plain_file_path = (_test_dir / "plain_reader.parquet").string();
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

    std::shared_ptr<arrow::Array> build_nullable_int32_array() {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.Append(1).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(3).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(5).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_all_null_int32_array() {
        arrow::Int32Builder builder;
        for (int64_t row = 0; row < ROW_COUNT; ++row) {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
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

    std::shared_ptr<arrow::Array> build_nullable_struct_with_decimal_array() {
        auto decimal_type = arrow::decimal128(38, 6);
        auto struct_type = arrow::struct_(
                {arrow::field("a", arrow::int32(), false), arrow::field("d", decimal_type, true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        auto a_array_builder = std::make_unique<arrow::Int32Builder>();
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
        auto d_array_builder = std::make_unique<arrow::Decimal128Builder>(
                decimal_type, arrow::default_memory_pool());
        field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(d_array_builder)));
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));
        auto* a_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
        auto* d_builder = assert_cast<arrow::Decimal128Builder*>(builder.field_builder(1));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(301).ok());
        EXPECT_TRUE(d_builder->Append(arrow::Decimal128(123456789)).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(303).ok());
        EXPECT_TRUE(d_builder->AppendNull().ok());
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(a_builder->Append(304).ok());
        EXPECT_TRUE(d_builder->Append(arrow::Decimal128(-987654321)).ok());
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

    std::shared_ptr<arrow::Array> build_nullable_struct_with_nested_struct_list_array() {
        auto list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto nested_type = arrow::struct_({arrow::field("xs", list_type, true)});
        auto struct_type = arrow::struct_({arrow::field("nested", nested_type, true)});

        auto value_builder = std::make_shared<arrow::Int32Builder>();
        auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                                 value_builder, list_type);
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> nested_field_builders;
        nested_field_builders.push_back(list_builder);
        auto nested_builder = std::make_shared<arrow::StructBuilder>(
                nested_type, arrow::default_memory_pool(), std::move(nested_field_builders));
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
        field_builders.push_back(nested_builder);
        arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                     std::move(field_builders));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(nested_builder->Append().ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(value_builder->Append(7).ok());
        EXPECT_TRUE(value_builder->Append(8).ok());

        EXPECT_TRUE(builder.AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(nested_builder->AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(nested_builder->Append().ok());
        EXPECT_TRUE(list_builder->AppendNull().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(nested_builder->Append().ok());
        EXPECT_TRUE(list_builder->AppendEmptyValue().ok());
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

    std::shared_ptr<arrow::Array> build_nullable_map_list_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto value_builder = std::make_shared<arrow::StringBuilder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        auto map_builder = std::make_shared<arrow::MapBuilder>(
                arrow::default_memory_pool(), key_builder, value_builder, map_type);
        arrow::ListBuilder builder(arrow::default_memory_pool(), map_builder,
                                   arrow::list(arrow::field("element", map_type, true)));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(1).ok());
        EXPECT_TRUE(value_builder->Append("a").ok());
        EXPECT_TRUE(key_builder->Append(2).ok());
        EXPECT_TRUE(value_builder->AppendNull().ok());
        EXPECT_TRUE(map_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(map_builder->AppendNull().ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(3).ok());
        EXPECT_TRUE(value_builder->Append("c").ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(4).ok());
        EXPECT_TRUE(value_builder->Append("d").ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_int_map_map_array() {
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto nested_key_builder = std::make_shared<arrow::Int32Builder>();
        auto nested_value_builder = std::make_shared<arrow::StringBuilder>();
        auto nested_map_type =
                arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        auto nested_map_builder = std::make_shared<arrow::MapBuilder>(
                arrow::default_memory_pool(), nested_key_builder, nested_value_builder,
                nested_map_type);
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", nested_map_type, true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, nested_map_builder,
                                  map_type);

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(10).ok());
        EXPECT_TRUE(nested_map_builder->Append().ok());
        EXPECT_TRUE(nested_key_builder->Append(101).ok());
        EXPECT_TRUE(nested_value_builder->Append("aa").ok());
        EXPECT_TRUE(key_builder->Append(20).ok());
        EXPECT_TRUE(nested_map_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(30).ok());
        EXPECT_TRUE(nested_map_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(40).ok());
        EXPECT_TRUE(nested_map_builder->Append().ok());
        EXPECT_TRUE(nested_key_builder->Append(401).ok());
        EXPECT_TRUE(nested_value_builder->AppendNull().ok());

        EXPECT_TRUE(builder.AppendEmptyValue().ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_deep_list_struct_map_list_array() {
        auto element_builder = std::make_shared<arrow::Int32Builder>();
        auto list_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                                 element_builder, list_type);
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", list_type, true));
        auto map_builder = std::make_shared<arrow::MapBuilder>(arrow::default_memory_pool(),
                                                               key_builder, list_builder, map_type);
        auto struct_type = arrow::struct_({arrow::field("kv", map_type, true)});
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> struct_field_builders;
        struct_field_builders.push_back(map_builder);
        auto struct_builder = std::make_shared<arrow::StructBuilder>(
                struct_type, arrow::default_memory_pool(), std::move(struct_field_builders));
        arrow::ListBuilder builder(arrow::default_memory_pool(), struct_builder,
                                   arrow::list(arrow::field("element", struct_type, true)));

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(1).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(element_builder->Append(10).ok());
        EXPECT_TRUE(element_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(2).ok());
        EXPECT_TRUE(list_builder->AppendEmptyValue().ok());
        EXPECT_TRUE(struct_builder->AppendNull().ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(map_builder->AppendNull().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(map_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_builder->Append().ok());
        EXPECT_TRUE(map_builder->Append().ok());
        EXPECT_TRUE(key_builder->Append(3).ok());
        EXPECT_TRUE(list_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(4).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(element_builder->Append(40).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_deep_map_list_map_array() {
        auto nested_key_builder = std::make_shared<arrow::Int32Builder>();
        auto nested_value_builder = std::make_shared<arrow::StringBuilder>();
        auto nested_map_type =
                arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        auto nested_map_builder = std::make_shared<arrow::MapBuilder>(
                arrow::default_memory_pool(), nested_key_builder, nested_value_builder,
                nested_map_type);
        auto list_type = arrow::list(arrow::field("element", nested_map_type, true));
        auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                                 nested_map_builder, list_type);
        auto key_builder = std::make_shared<arrow::Int32Builder>();
        auto map_type = arrow::map(arrow::int32(), arrow::field("value", list_type, true));
        arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, list_builder,
                                  map_type);

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(10).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(nested_map_builder->Append().ok());
        EXPECT_TRUE(nested_key_builder->Append(1).ok());
        EXPECT_TRUE(nested_value_builder->Append("a").ok());
        EXPECT_TRUE(nested_key_builder->Append(2).ok());
        EXPECT_TRUE(nested_value_builder->AppendNull().ok());
        EXPECT_TRUE(nested_map_builder->AppendEmptyValue().ok());
        EXPECT_TRUE(nested_map_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(20).ok());
        EXPECT_TRUE(list_builder->AppendEmptyValue().ok());

        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.AppendEmptyValue().ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(30).ok());
        EXPECT_TRUE(list_builder->AppendNull().ok());
        EXPECT_TRUE(key_builder->Append(40).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(nested_map_builder->Append().ok());
        EXPECT_TRUE(nested_key_builder->Append(3).ok());
        EXPECT_TRUE(nested_value_builder->Append("c").ok());

        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(key_builder->Append(50).ok());
        EXPECT_TRUE(list_builder->Append().ok());
        EXPECT_TRUE(nested_map_builder->AppendNull().ok());
        EXPECT_TRUE(nested_map_builder->Append().ok());
        EXPECT_TRUE(nested_key_builder->Append(4).ok());
        EXPECT_TRUE(nested_value_builder->Append("d").ok());
        return finish_array(&builder);
    }

    void add_field(const std::shared_ptr<arrow::Field>& field, std::shared_ptr<arrow::Array> array,
                   std::function<void(const ParquetColumnSchema&, const IColumn&)> validator) {
        _arrow_fields.push_back(field);
        _arrays.push_back(std::move(array));
        _expected_by_field.push_back(std::move(validator));
    }

    void write_parquet_file() {
        add_field(arrow::field("int32_col", arrow::int32(), false),
                  build_required_array<arrow::Int32Builder, int32_t>({10, 20, 30, 40, 50}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
                      const auto& values = assert_cast<const ColumnInt32&>(column);
                      EXPECT_EQ(values.get_element(0), 10);
                      EXPECT_EQ(values.get_element(4), 50);
                  });
        add_field(arrow::field("string_col", arrow::utf8(), false),
                  build_string_array({"alpha", "beta", "gamma", "delta", "epsilon"}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type_descriptor.is_string_like);
                      const auto& values = assert_cast<const ColumnString&>(column);
                      EXPECT_EQ(values.get_data_at(0).to_string(), "alpha");
                      EXPECT_EQ(values.get_data_at(4).to_string(), "epsilon");
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
        add_field(arrow::field("all_null_int_col", arrow::int32(), true),
                  build_all_null_int32_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      for (size_t row = 0; row < ROW_COUNT; ++row) {
                          EXPECT_TRUE(nullable_column.is_null_at(row));
                      }
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
                      const auto& b_values =
                              get_nullable_nested_column<ColumnString>(struct_column.get_column(1));
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
        add_field(arrow::field("nullable_struct_decimal_col",
                               arrow::struct_({
                                       arrow::field("a", arrow::int32(), false),
                                       arrow::field("d", arrow::decimal128(38, 6), true),
                               }),
                               true),
                  build_nullable_struct_with_decimal_array(),
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
                      const auto& d_values =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(1));
                      const auto& d_nested =
                              assert_cast<const ColumnDecimal128V3&>(d_values.get_nested_column());
                      EXPECT_EQ(a_values.get_element(0), 301);
                      EXPECT_EQ(a_values.get_element(2), 303);
                      EXPECT_EQ(a_values.get_element(3), 304);
                      EXPECT_FALSE(d_values.is_null_at(0));
                      EXPECT_TRUE(d_values.is_null_at(2));
                      EXPECT_FALSE(d_values.is_null_at(3));
                      EXPECT_EQ(d_nested.get_element(0), Decimal128V3(123456789));
                      EXPECT_EQ(d_nested.get_element(3), Decimal128V3(-987654321));
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
                      const auto& keys = get_nullable_nested_column<ColumnInt32>(kv_map.get_keys());
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
        auto nested_struct_list_type = arrow::struct_({
                arrow::field("nested",
                             arrow::struct_({
                                     arrow::field("xs",
                                                  arrow::list(arrow::field("element",
                                                                           arrow::int32(), true)),
                                                  true),
                             }),
                             true),
        });
        add_field(arrow::field("nullable_struct_nested_struct_list_col", nested_struct_list_type,
                               true),
                  build_nullable_struct_with_nested_struct_list_array(),
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
                      const auto& nested_nullable =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(0));
                      EXPECT_FALSE(nested_nullable.is_null_at(0));
                      EXPECT_TRUE(nested_nullable.is_null_at(2));
                      EXPECT_FALSE(nested_nullable.is_null_at(3));
                      EXPECT_FALSE(nested_nullable.is_null_at(4));
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
                      const auto& values =
                              get_nullable_nested_column<ColumnInt32>(array_column.get_data());
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
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
                      const auto& keys =
                              get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
                      const auto& values =
                              get_nullable_nested_column<ColumnString>(map_column.get_values());
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
                    const auto& keys =
                            get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
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

                      const auto& keys =
                              get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
                      const auto& values =
                              assert_cast<const ColumnNullable&>(map_column.get_values());
                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(values.get_nested_column());
                      const auto& a_values =
                              get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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

                    const auto& keys =
                            get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
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
        auto list_map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        add_field(arrow::field("nullable_list_map_int_string_col",
                               arrow::list(arrow::field("element", list_map_type, true)), true),
                  build_nullable_map_list_array(),
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
                      EXPECT_EQ(outer_offsets[0], 2);
                      EXPECT_EQ(outer_offsets[1], 2);
                      EXPECT_EQ(outer_offsets[2], 2);
                      EXPECT_EQ(outer_offsets[3], 4);
                      EXPECT_EQ(outer_offsets[4], 5);

                      const auto& map_values =
                              assert_cast<const ColumnNullable&>(outer_array.get_data());
                      ASSERT_EQ(map_values.size(), 5);
                      EXPECT_FALSE(map_values.is_null_at(0));
                      EXPECT_FALSE(map_values.is_null_at(1));
                      EXPECT_TRUE(map_values.is_null_at(2));
                      EXPECT_FALSE(map_values.is_null_at(3));
                      EXPECT_FALSE(map_values.is_null_at(4));

                      const auto& map_column =
                              assert_cast<const ColumnMap&>(map_values.get_nested_column());
                      const auto& map_offsets = map_column.get_offsets();
                      ASSERT_EQ(map_offsets.size(), 5);
                      EXPECT_EQ(map_offsets[0], 2);
                      EXPECT_EQ(map_offsets[1], 2);
                      EXPECT_EQ(map_offsets[2], 2);
                      EXPECT_EQ(map_offsets[3], 3);
                      EXPECT_EQ(map_offsets[4], 4);
                      const auto& keys =
                              get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
                      const auto& values =
                              assert_cast<const ColumnNullable&>(map_column.get_values());
                      const auto& value_data =
                              assert_cast<const ColumnString&>(values.get_nested_column());
                      ASSERT_EQ(keys.size(), 4);
                      EXPECT_EQ(keys.get_element(0), 1);
                      EXPECT_EQ(keys.get_element(1), 2);
                      EXPECT_EQ(keys.get_element(2), 3);
                      EXPECT_EQ(keys.get_element(3), 4);
                      EXPECT_EQ(value_data.get_data_at(0).to_string(), "a");
                      EXPECT_TRUE(values.is_null_at(1));
                      EXPECT_EQ(value_data.get_data_at(2).to_string(), "c");
                      EXPECT_EQ(value_data.get_data_at(3).to_string(), "d");
                  });
        auto nested_map_type =
                arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        add_field(arrow::field(
                          "nullable_map_int_map_int_string_col",
                          arrow::map(arrow::int32(), arrow::field("value", nested_map_type, true)),
                          true),
                  build_nullable_int_map_map_array(),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_TRUE(schema.type->is_nullable());
                      const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                      ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                      EXPECT_FALSE(nullable_column.is_null_at(0));
                      EXPECT_TRUE(nullable_column.is_null_at(1));
                      EXPECT_FALSE(nullable_column.is_null_at(2));
                      EXPECT_FALSE(nullable_column.is_null_at(3));
                      EXPECT_FALSE(nullable_column.is_null_at(4));

                      const auto& outer_map =
                              assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
                      const auto& outer_offsets = outer_map.get_offsets();
                      ASSERT_EQ(outer_offsets.size(), ROW_COUNT);
                      EXPECT_EQ(outer_offsets[0], 2);
                      EXPECT_EQ(outer_offsets[1], 2);
                      EXPECT_EQ(outer_offsets[2], 2);
                      EXPECT_EQ(outer_offsets[3], 4);
                      EXPECT_EQ(outer_offsets[4], 4);

                      const auto& outer_keys =
                              get_nullable_nested_column<ColumnInt32>(outer_map.get_keys());
                      ASSERT_EQ(outer_keys.size(), 4);
                      EXPECT_EQ(outer_keys.get_element(0), 10);
                      EXPECT_EQ(outer_keys.get_element(1), 20);
                      EXPECT_EQ(outer_keys.get_element(2), 30);
                      EXPECT_EQ(outer_keys.get_element(3), 40);

                      const auto& inner_values =
                              assert_cast<const ColumnNullable&>(outer_map.get_values());
                      ASSERT_EQ(inner_values.size(), 4);
                      EXPECT_FALSE(inner_values.is_null_at(0));
                      EXPECT_FALSE(inner_values.is_null_at(1));
                      EXPECT_TRUE(inner_values.is_null_at(2));
                      EXPECT_FALSE(inner_values.is_null_at(3));

                      const auto& inner_map =
                              assert_cast<const ColumnMap&>(inner_values.get_nested_column());
                      const auto& inner_offsets = inner_map.get_offsets();
                      ASSERT_EQ(inner_offsets.size(), 4);
                      EXPECT_EQ(inner_offsets[0], 1);
                      EXPECT_EQ(inner_offsets[1], 1);
                      EXPECT_EQ(inner_offsets[2], 1);
                      EXPECT_EQ(inner_offsets[3], 2);
                      const auto& inner_keys =
                              get_nullable_nested_column<ColumnInt32>(inner_map.get_keys());
                      const auto& inner_strings =
                              assert_cast<const ColumnNullable&>(inner_map.get_values());
                      const auto& inner_string_data =
                              assert_cast<const ColumnString&>(inner_strings.get_nested_column());
                      ASSERT_EQ(inner_keys.size(), 2);
                      EXPECT_EQ(inner_keys.get_element(0), 101);
                      EXPECT_EQ(inner_keys.get_element(1), 401);
                      EXPECT_EQ(inner_string_data.get_data_at(0).to_string(), "aa");
                      EXPECT_TRUE(inner_strings.is_null_at(1));
                  });
        auto deep_list_value_type = arrow::list(arrow::field("element", arrow::int32(), true));
        auto deep_list_map_type =
                arrow::map(arrow::int32(), arrow::field("value", deep_list_value_type, true));
        auto deep_list_struct_type = arrow::struct_({arrow::field("kv", deep_list_map_type, true)});
        add_field(arrow::field("nullable_list_struct_map_list_col",
                               arrow::list(arrow::field("element", deep_list_struct_type, true)),
                               true),
                  build_deep_list_struct_map_list_array(),
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
                      EXPECT_EQ(outer_offsets[0], 2);
                      EXPECT_EQ(outer_offsets[1], 2);
                      EXPECT_EQ(outer_offsets[2], 2);
                      EXPECT_EQ(outer_offsets[3], 4);
                      EXPECT_EQ(outer_offsets[4], 5);

                      const auto& struct_values =
                              assert_cast<const ColumnNullable&>(outer_array.get_data());
                      ASSERT_EQ(struct_values.size(), 5);
                      EXPECT_FALSE(struct_values.is_null_at(0));
                      EXPECT_TRUE(struct_values.is_null_at(1));
                      EXPECT_FALSE(struct_values.is_null_at(2));
                      EXPECT_FALSE(struct_values.is_null_at(3));
                      EXPECT_FALSE(struct_values.is_null_at(4));

                      const auto& struct_column =
                              assert_cast<const ColumnStruct&>(struct_values.get_nested_column());
                      const auto& map_values =
                              assert_cast<const ColumnNullable&>(struct_column.get_column(0));
                      ASSERT_EQ(map_values.size(), 5);
                      EXPECT_FALSE(map_values.is_null_at(0));
                      EXPECT_TRUE(map_values.is_null_at(1));
                      EXPECT_TRUE(map_values.is_null_at(2));
                      EXPECT_FALSE(map_values.is_null_at(3));
                      EXPECT_FALSE(map_values.is_null_at(4));

                      const auto& map_column =
                              assert_cast<const ColumnMap&>(map_values.get_nested_column());
                      const auto& map_offsets = map_column.get_offsets();
                      ASSERT_EQ(map_offsets.size(), 5);
                      EXPECT_EQ(map_offsets[0], 2);
                      EXPECT_EQ(map_offsets[1], 2);
                      EXPECT_EQ(map_offsets[2], 2);
                      EXPECT_EQ(map_offsets[3], 2);
                      EXPECT_EQ(map_offsets[4], 4);
                      const auto& keys =
                              get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
                      ASSERT_EQ(keys.size(), 4);
                      EXPECT_EQ(keys.get_element(0), 1);
                      EXPECT_EQ(keys.get_element(1), 2);
                      EXPECT_EQ(keys.get_element(2), 3);
                      EXPECT_EQ(keys.get_element(3), 4);

                      const auto& lists =
                              assert_cast<const ColumnNullable&>(map_column.get_values());
                      ASSERT_EQ(lists.size(), 4);
                      EXPECT_FALSE(lists.is_null_at(0));
                      EXPECT_FALSE(lists.is_null_at(1));
                      EXPECT_TRUE(lists.is_null_at(2));
                      EXPECT_FALSE(lists.is_null_at(3));
                      const auto& list_column =
                              assert_cast<const ColumnArray&>(lists.get_nested_column());
                      const auto& list_offsets = list_column.get_offsets();
                      ASSERT_EQ(list_offsets.size(), 4);
                      EXPECT_EQ(list_offsets[0], 2);
                      EXPECT_EQ(list_offsets[1], 2);
                      EXPECT_EQ(list_offsets[2], 2);
                      EXPECT_EQ(list_offsets[3], 3);
                      const auto& elements =
                              assert_cast<const ColumnNullable&>(list_column.get_data());
                      const auto& element_values =
                              assert_cast<const ColumnInt32&>(elements.get_nested_column());
                      ASSERT_EQ(elements.size(), 3);
                      EXPECT_EQ(element_values.get_element(0), 10);
                      EXPECT_TRUE(elements.is_null_at(1));
                      EXPECT_EQ(element_values.get_element(2), 40);
                  });
        auto deep_map_nested_map_type =
                arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
        auto deep_map_list_type =
                arrow::list(arrow::field("element", deep_map_nested_map_type, true));
        add_field(
                arrow::field(
                        "nullable_map_int_list_map_int_string_col",
                        arrow::map(arrow::int32(), arrow::field("value", deep_map_list_type, true)),
                        true),
                build_deep_map_list_map_array(),
                [](const ParquetColumnSchema& schema, const IColumn& column) {
                    EXPECT_TRUE(schema.type->is_nullable());
                    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
                    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
                    EXPECT_FALSE(nullable_column.is_null_at(0));
                    EXPECT_TRUE(nullable_column.is_null_at(1));
                    EXPECT_FALSE(nullable_column.is_null_at(2));
                    EXPECT_FALSE(nullable_column.is_null_at(3));
                    EXPECT_FALSE(nullable_column.is_null_at(4));

                    const auto& outer_map =
                            assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
                    const auto& outer_offsets = outer_map.get_offsets();
                    ASSERT_EQ(outer_offsets.size(), ROW_COUNT);
                    EXPECT_EQ(outer_offsets[0], 2);
                    EXPECT_EQ(outer_offsets[1], 2);
                    EXPECT_EQ(outer_offsets[2], 2);
                    EXPECT_EQ(outer_offsets[3], 4);
                    EXPECT_EQ(outer_offsets[4], 5);
                    const auto& outer_keys =
                            get_nullable_nested_column<ColumnInt32>(outer_map.get_keys());
                    ASSERT_EQ(outer_keys.size(), 5);
                    EXPECT_EQ(outer_keys.get_element(0), 10);
                    EXPECT_EQ(outer_keys.get_element(1), 20);
                    EXPECT_EQ(outer_keys.get_element(2), 30);
                    EXPECT_EQ(outer_keys.get_element(3), 40);
                    EXPECT_EQ(outer_keys.get_element(4), 50);

                    const auto& lists = assert_cast<const ColumnNullable&>(outer_map.get_values());
                    ASSERT_EQ(lists.size(), 5);
                    EXPECT_FALSE(lists.is_null_at(0));
                    EXPECT_FALSE(lists.is_null_at(1));
                    EXPECT_TRUE(lists.is_null_at(2));
                    EXPECT_FALSE(lists.is_null_at(3));
                    EXPECT_FALSE(lists.is_null_at(4));
                    const auto& list_column =
                            assert_cast<const ColumnArray&>(lists.get_nested_column());
                    const auto& list_offsets = list_column.get_offsets();
                    ASSERT_EQ(list_offsets.size(), 5);
                    EXPECT_EQ(list_offsets[0], 3);
                    EXPECT_EQ(list_offsets[1], 3);
                    EXPECT_EQ(list_offsets[2], 3);
                    EXPECT_EQ(list_offsets[3], 4);
                    EXPECT_EQ(list_offsets[4], 6);

                    const auto& inner_maps =
                            assert_cast<const ColumnNullable&>(list_column.get_data());
                    ASSERT_EQ(inner_maps.size(), 6);
                    EXPECT_FALSE(inner_maps.is_null_at(0));
                    EXPECT_FALSE(inner_maps.is_null_at(1));
                    EXPECT_TRUE(inner_maps.is_null_at(2));
                    EXPECT_FALSE(inner_maps.is_null_at(3));
                    EXPECT_TRUE(inner_maps.is_null_at(4));
                    EXPECT_FALSE(inner_maps.is_null_at(5));
                    const auto& inner_map_column =
                            assert_cast<const ColumnMap&>(inner_maps.get_nested_column());
                    const auto& inner_offsets = inner_map_column.get_offsets();
                    ASSERT_EQ(inner_offsets.size(), 6);
                    EXPECT_EQ(inner_offsets[0], 2);
                    EXPECT_EQ(inner_offsets[1], 2);
                    EXPECT_EQ(inner_offsets[2], 2);
                    EXPECT_EQ(inner_offsets[3], 3);
                    EXPECT_EQ(inner_offsets[4], 3);
                    EXPECT_EQ(inner_offsets[5], 4);
                    const auto& inner_keys =
                            get_nullable_nested_column<ColumnInt32>(inner_map_column.get_keys());
                    ASSERT_EQ(inner_keys.size(), 4);
                    EXPECT_EQ(inner_keys.get_element(0), 1);
                    EXPECT_EQ(inner_keys.get_element(1), 2);
                    EXPECT_EQ(inner_keys.get_element(2), 3);
                    EXPECT_EQ(inner_keys.get_element(3), 4);
                    const auto& strings =
                            assert_cast<const ColumnNullable&>(inner_map_column.get_values());
                    const auto& string_data =
                            assert_cast<const ColumnString&>(strings.get_nested_column());
                    ASSERT_EQ(strings.size(), 4);
                    EXPECT_EQ(string_data.get_data_at(0).to_string(), "a");
                    EXPECT_TRUE(strings.is_null_at(1));
                    EXPECT_EQ(string_data.get_data_at(2).to_string(), "c");
                    EXPECT_EQ(string_data.get_data_at(3).to_string(), "d");
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

    std::unique_ptr<ParquetColumnReader> create_plain_reader(size_t field_idx) {
        // Keep the normal fixture dictionary encoded. This one test writes a plain-encoded copy
        // because Arrow BinaryRecordReader has a stricter reset contract than
        // DictionaryRecordReader.
        auto schema = arrow::schema(_arrow_fields);
        auto table = arrow::Table::Make(schema, _arrays);
        auto plain_file_result = arrow::io::FileOutputStream::Open(_plain_file_path);
        DORIS_CHECK(plain_file_result.ok());
        std::shared_ptr<arrow::io::FileOutputStream> plain_out = *plain_file_result;
        ::parquet::WriterProperties::Builder plain_builder;
        plain_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
        plain_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
        plain_builder.compression(::parquet::Compression::UNCOMPRESSED);
        plain_builder.disable_dictionary();
        PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
                *table, arrow::default_memory_pool(), plain_out, ROW_COUNT, plain_builder.build()));
        DORIS_CHECK(plain_out->Close().ok());

        _plain_file_reader = ::parquet::ParquetFileReader::OpenFile(_plain_file_path, false);
        auto metadata = _plain_file_reader->metadata();
        DORIS_CHECK(metadata != nullptr);
        DORIS_CHECK(metadata->num_row_groups() == 1);
        _plain_row_group = _plain_file_reader->RowGroup(0);
        DORIS_CHECK(_plain_row_group != nullptr);

        ParquetColumnReaderFactory factory(_plain_row_group, metadata->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(*_fields[field_idx], &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    std::unique_ptr<ParquetColumnReader> create_projected_child_reader(size_t field_idx,
                                                                       size_t child_idx) const {
        const auto& struct_schema = *_fields[field_idx];
        EXPECT_LT(child_idx, struct_schema.children.size());

        format::LocalColumnIndex projection;
        projection.index = struct_schema.local_id;
        projection.project_all_children = false;
        format::LocalColumnIndex child_projection;
        child_projection.index = struct_schema.children[child_idx]->local_id;
        projection.children.push_back(std::move(child_projection));

        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(struct_schema, &projection, &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    std::unique_ptr<ParquetColumnReader> create_projected_grandchild_reader(
            size_t field_idx, size_t child_idx, size_t grandchild_idx) const {
        const auto& struct_schema = *_fields[field_idx];
        EXPECT_LT(child_idx, struct_schema.children.size());
        const auto& child_schema = *struct_schema.children[child_idx];
        EXPECT_LT(grandchild_idx, child_schema.children.size());

        format::LocalColumnIndex projection;
        projection.index = struct_schema.local_id;
        projection.project_all_children = false;
        format::LocalColumnIndex child_projection;
        child_projection.index = child_schema.local_id;
        child_projection.project_all_children = false;
        format::LocalColumnIndex grandchild_projection;
        grandchild_projection.index = child_schema.children[grandchild_idx]->local_id;
        child_projection.children.push_back(std::move(grandchild_projection));
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
    std::string _plain_file_path;
    std::unique_ptr<::parquet::ParquetFileReader> _file_reader;
    std::unique_ptr<::parquet::ParquetFileReader> _plain_file_reader;
    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    std::shared_ptr<::parquet::RowGroupReader> _plain_row_group;
    std::vector<std::unique_ptr<ParquetColumnSchema>> _fields;
    std::vector<std::shared_ptr<arrow::Field>> _arrow_fields;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
    std::vector<std::function<void(const ParquetColumnSchema&, const IColumn&)>> _expected_by_field;
};

TEST(ParquetColumnReaderBaseTest, SelectionVectorRangesAndValidation) {
    SelectionVector identity;
    ASSERT_TRUE(identity.verify(4, 5).ok());
    auto ranges = selection_to_ranges(identity, 4);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 4);

    std::array<SelectionVector::Index, 5> selected = {0, 2, 3, 6, 6};
    SelectionVector external(selected.data(), 4);
    auto status = external.verify(3, 7);
    ASSERT_TRUE(status.ok()) << status;
    ranges = selection_to_ranges(external, 3);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].start, 0);
    EXPECT_EQ(ranges[0].length, 1);
    EXPECT_EQ(ranges[1].start, 2);
    EXPECT_EQ(ranges[1].length, 2);

    EXPECT_FALSE(external.verify(8, 7).ok());
    EXPECT_FALSE(external.verify(5, 7).ok());
    EXPECT_FALSE(external.verify(4, 6).ok());

    std::array<SelectionVector::Index, 3> duplicate = {0, 2, 2};
    SelectionVector non_strict(duplicate.data(), duplicate.size());
    EXPECT_FALSE(non_strict.verify(3, 5).ok());
    EXPECT_FALSE(identity.verify(1, -1).ok());
}

TEST(ParquetColumnReaderBaseTest, DefaultSelectUsesSkipReadRangesAndNestedConsumeIsExplicit) {
    DefaultSelectReader reader;
    std::array<SelectionVector::Index, 3> selected = {1, 3, 4};
    SelectionVector selection(selected.data(), selected.size());
    auto column = ColumnInt32::create();
    MutableColumnPtr mutable_column = std::move(column);
    auto status = reader.select(selection, selected.size(), 6, mutable_column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& values = assert_cast<const ColumnInt32&>(*mutable_column);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(1), 3);
    EXPECT_EQ(values.get_element(2), 4);
    EXPECT_EQ(reader.skip_ranges(), std::vector<int64_t>({1, 1, 1}));
    EXPECT_EQ(reader.read_ranges(), std::vector<int64_t>({1, 2}));

    BaseUnsupportedReader unsupported_reader;
    auto skip_status = unsupported_reader.skip(1);
    EXPECT_FALSE(skip_status.ok());
    EXPECT_NE(skip_status.to_string().find("skip is not implemented"), std::string::npos);
    EXPECT_FALSE(unsupported_reader.load_nested_batch(1).ok());
    int64_t values_read = 0;
    EXPECT_FALSE(unsupported_reader.build_nested_column(1, mutable_column, &values_read).ok());
    EXPECT_FALSE(unsupported_reader.consume_nested_column(1, &values_read).ok());

    NestedSkipReader nested_reader;
    auto nested_status = nested_reader.consume_nested_column(3, &values_read);
    ASSERT_TRUE(nested_status.ok()) << nested_status;
    EXPECT_EQ(values_read, 3);
}

TEST_F(ParquetColumnReaderTest, ScalarReadCoversRequiredNullableAllNullAndMultipleBatches) {
    read_and_validate(find_field_idx("int32_col"));
    read_and_validate(find_field_idx("string_col"));
    read_and_validate(find_field_idx("nullable_int_col"));
    read_and_validate(find_field_idx("all_null_int_col"));

    auto reader = create_reader(find_field_idx("int32_col"));
    auto column = reader->type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader->read(2, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 2);
    ASSERT_TRUE(reader->read(3, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 3);
    const auto& values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(values.get_element(0), 10);
    EXPECT_EQ(values.get_element(1), 20);
    EXPECT_EQ(values.get_element(2), 30);
    EXPECT_EQ(values.get_element(4), 50);
}

TEST_F(ParquetColumnReaderTest, ScalarSkipCoversZeroSomeAllAndNulls) {
    auto reader = create_reader(find_field_idx("int32_col"));
    ASSERT_TRUE(reader->skip(0).ok());
    auto column = reader->type()->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader->read(1, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 1);
    const auto& first_value = assert_cast<const ColumnInt32&>(*column);
    EXPECT_EQ(first_value.get_element(0), 10);

    reader = create_reader(find_field_idx("int32_col"));
    ASSERT_TRUE(reader->skip(2).ok());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->read(2, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 2);
    const auto& skipped_values = assert_cast<const ColumnInt32&>(*column);
    EXPECT_EQ(skipped_values.get_element(0), 30);
    EXPECT_EQ(skipped_values.get_element(1), 40);

    reader = create_reader(find_field_idx("int32_col"));
    ASSERT_TRUE(reader->skip(ROW_COUNT).ok());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->read(1, column, &rows_read).ok());
    EXPECT_EQ(rows_read, 0);
    EXPECT_EQ(column->size(), 0);

    reader = create_reader(find_field_idx("nullable_int_col"));
    ASSERT_TRUE(reader->skip(1).ok());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->read(2, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 2);
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 2);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
}

TEST_F(ParquetColumnReaderTest, ScalarSelectCoversAllDisjointSingleZeroThenReadAndNulls) {
    auto reader = create_reader(find_field_idx("int32_col"));
    SelectionVector all_selected(ROW_COUNT);
    auto column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(all_selected, ROW_COUNT, ROW_COUNT, column).ok());
    const auto& all_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(all_values.size(), ROW_COUNT);
    EXPECT_EQ(all_values.get_element(0), 10);
    EXPECT_EQ(all_values.get_element(4), 50);

    reader = create_reader(find_field_idx("int32_col"));
    std::array<SelectionVector::Index, 3> disjoint = {0, 2, 4};
    SelectionVector disjoint_selection(disjoint.data(), disjoint.size());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(disjoint_selection, disjoint.size(), ROW_COUNT, column).ok());
    const auto& disjoint_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(disjoint_values.size(), 3);
    EXPECT_EQ(disjoint_values.get_element(0), 10);
    EXPECT_EQ(disjoint_values.get_element(1), 30);
    EXPECT_EQ(disjoint_values.get_element(2), 50);

    reader = create_reader(find_field_idx("int32_col"));
    std::array<SelectionVector::Index, 1> single = {2};
    SelectionVector single_selection(single.data(), single.size());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(single_selection, single.size(), ROW_COUNT, column).ok());
    const auto& single_value = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(single_value.size(), 1);
    EXPECT_EQ(single_value.get_element(0), 30);

    reader = create_reader(find_field_idx("int32_col"));
    std::array<SelectionVector::Index, 2> first_last = {0, 4};
    SelectionVector first_last_selection(first_last.data(), first_last.size());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(first_last_selection, first_last.size(), ROW_COUNT, column).ok());
    const auto& first_last_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(first_last_values.size(), 2);
    EXPECT_EQ(first_last_values.get_element(0), 10);
    EXPECT_EQ(first_last_values.get_element(1), 50);

    reader = create_reader(find_field_idx("int32_col"));
    SelectionVector empty_selection;
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(empty_selection, 0, 2, column).ok());
    ASSERT_EQ(column->size(), 0);
    int64_t rows_read = 0;
    ASSERT_TRUE(reader->read(1, column, &rows_read).ok());
    ASSERT_EQ(rows_read, 1);
    const auto& after_empty_select = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(after_empty_select.size(), 1);
    EXPECT_EQ(after_empty_select.get_element(0), 30);

    reader = create_reader(find_field_idx("nullable_int_col"));
    std::array<SelectionVector::Index, 3> nullable_rows = {0, 1, 2};
    SelectionVector nullable_selection(nullable_rows.data(), nullable_rows.size());
    column = reader->type()->create_column();
    ASSERT_TRUE(reader->select(nullable_selection, nullable_rows.size(), ROW_COUNT, column).ok());
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 3);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
}

TEST_F(ParquetColumnReaderTest, FactoryRejectsInvalidScalarInputsAndNestedScalarProjection) {
    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;

    const auto& int_schema = *_fields[find_field_idx("int32_col")];
    ParquetColumnSchema invalid_leaf;
    invalid_leaf.kind = ParquetColumnSchemaKind::PRIMITIVE;
    invalid_leaf.name = "invalid_leaf";
    invalid_leaf.type = int_schema.type;
    invalid_leaf.type_descriptor = int_schema.type_descriptor;
    invalid_leaf.descriptor = int_schema.descriptor;
    invalid_leaf.leaf_column_id = _file_reader->metadata()->num_columns();
    auto status = factory.create(invalid_leaf, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid parquet leaf column id"), std::string::npos);

    ParquetColumnSchema null_descriptor;
    null_descriptor.kind = ParquetColumnSchemaKind::PRIMITIVE;
    null_descriptor.name = "null_descriptor";
    null_descriptor.type = int_schema.type;
    null_descriptor.type_descriptor = int_schema.type_descriptor;
    null_descriptor.leaf_column_id = int_schema.leaf_column_id;
    status = factory.create(null_descriptor, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("descriptor is null"), std::string::npos);

    const auto& list_element_schema =
            *_fields[find_field_idx("nullable_list_int_col")]->children[0];
    status = factory.create(list_element_schema, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("flat primitive columns"), std::string::npos);

    const auto& list_schema = *_fields[find_field_idx("nullable_list_int_col")];
    format::LocalColumnIndex projection =
            format::LocalColumnIndex::partial_local(list_schema.local_id);
    format::LocalColumnIndex element_projection =
            format::LocalColumnIndex::partial_local(list_element_schema.local_id);
    projection.children.push_back(std::move(element_projection));
    status = factory.create(list_schema, &projection, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("scalar projection is invalid"), std::string::npos);
}

TEST_F(ParquetColumnReaderTest, FactoryRejectsInvalidComplexProjections) {
    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;

    const auto& struct_schema = *_fields[find_field_idx("struct_col")];
    format::LocalColumnIndex struct_empty =
            format::LocalColumnIndex::partial_local(struct_schema.local_id);
    auto status = factory.create(struct_schema, &struct_empty, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no children"), std::string::npos);

    format::LocalColumnIndex struct_invalid =
            format::LocalColumnIndex::partial_local(struct_schema.local_id);
    struct_invalid.children.push_back(format::LocalColumnIndex::local(9999));
    status = factory.create(struct_schema, &struct_invalid, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains invalid child"), std::string::npos);

    const auto& list_schema = *_fields[find_field_idx("nullable_list_int_col")];
    format::LocalColumnIndex list_empty =
            format::LocalColumnIndex::partial_local(list_schema.local_id);
    status = factory.create(list_schema, &list_empty, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no element"), std::string::npos);

    const auto& map_schema = *_fields[find_field_idx("nullable_map_int_struct_col")];
    const auto& value_schema = *map_schema.children[1];
    format::LocalColumnIndex map_invalid =
            format::LocalColumnIndex::partial_local(map_schema.local_id);
    map_invalid.children.push_back(format::LocalColumnIndex::local(value_schema.local_id));
    map_invalid.children.push_back(format::LocalColumnIndex::local(9999));
    status = factory.create(map_schema, &map_invalid, &reader);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains invalid child"), std::string::npos);
}

TEST_F(ParquetColumnReaderTest, ReadSupportedComplexTypes) {
    read_and_validate(find_field_idx("struct_col"));
    read_and_validate(find_field_idx("nullable_struct_col"));
    read_and_validate(find_field_idx("nullable_struct_decimal_col"));
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
    read_and_validate(find_field_idx("nullable_list_map_int_string_col"));
    read_and_validate(find_field_idx("nullable_map_int_map_int_string_col"));
    read_and_validate(find_field_idx("nullable_list_struct_map_list_col"));
    read_and_validate(find_field_idx("nullable_map_int_list_map_int_string_col"));
}

TEST_F(ParquetColumnReaderTest, SkipThenRead) {
    auto reader = create_reader(find_field_idx("int32_col"));
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
    auto reader = create_reader(find_field_idx("int32_col"));
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

    format::LocalColumnIndex projection;
    projection.index = struct_schema.local_id;
    projection.project_all_children = false;
    format::LocalColumnIndex child_projection;
    child_projection.index = struct_schema.children[1]->local_id;
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
    const auto& values = get_nullable_nested_column<ColumnString>(struct_column.get_column(0));
    EXPECT_EQ(values.get_data_at(0).to_string(), "sa");
    EXPECT_EQ(values.get_data_at(4).to_string(), "se");
}

TEST_F(ParquetColumnReaderTest, ReadProjectedNullableStructChildren) {
    const auto field_idx = find_field_idx("nullable_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& struct_schema = *_fields[field_idx];
    ASSERT_EQ(struct_schema.name, "nullable_struct_col");
    ASSERT_EQ(struct_schema.children.size(), 2);

    format::LocalColumnIndex projection;
    projection.index = struct_schema.local_id;
    projection.project_all_children = false;
    format::LocalColumnIndex child_projection;
    child_projection.index = struct_schema.children[1]->local_id;
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

    format::LocalColumnIndex projection;
    projection.index = list_schema.local_id;
    projection.project_all_children = false;
    format::LocalColumnIndex element_projection;
    element_projection.index = element_schema.local_id;
    element_projection.project_all_children = false;
    format::LocalColumnIndex child_projection;
    child_projection.index = element_schema.children[1]->local_id;
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
    ASSERT_EQ(map_schema.children.size(), 2);
    const auto& value_schema = *map_schema.children[1];
    ASSERT_EQ(value_schema.children.size(), 2);

    format::LocalColumnIndex projection;
    projection.index = map_schema.local_id;
    projection.project_all_children = false;
    format::LocalColumnIndex value_projection;
    value_projection.index = value_schema.local_id;
    value_projection.project_all_children = false;
    format::LocalColumnIndex child_projection;
    child_projection.index = value_schema.children[1]->local_id;
    value_projection.children.push_back(std::move(child_projection));
    projection.children.push_back(std::move(value_projection));

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
    const auto& keys = get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
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

TEST_F(ParquetColumnReaderTest, AllowsMapKeyWithValueProjection) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& map_schema = *_fields[field_idx];
    ASSERT_EQ(map_schema.children.size(), 2);
    const auto& key_schema = *map_schema.children[0];
    const auto& value_schema = *map_schema.children[1];

    auto projection = format::LocalColumnIndex::partial_local(map_schema.local_id);
    projection.children.push_back(format::LocalColumnIndex::local(key_schema.local_id));
    projection.children.push_back(format::LocalColumnIndex::local(value_schema.local_id));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    const auto st = factory.create(map_schema, &projection, &reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(reader, nullptr);
}

TEST_F(ParquetColumnReaderTest, RejectMapKeyOnlyProjection) {
    const auto field_idx = find_field_idx("nullable_map_int_struct_col");
    ASSERT_LT(field_idx, _fields.size());
    const auto& map_schema = *_fields[field_idx];
    ASSERT_EQ(map_schema.children.size(), 2);
    const auto& key_schema = *map_schema.children[0];

    auto projection = format::LocalColumnIndex::partial_local(map_schema.local_id);
    projection.children.push_back(format::LocalColumnIndex::local(key_schema.local_id));

    ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
    std::unique_ptr<ParquetColumnReader> reader;
    const auto st = factory.create(map_schema, &projection, &reader);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("contains no value"), std::string::npos);
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
    const auto& keys = get_nullable_nested_column<ColumnInt32>(kv_map.get_keys());
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

TEST_F(ParquetColumnReaderTest, NullableStructUsesListChildAsShapeSource) {
    const auto field_idx = find_field_idx("nullable_struct_list_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
}

TEST_F(ParquetColumnReaderTest, NullableStructUsesMapChildAsShapeSource) {
    const auto field_idx = find_field_idx("nullable_struct_map_col");
    auto reader = create_projected_child_reader(field_idx, 1);
    ASSERT_NE(reader, nullptr);

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
}

TEST_F(ParquetColumnReaderTest, NullableStructUsesNestedStructComplexChildAsShapeSource) {
    const auto field_idx = find_field_idx("nullable_struct_nested_struct_list_col");
    auto reader = create_projected_grandchild_reader(field_idx, 0, 0);
    ASSERT_NE(reader, nullptr);

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(ROW_COUNT, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, ROW_COUNT);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));

    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& nested_nullable = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    EXPECT_FALSE(nested_nullable.is_null_at(0));
    EXPECT_TRUE(nested_nullable.is_null_at(2));
    EXPECT_FALSE(nested_nullable.is_null_at(3));
    EXPECT_FALSE(nested_nullable.is_null_at(4));
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
    const auto& keys = get_nullable_nested_column<ColumnInt32>(kv_map.get_keys());
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
    const auto& a_values = get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
    const auto& a_values = get_nullable_nested_column<ColumnInt32>(struct_column.get_column(0));
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
    const auto& keys = get_nullable_nested_column<ColumnInt32>(kv_map.get_keys());
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

TEST_F(ParquetColumnReaderTest, SkipPlainBinaryMapThenReadResetsArrowBuilder) {
    const auto field_idx = find_field_idx("nullable_map_int_string_col");
    auto reader = create_plain_reader(field_idx);

    // Row 0 contains two STRING values. The levels-only skip must release (and discard) those
    // Arrow BinaryRecordReader builder chunks before the next normal read. If they leak into the
    // next batch, ParquetLeafReader observes more values than current definition/repetition levels.
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
    ASSERT_EQ(map_column.get_offsets().size(), 3);
    EXPECT_EQ(map_column.get_offsets()[0], 0);
    EXPECT_EQ(map_column.get_offsets()[1], 0);
    EXPECT_EQ(map_column.get_offsets()[2], 1);
    const auto& values = get_nullable_nested_column<ColumnString>(map_column.get_values());
    ASSERT_EQ(values.size(), 1);
    EXPECT_EQ(values.get_data_at(0).to_string(), "cc");
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

TEST_F(ParquetColumnReaderTest, ReadDeepListStructMapListAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_list_struct_map_list_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(1, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 1);
    st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipDeepListStructMapListThenRead) {
    const auto field_idx = find_field_idx("nullable_list_struct_map_list_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(4, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 4);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 4);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));

    const auto& outer_array = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_array.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 4);
    EXPECT_EQ(outer_offsets[0], 0);
    EXPECT_EQ(outer_offsets[1], 0);
    EXPECT_EQ(outer_offsets[2], 2);
    EXPECT_EQ(outer_offsets[3], 3);

    const auto& struct_values = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(struct_values.size(), 3);
    EXPECT_FALSE(struct_values.is_null_at(0));
    EXPECT_FALSE(struct_values.is_null_at(1));
    EXPECT_FALSE(struct_values.is_null_at(2));
    const auto& struct_column = assert_cast<const ColumnStruct&>(struct_values.get_nested_column());
    const auto& map_values = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(map_values.size(), 3);
    EXPECT_TRUE(map_values.is_null_at(0));
    EXPECT_FALSE(map_values.is_null_at(1));
    EXPECT_FALSE(map_values.is_null_at(2));

    const auto& map_column = assert_cast<const ColumnMap&>(map_values.get_nested_column());
    const auto& map_offsets = map_column.get_offsets();
    ASSERT_EQ(map_offsets.size(), 3);
    EXPECT_EQ(map_offsets[0], 0);
    EXPECT_EQ(map_offsets[1], 0);
    EXPECT_EQ(map_offsets[2], 2);
    const auto& keys = get_nullable_nested_column<ColumnInt32>(map_column.get_keys());
    ASSERT_EQ(keys.size(), 2);
    EXPECT_EQ(keys.get_element(0), 3);
    EXPECT_EQ(keys.get_element(1), 4);
    const auto& lists = assert_cast<const ColumnNullable&>(map_column.get_values());
    ASSERT_EQ(lists.size(), 2);
    EXPECT_TRUE(lists.is_null_at(0));
    EXPECT_FALSE(lists.is_null_at(1));
    const auto& list_column = assert_cast<const ColumnArray&>(lists.get_nested_column());
    const auto& list_offsets = list_column.get_offsets();
    ASSERT_EQ(list_offsets.size(), 2);
    EXPECT_EQ(list_offsets[0], 0);
    EXPECT_EQ(list_offsets[1], 1);
}

TEST_F(ParquetColumnReaderTest, SelectDeepListStructMapList) {
    const auto field_idx = find_field_idx("nullable_list_struct_map_list_col");
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
    EXPECT_EQ(outer_offsets[0], 2);
    EXPECT_EQ(outer_offsets[1], 4);
    EXPECT_EQ(outer_offsets[2], 5);

    const auto& struct_values = assert_cast<const ColumnNullable&>(outer_array.get_data());
    ASSERT_EQ(struct_values.size(), 5);
    EXPECT_FALSE(struct_values.is_null_at(0));
    EXPECT_TRUE(struct_values.is_null_at(1));
    EXPECT_FALSE(struct_values.is_null_at(2));
    EXPECT_FALSE(struct_values.is_null_at(3));
    EXPECT_FALSE(struct_values.is_null_at(4));
    const auto& struct_column = assert_cast<const ColumnStruct&>(struct_values.get_nested_column());
    const auto& map_values = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    ASSERT_EQ(map_values.size(), 5);
    EXPECT_FALSE(map_values.is_null_at(0));
    EXPECT_TRUE(map_values.is_null_at(1));
    EXPECT_TRUE(map_values.is_null_at(2));
    EXPECT_FALSE(map_values.is_null_at(3));
    EXPECT_FALSE(map_values.is_null_at(4));
    const auto& map_column = assert_cast<const ColumnMap&>(map_values.get_nested_column());
    const auto& map_offsets = map_column.get_offsets();
    ASSERT_EQ(map_offsets.size(), 5);
    EXPECT_EQ(map_offsets[0], 2);
    EXPECT_EQ(map_offsets[1], 2);
    EXPECT_EQ(map_offsets[2], 2);
    EXPECT_EQ(map_offsets[3], 2);
    EXPECT_EQ(map_offsets[4], 4);
}

TEST_F(ParquetColumnReaderTest, ReadDeepMapListMapAcrossChunks) {
    const auto field_idx = find_field_idx("nullable_map_int_list_map_int_string_col");
    auto reader = create_reader(field_idx);
    MutableColumnPtr column = reader->type()->create_column();

    int64_t rows_read = 0;
    auto st = reader->read(1, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 1);
    st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);
    st = reader->read(2, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 2);

    _expected_by_field[field_idx](*_fields[field_idx], *column);
}

TEST_F(ParquetColumnReaderTest, SkipDeepMapListMapThenRead) {
    const auto field_idx = find_field_idx("nullable_map_int_list_map_int_string_col");
    auto reader = create_reader(field_idx);
    auto st = reader->skip(1);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    st = reader->read(4, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 4);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable_column.size(), 4);
    EXPECT_TRUE(nullable_column.is_null_at(0));
    EXPECT_FALSE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_FALSE(nullable_column.is_null_at(3));
    const auto& outer_map = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_map.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 4);
    EXPECT_EQ(outer_offsets[0], 0);
    EXPECT_EQ(outer_offsets[1], 0);
    EXPECT_EQ(outer_offsets[2], 2);
    EXPECT_EQ(outer_offsets[3], 3);
    const auto& outer_keys = get_nullable_nested_column<ColumnInt32>(outer_map.get_keys());
    ASSERT_EQ(outer_keys.size(), 3);
    EXPECT_EQ(outer_keys.get_element(0), 30);
    EXPECT_EQ(outer_keys.get_element(1), 40);
    EXPECT_EQ(outer_keys.get_element(2), 50);

    const auto& lists = assert_cast<const ColumnNullable&>(outer_map.get_values());
    ASSERT_EQ(lists.size(), 3);
    EXPECT_TRUE(lists.is_null_at(0));
    EXPECT_FALSE(lists.is_null_at(1));
    EXPECT_FALSE(lists.is_null_at(2));
    const auto& list_column = assert_cast<const ColumnArray&>(lists.get_nested_column());
    const auto& list_offsets = list_column.get_offsets();
    ASSERT_EQ(list_offsets.size(), 3);
    EXPECT_EQ(list_offsets[0], 0);
    EXPECT_EQ(list_offsets[1], 1);
    EXPECT_EQ(list_offsets[2], 3);
    const auto& inner_maps = assert_cast<const ColumnNullable&>(list_column.get_data());
    ASSERT_EQ(inner_maps.size(), 3);
    EXPECT_FALSE(inner_maps.is_null_at(0));
    EXPECT_TRUE(inner_maps.is_null_at(1));
    EXPECT_FALSE(inner_maps.is_null_at(2));
}

TEST_F(ParquetColumnReaderTest, SelectDeepMapListMap) {
    const auto field_idx = find_field_idx("nullable_map_int_list_map_int_string_col");
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
    const auto& outer_map = assert_cast<const ColumnMap&>(nullable_column.get_nested_column());
    const auto& outer_offsets = outer_map.get_offsets();
    ASSERT_EQ(outer_offsets.size(), 3);
    EXPECT_EQ(outer_offsets[0], 2);
    EXPECT_EQ(outer_offsets[1], 4);
    EXPECT_EQ(outer_offsets[2], 5);
    const auto& outer_keys = get_nullable_nested_column<ColumnInt32>(outer_map.get_keys());
    ASSERT_EQ(outer_keys.size(), 5);
    EXPECT_EQ(outer_keys.get_element(0), 10);
    EXPECT_EQ(outer_keys.get_element(1), 20);
    EXPECT_EQ(outer_keys.get_element(2), 30);
    EXPECT_EQ(outer_keys.get_element(3), 40);
    EXPECT_EQ(outer_keys.get_element(4), 50);

    const auto& lists = assert_cast<const ColumnNullable&>(outer_map.get_values());
    ASSERT_EQ(lists.size(), 5);
    EXPECT_FALSE(lists.is_null_at(0));
    EXPECT_FALSE(lists.is_null_at(1));
    EXPECT_TRUE(lists.is_null_at(2));
    EXPECT_FALSE(lists.is_null_at(3));
    EXPECT_FALSE(lists.is_null_at(4));
    const auto& list_column = assert_cast<const ColumnArray&>(lists.get_nested_column());
    const auto& list_offsets = list_column.get_offsets();
    ASSERT_EQ(list_offsets.size(), 5);
    EXPECT_EQ(list_offsets[0], 3);
    EXPECT_EQ(list_offsets[1], 3);
    EXPECT_EQ(list_offsets[2], 3);
    EXPECT_EQ(list_offsets[3], 4);
    EXPECT_EQ(list_offsets[4], 6);
}

} // namespace
} // namespace doris::format::parquet
