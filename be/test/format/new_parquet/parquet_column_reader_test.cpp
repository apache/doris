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
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "format/new_parquet/column_reader.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/selection_vector.h"

namespace doris::parquet {
namespace {

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
        ASSERT_EQ(_fields.size(), 4);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    static void append_int32(arrow::Int32Builder* builder, const std::vector<int32_t>& values) {
        for (const auto value : values) {
            ASSERT_TRUE(builder->Append(value).ok());
        }
    }

    void write_parquet_file() {
        arrow::Int32Builder int_builder;
        append_int32(&int_builder, {10, 20, 30, 40, 50});
        std::shared_ptr<arrow::Array> int_array;
        ASSERT_TRUE(int_builder.Finish(&int_array).ok());

        arrow::StringBuilder string_builder;
        ASSERT_TRUE(string_builder.Append("alpha").ok());
        ASSERT_TRUE(string_builder.Append("beta").ok());
        ASSERT_TRUE(string_builder.Append("gamma").ok());
        ASSERT_TRUE(string_builder.Append("delta").ok());
        ASSERT_TRUE(string_builder.Append("epsilon").ok());
        std::shared_ptr<arrow::Array> string_array;
        ASSERT_TRUE(string_builder.Finish(&string_array).ok());

        arrow::Int32Builder nullable_builder;
        ASSERT_TRUE(nullable_builder.Append(1).ok());
        ASSERT_TRUE(nullable_builder.AppendNull().ok());
        ASSERT_TRUE(nullable_builder.Append(3).ok());
        ASSERT_TRUE(nullable_builder.AppendNull().ok());
        ASSERT_TRUE(nullable_builder.Append(5).ok());
        std::shared_ptr<arrow::Array> nullable_array;
        ASSERT_TRUE(nullable_builder.Finish(&nullable_array).ok());

        arrow::BooleanBuilder bool_builder;
        ASSERT_TRUE(bool_builder.Append(true).ok());
        ASSERT_TRUE(bool_builder.Append(false).ok());
        ASSERT_TRUE(bool_builder.Append(true).ok());
        ASSERT_TRUE(bool_builder.Append(false).ok());
        ASSERT_TRUE(bool_builder.Append(true).ok());
        std::shared_ptr<arrow::Array> bool_array;
        ASSERT_TRUE(bool_builder.Finish(&bool_array).ok());

        auto schema = arrow::schema({
                arrow::field("int_col", arrow::int32(), false),
                arrow::field("string_col", arrow::utf8(), false),
                arrow::field("nullable_int_col", arrow::int32(), true),
                arrow::field("bool_col", arrow::boolean(), false),
        });
        auto table =
                arrow::Table::Make(schema, {int_array, string_array, nullable_array, bool_array});

        auto file_result = arrow::io::FileOutputStream::Open(_file_path);
        ASSERT_TRUE(file_result.ok()) << file_result.status();
        std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

        ::parquet::WriterProperties::Builder builder;
        builder.version(::parquet::ParquetVersion::PARQUET_2_6);
        builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
        builder.compression(::parquet::Compression::UNCOMPRESSED);
        PARQUET_THROW_NOT_OK(
                ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 5,
                                             builder.build()));
    }

    std::unique_ptr<ParquetColumnReader> create_reader(size_t field_idx) const {
        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(*_fields[field_idx], &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
    std::unique_ptr<::parquet::ParquetFileReader> _file_reader;
    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    std::vector<std::unique_ptr<ParquetColumnSchema>> _fields;
};

TEST_F(ParquetColumnReaderTest, ReadPrimitiveColumnsFromParquetFile) {
    auto int_reader = create_reader(0);
    MutableColumnPtr int_column = int_reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = int_reader->read(5, int_column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 5);

    const auto& int_values = assert_cast<const ColumnInt32&>(*int_column);
    ASSERT_EQ(int_values.size(), 5);
    EXPECT_EQ(int_values.get_element(0), 10);
    EXPECT_EQ(int_values.get_element(4), 50);

    auto string_reader = create_reader(1);
    MutableColumnPtr string_column = string_reader->type()->create_column();
    st = string_reader->read(5, string_column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 5);

    const auto& string_values = assert_cast<const ColumnString&>(*string_column);
    ASSERT_EQ(string_values.size(), 5);
    EXPECT_EQ(string_values.get_data_at(0).to_string(), "alpha");
    EXPECT_EQ(string_values.get_data_at(3).to_string(), "delta");

    auto bool_reader = create_reader(3);
    MutableColumnPtr bool_column = bool_reader->type()->create_column();
    st = bool_reader->read(5, bool_column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 5);

    const auto& bool_values = assert_cast<const ColumnBool&>(*bool_column);
    ASSERT_EQ(bool_values.size(), 5);
    EXPECT_EQ(bool_values.get_element(0), 1);
    EXPECT_EQ(bool_values.get_element(1), 0);
}

TEST_F(ParquetColumnReaderTest, ReadNullableColumnFromParquetFile) {
    auto reader = create_reader(2);
    MutableColumnPtr column = reader->type()->create_column();
    int64_t rows_read = 0;
    auto st = reader->read(5, column, &rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(rows_read, 5);

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    const auto& nested_column = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    ASSERT_EQ(nullable_column.size(), 5);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    EXPECT_TRUE(nullable_column.is_null_at(1));
    EXPECT_FALSE(nullable_column.is_null_at(2));
    EXPECT_TRUE(nullable_column.is_null_at(3));
    EXPECT_FALSE(nullable_column.is_null_at(4));
    EXPECT_EQ(nested_column.get_element(0), 1);
    EXPECT_EQ(nested_column.get_element(2), 3);
    EXPECT_EQ(nested_column.get_element(4), 5);
}

TEST_F(ParquetColumnReaderTest, SkipThenRead) {
    auto reader = create_reader(0);
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
    auto reader = create_reader(0);
    SelectionVector selection(3);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);

    MutableColumnPtr column = reader->type()->create_column();
    auto st = reader->select(selection, 3, 5, column);
    ASSERT_TRUE(st.ok()) << st;

    const auto& int_values = assert_cast<const ColumnInt32&>(*column);
    ASSERT_EQ(int_values.size(), 3);
    EXPECT_EQ(int_values.get_element(0), 10);
    EXPECT_EQ(int_values.get_element(1), 30);
    EXPECT_EQ(int_values.get_element(2), 50);
}

} // namespace
} // namespace doris::parquet
