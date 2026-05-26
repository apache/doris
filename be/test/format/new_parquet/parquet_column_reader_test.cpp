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
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "format/new_parquet/column_reader.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/selection_vector.h"

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
            const std::shared_ptr<arrow::DataType>& type,
            const std::vector<std::string>& values) {
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
            const std::shared_ptr<arrow::DataType>& type,
            const std::vector<int64_t>& values) {
        arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
        for (const auto value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_decimal_array(
            const std::shared_ptr<arrow::DataType>& type,
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
        add_field(arrow::field("bool_col", arrow::boolean(), false),
                  build_required_array<arrow::BooleanBuilder, bool>(
                          {true, false, true, false, true}),
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
        add_field(arrow::field("float_col", arrow::float32(), false),
                  build_required_array<arrow::FloatBuilder, float>(
                          {1.5F, -2.25F, 3.0F, 4.5F, 5.75F}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FLOAT);
                      const auto& values = assert_cast<const ColumnFloat32&>(column);
                      EXPECT_FLOAT_EQ(values.get_element(0), 1.5F);
                      EXPECT_FLOAT_EQ(values.get_element(1), -2.25F);
                  });
        add_field(arrow::field("double_col", arrow::float64(), false),
                  build_required_array<arrow::DoubleBuilder, double>(
                          {3.5, -4.75, 6.0, 7.25, 8.5}),
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
        add_field(arrow::field("timestamp_millis_col",
                               arrow::timestamp(arrow::TimeUnit::MILLI), false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MILLI),
                                        {0, 1234, 1609459200000, 1609459201000, -1}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(),
                                TYPE_DATETIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1),
                                "1970-01-01 00:00:01.234");
                      EXPECT_EQ(schema.type->to_string(column, 4),
                                "1969-12-31 23:59:59.999");
                  });
        add_field(arrow::field("timestamp_micros_col",
                               arrow::timestamp(arrow::TimeUnit::MICRO), false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO),
                                        {0, 1234567, 1609459200000000, 1609459201000000, -1}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                      EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(),
                                TYPE_DATETIMEV2);
                      EXPECT_EQ(schema.type->to_string(column, 1),
                                "1970-01-01 00:00:01.234567");
                      EXPECT_EQ(schema.type->to_string(column, 4),
                                "1969-12-31 23:59:59.999999");
                  });
        add_field(arrow::field("decimal_fixed_binary_9_2_col", arrow::decimal128(9, 2), false),
                  build_decimal_array(arrow::decimal128(9, 2), {12345, -67, 0, 987, 1000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type,
                                ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
                      EXPECT_TRUE(schema.type_descriptor.is_decimal);
                      const auto& values = assert_cast<const ColumnDecimal128V3&>(column);
                      EXPECT_EQ(values.get_element(0), Decimal128V3(12345));
                      EXPECT_EQ(schema.type->to_string(column, 0), "123.45");
                  });
        add_field(arrow::field("decimal_fixed_binary_18_6_col", arrow::decimal128(18, 6), false),
                  build_decimal_array(arrow::decimal128(18, 6),
                                      {1234567, -670000, 0, 9870000, 1000000}),
                  [](const ParquetColumnSchema& schema, const IColumn& column) {
                      EXPECT_EQ(schema.type_descriptor.physical_type,
                                ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
                      EXPECT_TRUE(schema.type_descriptor.is_decimal);
                      const auto& values = assert_cast<const ColumnDecimal128V3&>(column);
                      EXPECT_EQ(values.get_element(0), Decimal128V3(1234567));
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

        auto schema = arrow::schema(_arrow_fields);
        auto table = arrow::Table::Make(schema, _arrays);

        auto file_result = arrow::io::FileOutputStream::Open(_file_path);
        ASSERT_TRUE(file_result.ok()) << file_result.status();
        std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

        ::parquet::WriterProperties::Builder builder;
        builder.version(::parquet::ParquetVersion::PARQUET_2_6);
        builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
        builder.compression(::parquet::Compression::UNCOMPRESSED);
        PARQUET_THROW_NOT_OK(
                ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, ROW_COUNT,
                                             builder.build()));
    }

    std::unique_ptr<ParquetColumnReader> create_reader(size_t field_idx) const {
        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(*_fields[field_idx], &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    void read_and_validate(size_t field_idx) const {
        auto reader = create_reader(field_idx);
        MutableColumnPtr column = reader->type()->create_column();
        int64_t rows_read = 0;
        auto st = reader->read(ROW_COUNT, column, &rows_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(rows_read, ROW_COUNT);
        ASSERT_EQ(column->size(), ROW_COUNT);
        _expected_by_field[field_idx](*_fields[field_idx], *column);
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
        ASSERT_TRUE(supports_record_reader(_fields[field_idx]->type_descriptor));
        read_and_validate(field_idx);
    }
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

TEST_F(ParquetColumnReaderTest, ResolveSupportedPhysicalAndLogicalSchemas) {
    std::vector<::parquet::schema::NodePtr> nodes = {
            ::parquet::schema::PrimitiveNode::Make(
                    "required_bool", ::parquet::Repetition::REQUIRED, ::parquet::Type::BOOLEAN),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_int64", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_float", ::parquet::Repetition::REQUIRED, ::parquet::Type::FLOAT),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_double", ::parquet::Repetition::REQUIRED, ::parquet::Type::DOUBLE),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY),
            ::parquet::schema::PrimitiveNode::Make(
                    "required_fixed_binary", ::parquet::Repetition::REQUIRED,
                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::NONE, 4),
            ::parquet::schema::PrimitiveNode::Make(
                    "optional_int32", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32),
            ::parquet::schema::PrimitiveNode::Make(
                    "utf8_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::UTF8),
            ::parquet::schema::PrimitiveNode::Make(
                    "enum_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::ENUM),
            ::parquet::schema::PrimitiveNode::Make(
                    "json_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::JSON),
            ::parquet::schema::PrimitiveNode::Make(
                    "bson_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::BSON),
            ::parquet::schema::PrimitiveNode::Make(
                    "decimal_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::DECIMAL, -1, 9, 2),
            ::parquet::schema::PrimitiveNode::Make(
                    "decimal_int64", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
                    ::parquet::ConvertedType::DECIMAL, -1, 18, 6),
            ::parquet::schema::PrimitiveNode::Make(
                    "decimal_binary", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY,
                    ::parquet::ConvertedType::DECIMAL, -1, 18, 6),
            ::parquet::schema::PrimitiveNode::Make(
                    "decimal_fixed_binary", ::parquet::Repetition::REQUIRED,
                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::DECIMAL, 8,
                    18, 6),
            ::parquet::schema::PrimitiveNode::Make(
                    "date_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
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
            ::parquet::schema::PrimitiveNode::Make(
                    "int8_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::INT_8),
            ::parquet::schema::PrimitiveNode::Make(
                    "uint8_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::UINT_8),
            ::parquet::schema::PrimitiveNode::Make(
                    "int16_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::INT_16),
            ::parquet::schema::PrimitiveNode::Make(
                    "uint16_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::UINT_16),
            ::parquet::schema::PrimitiveNode::Make(
                    "int32_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::INT_32),
            ::parquet::schema::PrimitiveNode::Make(
                    "uint32_int32", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32,
                    ::parquet::ConvertedType::UINT_32),
            ::parquet::schema::PrimitiveNode::Make(
                    "int64_int64", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
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
                    ::parquet::schema::PrimitiveNode::Make(
                            "repeated_int32_col", ::parquet::Repetition::REPEATED,
                            ::parquet::Type::INT32),
                    ::parquet::schema::PrimitiveNode::Make(
                            "decimal256_fixed_col", ::parquet::Repetition::REQUIRED,
                            ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::DECIMAL,
                            20, 39, 6),
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
