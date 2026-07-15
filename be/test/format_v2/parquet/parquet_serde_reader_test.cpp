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

#include <cmath>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace doris::format::parquet {
namespace {

constexpr int64_t ROW_COUNT = 5;

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

class ParquetSerdeReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_parquet_serde_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "serde.parquet").string();
        write_parquet_file();
        open_file(_file_path);
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

    std::shared_ptr<arrow::Array> build_nullable_int32_array() {
        arrow::Int32Builder builder;
        EXPECT_TRUE(builder.Append(1).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(3).ok());
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(5).ok());
        return finish_array(&builder);
    }

    std::shared_ptr<arrow::Array> build_nullable_float16_array() {
        arrow::HalfFloatBuilder builder;
        EXPECT_TRUE(builder.AppendNull().ok());
        EXPECT_TRUE(builder.Append(0x0000).ok());
        EXPECT_TRUE(builder.Append(0x8000).ok());
        EXPECT_TRUE(builder.Append(0x3E00).ok());
        EXPECT_TRUE(builder.Append(0x7E00).ok());
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

    std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
        arrow::StringBuilder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
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

    void add_field(const std::shared_ptr<arrow::Field>& field,
                   std::shared_ptr<arrow::Array> array) {
        _arrow_fields.push_back(field);
        _arrays.push_back(std::move(array));
    }

    void write_table(const std::string& file_path, const std::shared_ptr<arrow::Table>& table,
                     std::shared_ptr<::parquet::ArrowWriterProperties> arrow_properties = nullptr) {
        auto file_result = arrow::io::FileOutputStream::Open(file_path);
        ASSERT_TRUE(file_result.ok()) << file_result.status();
        ::parquet::WriterProperties::Builder writer_builder;
        writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
        writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
        writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
        if (arrow_properties == nullptr) {
            ::parquet::ArrowWriterProperties::Builder arrow_builder;
            arrow_properties = arrow_builder.build();
        }
        PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
                *table, arrow::default_memory_pool(), *file_result, ROW_COUNT,
                writer_builder.build(), std::move(arrow_properties)));
    }

    void write_parquet_file() {
        add_field(arrow::field("bool_col", arrow::boolean(), false),
                  build_required_array<arrow::BooleanBuilder, bool>(
                          {true, false, true, false, true}));
        add_field(arrow::field("int32_col", arrow::int32(), false),
                  build_required_array<arrow::Int32Builder, int32_t>({10, 20, 30, 40, 50}));
        add_field(arrow::field("int64_col", arrow::int64(), false),
                  build_required_array<arrow::Int64Builder, int64_t>(
                          {10000000000L, -9L, 42L, 77L, 123L}));
        add_field(arrow::field("uint32_col", arrow::uint32(), false),
                  build_required_array<arrow::UInt32Builder, uint32_t>(
                          {0U, 1U, 1U << 31, std::numeric_limits<uint32_t>::max(), 42U}));
        add_field(arrow::field("uint64_col", arrow::uint64(), false),
                  build_required_array<arrow::UInt64Builder, uint64_t>(
                          {0ULL, 1ULL, 1ULL << 63, std::numeric_limits<uint64_t>::max(), 42ULL}));
        add_field(arrow::field("float_col", arrow::float32(), false),
                  build_required_array<arrow::FloatBuilder, float>(
                          {1.5F, -2.25F, 3.0F, 4.5F, 5.75F}));
        add_field(arrow::field("double_col", arrow::float64(), false),
                  build_required_array<arrow::DoubleBuilder, double>({3.5, -4.75, 6.0, 7.25, 8.5}));
        add_field(arrow::field("nullable_float16_col", arrow::float16(), true),
                  build_nullable_float16_array());
        add_field(arrow::field("binary_col", arrow::binary(), false),
                  build_binary_array({"bin_a", "bin_b", "bin_c", "bin_d", "bin_e"}));
        add_field(arrow::field("string_col", arrow::utf8(), false),
                  build_string_array({"alpha", "beta", "gamma", "delta", "epsilon"}));
        add_field(arrow::field("fixed_binary_col", arrow::fixed_size_binary(4), false),
                  build_fixed_binary_array(arrow::fixed_size_binary(4),
                                           {"aaaa", "bbbb", "cccc", "dddd", "eeee"}));
        add_field(arrow::field("date_col", arrow::date32(), false),
                  build_required_array<arrow::Date32Builder, int32_t>({0, 1, 18628, 18629, 18630}));
        add_field(arrow::field("timestamp_millis_col", arrow::timestamp(arrow::TimeUnit::MILLI),
                               false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MILLI),
                                        {0, 1234, 1609459200000, 1609459201000, -1}));
        add_field(arrow::field("timestamp_micros_col", arrow::timestamp(arrow::TimeUnit::MICRO),
                               false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO),
                                        {0, 1234567, 1609459200000000, 1609459201000000, -1}));
        add_field(arrow::field("timestamp_micros_utc_col",
                               arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false),
                  build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                        {0, 1234567, 1609459200000000, 1609459201000000, -1}));
        add_field(arrow::field("decimal_fixed_binary_9_2_col", arrow::decimal128(9, 2), false),
                  build_decimal_array(arrow::decimal128(9, 2), {12345, -67, 0, 987, 1000}));
        add_field(arrow::field("decimal_fixed_binary_18_6_col", arrow::decimal128(18, 6), false),
                  build_decimal_array(arrow::decimal128(18, 6),
                                      {1234567, -670000, 0, 9870000, 1000000}));
        add_field(arrow::field("nullable_int_col", arrow::int32(), true),
                  build_nullable_int32_array());

        write_table(_file_path, arrow::Table::Make(arrow::schema(_arrow_fields), _arrays));
    }

    void open_file(const std::string& file_path) {
        _file_reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
        ASSERT_NE(_file_reader, nullptr);
        ASSERT_EQ(_file_reader->metadata()->num_row_groups(), 1);
        _row_group = _file_reader->RowGroup(0);
        ASSERT_NE(_row_group, nullptr);
        auto schema_descriptor = _file_reader->metadata()->schema();
        ASSERT_NE(schema_descriptor, nullptr);
        auto st = build_parquet_column_schema(*schema_descriptor, &_fields);
        ASSERT_TRUE(st.ok()) << st;
    }

    size_t find_field_idx(const std::string& name) const {
        for (size_t field_idx = 0; field_idx < _fields.size(); ++field_idx) {
            if (_fields[field_idx]->name == name) {
                return field_idx;
            }
        }
        ADD_FAILURE() << "Cannot find parquet serde test field " << name;
        return _fields.size();
    }

    std::unique_ptr<ParquetColumnReader> create_reader(size_t field_idx) const {
        ParquetColumnReaderFactory factory(_row_group, _file_reader->metadata()->num_columns());
        std::unique_ptr<ParquetColumnReader> reader;
        auto st = factory.create(*_fields[field_idx], &reader);
        EXPECT_TRUE(st.ok()) << st;
        return reader;
    }

    template <typename Validator>
    void read_and_validate(const std::string& name, Validator validator) const {
        const auto field_idx = find_field_idx(name);
        ASSERT_TRUE(supports_record_reader(_fields[field_idx]->type_descriptor));
        auto reader = create_reader(field_idx);
        ASSERT_NE(reader, nullptr);
        MutableColumnPtr column = reader->type()->create_column();
        int64_t rows_read = 0;
        auto st = reader->read(ROW_COUNT, column, &rows_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(rows_read, ROW_COUNT);
        ASSERT_EQ(column->size(), ROW_COUNT);
        validator(*_fields[field_idx], *column);
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
    std::unique_ptr<::parquet::ParquetFileReader> _file_reader;
    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    std::vector<std::unique_ptr<ParquetColumnSchema>> _fields;
    std::vector<std::shared_ptr<arrow::Field>> _arrow_fields;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

TEST_F(ParquetSerdeReaderTest, ReadAllSupportedPhysicalAndLogicalTypes) {
    read_and_validate("bool_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::BOOLEAN);
        const auto& values = assert_cast<const ColumnBool&>(column);
        EXPECT_EQ(values.get_element(0), 1);
        EXPECT_EQ(values.get_element(1), 0);
        EXPECT_EQ(values.get_element(4), 1);
    });
    read_and_validate("int32_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
        const auto& values = assert_cast<const ColumnInt32&>(column);
        EXPECT_EQ(values.get_element(0), 10);
        EXPECT_EQ(values.get_element(4), 50);
    });
    read_and_validate("int64_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
        const auto& values = assert_cast<const ColumnInt64&>(column);
        EXPECT_EQ(values.get_element(0), 10000000000L);
        EXPECT_EQ(values.get_element(1), -9L);
    });
    read_and_validate("uint32_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
        EXPECT_TRUE(schema.type_descriptor.is_unsigned_integer);
        EXPECT_EQ(schema.type_descriptor.integer_bit_width, 32);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_BIGINT);
        const auto& values = assert_cast<const ColumnInt64&>(column);
        EXPECT_EQ(values.get_element(2), 2147483648L);
        EXPECT_EQ(values.get_element(3),
                  static_cast<int64_t>(std::numeric_limits<uint32_t>::max()));
    });
    read_and_validate("uint64_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
        EXPECT_TRUE(schema.type_descriptor.is_unsigned_integer);
        EXPECT_EQ(schema.type_descriptor.integer_bit_width, 64);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_LARGEINT);
        const auto& values = assert_cast<const ColumnInt128&>(column);
        EXPECT_EQ(values.get_element(2), static_cast<int128_t>(1) << 63);
        EXPECT_EQ(values.get_element(3),
                  static_cast<int128_t>(std::numeric_limits<uint64_t>::max()));
    });
    read_and_validate("float_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FLOAT);
        const auto& values = assert_cast<const ColumnFloat32&>(column);
        EXPECT_FLOAT_EQ(values.get_element(0), 1.5F);
        EXPECT_FLOAT_EQ(values.get_element(1), -2.25F);
    });
    read_and_validate("double_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::DOUBLE);
        const auto& values = assert_cast<const ColumnFloat64&>(column);
        EXPECT_DOUBLE_EQ(values.get_element(0), 3.5);
        EXPECT_DOUBLE_EQ(values.get_element(1), -4.75);
    });
    read_and_validate("nullable_float16_col", [](const ParquetColumnSchema& schema,
                                                 const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
        EXPECT_EQ(schema.type_descriptor.fixed_length, 2);
        EXPECT_EQ(schema.type_descriptor.extra_type_info, ParquetExtraTypeInfo::FLOAT16);
        EXPECT_FALSE(schema.type_descriptor.is_string_like);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_FLOAT);
        const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
        const auto& values = assert_cast<const ColumnFloat32&>(nullable_column.get_nested_column());
        ASSERT_EQ(nullable_column.size(), ROW_COUNT);
        EXPECT_TRUE(nullable_column.is_null_at(0));
        EXPECT_FLOAT_EQ(values.get_element(1), 0.0F);
        EXPECT_FALSE(std::signbit(values.get_element(1)));
        EXPECT_FLOAT_EQ(values.get_element(2), -0.0F);
        EXPECT_TRUE(std::signbit(values.get_element(2)));
        EXPECT_FLOAT_EQ(values.get_element(3), 1.5F);
        EXPECT_TRUE(std::isnan(values.get_element(4)));
    });
    read_and_validate("binary_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::BYTE_ARRAY);
        const auto& values = assert_cast<const ColumnString&>(column);
        EXPECT_EQ(values.get_data_at(0).to_string(), "bin_a");
        EXPECT_EQ(values.get_data_at(3).to_string(), "bin_d");
    });
    read_and_validate("string_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_TRUE(schema.type_descriptor.is_string_like);
        const auto& values = assert_cast<const ColumnString&>(column);
        EXPECT_EQ(values.get_data_at(0).to_string(), "alpha");
        EXPECT_EQ(values.get_data_at(4).to_string(), "epsilon");
    });
    read_and_validate("fixed_binary_col", [](const ParquetColumnSchema& schema,
                                             const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
        EXPECT_EQ(schema.type_descriptor.fixed_length, 4);
        const auto& values = assert_cast<const ColumnString&>(column);
        EXPECT_EQ(values.get_data_at(0).to_string(), "aaaa");
        EXPECT_EQ(values.get_data_at(2).to_string(), "cccc");
    });
    read_and_validate("date_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT32);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DATEV2);
        EXPECT_EQ(schema.type->to_string(column, 0), "1970-01-01");
        EXPECT_EQ(schema.type->to_string(column, 2), "2021-01-01");
    });
    read_and_validate(
            "timestamp_millis_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
                EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DATETIMEV2);
                EXPECT_EQ(schema.type->to_string(column, 1), "1970-01-01 00:00:01.234");
                EXPECT_EQ(schema.type->to_string(column, 4), "1969-12-31 23:59:59.999");
            });
    read_and_validate(
            "timestamp_micros_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
                EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
                EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DATETIMEV2);
                EXPECT_EQ(schema.type->to_string(column, 1), "1970-01-01 00:00:01.234567");
                EXPECT_EQ(schema.type->to_string(column, 4), "1969-12-31 23:59:59.999999");
            });
    read_and_validate("timestamp_micros_utc_col", [](const ParquetColumnSchema& schema,
                                                     const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::INT64);
        EXPECT_TRUE(schema.type_descriptor.timestamp_is_adjusted_to_utc);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DATETIMEV2);
        EXPECT_EQ(schema.type->to_string(column, 1), "1970-01-01 00:00:01.234567");
        EXPECT_EQ(schema.type->to_string(column, 4), "1969-12-31 23:59:59.999999");
    });
    read_and_validate("decimal_fixed_binary_9_2_col", [](const ParquetColumnSchema& schema,
                                                         const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
        EXPECT_TRUE(schema.type_descriptor.is_decimal);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DECIMAL32);
        const auto& values = assert_cast<const ColumnDecimal32&>(column);
        EXPECT_EQ(values.get_element(0), Decimal32(12345));
        EXPECT_EQ(schema.type->to_string(column, 0), "123.45");
    });
    read_and_validate("decimal_fixed_binary_18_6_col", [](const ParquetColumnSchema& schema,
                                                          const IColumn& column) {
        EXPECT_EQ(schema.type_descriptor.physical_type, ::parquet::Type::FIXED_LEN_BYTE_ARRAY);
        EXPECT_TRUE(schema.type_descriptor.is_decimal);
        EXPECT_EQ(remove_nullable(schema.type)->get_primitive_type(), TYPE_DECIMAL64);
        const auto& values = assert_cast<const ColumnDecimal64&>(column);
        EXPECT_EQ(values.get_element(0), Decimal64(1234567));
        EXPECT_EQ(schema.type->to_string(column, 0), "1.234567");
    });
    read_and_validate(
            "nullable_int_col", [](const ParquetColumnSchema& schema, const IColumn& column) {
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
}

TEST_F(ParquetSerdeReaderTest, ReadInt96TimestampAsDateTimeV2) {
    const auto file_path = (_test_dir / "int96_timestamp.parquet").string();
    auto field = arrow::field("col_datetime", arrow::timestamp(arrow::TimeUnit::MICRO), false);
    auto array = build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO),
                                       {0, 1234567, 1609459200000000, 1609459201000000, -1});
    auto table = arrow::Table::Make(arrow::schema({field}), {array});

    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    _fields.clear();
    _file_reader.reset();
    _row_group.reset();
    write_table(file_path, table, arrow_builder.build());
    open_file(file_path);

    ASSERT_EQ(_fields.size(), 1);
    EXPECT_EQ(_fields[0]->type_descriptor.physical_type, ::parquet::Type::INT96);
    EXPECT_EQ(_fields[0]->type_descriptor.extra_type_info, ParquetExtraTypeInfo::IMPALA_TIMESTAMP);
    ASSERT_TRUE(supports_record_reader(_fields[0]->type_descriptor));
    ASSERT_EQ(remove_nullable(_fields[0]->type)->get_primitive_type(), TYPE_DATETIMEV2);

    auto reader = create_reader(0);
    ASSERT_NE(reader, nullptr);
    auto column = _fields[0]->type->create_column();
    int64_t rows_read = 0;
    ASSERT_TRUE(reader->read(ROW_COUNT, column, &rows_read).ok());
    ASSERT_EQ(rows_read, ROW_COUNT);
    EXPECT_EQ(_fields[0]->type->to_string(*column, 0), "1970-01-01 00:00:00.000000");
    EXPECT_EQ(_fields[0]->type->to_string(*column, 1), "1970-01-01 00:00:01.234567");
    EXPECT_EQ(_fields[0]->type->to_string(*column, 2), "2021-01-01 00:00:00.000000");
    EXPECT_EQ(_fields[0]->type->to_string(*column, 4), "1969-12-31 23:59:59.999999");
}

} // namespace
} // namespace doris::format::parquet
