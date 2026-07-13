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

#include <arrow/array/builder_binary.h>
#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <orc/Vector.hh>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "util/mysql_row_buffer.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_json.h"

namespace doris {
namespace {

constexpr uint32_t pack_olap_date(uint32_t year, uint32_t month, uint32_t day) {
    return (year << 9) | (month << 5) | day;
}

ColumnPtr nullable(MutableColumnPtr nested, std::span<const uint8_t> nulls) {
    auto null_map = ColumnUInt8::create();
    null_map->get_data().insert(nulls.begin(), nulls.end());
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnVariantV2::MutablePtr typed_strings(
        std::initializer_list<std::optional<std::string_view>> values) {
    auto nested = ColumnString::create();
    std::vector<uint8_t> nulls;
    for (const auto& value : values) {
        if (value) {
            nested->insert_data(value->data(), value->size());
            nulls.push_back(0);
        } else {
            nested->insert_default();
            nulls.push_back(1);
        }
    }
    return ColumnVariantV2::create_typed_from_cast(nullable(std::move(nested), nulls),
                                                   std::make_shared<DataTypeString>());
}

ColumnVariantV2::MutablePtr typed_doubles(std::initializer_list<double> values) {
    auto nested = ColumnFloat64::create();
    for (double value : values) {
        nested->insert_value(value);
    }
    const std::vector<uint8_t> nulls(values.size(), 0);
    return ColumnVariantV2::create_typed_from_cast(nullable(std::move(nested), nulls),
                                                   std::make_shared<DataTypeFloat64>());
}

ColumnPtr encoded_copy(const ColumnVariantV2& typed) {
    ColumnPtr copy = typed.clone();
    MutableColumnPtr mutable_copy = IColumn::mutate(std::move(copy));
    assert_cast<ColumnVariantV2&>(*mutable_copy).ensure_encoded();
    return mutable_copy;
}

ColumnVariantV2::MutablePtr encoded_json(std::initializer_list<std::string_view> rows) {
    JsonToVariantEncoder encoder({.max_json_key_length = 1024,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    for (std::string_view row : rows) {
        encoder.add_json({row.data(), row.size()});
    }
    VariantEncodedBlock block = encoder.finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
    return result;
}

std::string cell_json(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(column, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) {
        return {};
    }
    writer.commit();
    return output->get_data_at(0).to_string();
}

std::string to_string_value(const DataTypeVariantV2SerDe& serde, const IColumn& column,
                            size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    serde.to_string(column, row, writer, options);
    writer.commit();
    return output->get_data_at(0).to_string();
}

std::string mysql_binary(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row,
                         bool col_const = false) {
    MysqlRowBinaryBuffer buffer;
    DataTypeSerDe::FormatOptions options;
    const Status status =
            serde.write_column_to_mysql_binary(column, buffer, row, col_const, options);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok() || buffer.length() == 0) {
        return {};
    }
    const auto length = static_cast<uint8_t>(buffer.buf()[0]);
    EXPECT_EQ(buffer.length(), static_cast<int64_t>(length) + 1);
    return {buffer.buf() + 1, length};
}

std::optional<std::string> mysql_text(const DataTypeSerDe& serde, const IColumn& column,
                                      size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    if (!serde.write_column_to_mysql_text(column, writer, row, options)) {
        return std::nullopt;
    }
    writer.commit();
    return output->get_data_at(0).to_string();
}

template <typename Builder, typename Array>
std::vector<std::optional<std::string>> arrow_values(const DataTypeVariantV2SerDe& serde,
                                                     const IColumn& column,
                                                     const NullMap* null_map = nullptr) {
    Builder builder;
    const Status status = serde.write_column_to_arrow(column, null_map, &builder, 0, column.size(),
                                                      cctz::utc_time_zone());
    EXPECT_TRUE(status.ok()) << status;
    std::shared_ptr<arrow::Array> output;
    EXPECT_TRUE(builder.Finish(&output).ok());
    const auto& array = assert_cast<const Array&>(*output);
    std::vector<std::optional<std::string>> result;
    for (int64_t row = 0; row < array.length(); ++row) {
        if (array.IsNull(row)) {
            result.emplace_back();
        } else {
            const auto value = array.GetView(row);
            result.emplace_back(std::string(value.data(), value.size()));
        }
    }
    return result;
}

std::vector<std::optional<std::string>> orc_values(const DataTypeVariantV2SerDe& serde,
                                                   const IColumn& column,
                                                   const NullMap* null_map = nullptr) {
    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(column.size(), *orc::getDefaultPool());
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.write_column_to_orc("UTC", column, null_map, batch.get(), 0,
                                                    column.size(), arena, options);
    EXPECT_TRUE(status.ok()) << status;
    std::vector<std::optional<std::string>> result;
    for (size_t row = 0; row < column.size(); ++row) {
        if (batch->notNull[row] == 0) {
            result.emplace_back();
        } else {
            result.emplace_back(std::string(batch->data[row], batch->length[row]));
        }
    }
    return result;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the matrix.
void expect_text_surfaces(const DataTypeVariantV2SerDe& serde, const IColumn& encoded,
                          const ColumnVariantV2& typed,
                          std::span<const std::string_view> expected) {
    ASSERT_EQ(encoded.size(), expected.size());
    ASSERT_EQ(typed.size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(cell_json(serde, encoded, row), expected[row]);
        EXPECT_EQ(cell_json(serde, typed, row), expected[row]);
        EXPECT_EQ(to_string_value(serde, encoded, row), expected[row]);
        EXPECT_EQ(to_string_value(serde, typed, row), expected[row]);
        EXPECT_EQ(mysql_binary(serde, encoded, row), expected[row]);
        EXPECT_EQ(mysql_binary(serde, typed, row), expected[row]);
        EXPECT_EQ(mysql_text(serde, encoded, row), expected[row]);
        EXPECT_EQ(mysql_text(serde, typed, row), expected[row]);
    }
    std::vector<std::optional<std::string>> expected_values;
    for (std::string_view value : expected) {
        expected_values.emplace_back(value);
    }
    EXPECT_EQ((arrow_values<arrow::StringBuilder, arrow::StringArray>(serde, encoded)),
              expected_values);
    EXPECT_EQ((arrow_values<arrow::StringBuilder, arrow::StringArray>(serde, typed)),
              expected_values);
    EXPECT_EQ((arrow_values<arrow::LargeStringBuilder, arrow::LargeStringArray>(serde, encoded)),
              expected_values);
    EXPECT_EQ((arrow_values<arrow::LargeStringBuilder, arrow::LargeStringArray>(serde, typed)),
              expected_values);
    EXPECT_EQ(orc_values(serde, encoded), expected_values);
    EXPECT_EQ(orc_values(serde, typed), expected_values);
    EXPECT_TRUE(typed.is_typed());
}

ColumnVariantV2::MutablePtr invalid_date_column() {
    auto dates = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> invalid;
    dates->insert_value(invalid);
    dates->insert_value(
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2)));
    const std::array<uint8_t, 2> nulls {0, 0};
    return ColumnVariantV2::create_typed_from_cast(nullable(std::move(dates), nulls),
                                                   std::make_shared<DataTypeDateV2>());
}

ColumnVariantV2::MutablePtr trailing_value_column() {
    constexpr std::array<char, 3> metadata {static_cast<char>(0x11), 0, 0};
    constexpr std::array<uint32_t, 2> metadata_offsets {0, 3};
    constexpr std::array<uint32_t, 2> ids {0, 0};
    constexpr std::array<char, 3> values {0, 0, 0};
    constexpr std::array<uint32_t, 3> value_offsets {0, 1, 3};
    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows({.metadata_bytes = {metadata.data(), metadata.size()},
                                 .metadata_offsets = metadata_offsets,
                                 .meta_ids = ids,
                                 .value_bytes = {values.data(), values.size()},
                                 .value_offsets = value_offsets});
    return result;
}

} // namespace

TEST(DataTypeVariantV2SerdeOutputTest, DormantDirectClassExists) {
    DataTypeVariantV2SerDe serde;
    EXPECT_EQ(serde.get_name(), "Variant");
}

TEST(DataTypeVariantV2SerdeOutputTest, EncodedAndTypedUseOneExactJsonPrinterOnAllSurfaces) {
    DataTypeVariantV2SerDe serde;
    auto strings = typed_strings(
            {std::string_view("a\"\n"), std::nullopt, std::string_view(R"({"k":1})")});
    ColumnPtr encoded_strings = encoded_copy(*strings);
    const std::array<std::string_view, 3> string_expected {R"("a\"\n")", "null", R"("{\"k\":1}")"};
    expect_text_surfaces(serde, *encoded_strings, *strings, string_expected);

    auto doubles = typed_doubles({1.25, 1e100, std::numeric_limits<double>::quiet_NaN(),
                                  std::numeric_limits<double>::infinity(),
                                  -std::numeric_limits<double>::infinity()});
    ColumnPtr encoded_doubles = encoded_copy(*doubles);
    const std::array<std::string_view, 5> double_expected {"1.25", "1e+100", R"("NaN")",
                                                           R"("Infinity")", R"("-Infinity")"};
    expect_text_surfaces(serde, *encoded_doubles, *doubles, double_expected);
}

TEST(DataTypeVariantV2SerdeOutputTest, DocumentsTemporalAndColumnJsonAreExact) {
    DataTypeVariantV2SerDe serde;
    auto documents = encoded_json({R"({"a":[1,null,"x"]})", "[]", "null"});
    EXPECT_EQ(cell_json(serde, *documents, 0), R"({"a":[1,null,"x"]})");
    EXPECT_EQ(cell_json(serde, *documents, 1), "[]");
    EXPECT_EQ(cell_json(serde, *documents, 2), "null");
    const std::vector<std::optional<std::string>> document_expected {R"({"a":[1,null,"x"]})", "[]",
                                                                     "null"};
    for (size_t row = 0; row < documents->size(); ++row) {
        EXPECT_EQ(to_string_value(serde, *documents, row), document_expected[row]);
        EXPECT_EQ(mysql_text(serde, *documents, row), document_expected[row]);
        EXPECT_EQ(mysql_binary(serde, *documents, row), *document_expected[row]);
    }
    EXPECT_EQ((arrow_values<arrow::StringBuilder, arrow::StringArray>(serde, *documents)),
              document_expected);
    EXPECT_EQ(orc_values(serde, *documents), document_expected);

    auto date = ColumnDateV2::create();
    date->insert_value(
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2)));
    const std::array<uint8_t, 1> not_null {0};
    auto typed_date = ColumnVariantV2::create_typed_from_cast(nullable(std::move(date), not_null),
                                                              std::make_shared<DataTypeDateV2>());
    ColumnPtr encoded_date = encoded_copy(*typed_date);
    const std::array<std::string_view, 1> date_expected {R"("1970-01-02")"};
    expect_text_surfaces(serde, *encoded_date, *typed_date, date_expected);

    auto timestamp = ColumnDateTimeV2::create();
    timestamp->insert_value(
            DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL));
    auto typed_timestamp = ColumnVariantV2::create_typed_from_cast(
            nullable(std::move(timestamp), not_null), std::make_shared<DataTypeDateTimeV2>(6));
    ColumnPtr encoded_timestamp = encoded_copy(*typed_timestamp);
    const std::array<std::string_view, 1> timestamp_expected {R"("1970-01-01 00:00:01.000000")"};
    expect_text_surfaces(serde, *encoded_timestamp, *typed_timestamp, timestamp_expected);

    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    options.field_delim = "|";
    ASSERT_TRUE(serde.serialize_column_to_json(*documents, 0, 3, writer, options).ok());
    writer.commit();
    EXPECT_EQ(output->get_data_at(0), StringRef("{\"a\":[1,null,\"x\"]}|[]|null"));
}

TEST(DataTypeVariantV2SerdeOutputTest, ConstNullableAndOuterMasksPreserveBoundaries) {
    DataTypeVariantV2SerDe serde;
    auto one = typed_strings({std::string_view("constant")});
    ColumnPtr constant = ColumnConst::create(one->get_ptr(), 3);
    EXPECT_EQ(cell_json(serde, *constant, 2), R"("constant")");
    EXPECT_EQ(mysql_binary(serde, *one, 99, true), R"("constant")");
    const std::vector<std::optional<std::string>> constant_expected {
            R"("constant")", R"("constant")", R"("constant")"};
    EXPECT_EQ((arrow_values<arrow::StringBuilder, arrow::StringArray>(serde, *constant)),
              constant_expected);
    EXPECT_EQ(orc_values(serde, *constant), constant_expected);
    EXPECT_TRUE(one->is_typed());

    auto primitive_null = typed_strings({std::nullopt});
    EXPECT_EQ(cell_json(serde, *primitive_null, 0), "null");
    auto outer_nested = typed_strings({std::string_view("hidden"), std::string_view("shown")});
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->get_data().push_back(1);
    outer_nulls->get_data().push_back(0);
    MutableColumnPtr outer_values = std::move(outer_nested);
    ColumnPtr outer = ColumnNullable::create(std::move(outer_values), std::move(outer_nulls));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    EXPECT_FALSE(mysql_text(nullable_serde, *outer, 0).has_value());
    EXPECT_EQ(mysql_text(nullable_serde, *outer, 1), R"("shown")");
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(nullable_serde.serialize_one_cell_to_json(*outer, 0, writer, options).ok());
    writer.commit();
    EXPECT_EQ(output->get_data_at(0), StringRef("\\N"));

    auto invalid_dates = invalid_date_column();
    NullMap mask {1, 0};
    const std::vector<std::optional<std::string>> expected {std::nullopt, R"("1970-01-02")"};
    EXPECT_EQ(
            (arrow_values<arrow::StringBuilder, arrow::StringArray>(serde, *invalid_dates, &mask)),
            expected);
    EXPECT_EQ(orc_values(serde, *invalid_dates, &mask), expected);
    EXPECT_TRUE(invalid_dates->is_typed());

    arrow::StringBuilder arrow_builder;
    Status status = serde.write_column_to_arrow(*invalid_dates, nullptr, &arrow_builder, 0, 2,
                                                cctz::utc_time_zone());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(arrow_builder.length(), 0);
    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(2, *orc::getDefaultPool());
    batch->numElements = 7;
    status = serde.write_column_to_orc("UTC", *invalid_dates, nullptr, batch.get(), 0, 2, arena,
                                       options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(batch->numElements, 7);

    MysqlRowBinaryBuffer mysql_buffer;
    ASSERT_EQ(mysql_buffer.push_string("sentinel", 8), 0);
    const int64_t mysql_length = mysql_buffer.length();
    status = serde.write_column_to_mysql_binary(*invalid_dates, mysql_buffer, 0, false, options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(mysql_buffer.length(), mysql_length);

    arrow::StringBuilder reversed_builder;
    status = serde.write_column_to_arrow(*invalid_dates, nullptr, &reversed_builder, 2, 1,
                                         cctz::utc_time_zone());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(reversed_builder.length(), 0);
    EXPECT_TRUE(invalid_dates->is_typed());
}

TEST(DataTypeVariantV2SerdeOutputTest, BadEncodedRowsDoNotPublishPartialOutput) {
    DataTypeVariantV2SerDe serde;
    auto bad = trailing_value_column();
    auto output = ColumnString::create();
    output->insert_data("sentinel", 8);
    const size_t chars_before = output->get_chars().size();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_column_to_json(*bad, 0, 2, writer, options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output->size(), 1);
    EXPECT_EQ(output->get_chars().size(), chars_before);

    arrow::StringBuilder builder;
    EXPECT_EQ(serde.write_column_to_arrow(*bad, nullptr, &builder, 0, 2, cctz::utc_time_zone())
                      .code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(builder.length(), 0);
}

} // namespace doris
