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

#include <array>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/decimalv2_value.h"
#include "core/value/jsonb_value.h"
#include "core/value/timestamptz_value.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/segment/variant/v2/variant_assembler.h"
#include "storage/segment/variant/v2/variant_column_reader.h"
#include "storage/segment/variant/v2/variant_storage_cell.h"
#include "util/variant/variant_test_utils.h"

namespace doris::segment_v2::variant_v2 {
namespace {

struct JsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string json_at(ColumnVariantV2& column, size_t row) {
    if (column.is_typed()) {
        column.ensure_encoded();
    }
    JsonWriter writer;
    to_json(column.get_value_ref(row), writer);
    return writer.value;
}

ColumnVariantV2& assembled_values(const ColumnNullable::MutablePtr& output) {
    return assert_cast<ColumnVariantV2&>(output->get_nested_column());
}

std::string serialize_storage_cell(const DataTypePtr& type, const IColumn& column, size_t row) {
    ColumnString::Chars chars;
    type->get_serde()->write_one_cell_to_binary(column, chars, row);
    return {reinterpret_cast<const char*>(chars.data()), chars.size()};
}

template <typename T>
std::string fixed_storage_cell(FieldType type, const T& value) {
    std::string cell(1, static_cast<char>(type));
    cell.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return cell;
}

template <typename T>
std::string scaled_storage_cell(FieldType type, uint8_t scale, const T& value) {
    std::string cell(1, static_cast<char>(type));
    cell.push_back(static_cast<char>(scale));
    cell.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return cell;
}

template <typename T>
std::string decimal_storage_cell(FieldType type, uint8_t precision, uint8_t scale, const T& value) {
    std::string cell(1, static_cast<char>(type));
    cell.push_back(static_cast<char>(precision));
    cell.push_back(static_cast<char>(scale));
    cell.append(reinterpret_cast<const char*>(&value), sizeof(value));
    return cell;
}

std::string string_storage_cell(std::string_view value) {
    std::string cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_STRING));
    const size_t size = value.size();
    cell.append(reinterpret_cast<const char*>(&size), sizeof(size));
    cell.append(value);
    return cell;
}

std::string jsonb_storage_cell(std::string_view value) {
    JsonBinaryValue jsonb;
    const Status status = jsonb.from_json_string(value.data(), value.size());
    EXPECT_TRUE(status.ok()) << status;
    std::string cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_JSONB));
    const size_t size = jsonb.size();
    cell.append(reinterpret_cast<const char*>(&size), sizeof(size));
    cell.append(jsonb.value(), jsonb.size());
    return cell;
}

ColumnString::MutablePtr jsonb_column(std::string_view value) {
    JsonBinaryValue jsonb;
    const Status status = jsonb.from_json_string(value.data(), value.size());
    EXPECT_TRUE(status.ok()) << status;
    auto column = ColumnString::create();
    column->insert_data(jsonb.value(), jsonb.size());
    return column;
}

std::string nested_array_storage_cell(size_t array_depth) {
    std::string cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE));
    constexpr size_t ONE_ELEMENT = 1;
    for (size_t depth = 0; depth < array_depth; ++depth) {
        std::string parent(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_ARRAY));
        parent.append(reinterpret_cast<const char*>(&ONE_ELEMENT), sizeof(ONE_ELEMENT));
        parent.append(cell);
        cell = std::move(parent);
    }
    return cell;
}

std::string dotted_path(size_t parts) {
    std::string path;
    for (size_t part = 0; part < parts; ++part) {
        if (!path.empty()) {
            path.push_back('.');
        }
        path.append("p");
        path.append(std::to_string(part));
    }
    return path;
}

std::unique_ptr<VariantAssembler> create_assembler(VariantAssemblerOptions options) {
    auto result = VariantAssembler::create(std::move(options));
    DORIS_CHECK(result.has_value()) << result.error();
    return std::move(result).value();
}

Status assemble_binary_cells(std::span<const StringRef> cells, ColumnNullable::MutablePtr* output,
                             std::span<const uint8_t> outer_nulls = {},
                             std::span<const uint8_t> missing = {}) {
    return decode_v1_storage_cells(cells, outer_nulls, missing, output);
}

void append_binary_batch(MutableColumnPtr& destination, std::span<const StringRef> cells,
                         bool expected_typed, std::span<const uint8_t> outer_nulls = {}) {
    ColumnNullable::MutablePtr assembled;
    ASSERT_TRUE(assemble_binary_cells(cells, &assembled, outer_nulls).ok());
    ASSERT_EQ(assembled_values(assembled).is_typed(), expected_typed);
    ASSERT_TRUE(append_assembled_variant(destination, std::move(assembled)).ok());
}

using MapEntries = std::vector<std::pair<std::string, std::string>>;

ColumnMap::MutablePtr map_column_rows(const std::vector<MapEntries>& rows) {
    auto paths = ColumnString::create();
    auto values = ColumnString::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (const auto& entries : rows) {
        for (const auto& [path, value] : entries) {
            paths->insert_data(path.data(), path.size());
            values->insert_data(value.data(), value.size());
        }
        offsets->insert_value(paths->size());
    }
    return ColumnMap::create(std::move(paths), std::move(values), std::move(offsets));
}

ColumnMap::MutablePtr map_column(const MapEntries& entries) {
    return map_column_rows({entries});
}

struct LegacyCells {
    DataTypePtr date_type = std::make_shared<DataTypeDate>();
    DataTypePtr datetime_type = std::make_shared<DataTypeDateTime>();
    DataTypePtr decimal_type = std::make_shared<DataTypeDecimalV2>(27, 9, 10, 2);

    ColumnDate::MutablePtr dates = ColumnDate::create();
    ColumnDateTime::MutablePtr datetimes = ColumnDateTime::create();
    ColumnDecimal128V2::MutablePtr decimals = ColumnDecimal128V2::create(0, 9);

    std::array<std::string, 2> date_cells;
    std::array<std::string, 2> datetime_cells;
    std::array<std::string, 2> decimal_cells;
    std::string date_array_cell;

    LegacyCells() {
        dates->insert_value(
                VecDateTimeValue::create_from_olap_date((1970U << 9U) | (1U << 5U) | 2U));
        dates->insert_value(
                VecDateTimeValue::create_from_olap_date((1970U << 9U) | (1U << 5U) | 3U));
        datetimes->insert_value(
                VecDateTimeValue::create_from_olap_datetime(uint64_t {19700101000001ULL}));
        datetimes->insert_value(
                VecDateTimeValue::create_from_olap_datetime(uint64_t {19700101000002ULL}));
        decimals->insert_value(DecimalV2Value(std::string_view {"12.34"}));
        decimals->insert_value(DecimalV2Value(std::string_view {"-56.78"}));

        for (size_t row = 0; row < 2; ++row) {
            date_cells[row] = serialize_storage_cell(date_type, *dates, row);
            datetime_cells[row] = serialize_storage_cell(datetime_type, *datetimes, row);
            decimal_cells[row] = serialize_storage_cell(decimal_type, *decimals, row);
            EXPECT_EQ(static_cast<uint8_t>(date_cells[row][0]),
                      static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DATE));
            EXPECT_EQ(static_cast<uint8_t>(datetime_cells[row][0]),
                      static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DATETIME));
            EXPECT_EQ(static_cast<uint8_t>(decimal_cells[row][0]),
                      static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL));
        }

        auto array_dates = ColumnDate::create();
        array_dates->insert_value(dates->get_data()[0]);
        array_dates->insert_value(dates->get_data()[1]);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(2);
        auto array = ColumnArray::create(
                ColumnNullable::create(std::move(array_dates), ColumnUInt8::create(2, 0)),
                std::move(offsets));
        date_array_cell =
                serialize_storage_cell(std::make_shared<DataTypeArray>(date_type), *array, 0);
    }
};

void expect_typed_cells(const std::array<std::string, 2>& owned_cells, PrimitiveType expected_type,
                        const std::array<std::string_view, 2>& expected_json) {
    const std::array<StringRef, 3> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1]),
                                          StringRef()};
    constexpr std::array<uint8_t, 3> MISSING {0, 0, 1};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(cells, &output, {}, MISSING).ok());
    auto& values = assembled_values(output);
    ASSERT_TRUE(values.is_typed());
    EXPECT_EQ(values.typed_type()->get_primitive_type(), expected_type);
    if (expected_type == TYPE_DECIMALV2) {
        const auto& decimal_type = assert_cast<const DataTypeDecimalV2&>(*values.typed_type());
        EXPECT_EQ(decimal_type.get_original_precision(), 27);
        EXPECT_EQ(decimal_type.get_original_scale(), 2);
    }

    const auto& nullable = assert_cast<const ColumnNullable&>(values.typed_column());
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 1}));
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {0, 0, 1}));
    EXPECT_EQ(json_at(values, 0), expected_json[0]);
    EXPECT_EQ(json_at(values, 1), expected_json[1]);
    EXPECT_EQ(json_at(values, 2), "null");
}

TEST(VariantAssemblerLegacyTest, BinaryExtractPublishesHomogeneousLegacyCellsAsTyped) {
    LegacyCells source;
    expect_typed_cells(source.date_cells, TYPE_DATE, {R"("1970-01-02")", R"("1970-01-03")"});
    expect_typed_cells(source.datetime_cells, TYPE_DATETIME,
                       {R"("1970-01-01 00:00:01.000000")", R"("1970-01-01 00:00:02.000000")"});
    expect_typed_cells(source.decimal_cells, TYPE_DECIMALV2, {"12.340000000", "-56.780000000"});
}

TEST(VariantAssemblerLegacyTest, BinaryExtractScalarTypeMatrixPreservesTypedState) {
    expect_typed_cells({fixed_storage_cell<uint8_t>(FieldType::OLAP_FIELD_TYPE_BOOL, 0),
                        fixed_storage_cell<uint8_t>(FieldType::OLAP_FIELD_TYPE_BOOL, 1)},
                       TYPE_BOOLEAN, {"false", "true"});
    expect_typed_cells({fixed_storage_cell<int8_t>(FieldType::OLAP_FIELD_TYPE_TINYINT, -7),
                        fixed_storage_cell<int8_t>(FieldType::OLAP_FIELD_TYPE_TINYINT, 8)},
                       TYPE_TINYINT, {"-7", "8"});
    expect_typed_cells({fixed_storage_cell<int16_t>(FieldType::OLAP_FIELD_TYPE_SMALLINT, -123),
                        fixed_storage_cell<int16_t>(FieldType::OLAP_FIELD_TYPE_SMALLINT, 456)},
                       TYPE_SMALLINT, {"-123", "456"});
    expect_typed_cells({fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, -123456),
                        fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 654321)},
                       TYPE_INT, {"-123456", "654321"});
    expect_typed_cells(
            {fixed_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_BIGINT, -1234567890123LL),
             fixed_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_BIGINT, 9876543210123LL)},
            TYPE_BIGINT, {"-1234567890123", "9876543210123"});
    expect_typed_cells({fixed_storage_cell<__int128>(FieldType::OLAP_FIELD_TYPE_LARGEINT,
                                                     static_cast<__int128>(-1234567890123LL)),
                        fixed_storage_cell<__int128>(FieldType::OLAP_FIELD_TYPE_LARGEINT,
                                                     static_cast<__int128>(9876543210123LL))},
                       TYPE_LARGEINT, {"-1234567890123", "9876543210123"});
    expect_typed_cells({fixed_storage_cell<float>(FieldType::OLAP_FIELD_TYPE_FLOAT, 1.25F),
                        fixed_storage_cell<float>(FieldType::OLAP_FIELD_TYPE_FLOAT, -2.5F)},
                       TYPE_FLOAT, {"1.25", "-2.5"});
    expect_typed_cells({fixed_storage_cell<double>(FieldType::OLAP_FIELD_TYPE_DOUBLE, 3.5),
                        fixed_storage_cell<double>(FieldType::OLAP_FIELD_TYPE_DOUBLE, -4.25)},
                       TYPE_DOUBLE, {"3.5", "-4.25"});
    expect_typed_cells({string_storage_cell("alpha"), string_storage_cell("beta")}, TYPE_STRING,
                       {R"("alpha")", R"("beta")"});
    expect_typed_cells({fixed_storage_cell<IPv4>(FieldType::OLAP_FIELD_TYPE_IPV4, IPv4 {}),
                        fixed_storage_cell<IPv4>(FieldType::OLAP_FIELD_TYPE_IPV4, IPv4 {})},
                       TYPE_IPV4, {R"("0.0.0.0")", R"("0.0.0.0")"});
    expect_typed_cells({fixed_storage_cell<IPv6>(FieldType::OLAP_FIELD_TYPE_IPV6, IPv6 {}),
                        fixed_storage_cell<IPv6>(FieldType::OLAP_FIELD_TYPE_IPV6, IPv6 {})},
                       TYPE_IPV6, {R"("::")", R"("::")"});

    DateV2Value<DateV2ValueType> date_one;
    date_one.unchecked_set_time(1970, 1, 2, 0, 0, 0, 0);
    DateV2Value<DateV2ValueType> date_two;
    date_two.unchecked_set_time(1970, 1, 3, 0, 0, 0, 0);
    expect_typed_cells({fixed_storage_cell<UInt32>(
                                FieldType::OLAP_FIELD_TYPE_DATEV2,
                                binary_cast<DateV2Value<DateV2ValueType>, UInt32>(date_one)),
                        fixed_storage_cell<UInt32>(
                                FieldType::OLAP_FIELD_TYPE_DATEV2,
                                binary_cast<DateV2Value<DateV2ValueType>, UInt32>(date_two))},
                       TYPE_DATEV2, {R"("1970-01-02")", R"("1970-01-03")"});

    DateV2Value<DateTimeV2ValueType> datetime_one;
    datetime_one.unchecked_set_time(1970, 1, 1, 0, 0, 1, 0);
    DateV2Value<DateTimeV2ValueType> datetime_two;
    datetime_two.unchecked_set_time(1970, 1, 1, 0, 0, 2, 0);
    expect_typed_cells(
            {scaled_storage_cell<UInt64>(
                     FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 6,
                     binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime_one)),
             scaled_storage_cell<UInt64>(
                     FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 6,
                     binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime_two))},
            TYPE_DATETIMEV2,
            {R"("1970-01-01 00:00:01.000000")", R"("1970-01-01 00:00:02.000000")"});

    TimestampTzValue timestamp_one;
    timestamp_one.unchecked_set_time(1970, 1, 1, 0, 0, 1, 0);
    TimestampTzValue timestamp_two;
    timestamp_two.unchecked_set_time(1970, 1, 1, 0, 0, 2, 0);
    expect_typed_cells(
            {scaled_storage_cell<UInt64>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 6,
                                         binary_cast<TimestampTzValue, UInt64>(timestamp_one)),
             scaled_storage_cell<UInt64>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 6,
                                         binary_cast<TimestampTzValue, UInt64>(timestamp_two))},
            TYPE_TIMESTAMPTZ,
            {R"("1970-01-01 00:00:01.000000+00:00")", R"("1970-01-01 00:00:02.000000+00:00")"});

    expect_typed_cells(
            {decimal_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 2, 1234),
             decimal_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 9, 2, -5678)},
            TYPE_DECIMAL32, {"12.34", "-56.78"});
    expect_typed_cells(
            {decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 3, 12345),
             decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 3, -67890)},
            TYPE_DECIMAL64, {"12.345", "-67.890"});
    expect_typed_cells({decimal_storage_cell<__int128>(FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38,
                                                       4, static_cast<__int128>(123456)),
                        decimal_storage_cell<__int128>(FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 38,
                                                       4, static_cast<__int128>(-654321))},
                       TYPE_DECIMAL128I, {"12.3456", "-65.4321"});
}

TEST(VariantAssemblerLegacyTest, BinaryExtractHeterogeneousComplexAndUnsupportedCells) {
    const std::array<std::string, 2> heterogeneous {
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 7),
            fixed_storage_cell<float>(FieldType::OLAP_FIELD_TYPE_FLOAT, 1.5F)};
    const std::array<StringRef, 2> heterogeneous_refs {StringRef(heterogeneous[0]),
                                                       StringRef(heterogeneous[1])};
    ColumnNullable::MutablePtr heterogeneous_output;
    ASSERT_TRUE(assemble_binary_cells(heterogeneous_refs, &heterogeneous_output).ok());
    EXPECT_FALSE(assembled_values(heterogeneous_output).is_typed());
    EXPECT_EQ(json_at(assembled_values(heterogeneous_output), 0), "7");
    EXPECT_EQ(json_at(assembled_values(heterogeneous_output), 1), "1.5");

    LegacyCells source;
    const std::array<StringRef, 2> arrays {StringRef(source.date_array_cell),
                                           StringRef(source.date_array_cell)};
    ColumnNullable::MutablePtr array_output;
    ASSERT_TRUE(assemble_binary_cells(arrays, &array_output).ok());
    EXPECT_FALSE(assembled_values(array_output).is_typed());
    EXPECT_EQ(json_at(assembled_values(array_output), 0), R"(["1970-01-02","1970-01-03"])");
    EXPECT_EQ(json_at(assembled_values(array_output), 1), R"(["1970-01-02","1970-01-03"])");

    const std::array<std::string, 2> decimal256_cells {
            decimal_storage_cell<wide::Int256>(FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 4,
                                               wide::Int256 {123456}),
            decimal_storage_cell<wide::Int256>(FieldType::OLAP_FIELD_TYPE_DECIMAL256, 76, 4,
                                               wide::Int256 {-654321})};
    const std::array<StringRef, 2> decimal256_refs {StringRef(decimal256_cells[0]),
                                                    StringRef(decimal256_cells[1])};
    ColumnNullable::MutablePtr decimal256_output;
    const Status status = assemble_binary_cells(decimal256_refs, &decimal256_output);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Decimal256 storage cell"), std::string::npos);
}

TEST(VariantAssemblerLegacyTest,
     BinaryExtractSameTagDifferentDecimalMetadataFallsBackToCanonicalEncoded) {
    const std::array<std::array<std::string, 2>, 2> cases {{
            {decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 17, 2, 1234),
             decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 2, 5678)},
            {decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 2, 1234),
             decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 18, 3, 5678)},
    }};
    const std::array<std::array<std::string_view, 2>, 2> expected {{
            {"12.34", "56.78"},
            {"12.34", "5.678"},
    }};

    for (size_t case_index = 0; case_index < cases.size(); ++case_index) {
        SCOPED_TRACE(case_index);
        const auto& owned_cells = cases[case_index];
        const std::array<StringRef, 2> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1])};
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assemble_binary_cells(cells, &output).ok());
        auto& values = assembled_values(output);
        ASSERT_FALSE(values.is_typed());
        for (size_t row = 0; row < cells.size(); ++row) {
            EXPECT_EQ(json_at(values, row), expected[case_index][row]);
            ASSERT_NO_THROW(validate_canonical(values.get_value_ref(row)));
        }
    }
}

TEST(VariantAssemblerLegacyTest,
     BinaryExtractSameTimestampTagDifferentScaleFallsBackToCanonicalEncoded) {
    DateV2Value<DateTimeV2ValueType> datetime_one;
    datetime_one.unchecked_set_time(1970, 1, 1, 0, 0, 1, 0);
    DateV2Value<DateTimeV2ValueType> datetime_two;
    datetime_two.unchecked_set_time(1970, 1, 1, 0, 0, 2, 0);
    const std::array<std::string, 2> datetime_cells {
            scaled_storage_cell<UInt64>(
                    FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 6,
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime_one)),
            scaled_storage_cell<UInt64>(
                    FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 3,
                    binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime_two)),
    };

    TimestampTzValue timestamp_one;
    timestamp_one.unchecked_set_time(1970, 1, 1, 0, 0, 1, 0);
    TimestampTzValue timestamp_two;
    timestamp_two.unchecked_set_time(1970, 1, 1, 0, 0, 2, 0);
    const std::array<std::string, 2> timestamp_cells {
            scaled_storage_cell<UInt64>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 6,
                                        binary_cast<TimestampTzValue, UInt64>(timestamp_one)),
            scaled_storage_cell<UInt64>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ, 3,
                                        binary_cast<TimestampTzValue, UInt64>(timestamp_two)),
    };

    const auto expect_fallback = [](const std::array<std::string, 2>& owned_cells) {
        const std::array<StringRef, 2> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1])};
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assemble_binary_cells(cells, &output).ok());
        auto& values = assembled_values(output);
        ASSERT_FALSE(values.is_typed());
        for (size_t row = 0; row < cells.size(); ++row) {
            ASSERT_NO_THROW(validate_canonical(values.get_value_ref(row)));
        }
    };
    expect_fallback(datetime_cells);
    expect_fallback(timestamp_cells);
}

TEST(VariantAssemblerLegacyTest, BinaryExtractLateSignatureMismatchReplaysWholeBatch) {
    const std::array<std::string, 6> owned_cells {
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 1),
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 2),
            std::string(1, static_cast<char>(0xFF)),
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 3),
            string_storage_cell("late"),
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 4),
    };
    std::array<StringRef, owned_cells.size()> cells;
    for (size_t row = 0; row < cells.size(); ++row) {
        cells[row] = StringRef(owned_cells[row]);
    }
    constexpr std::array<uint8_t, cells.size()> OUTER_NULLS {0, 0, 1, 0, 0, 0};

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(cells, &output, OUTER_NULLS).ok());
    auto& values = assembled_values(output);
    ASSERT_FALSE(values.is_typed());
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {0, 0, 1, 0, 0, 0}));
    const std::array<std::string_view, cells.size()> expected {"1", "2",         "null",
                                                               "3", R"("late")", "4"};
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(json_at(values, row), expected[row]) << "row=" << row;
    }
}

TEST(VariantAssemblerLegacyTest, BinaryExtractKeepsPresentVariantNullsInsidePayload) {
    const std::array<std::string, 3> owned_cells {
            std::string(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE)),
            jsonb_storage_cell("null"),
            jsonb_storage_cell(R"({"a":7,"b":[true,null]})"),
    };
    const std::array<StringRef, 3> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1]),
                                          StringRef(owned_cells[2])};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(cells, &output).ok());
    auto& values = assembled_values(output);
    EXPECT_FALSE(values.is_typed());
    EXPECT_EQ(json_at(values, 0), "null");
    EXPECT_EQ(json_at(values, 1), "null");
    EXPECT_EQ(json_at(values, 2), R"({"a":7,"b":[true,null]})");
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {0, 0, 0}));

    MutableColumnPtr non_nullable_destination = ColumnVariantV2::create();
    const Status status = append_assembled_variant(non_nullable_destination, std::move(output));
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(non_nullable_destination->size(), 3);
}

TEST(VariantAssemblerLegacyTest, BinaryExtractDoesNotDecodeMaskedMalformedCells) {
    const std::array<std::string, 3> owned_cells {
            std::string {},
            std::string(1, static_cast<char>(0xFF)),
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 42),
    };
    const std::array<StringRef, 3> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1]),
                                          StringRef(owned_cells[2])};
    constexpr std::array<uint8_t, 3> OUTER_NULLS {0, 1, 0};
    constexpr std::array<uint8_t, 3> MISSING {1, 0, 0};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(cells, &output, OUTER_NULLS, MISSING).ok());
    auto& values = assembled_values(output);
    ASSERT_TRUE(values.is_typed());
    EXPECT_EQ(values.typed_type()->get_primitive_type(), TYPE_INT);
    const auto& nullable = assert_cast<const ColumnNullable&>(values.typed_column());
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {1, 1, 0}));
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {1, 1, 0}));
    EXPECT_EQ(json_at(values, 0), "null");
    EXPECT_EQ(json_at(values, 1), "null");
    EXPECT_EQ(json_at(values, 2), "42");
}

TEST(VariantAssemblerLegacyTest, BinaryExtractAllMissingCellsUseGenericFallback) {
    const std::array<std::string, 3> owned_cells {
            std::string {},
            std::string(1, static_cast<char>(0xFF)),
            std::string {},
    };
    const std::array<StringRef, 3> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1]),
                                          StringRef(owned_cells[2])};
    constexpr std::array<uint8_t, 3> OUTER_NULLS {1, 0, 1};
    constexpr std::array<uint8_t, 3> MISSING {0, 1, 0};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(cells, &output, OUTER_NULLS, MISSING).ok());

    auto& values = assembled_values(output);
    EXPECT_FALSE(values.is_typed());
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {1, 1, 1}));
    for (size_t row = 0; row < cells.size(); ++row) {
        EXPECT_EQ(json_at(values, row), "null");
    }
}

TEST(VariantAssemblerLegacyTest, BinaryExtractRejectsMalformedLaterTypedCellAtomically) {
    auto trailing_int = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 2);
    trailing_int.push_back('\0');
    auto truncated_string = string_storage_cell("truncated");
    truncated_string.pop_back();
    const std::array<std::array<std::string, 2>, 2> cases {{
            {fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 1),
             std::move(trailing_int)},
            {string_storage_cell("valid"), std::move(truncated_string)},
    }};

    for (const auto& owned_cells : cases) {
        const std::array<StringRef, 2> cells {StringRef(owned_cells[0]), StringRef(owned_cells[1])};
        ColumnNullable::MutablePtr output;
        const Status status = assemble_binary_cells(cells, &output);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        EXPECT_EQ(output.get(), nullptr);
    }
}

TEST(VariantAssemblerLegacyTest, BinaryExtractLateFallbackFailureIsAtomicAndReusable) {
    const std::string initial = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 42);
    const std::array<StringRef, 1> initial_cells {StringRef(initial)};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(initial_cells, &output).ok());
    const auto* output_before = output.get();
    EXPECT_EQ(json_at(assembled_values(output), 0), "42");

    auto truncated = string_storage_cell("truncated");
    truncated.pop_back();
    const std::array<std::string, 4> malformed {
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 1),
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 2),
            string_storage_cell("fallback"),
            std::move(truncated),
    };
    const std::array<StringRef, malformed.size()> malformed_cells {
            StringRef(malformed[0]), StringRef(malformed[1]), StringRef(malformed[2]),
            StringRef(malformed[3])};
    const Status failure = assemble_binary_cells(malformed_cells, &output);
    EXPECT_TRUE(failure.is<ErrorCode::CORRUPTION>()) << failure;
    EXPECT_EQ(output.get(), output_before);
    EXPECT_EQ(json_at(assembled_values(output), 0), "42");

    const std::string recovery = string_storage_cell("recovered");
    const std::array<StringRef, 1> recovery_cells {StringRef(recovery)};
    ASSERT_TRUE(assemble_binary_cells(recovery_cells, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"("recovered")");
}

TEST(VariantAssemblerLegacyTest, BinaryExtractBatchesAppendAcrossTypedAndEncodedStates) {
    MutableColumnPtr destination =
            ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());

    const std::array<std::string, 2> int_cells {
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 42), std::string {}};
    const std::array<StringRef, 2> int_refs {StringRef(int_cells[0]), StringRef(int_cells[1])};
    constexpr std::array<uint8_t, 2> INT_OUTER_NULLS {0, 1};
    append_binary_batch(destination, int_refs, true, INT_OUTER_NULLS);

    const std::array<std::string, 2> heterogeneous_cells {
            fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 7),
            string_storage_cell("seven")};
    const std::array<StringRef, 2> heterogeneous_refs {StringRef(heterogeneous_cells[0]),
                                                       StringRef(heterogeneous_cells[1])};
    append_binary_batch(destination, heterogeneous_refs, false);

    const std::array<std::string, 2> string_cells {string_storage_cell("alpha"),
                                                   string_storage_cell("beta")};
    const std::array<StringRef, 2> string_refs {StringRef(string_cells[0]),
                                                StringRef(string_cells[1])};
    append_binary_batch(destination, string_refs, true);

    auto& nullable = assert_cast<ColumnNullable&>(*destination);
    auto& values = assert_cast<ColumnVariantV2&>(nullable.get_nested_column());
    ASSERT_FALSE(values.is_typed());
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1, 0, 0, 0, 0}));
    const std::array<std::string_view, 6> expected {"42",         "null",       "7",
                                                    R"("seven")", R"("alpha")", R"("beta")"};
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(json_at(values, row), expected[row]) << "row=" << row;
    }
}

TEST(VariantAssemblerLegacyTest, HierarchicalMaterializedDecimalAndRecursiveArray) {
    auto decimal_type = std::make_shared<DataTypeDecimal64>(18, 2);
    auto decimal = ColumnDecimal64::create(0, 2);
    decimal->insert_value(Decimal64 {7890});

    auto nested_decimals = ColumnDecimal64::create(0, 2);
    nested_decimals->insert_value(Decimal64 {123});
    nested_decimals->insert_value(Decimal64 {});
    nested_decimals->insert_value(Decimal64 {-456});
    auto nested_decimal_nulls = ColumnUInt8::create();
    nested_decimal_nulls->insert_value(0);
    nested_decimal_nulls->insert_value(1);
    nested_decimal_nulls->insert_value(0);
    auto nullable_decimals =
            ColumnNullable::create(std::move(nested_decimals), std::move(nested_decimal_nulls));

    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    inner_offsets->insert_value(2);
    inner_offsets->insert_value(3);
    inner_offsets->insert_value(3);
    auto inner_arrays = ColumnArray::create(std::move(nullable_decimals), std::move(inner_offsets));
    auto inner_array_nulls = ColumnUInt8::create();
    inner_array_nulls->insert_value(0);
    inner_array_nulls->insert_value(0);
    inner_array_nulls->insert_value(1);
    auto nullable_inner_arrays =
            ColumnNullable::create(std::move(inner_arrays), std::move(inner_array_nulls));

    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    outer_offsets->insert_value(3);
    auto outer_array =
            ColumnArray::create(std::move(nullable_inner_arrays), std::move(outer_offsets));
    auto nullable_decimal_type = std::make_shared<DataTypeNullable>(decimal_type);
    auto inner_array_type = std::make_shared<DataTypeArray>(nullable_decimal_type);
    auto nullable_inner_array_type = std::make_shared<DataTypeNullable>(inner_array_type);
    auto outer_array_type = std::make_shared<DataTypeArray>(nullable_inner_array_type);

    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData("a"), .type = outer_array_type},
            {.path = PathInData("d"), .type = decimal_type},
    };
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);
    const std::array<const IColumn*, 2> materialized {outer_array.get(), decimal.get()};
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = materialized;

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_FALSE(assembled_values(output).is_typed());
    EXPECT_EQ(json_at(assembled_values(output), 0),
              R"({"a":[[1.23,null],[-4.56],null],"d":78.90})");
}

TEST(VariantAssemblerLegacyTest, HierarchicalMaterializedDecimal256ReturnsNotSupported) {
    auto decimal_type = std::make_shared<DataTypeDecimal256>(76, 2);
    auto decimal = ColumnDecimal256::create(0, 2);
    decimal->insert_value(Decimal256 {wide::Int256 {123}});

    VariantAssemblerOptions options;
    options.materialized_paths = {{.path = PathInData("d"), .type = decimal_type}};
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);

    const IColumn* materialized = decimal.get();
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = {&materialized, 1};
    ColumnNullable::MutablePtr output;
    const Status status = assembler->assemble(batch, &output);

    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Decimal256 materialized storage column"), std::string::npos);
    EXPECT_EQ(output.get(), nullptr);
}

TEST(VariantAssemblerLegacyTest, MaterializedArraysPreserveExplicitValues) {
    auto values = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    auto insert_jsonb = [&](std::string_view json) {
        JsonBinaryValue jsonb;
        ASSERT_TRUE(jsonb.from_json_string(json.data(), json.size()).ok());
        values->insert_data(jsonb.value(), jsonb.size());
        nulls->insert_value(0);
    };
    auto insert_null = [&]() {
        values->insert_default();
        nulls->insert_value(1);
    };

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(0); // []
    insert_null();
    offsets->insert_value(1); // [null]
    insert_jsonb("{}");
    offsets->insert_value(2); // [{}]
    insert_jsonb(R"({"L2":[]})");
    offsets->insert_value(3); // [{"L2":[]}]
    insert_jsonb("1");
    offsets->insert_value(4); // [1]
    insert_null();
    insert_jsonb(R"({"x":1})");
    offsets->insert_value(6); // [null, {"x":1}]
    offsets->insert_value(6); // SQL NULL row, carried by the nullable materialized/root columns

    auto nested = ColumnNullable::create(std::move(values), std::move(nulls));
    auto arrays = ColumnArray::create(std::move(nested), std::move(offsets));
    auto array_nulls = ColumnUInt8::create();
    for (uint8_t is_null : {0, 0, 0, 0, 0, 0, 1}) {
        array_nulls->insert_value(is_null);
    }
    auto nullable_arrays = ColumnNullable::create(std::move(arrays), std::move(array_nulls));
    auto array_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeJsonb>()));
    auto nullable_array_type = make_nullable(array_type);
    constexpr std::array<uint8_t, 7> OUTER_NULLS {0, 0, 0, 0, 0, 0, 1};
    auto root_values = ColumnString::create();
    auto root_nulls = ColumnUInt8::create();
    for (uint8_t is_null : OUTER_NULLS) {
        root_values->insert_default();
        root_nulls->insert_value(is_null);
    }
    auto nullable_root = ColumnNullable::create(std::move(root_values), std::move(root_nulls));
    const std::array<const IColumn*, 1> materialized {nullable_arrays.get()};

    VariantAssemblerOptions whole_options;
    whole_options.materialized_paths = {{.path = PathInData("a"), .type = nullable_array_type}};
    whole_options.has_root = true;
    auto whole_assembler = create_assembler(std::move(whole_options));
    ASSERT_NE(whole_assembler, nullptr);
    VariantAssemblerBatchView whole_batch;
    whole_batch.num_rows = OUTER_NULLS.size();
    whole_batch.root_jsonb = nullable_root.get();
    whole_batch.materialized_columns = materialized;
    ColumnNullable::MutablePtr whole_output;
    ASSERT_TRUE(whole_assembler->assemble(whole_batch, &whole_output).ok());
    const std::array<std::string_view, 7> expected_whole {
            "{}",           R"({"a":[null]})",         R"({"a":[{}]})", R"({"a":[{"L2":[]}]})",
            R"({"a":[1]})", R"({"a":[null,{"x":1}]})", "null"};
    for (size_t row = 0; row < expected_whole.size(); ++row) {
        EXPECT_EQ(json_at(assembled_values(whole_output), row), expected_whole[row])
                << "row=" << row;
    }
    EXPECT_EQ(whole_output->get_null_map_data(), (PaddedPODArray<uint8_t> {0, 0, 0, 0, 0, 0, 1}));

    VariantAssemblerOptions subtree_options;
    subtree_options.requested_path = PathInData("a");
    subtree_options.materialized_paths = {{.path = PathInData("a"), .type = nullable_array_type}};
    auto subtree_assembler = create_assembler(std::move(subtree_options));
    ASSERT_NE(subtree_assembler, nullptr);
    VariantAssemblerBatchView subtree_batch;
    subtree_batch.num_rows = OUTER_NULLS.size();
    subtree_batch.materialized_columns = materialized;
    ColumnNullable::MutablePtr subtree_output;
    ASSERT_TRUE(subtree_assembler->assemble(subtree_batch, &subtree_output).ok());
    const std::array<std::string_view, 7> expected_subtree {
            "null", "[null]", "[{}]", R"([{"L2":[]}])", "[1]", R"([null,{"x":1}])", "null"};
    for (size_t row = 0; row < expected_subtree.size(); ++row) {
        EXPECT_EQ(json_at(assembled_values(subtree_output), row), expected_subtree[row])
                << "row=" << row;
    }
    EXPECT_EQ(subtree_output->get_null_map_data(), (PaddedPODArray<uint8_t> {1, 0, 0, 0, 0, 0, 1}));
}

TEST(VariantAssemblerLegacyTest, HierarchicalMaterializedLegacyDateTimeAndDecimalV2) {
    LegacyCells source;
    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData("dt"), .type = source.datetime_type},
            {.path = PathInData("m"), .type = source.decimal_type},
    };
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);
    const std::array<const IColumn*, 2> materialized {source.datetimes.get(),
                                                      source.decimals.get()};
    VariantAssemblerBatchView batch;
    batch.num_rows = 2;
    batch.materialized_columns = materialized;

    ColumnNullable::MutablePtr output;
    const Status status = assembler->assemble(batch, &output);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(assembled_values(output).is_typed());
    EXPECT_EQ(json_at(assembled_values(output), 0),
              R"({"dt":"1970-01-01 00:00:01.000000","m":12.340000000})");
    EXPECT_EQ(json_at(assembled_values(output), 1),
              R"({"dt":"1970-01-01 00:00:02.000000","m":-56.780000000})");
}

TEST(VariantAssemblerLegacyTest, UnsortedMaterializedPathsKeepSourceColumns) {
    auto z = ColumnInt32::create();
    z->insert_value(10);
    auto a = ColumnInt32::create();
    a->insert_value(20);
    auto child = ColumnInt32::create();
    child->insert_value(30);

    const auto int_type = std::make_shared<DataTypeInt32>();
    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData("z"), .type = int_type},
            {.path = PathInData("a"), .type = int_type},
            {.path = PathInData("m.child"), .type = int_type},
    };
    auto assembler = create_assembler(std::move(options));
    const std::array<const IColumn*, 3> materialized {z.get(), a.get(), child.get()};
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = materialized;

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"a":20,"m":{"child":30},"z":10})");
}

TEST(VariantAssemblerLegacyTest, RootSidecarYieldsToVisibleHierarchicalStreams) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::string sparse_cell = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 20);

    {
        VariantAssemblerOptions options;
        options.has_root = true;
        auto assembler = create_assembler(std::move(options));
        auto root = jsonb_column(R"({"source":"root"})");
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = root.get();
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assembler->assemble(batch, &output).ok());
        EXPECT_EQ(json_at(assembled_values(output), 0), R"({"source":"root"})");
    }

    {
        VariantAssemblerOptions options;
        options.has_root = true;
        options.storage_map_kind = StorageMapKind::SPARSE;
        auto assembler = create_assembler(std::move(options));
        auto root = jsonb_column(R"({"source":"root"})");
        auto sparse = map_column({});
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = root.get();
        batch.storage_map = sparse.get();
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assembler->assemble(batch, &output).ok());
        EXPECT_EQ(json_at(assembled_values(output), 0), R"({"source":"root"})");
    }

    {
        VariantAssemblerOptions options;
        options.has_root = true;
        options.storage_map_kind = StorageMapKind::SPARSE;
        auto assembler = create_assembler(std::move(options));
        auto root = jsonb_column(R"({"source":"root"})");
        auto sparse = map_column({{"sparse", sparse_cell}});
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = root.get();
        batch.storage_map = sparse.get();
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assembler->assemble(batch, &output).ok());
        EXPECT_EQ(json_at(assembled_values(output), 0), R"({"sparse":20})");
    }

    {
        VariantAssemblerOptions options;
        options.has_root = true;
        options.materialized_paths = {{.path = PathInData("materialized"), .type = int_type}};
        auto assembler = create_assembler(std::move(options));
        auto root = jsonb_column(R"({"source":"root"})");
        auto materialized = ColumnInt32::create();
        materialized->insert_value(30);
        const IColumn* materialized_ptr = materialized.get();
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = root.get();
        batch.materialized_columns = {&materialized_ptr, 1};
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assembler->assemble(batch, &output).ok());
        EXPECT_EQ(json_at(assembled_values(output), 0), R"({"materialized":30})");
    }

    {
        VariantAssemblerOptions options;
        options.has_root = true;
        options.storage_map_kind = StorageMapKind::DOC;
        auto assembler = create_assembler(std::move(options));
        auto root = jsonb_column(R"({"source":"root"})");
        auto doc = map_column({{"doc", sparse_cell}});
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = root.get();
        batch.storage_map = doc.get();
        ColumnNullable::MutablePtr output;
        ASSERT_TRUE(assembler->assemble(batch, &output).ok());
        EXPECT_EQ(json_at(assembled_values(output), 0), R"({"doc":20})");
    }
}

TEST(VariantAssemblerLegacyTest, HierarchicalFailuresAreAtomicAndAssemblerCanBeReused) {
    VariantAssemblerOptions options;
    options.storage_map_kind = StorageMapKind::SPARSE;
    auto assembler = create_assembler(std::move(options));

    const std::string valid_42 = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 42);
    auto valid = map_column({{"a", valid_42}});
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.storage_map = valid.get();
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"a":42})");

    const auto* wrapper_before = output.get();
    const auto* values_before = output->get_nested_column_ptr().get();
    const auto* null_map_before = output->get_null_map_column_ptr().get();
    const std::string json_before = json_at(assembled_values(output), 0);

    auto truncated = valid_42;
    truncated.pop_back();
    auto malformed = map_column({{"a", truncated}});
    batch.storage_map = malformed.get();
    const Status status = assembler->assemble(batch, &output);
    ASSERT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(output.get(), wrapper_before);
    EXPECT_EQ(output->get_nested_column_ptr().get(), values_before);
    EXPECT_EQ(output->get_null_map_column_ptr().get(), null_map_before);
    EXPECT_EQ(json_at(assembled_values(output), 0), json_before);

    const std::string valid_43 = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 43);
    auto next = map_column({{"a", valid_43}});
    batch.storage_map = next.get();
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"a":43})");
}

TEST(VariantAssemblerLegacyTest, DepthBoundaries) {
    {
        VariantAssemblerOptions options;
        options.materialized_paths.push_back(
                {.path = PathInData(dotted_path(VARIANT_MAX_NESTING_DEPTH)),
                 .type = std::make_shared<DataTypeInt32>()});
        auto assembler = create_assembler(std::move(options));
        EXPECT_NE(assembler, nullptr);
    }
    const std::array<std::string, 2> owned_cells {
            nested_array_storage_cell(VARIANT_MAX_NESTING_DEPTH),
            nested_array_storage_cell(VARIANT_MAX_NESTING_DEPTH + 1),
    };
    for (size_t row = 0; row < owned_cells.size(); ++row) {
        const std::array<StringRef, 1> cells {StringRef(owned_cells[row])};
        ColumnNullable::MutablePtr output;
        const Status status = assemble_binary_cells(cells, &output);
        if (row == 0) {
            EXPECT_TRUE(status.ok()) << status;
        } else {
            EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        }
    }

    const std::string value = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 1);
    VariantAssemblerOptions sparse_options;
    sparse_options.storage_map_kind = StorageMapKind::SPARSE;
    auto sparse_assembler = create_assembler(std::move(sparse_options));
    for (size_t depth : {VARIANT_MAX_NESTING_DEPTH, VARIANT_MAX_NESTING_DEPTH + 1}) {
        auto sparse = map_column({{dotted_path(depth), value}});
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.storage_map = sparse.get();
        ColumnNullable::MutablePtr output;
        const Status status = sparse_assembler->assemble(batch, &output);
        if (depth == VARIANT_MAX_NESTING_DEPTH) {
            EXPECT_TRUE(status.ok()) << status;
        } else {
            EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
            EXPECT_EQ(output.get(), nullptr);
        }
    }
}

TEST(VariantAssemblerLegacyTest, MalformedStorageCellsFailAtomicallyAndAllowLaterBatches) {
    LegacyCells source;
    auto truncated_date = source.date_cells[0];
    truncated_date.pop_back();
    auto truncated_datetime = source.datetime_cells[0];
    truncated_datetime.pop_back();
    auto truncated_decimal = source.decimal_cells[0];
    truncated_decimal.pop_back();
    auto truncated_string = string_storage_cell("truncated");
    truncated_string.pop_back();
    std::vector<std::string> malformed {
            {},
            fixed_storage_cell<size_t>(FieldType::OLAP_FIELD_TYPE_ARRAY, 2),
            std::move(truncated_string),
            std::move(truncated_date),
            std::move(truncated_datetime),
            std::move(truncated_decimal),
            // The tag fixes the payload width. A precision selecting another physical Decimal
            // column would make the typed SerDe cast its destination to the wrong column type.
            decimal_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL32, 18, 2, 1234),
            decimal_storage_cell<int64_t>(FieldType::OLAP_FIELD_TYPE_DECIMAL64, 9, 2, 1234),
            std::string(1, static_cast<char>(0xFF)),
    };
    auto trailing = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 1);
    trailing.push_back('\0');
    malformed.push_back(std::move(trailing));

    for (const std::string& cell : malformed) {
        const std::array<StringRef, 1> cells {StringRef(cell)};
        ColumnNullable::MutablePtr output;
        const Status status = assemble_binary_cells(cells, &output);
        EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
        EXPECT_EQ(output.get(), nullptr);
    }

    const std::string valid = fixed_storage_cell<int32_t>(FieldType::OLAP_FIELD_TYPE_INT, 42);
    const std::array<StringRef, 1> valid_cells {StringRef(valid)};
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assemble_binary_cells(valid_cells, &output).ok());
    ASSERT_NE(output.get(), nullptr);
    const auto* wrapper_before = output.get();
    const auto& output_before = static_cast<const ColumnNullable&>(*output);
    const auto* values_before = output_before.get_nested_column_ptr().get();
    const auto* null_map_before = output_before.get_null_map_column_ptr().get();
    const std::string json_before = json_at(assembled_values(output), 0);

    const std::array<StringRef, 1> malformed_cells {StringRef(malformed.back())};
    const Status first = assemble_binary_cells(malformed_cells, &output);
    ASSERT_TRUE(first.is<ErrorCode::CORRUPTION>()) << first;
    EXPECT_EQ(output.get(), wrapper_before);
    const auto& output_after = static_cast<const ColumnNullable&>(*output);
    EXPECT_EQ(output_after.get_nested_column_ptr().get(), values_before);
    EXPECT_EQ(output_after.get_null_map_column_ptr().get(), null_map_before);
    EXPECT_EQ(json_at(assembled_values(output), 0), json_before);

    ASSERT_TRUE(assemble_binary_cells(valid_cells, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), "42");
}

TEST(VariantAssemblerLegacyTest, SparseAndDocDecodeLegacyCellsToCanonicalEncoding) {
    LegacyCells source;
    const std::vector<std::pair<std::string, std::string>> entries {
            {"a", source.date_array_cell},
            {"d", source.date_cells[0]},
            {"dt", source.datetime_cells[0]},
            {"m", source.decimal_cells[0]},
    };
    auto values = map_column(entries);
    constexpr std::string_view EXPECTED = R"({"a":["1970-01-02","1970-01-03"],"d":"1970-01-02",)"
                                          R"("dt":"1970-01-01 00:00:01.000000",)"
                                          R"("m":12.340000000})";

    VariantAssemblerOptions sparse_options;
    sparse_options.storage_map_kind = StorageMapKind::SPARSE;
    auto sparse_assembler = create_assembler(std::move(sparse_options));
    ASSERT_NE(sparse_assembler, nullptr);
    const ColumnMap* sparse = values.get();
    VariantAssemblerBatchView sparse_batch;
    sparse_batch.num_rows = 1;
    sparse_batch.storage_map = sparse;
    ColumnNullable::MutablePtr sparse_output;
    ASSERT_TRUE(sparse_assembler->assemble(sparse_batch, &sparse_output).ok());
    EXPECT_FALSE(assembled_values(sparse_output).is_typed());
    EXPECT_EQ(json_at(assembled_values(sparse_output), 0), EXPECTED);

    VariantAssemblerOptions doc_options;
    doc_options.storage_map_kind = StorageMapKind::DOC;
    auto doc_assembler = create_assembler(std::move(doc_options));
    ASSERT_NE(doc_assembler, nullptr);
    VariantAssemblerBatchView doc_batch;
    doc_batch.num_rows = 1;
    doc_batch.storage_map = values.get();
    ColumnNullable::MutablePtr doc_output;
    ASSERT_TRUE(doc_assembler->assemble(doc_batch, &doc_output).ok());
    EXPECT_FALSE(assembled_values(doc_output).is_typed());
    EXPECT_EQ(json_at(assembled_values(doc_output), 0), EXPECTED);
}

TEST(VariantAssemblerLegacyTest, LegacyAncestorDescendantConflictsPreferDescendants) {
    LegacyCells source;
    auto same_stream = map_column_rows({
            {{"0", source.date_cells[0]},
             {"a", source.date_cells[0]},
             {"a-", source.date_cells[0]},
             {"a.b", source.date_cells[1]},
             {"b", source.date_cells[0]}},
            {{"0", source.date_cells[0]},
             {"a", source.date_cells[1]},
             {"a-", source.date_cells[0]},
             {"b", source.date_cells[0]}},
    });

    VariantAssemblerOptions sparse_options;
    sparse_options.requested_path = PathInData("a");
    sparse_options.storage_map_kind = StorageMapKind::SPARSE;
    auto sparse_assembler = create_assembler(std::move(sparse_options));
    ASSERT_NE(sparse_assembler, nullptr);
    const ColumnMap* sparse = same_stream.get();
    VariantAssemblerBatchView sparse_batch;
    sparse_batch.num_rows = 2;
    sparse_batch.storage_map = sparse;
    ColumnNullable::MutablePtr sparse_output;
    ASSERT_TRUE(sparse_assembler->assemble(sparse_batch, &sparse_output).ok());
    EXPECT_EQ(json_at(assembled_values(sparse_output), 0), R"({"b":"1970-01-03"})");
    EXPECT_EQ(json_at(assembled_values(sparse_output), 1), R"("1970-01-03")");

    VariantAssemblerOptions doc_options;
    doc_options.requested_path = PathInData("a");
    doc_options.storage_map_kind = StorageMapKind::DOC;
    auto doc_assembler = create_assembler(std::move(doc_options));
    ASSERT_NE(doc_assembler, nullptr);
    VariantAssemblerBatchView doc_batch;
    doc_batch.num_rows = 2;
    doc_batch.storage_map = same_stream.get();
    ColumnNullable::MutablePtr doc_output;
    ASSERT_TRUE(doc_assembler->assemble(doc_batch, &doc_output).ok());
    EXPECT_EQ(json_at(assembled_values(doc_output), 0), R"({"b":"1970-01-03"})");
    EXPECT_EQ(json_at(assembled_values(doc_output), 1), R"("1970-01-03")");

    auto sparse_child = map_column_rows({
            {{"a.b", source.date_cells[1]}},
            {},
    });
    VariantAssemblerOptions materialized_ancestor_options;
    materialized_ancestor_options.requested_path = PathInData("a");
    materialized_ancestor_options.materialized_paths.push_back(
            {.path = PathInData("a"), .type = source.date_type});
    materialized_ancestor_options.storage_map_kind = StorageMapKind::SPARSE;
    auto materialized_ancestor_assembler =
            create_assembler(std::move(materialized_ancestor_options));
    ASSERT_NE(materialized_ancestor_assembler, nullptr);
    const IColumn* materialized_ancestor = source.dates.get();
    const ColumnMap* sparse_child_ptr = sparse_child.get();
    VariantAssemblerBatchView materialized_ancestor_batch;
    materialized_ancestor_batch.num_rows = 2;
    materialized_ancestor_batch.materialized_columns = {&materialized_ancestor, 1};
    materialized_ancestor_batch.storage_map = sparse_child_ptr;
    ColumnNullable::MutablePtr materialized_ancestor_output;
    ASSERT_TRUE(materialized_ancestor_assembler
                        ->assemble(materialized_ancestor_batch, &materialized_ancestor_output)
                        .ok());
    EXPECT_EQ(json_at(assembled_values(materialized_ancestor_output), 0), R"({"b":"1970-01-03"})");
    EXPECT_EQ(json_at(assembled_values(materialized_ancestor_output), 1), R"("1970-01-03")");

    auto sparse_ancestor = map_column_rows({
            {{"a", source.date_cells[0]}},
            {{"a", source.date_cells[1]}},
    });
    VariantAssemblerOptions materialized_descendant_options;
    materialized_descendant_options.requested_path = PathInData("a");
    materialized_descendant_options.materialized_paths.push_back(
            {.path = PathInData("a.b"), .type = source.date_type});
    materialized_descendant_options.storage_map_kind = StorageMapKind::SPARSE;
    auto materialized_descendant_assembler =
            create_assembler(std::move(materialized_descendant_options));
    ASSERT_NE(materialized_descendant_assembler, nullptr);
    const IColumn* materialized_descendant = source.dates.get();
    const ColumnMap* sparse_ancestor_ptr = sparse_ancestor.get();
    VariantAssemblerBatchView materialized_descendant_batch;
    materialized_descendant_batch.num_rows = 2;
    materialized_descendant_batch.materialized_columns = {&materialized_descendant, 1};
    materialized_descendant_batch.storage_map = sparse_ancestor_ptr;
    ColumnNullable::MutablePtr materialized_descendant_output;
    ASSERT_TRUE(materialized_descendant_assembler
                        ->assemble(materialized_descendant_batch, &materialized_descendant_output)
                        .ok());
    EXPECT_EQ(json_at(assembled_values(materialized_descendant_output), 0),
              R"({"b":"1970-01-02"})");
    EXPECT_EQ(json_at(assembled_values(materialized_descendant_output), 1),
              R"({"b":"1970-01-03"})");
}

TEST(VariantAssemblerLegacyTest, RawOrderedConflictsKeepDescendantsWithoutSorting) {
    LegacyCells source;
    auto values = map_column_rows({
            {{"a-", source.date_cells[0]}, {"a.b", source.date_cells[1]}},
            {{"a", source.date_cells[0]},
             {"a-", source.date_cells[0]},
             {"a-.b", source.date_cells[1]},
             {"a.b", source.date_cells[1]}},
    });
    constexpr std::array<std::string_view, 2> EXPECTED {
            R"({"a":{"b":"1970-01-03"},"a-":"1970-01-02"})",
            R"({"a":{"b":"1970-01-03"},"a-":{"b":"1970-01-03"}})",
    };

    VariantAssemblerOptions sparse_options;
    sparse_options.storage_map_kind = StorageMapKind::SPARSE;
    auto sparse_assembler = create_assembler(std::move(sparse_options));
    ASSERT_NE(sparse_assembler, nullptr);
    const ColumnMap* sparse = values.get();
    VariantAssemblerBatchView sparse_batch;
    sparse_batch.num_rows = 2;
    sparse_batch.storage_map = sparse;
    ColumnNullable::MutablePtr sparse_output;
    ASSERT_TRUE(sparse_assembler->assemble(sparse_batch, &sparse_output).ok());
    EXPECT_EQ(json_at(assembled_values(sparse_output), 0), EXPECTED[0]);
    EXPECT_EQ(json_at(assembled_values(sparse_output), 1), EXPECTED[1]);

    VariantAssemblerOptions doc_options;
    doc_options.storage_map_kind = StorageMapKind::DOC;
    auto doc_assembler = create_assembler(std::move(doc_options));
    ASSERT_NE(doc_assembler, nullptr);
    VariantAssemblerBatchView doc_batch;
    doc_batch.num_rows = 2;
    doc_batch.storage_map = values.get();
    ColumnNullable::MutablePtr doc_output;
    ASSERT_TRUE(doc_assembler->assemble(doc_batch, &doc_output).ok());
    EXPECT_EQ(json_at(assembled_values(doc_output), 0), EXPECTED[0]);
    EXPECT_EQ(json_at(assembled_values(doc_output), 1), EXPECTED[1]);

    auto materialized_a = ColumnDate::create();
    materialized_a->insert_value(source.dates->get_data()[0]);
    auto sparse_interposed = map_column_rows({
            {{"a-", source.date_cells[0]}, {"a.b", source.date_cells[1]}},
    });
    VariantAssemblerOptions cross_source_options;
    cross_source_options.materialized_paths.push_back(
            {.path = PathInData("a"), .type = source.date_type});
    cross_source_options.storage_map_kind = StorageMapKind::SPARSE;
    auto cross_source_assembler = create_assembler(std::move(cross_source_options));
    ASSERT_NE(cross_source_assembler, nullptr);
    const IColumn* materialized_a_ptr = materialized_a.get();
    const ColumnMap* sparse_interposed_ptr = sparse_interposed.get();
    VariantAssemblerBatchView cross_source_batch;
    cross_source_batch.num_rows = 1;
    cross_source_batch.materialized_columns = {&materialized_a_ptr, 1};
    cross_source_batch.storage_map = sparse_interposed_ptr;
    ColumnNullable::MutablePtr cross_source_output;
    ASSERT_TRUE(cross_source_assembler->assemble(cross_source_batch, &cross_source_output).ok());
    EXPECT_EQ(json_at(assembled_values(cross_source_output), 0), EXPECTED[0]);

    auto materialized_root = ColumnDate::create();
    materialized_root->insert_value(source.dates->get_data()[0]);
    auto materialized_dash = ColumnDate::create();
    materialized_dash->insert_value(source.dates->get_data()[0]);
    auto materialized_child = ColumnDate::create();
    materialized_child->insert_value(source.dates->get_data()[1]);
    VariantAssemblerOptions materialized_options;
    materialized_options.materialized_paths = {
            {.path = PathInData("a"), .type = source.date_type},
            {.path = PathInData("a-"), .type = source.date_type},
            {.path = PathInData("a.b"), .type = source.date_type},
    };
    auto materialized_assembler = create_assembler(std::move(materialized_options));
    const std::array<const IColumn*, 3> materialized_columns {
            materialized_root.get(), materialized_dash.get(), materialized_child.get()};
    VariantAssemblerBatchView materialized_batch;
    materialized_batch.num_rows = 1;
    materialized_batch.materialized_columns = materialized_columns;
    ColumnNullable::MutablePtr materialized_output;
    ASSERT_TRUE(materialized_assembler->assemble(materialized_batch, &materialized_output).ok());
    EXPECT_EQ(json_at(assembled_values(materialized_output), 0), EXPECTED[0]);
}

TEST(VariantAssemblerLegacyTest, EmptyObjectKeyAncestorPrefersDescendantWithoutNullMemcmp) {
    LegacyCells source;
    auto values = map_column({{"", source.date_cells[0]}, {".child", source.date_cells[1]}});

    VariantAssemblerOptions options;
    options.storage_map_kind = StorageMapKind::DOC;
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.storage_map = values.get();

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"":{"child":"1970-01-03"}})");

    VariantAssemblerOptions subtree_options;
    subtree_options.requested_path = PathInData("");
    subtree_options.storage_map_kind = StorageMapKind::DOC;
    auto subtree_assembler = create_assembler(std::move(subtree_options));
    ColumnNullable::MutablePtr subtree_output;
    ASSERT_TRUE(subtree_assembler->assemble(batch, &subtree_output).ok());
    EXPECT_EQ(json_at(assembled_values(subtree_output), 0), R"({"child":"1970-01-03"})");
}

TEST(VariantAssemblerLegacyTest, RequestedPathUsesExactDotBoundary) {
    LegacyCells source;
    auto values = map_column_rows({
            {{"a.b", source.date_cells[0]},
             {"a.b-", source.date_cells[0]},
             {"a.b.c", source.date_cells[1]}},
            {{"a.b-", source.date_cells[0]}},
    });

    VariantAssemblerOptions options;
    options.requested_path = PathInData("a.b");
    options.storage_map_kind = StorageMapKind::SPARSE;
    auto assembler = create_assembler(std::move(options));
    VariantAssemblerBatchView batch;
    batch.num_rows = 2;
    batch.storage_map = values.get();

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"c":"1970-01-03"})");
    EXPECT_EQ(json_at(assembled_values(output), 1), "null");
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {0, 1}));
}

TEST(VariantAssemblerLegacyTest, LogicalRootAndEmptyKeyShareRawBytesButNotSemantics) {
    LegacyCells source;
    auto root = ColumnDate::create();
    root->insert_value(source.dates->get_data()[0]);
    auto empty_key = ColumnDate::create();
    empty_key->insert_value(source.dates->get_data()[1]);

    VariantAssemblerOptions options;
    options.materialized_paths = {
            {.path = PathInData(), .type = source.date_type},
            {.path = PathInData(""), .type = source.date_type},
    };
    auto assembler = create_assembler(std::move(options));
    const std::array<const IColumn*, 2> materialized {root.get(), empty_key.get()};
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = materialized;

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), R"({"":"1970-01-03"})");
}

TEST(VariantAssemblerLegacyTest, EmptyDocRowKeepsRawOrderedMaterializedPaths) {
    LegacyCells source;
    auto dash = ColumnDate::create();
    dash->insert_value(source.dates->get_data()[0]);
    auto child = ColumnDate::create();
    child->insert_value(source.dates->get_data()[1]);
    auto empty_doc = map_column_rows({{}});

    VariantAssemblerOptions options;
    options.storage_map_kind = StorageMapKind::DOC;
    options.materialized_paths = {
            {.path = PathInData("a-"), .type = source.date_type},
            {.path = PathInData("a.b"), .type = source.date_type},
    };
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);
    const std::array<const IColumn*, 2> materialized {dash.get(), child.get()};
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = materialized;
    batch.storage_map = empty_doc.get();

    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0),
              R"({"a":{"b":"1970-01-03"},"a-":"1970-01-02"})");
}

TEST(VariantAssemblerLegacyTest, EmptyPhysicalRowsPublishAsNull) {
    VariantAssemblerOptions root_options;
    root_options.has_root = true;
    auto root_assembler = create_assembler(std::move(root_options));
    ASSERT_NE(root_assembler, nullptr);
    auto empty_root = ColumnString::create();
    empty_root->insert_default();
    empty_root->insert_default();
    auto root_nulls = ColumnUInt8::create();
    root_nulls->insert_value(0);
    root_nulls->insert_value(1);
    auto nullable_root = ColumnNullable::create(std::move(empty_root), std::move(root_nulls));
    VariantAssemblerBatchView root_batch;
    root_batch.num_rows = 2;
    root_batch.root_jsonb = nullable_root.get();
    ColumnNullable::MutablePtr root_output;
    ASSERT_TRUE(root_assembler->assemble(root_batch, &root_output).ok());
    EXPECT_EQ(json_at(assembled_values(root_output), 0), "null");
    EXPECT_EQ(json_at(assembled_values(root_output), 1), "null");
    EXPECT_EQ(root_output->get_null_map_data(), (PaddedPODArray<uint8_t> {1, 1}));

    VariantAssemblerOptions subtree_options;
    subtree_options.requested_path = PathInData("a");
    subtree_options.storage_map_kind = StorageMapKind::SPARSE;
    auto subtree_assembler = create_assembler(std::move(subtree_options));
    ASSERT_NE(subtree_assembler, nullptr);
    auto empty_sparse = map_column_rows({{}});
    VariantAssemblerBatchView subtree_batch;
    subtree_batch.num_rows = 1;
    subtree_batch.storage_map = empty_sparse.get();
    ColumnNullable::MutablePtr subtree_output;
    ASSERT_TRUE(subtree_assembler->assemble(subtree_batch, &subtree_output).ok());
    EXPECT_EQ(json_at(assembled_values(subtree_output), 0), "null");
    EXPECT_EQ(subtree_output->get_null_map_data(), (PaddedPODArray<uint8_t> {1}));
}

TEST(VariantAssemblerLegacyTest, HierarchicalStorageKeepsPresentVariantNullsInsidePayload) {
    VariantAssemblerOptions root_options;
    root_options.has_root = true;
    auto root_assembler = create_assembler(std::move(root_options));
    ASSERT_NE(root_assembler, nullptr);
    auto root = jsonb_column("null");
    VariantAssemblerBatchView root_batch;
    root_batch.num_rows = 1;
    root_batch.root_jsonb = root.get();
    ColumnNullable::MutablePtr root_output;
    ASSERT_TRUE(root_assembler->assemble(root_batch, &root_output).ok());
    EXPECT_EQ(json_at(assembled_values(root_output), 0), "null");
    EXPECT_EQ(root_output->get_null_map_data(), (PaddedPODArray<uint8_t> {0}));

    VariantAssemblerOptions doc_options;
    doc_options.storage_map_kind = StorageMapKind::DOC;
    doc_options.requested_path = PathInData("a");
    auto doc_assembler = create_assembler(std::move(doc_options));
    ASSERT_NE(doc_assembler, nullptr);
    auto doc = map_column({{"a", jsonb_storage_cell("null")}});
    VariantAssemblerBatchView doc_batch;
    doc_batch.num_rows = 1;
    doc_batch.storage_map = doc.get();
    ColumnNullable::MutablePtr doc_output;
    ASSERT_TRUE(doc_assembler->assemble(doc_batch, &doc_output).ok());
    EXPECT_EQ(json_at(assembled_values(doc_output), 0), "null");
    EXPECT_EQ(doc_output->get_null_map_data(), (PaddedPODArray<uint8_t> {0}));
}

TEST(VariantAssemblerLegacyTest, EmptyHierarchicalRootRowRemainsAnObject) {
    VariantAssemblerOptions options;
    options.storage_map_kind = StorageMapKind::DOC;
    auto assembler = create_assembler(std::move(options));
    ASSERT_NE(assembler, nullptr);
    auto doc = map_column({});
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.storage_map = doc.get();
    ColumnNullable::MutablePtr output;
    ASSERT_TRUE(assembler->assemble(batch, &output).ok());
    EXPECT_EQ(json_at(assembled_values(output), 0), "{}");
    EXPECT_EQ(output->get_null_map_data(), (PaddedPODArray<uint8_t> {0}));
}

TEST(VariantAssemblerLegacyTest, NestedArrayPathReturnsNotSupported) {
    PathInDataBuilder requested_builder;
    requested_builder.append("items", false);
    requested_builder.append("id", true);
    VariantAssemblerOptions requested_options;
    requested_options.requested_path = requested_builder.build();
    auto requested = VariantAssembler::create(std::move(requested_options));
    ASSERT_FALSE(requested.has_value());
    EXPECT_TRUE(requested.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << requested.error();
    EXPECT_NE(requested.error().to_string().find("nested array path 'items.id'"),
              std::string::npos);

    PathInDataBuilder materialized_builder;
    materialized_builder.append("items", false);
    materialized_builder.append("id", true);
    VariantAssemblerOptions materialized_options;
    materialized_options.materialized_paths.push_back(
            {.path = materialized_builder.build(), .type = std::make_shared<DataTypeInt32>()});
    auto materialized = VariantAssembler::create(std::move(materialized_options));
    ASSERT_FALSE(materialized.has_value());
    EXPECT_TRUE(materialized.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>())
            << materialized.error();
    EXPECT_NE(materialized.error().to_string().find("nested array path 'items.id'"),
              std::string::npos);
}

} // namespace
} // namespace doris::segment_v2::variant_v2
