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

#include "core/column/variant_v2/column_variant_v2.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/decimalv2_value.h"
#include "core/value/ipv4_value.h"
#include "core/value/ipv6_value.h"
#include "core/value/timestamptz_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/string_hash_map.h"
#include "exec/common/sip_hash.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVector<TYPE_UINT32>;

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder block = encoder.finish_batch();
    return VariantField::encode(block.value_at(0));
}

void append_unsigned(std::string& output, unsigned __int128 value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

std::string empty_metadata_bytes() {
    std::string metadata;
    metadata.push_back(
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK));
    metadata.push_back(0);
    metadata.push_back(0);
    return metadata;
}

VariantField encoded_field(std::string metadata, std::string value) {
    std::string field;
    append_unsigned(field, metadata.size(), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return VariantField::decode({field.data(), field.size()});
}

std::string integer_value_bytes(int64_t value, uint8_t width) {
    VariantPrimitiveId id = VariantPrimitiveId::INT64;
    if (width == 1) {
        id = VariantPrimitiveId::INT8;
    } else if (width == 2) {
        id = VariantPrimitiveId::INT16;
    } else if (width == 4) {
        id = VariantPrimitiveId::INT32;
    }
    std::string encoded;
    encoded.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    append_unsigned(encoded, static_cast<unsigned __int128>(static_cast<__int128>(value)), width);
    return encoded;
}

VariantField encoded_integer(int64_t value, uint8_t width) {
    return encoded_field(empty_metadata_bytes(), integer_value_bytes(value, width));
}

VariantField encoded_decimal(__int128 unscaled, uint8_t scale) {
    std::string value;
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DECIMAL16)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(static_cast<char>(scale));
    append_unsigned(value, static_cast<unsigned __int128>(unscaled), 16);
    return encoded_field(empty_metadata_bytes(), std::move(value));
}

VariantField encoded_double(double number) {
    std::string value;
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DOUBLE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    append_unsigned(value, std::bit_cast<uint64_t>(number), sizeof(uint64_t));
    return encoded_field(empty_metadata_bytes(), std::move(value));
}

VariantField encoded_integer_array(int64_t value, uint8_t width) {
    const std::string child = integer_value_bytes(value, width);
    std::string array;
    array.push_back(static_cast<char>(VariantBasicType::ARRAY));
    array.push_back(1);
    array.push_back(0);
    array.push_back(static_cast<char>(child.size()));
    array.append(child);
    return encoded_field(empty_metadata_bytes(), std::move(array));
}

std::string noncanonical_object_field_bytes() {
    std::string metadata;
    metadata.push_back(static_cast<char>(VARIANT_ENCODING_VERSION));
    append_unsigned(metadata, 3, 1);
    append_unsigned(metadata, 0, 1);
    append_unsigned(metadata, 1, 1);
    append_unsigned(metadata, 2, 1);
    append_unsigned(metadata, 8, 1);
    metadata.append("baunused");

    std::string value;
    value.push_back(static_cast<char>(VariantBasicType::OBJECT));
    append_unsigned(value, 2, 1);
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    // Logical key order is a,b through ids 1,0. Their physical values are stored as b,a.
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 2, 1);
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));

    std::string field;
    append_unsigned(field, static_cast<uint32_t>(metadata.size()), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return field;
}

std::string_view as_view(StringRef value) {
    return {value.data, value.size};
}

struct OwnedEncodedData {
    std::string metadata_bytes;
    std::vector<uint32_t> metadata_offsets {0};
    std::vector<uint32_t> meta_ids;
    std::string value_bytes;
    std::vector<uint32_t> value_offsets {0};

    uint32_t add_metadata(VariantMetadataRef metadata) {
        EXPECT_LE(metadata_bytes.size() + metadata.size, std::numeric_limits<uint32_t>::max());
        metadata_bytes.append(metadata.data, metadata.size);
        metadata_offsets.push_back(static_cast<uint32_t>(metadata_bytes.size()));
        return static_cast<uint32_t>(metadata_offsets.size() - 2);
    }

    void add_value(VariantRef value, uint32_t metadata_id) {
        EXPECT_LE(value_bytes.size() + value.value.size, std::numeric_limits<uint32_t>::max());
        value_bytes.append(value.value.data, value.value.size);
        value_offsets.push_back(static_cast<uint32_t>(value_bytes.size()));
        meta_ids.push_back(metadata_id);
    }

    ColumnVariantV2::EncodedDataView view(bool omit_single_metadata_ids = false) const {
        const std::span<const uint32_t> ids = omit_single_metadata_ids
                                                      ? std::span<const uint32_t> {}
                                                      : std::span<const uint32_t>(meta_ids);
        return {.metadata_bytes = {metadata_bytes.data(), metadata_bytes.size()},
                .metadata_offsets = metadata_offsets,
                .meta_ids = ids,
                .value_bytes = {value_bytes.data(), value_bytes.size()},
                .value_offsets = value_offsets};
    }
};

template <typename Function>
void expect_not_implemented(Function&& function, std::string_view marker) {
    try {
        std::invoke(std::forward<Function>(function));
        ADD_FAILURE() << "expected NOT_IMPLEMENTED_ERROR containing " << marker;
    } catch (const Exception& exception) {
        EXPECT_EQ(exception.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
        EXPECT_NE(exception.message().find(marker), std::string::npos) << exception.message();
    }
}

std::vector<ColumnPtr> subcolumns(ColumnVariantV2& column) {
    std::vector<ColumnPtr> result;
    column.for_each_subcolumn(
            [&](const IColumn& subcolumn) { result.push_back(subcolumn.get_ptr()); });
    return result;
}

const IColumn* subcolumn_address(ColumnVariantV2& column, size_t target) {
    const IColumn* result = nullptr;
    size_t index = 0;
    column.for_each_subcolumn([&](const IColumn& subcolumn) {
        if (index == target) {
            result = &subcolumn;
        }
        ++index;
    });
    EXPECT_EQ(index, 3);
    return result;
}

size_t metadata_count(ColumnVariantV2& column) {
    return assert_cast<const ColumnString&>(*subcolumn_address(column, 0)).size();
}

struct JsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string json_at(const ColumnVariantV2& column, size_t row) {
    JsonWriter writer;
    to_json(column.get_value_ref(row), writer);
    return writer.value;
}

void append_json_row(ColumnVariantV2& column, ColumnString& reference, std::string_view json) {
    insert_encoded_field(column, encode_json(json));
    reference.insert_data(json.data(), json.size());
}

void fill_row_transform_columns(ColumnVariantV2& column, ColumnString& reference) {
    append_json_row(column, reference, R"({"a":1})");
    append_json_row(column, reference, R"({"b":2})");
    append_json_row(column, reference, R"({"c":3})");
    append_json_row(column, reference, R"({"a":4})");
    append_json_row(column, reference, "[5]");
}

void expect_values_match(const ColumnVariantV2& column, const ColumnString& reference) {
    ASSERT_EQ(column.size(), reference.size());
    for (size_t row = 0; row < reference.size(); ++row) {
        EXPECT_EQ(json_at(column, row), as_view(reference.get_data_at(row))) << "row " << row;
    }
    column.sanity_check();
}

void fill_equivalent_columns(ColumnVariantV2& canonical, ColumnVariantV2& external) {
    insert_encoded_field(canonical, encode_json("42"));
    insert_encoded_field(canonical, encode_json(R"({"a":true,"b":null})"));
    insert_encoded_field(canonical, encode_json("[42]"));
    insert_encoded_field(canonical, encode_json("42"));
    insert_encoded_field(canonical, encode_json("42"));

    insert_encoded_field(external, encoded_integer(42, sizeof(int64_t)));
    const std::string object = noncanonical_object_field_bytes();
    insert_encoded_field(external, VariantField::decode({object.data(), object.size()}));
    insert_encoded_field(external, encoded_integer_array(42, sizeof(int64_t)));
    insert_encoded_field(external, encoded_decimal(42, 0));
    insert_encoded_field(external, encoded_double(42.0));
}

uint64_t canonical_xx_hash(VariantRef value, uint64_t seed) {
    VariantXxHashSink sink(seed);
    canonical_hash(value, sink);
    return sink.digest();
}

uint32_t canonical_crc_hash(VariantRef value, uint32_t seed) {
    VariantCrc32HashSink sink(seed);
    canonical_hash(value, sink);
    return sink.digest();
}

uint32_t canonical_crc32c_hash(VariantRef value, uint32_t seed) {
    VariantCrc32cHashSink sink(seed);
    canonical_hash(value, sink);
    return sink.digest();
}

void replace_subcolumn(ColumnVariantV2& column, size_t target, ColumnPtr replacement) {
    ColumnVariantV2::TestAccess::replace_encoded_subcolumn(column, target, std::move(replacement));
}

ColumnPtr nullable_int32(std::span<const int32_t> values, std::span<const uint8_t> null_map) {
    EXPECT_EQ(values.size(), null_map.size());
    auto nested = ColumnInt32::create();
    nested->get_data().insert(values.begin(), values.end());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().insert(null_map.begin(), null_map.end());
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

ColumnVariantV2::MutablePtr typed_int32(std::span<const int32_t> values,
                                        std::span<const uint8_t> null_map) {
    return ColumnVariantV2::create_typed(nullable_int32(values, null_map),
                                         std::make_shared<DataTypeInt32>());
}

ColumnPtr nullable_strings(std::span<const std::string_view> values,
                           std::span<const uint8_t> null_map) {
    EXPECT_EQ(values.size(), null_map.size());
    auto nested = ColumnString::create();
    for (std::string_view value : values) {
        nested->insert_data(value.data(), value.size());
    }
    auto nulls = ColumnUInt8::create();
    nulls->get_data().insert(null_map.begin(), null_map.end());
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

ColumnVariantV2::MutablePtr typed_strings(std::span<const std::string_view> values,
                                          std::span<const uint8_t> null_map) {
    return ColumnVariantV2::create_typed(nullable_strings(values, null_map),
                                         std::make_shared<DataTypeString>());
}

ColumnPtr wrap_nullable(MutableColumnPtr nested, std::span<const uint8_t> null_map) {
    EXPECT_EQ(nested->size(), null_map.size());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().insert(null_map.begin(), null_map.end());
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_fixed(std::initializer_list<Value> values,
                         std::initializer_list<uint8_t> null_map) {
    auto nested = ColumnType::create();
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), {null_map.begin(), null_map.size()});
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_decimal(uint32_t scale, std::initializer_list<Value> values,
                           std::initializer_list<uint8_t> null_map) {
    auto nested = ColumnType::create(0, scale);
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), {null_map.begin(), null_map.size()});
}

__int128 power_of_ten_i128(uint8_t exponent) {
    __int128 value = 1;
    for (uint8_t digit = 0; digit < exponent; ++digit) {
        value *= 10;
    }
    return value;
}

constexpr uint32_t pack_olap_date(uint32_t year, uint32_t month, uint32_t day) {
    return (year << 9) | (month << 5) | day;
}

void validate_encoded_column(const ColumnVariantV2& column) {
    ASSERT_FALSE(column.is_typed());
    for (size_t row = 0; row < column.size(); ++row) {
        validate_canonical(column.get_value_ref(row));
    }
}

template <typename CheckValue>
void expect_single_typed_encoding(ColumnPtr nullable, DataTypePtr type, CheckValue&& check_value) {
    auto column = ColumnVariantV2::create_typed(std::move(nullable), std::move(type));
    column->ensure_encoded();
    EXPECT_FALSE(column->is_typed());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(metadata_count(*column), 1);
    check_value(column->get_value_ref(0));
    validate_encoded_column(*column);
}

void expect_int32_rows(const ColumnVariantV2& column, std::span<const int32_t> expected_values,
                       std::span<const uint8_t> expected_nulls) {
    ASSERT_EQ(expected_values.size(), expected_nulls.size());
    MutableColumnPtr materialized_base = column.clone();
    auto& materialized = assert_cast<ColumnVariantV2&>(*materialized_base);
    if (materialized.is_typed()) {
        materialized.ensure_encoded();
    }
    validate_encoded_column(materialized);
    ASSERT_EQ(materialized.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        if (expected_nulls[row] != 0) {
            EXPECT_TRUE(materialized.get_value_ref(row).is_null()) << row;
        } else {
            EXPECT_EQ(materialized.get_value_ref(row).get_int(), expected_values[row]) << row;
        }
    }
}

VariantField encoded_null() {
    std::string value(1, static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                           << VARIANT_VALUE_HEADER_SHIFT));
    return encoded_field(empty_metadata_bytes(), std::move(value));
}

VariantField encoded_string(std::string_view text) {
    EXPECT_LE(text.size(), std::numeric_limits<uint32_t>::max());
    std::string value;
    if (text.size() <= VARIANT_MAX_SHORT_STRING_SIZE) {
        value.push_back(static_cast<char>((text.size() << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
    } else {
        value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::STRING)
                                          << VARIANT_VALUE_HEADER_SHIFT));
        append_unsigned(value, text.size(), sizeof(uint32_t));
    }
    value.append(text);
    return encoded_field(empty_metadata_bytes(), std::move(value));
}

VariantField encoded_date(int32_t days_since_epoch) {
    std::string value;
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DATE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    append_unsigned(value, std::bit_cast<uint32_t>(days_since_epoch), sizeof(int32_t));
    return encoded_field(empty_metadata_bytes(), std::move(value));
}

VariantField encoded_timestamp(int64_t value, bool utc_adjusted, bool nanos) {
    VariantPrimitiveId id = VariantPrimitiveId::TIMESTAMP_NTZ_MICROS;
    if (utc_adjusted) {
        id = nanos ? VariantPrimitiveId::TIMESTAMP_NANOS : VariantPrimitiveId::TIMESTAMP_MICROS;
    } else if (nanos) {
        id = VariantPrimitiveId::TIMESTAMP_NTZ_NANOS;
    }
    std::string encoded;
    encoded.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    append_unsigned(encoded, std::bit_cast<uint64_t>(value), sizeof(int64_t));
    return encoded_field(empty_metadata_bytes(), std::move(encoded));
}

ColumnVariantV2::MutablePtr encoded_rows(std::span<const VariantField> values) {
    auto column = ColumnVariantV2::create();
    for (const VariantField& value : values) {
        insert_encoded_field(*column, value);
    }
    return column;
}

void expect_canonical_rows_equal(const ColumnVariantV2& left, const ColumnVariantV2& right) {
    ASSERT_EQ(left.size(), right.size());
    for (size_t row = 0; row < left.size(); ++row) {
        std::string left_cell(left.serialize_size_at(row), '\0');
        std::string right_cell(right.serialize_size_at(row), '\0');
        EXPECT_EQ(left.serialize_impl(left_cell.data(), row), left_cell.size());
        EXPECT_EQ(right.serialize_impl(right_cell.data(), row), right_cell.size());
        EXPECT_EQ(left_cell, right_cell) << row;
    }
}

struct MixedNumericColumns {
    ColumnVariantV2::MutablePtr typed;
    ColumnVariantV2::MutablePtr encoded;
};

MixedNumericColumns mixed_numeric_columns() {
    constexpr std::array<int32_t, 4> VALUES {1, 2, 3, 4};
    constexpr std::array<uint8_t, 4> NULLS {0, 1, 0, 0};
    const std::array<VariantField, 4> encoded_values {encoded_integer(1, sizeof(int8_t)),
                                                      encoded_null(), encoded_decimal(30, 1),
                                                      encoded_double(4.0)};
    return {.typed = typed_int32(VALUES, NULLS), .encoded = encoded_rows(encoded_values)};
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_fixed_values(std::span<const Value> values, std::span<const uint8_t> null_map) {
    EXPECT_EQ(values.size(), null_map.size());
    auto nested = ColumnType::create();
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), null_map);
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_decimal_values(uint32_t scale, std::span<const Value> values,
                                  std::span<const uint8_t> null_map) {
    EXPECT_EQ(values.size(), null_map.size());
    auto nested = ColumnType::create(0, scale);
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), null_map);
}

struct ETCrossCheckRepresentation {
    std::string name;
    ColumnVariantV2::MutablePtr column;
};

struct ETCrossCheckObservation {
    std::string serialized;
    std::string batch_serialized;
    std::string arena_serialized;
    uint64_t sip = 0;
    uint64_t xx = 0;
    uint32_t crc = 0;
    uint32_t crc32c = 0;
};

struct ETCrossCheckCoverage {
    bool encoded_encoded = false;
    bool encoded_typed = false;
    bool typed_typed = false;
};

std::vector<ETCrossCheckObservation> observe_et_cross_check(
        const ETCrossCheckRepresentation& representation, std::string_view group) {
    const ColumnVariantV2& column = *representation.column;
    const bool was_typed = column.is_typed();
    const size_t rows = column.size();
    std::vector<ETCrossCheckObservation> observations(rows);

    std::vector<uint64_t> xx(rows);
    std::vector<uint32_t> crc(rows);
    std::vector<uint32_t> crc32c(rows);
    for (size_t row = 0; row < rows; ++row) {
        xx[row] = 0x9E3779B185EBCA87ULL + row;
        crc[row] = 0xA5A5A5A5U + static_cast<uint32_t>(row);
        crc32c[row] = 0x5A5A5A5AU + static_cast<uint32_t>(row);
    }
    column.update_hashes_with_value(xx.data(), nullptr);
    column.update_crcs_with_value(crc.data(), PrimitiveType::TYPE_VARIANT,
                                  static_cast<uint32_t>(rows), 0, nullptr);
    column.update_crc32c_batch(crc32c.data(), nullptr);

    const size_t maximum_size = column.get_max_row_byte_size();
    std::vector<std::vector<char>> batch_buffers(rows, std::vector<char>(maximum_size));
    std::vector<StringRef> batch_keys;
    batch_keys.reserve(rows);
    for (std::vector<char>& buffer : batch_buffers) {
        batch_keys.emplace_back(buffer.data(), 0);
    }
    column.serialize(batch_keys.data(), batch_keys.size());

    Arena arena(16);
    const char* begin = nullptr;
    for (size_t row = 0; row < rows; ++row) {
        SCOPED_TRACE(testing::Message() << "group=" << group << ", row=" << row
                                        << ", representation=" << representation.name);
        ETCrossCheckObservation& observation = observations[row];
        observation.serialized.resize(column.serialize_size_at(row));
        EXPECT_EQ(column.serialize_impl(observation.serialized.data(), row),
                  observation.serialized.size());
        observation.batch_serialized.assign(batch_keys[row].data, batch_keys[row].size);
        const StringRef arena_cell = column.serialize_value_into_arena(row, arena, begin);
        observation.arena_serialized.assign(arena_cell.data, arena_cell.size);
        SipHash sip;
        column.update_hash_with_value(row, sip);
        observation.sip = sip.get64();
        observation.xx = xx[row];
        observation.crc = crc[row];
        observation.crc32c = crc32c[row];
    }
    EXPECT_EQ(column.is_typed(), was_typed) << representation.name;
    return observations;
}

void expect_et_cross_check_canonical(const ETCrossCheckRepresentation& left,
                                     const ETCrossCheckRepresentation& right, size_t row,
                                     const ETCrossCheckObservation& left_observation,
                                     const ETCrossCheckObservation& right_observation) {
    const VariantRef left_canonical = parse_canonical_serialized(
            {left_observation.serialized.data(), left_observation.serialized.size()});
    const VariantRef right_canonical = parse_canonical_serialized(
            {right_observation.serialized.data(), right_observation.serialized.size()});
    EXPECT_TRUE(canonical_equals(left_canonical, right_canonical));
    if (!left.column->is_typed() && !right.column->is_typed()) {
        EXPECT_TRUE(canonical_equals(left.column->get_value_ref(row),
                                     right.column->get_value_ref(row)));
    } else if (!left.column->is_typed()) {
        EXPECT_TRUE(canonical_equals(left.column->get_value_ref(row), right_canonical));
    } else if (!right.column->is_typed()) {
        EXPECT_TRUE(canonical_equals(left_canonical, right.column->get_value_ref(row)));
    }
}

void expect_et_cross_check_storage_paths(const ETCrossCheckObservation& observation) {
    EXPECT_EQ(observation.serialized, observation.batch_serialized);
    EXPECT_EQ(observation.serialized, observation.arena_serialized);
}

void expect_et_cross_check_hashes(const ETCrossCheckObservation& left,
                                  const ETCrossCheckObservation& right) {
    EXPECT_EQ(left.sip, right.sip);
    EXPECT_EQ(left.xx, right.xx);
    EXPECT_EQ(left.crc, right.crc);
    EXPECT_EQ(left.crc32c, right.crc32c);
}

void expect_et_cross_check_row(std::string_view group, size_t row,
                               const ETCrossCheckRepresentation& left,
                               const ETCrossCheckRepresentation& right,
                               const ETCrossCheckObservation& left_observation,
                               const ETCrossCheckObservation& right_observation) {
    SCOPED_TRACE(testing::Message() << "group=" << group << ", row=" << row
                                    << ", left=" << left.name << ", right=" << right.name);
    expect_et_cross_check_canonical(left, right, row, left_observation, right_observation);
    expect_et_cross_check_storage_paths(left_observation);
    expect_et_cross_check_storage_paths(right_observation);
    EXPECT_EQ(left_observation.serialized, right_observation.serialized);
    expect_et_cross_check_hashes(left_observation, right_observation);
}

void expect_et_cross_check_pair(std::string_view group, const ETCrossCheckRepresentation& left,
                                const ETCrossCheckRepresentation& right,
                                const std::vector<ETCrossCheckObservation>& left_observations,
                                const std::vector<ETCrossCheckObservation>& right_observations,
                                ETCrossCheckCoverage& coverage) {
    ASSERT_EQ(left_observations.size(), right_observations.size());
    const bool left_typed = left.column->is_typed();
    const bool right_typed = right.column->is_typed();
    coverage.encoded_encoded |= !left_typed && !right_typed;
    coverage.encoded_typed |= left_typed != right_typed;
    coverage.typed_typed |= left_typed && right_typed;
    for (size_t row = 0; row < left_observations.size(); ++row) {
        expect_et_cross_check_row(group, row, left, right, left_observations[row],
                                  right_observations[row]);
    }
}

void expect_et_cross_check_group(std::string_view group,
                                 const std::vector<ETCrossCheckRepresentation>& representations,
                                 ETCrossCheckCoverage& coverage) {
    ASSERT_GE(representations.size(), 2);
    const size_t rows = representations.front().column->size();
    std::vector<std::vector<ETCrossCheckObservation>> observations;
    observations.reserve(representations.size());
    for (const ETCrossCheckRepresentation& representation : representations) {
        ASSERT_EQ(representation.column->size(), rows) << representation.name;
        observations.push_back(observe_et_cross_check(representation, group));
    }

    for (size_t left_index = 0; left_index < representations.size(); ++left_index) {
        for (size_t right_index = left_index + 1; right_index < representations.size();
             ++right_index) {
            expect_et_cross_check_pair(group, representations[left_index],
                                       representations[right_index], observations[left_index],
                                       observations[right_index], coverage);
        }
    }

    for (const ETCrossCheckRepresentation& representation : representations) {
        EXPECT_EQ(representation.column->is_typed(), representation.name.starts_with("T/"))
                << representation.name;
    }
}

void expect_et_cross_check_distinct(std::string_view group, const ETCrossCheckRepresentation& left,
                                    size_t left_row, const ETCrossCheckRepresentation& right,
                                    size_t right_row) {
    SCOPED_TRACE(testing::Message()
                 << "group=" << group << ", left-row=" << left_row << ", right-row=" << right_row
                 << ", left=" << left.name << ", right=" << right.name);
    const std::vector<ETCrossCheckObservation> left_observations =
            observe_et_cross_check(left, group);
    const std::vector<ETCrossCheckObservation> right_observations =
            observe_et_cross_check(right, group);
    ASSERT_LT(left_row, left_observations.size());
    ASSERT_LT(right_row, right_observations.size());
    const ETCrossCheckObservation& left_observation = left_observations[left_row];
    const ETCrossCheckObservation& right_observation = right_observations[right_row];
    const VariantRef left_canonical = parse_canonical_serialized(
            {left_observation.serialized.data(), left_observation.serialized.size()});
    const VariantRef right_canonical = parse_canonical_serialized(
            {right_observation.serialized.data(), right_observation.serialized.size()});
    EXPECT_FALSE(canonical_equals(left_canonical, right_canonical));
    if (!left.column->is_typed() && !right.column->is_typed()) {
        EXPECT_FALSE(canonical_equals(left.column->get_value_ref(left_row),
                                      right.column->get_value_ref(right_row)));
    } else if (!left.column->is_typed()) {
        EXPECT_FALSE(canonical_equals(left.column->get_value_ref(left_row), right_canonical));
    } else if (!right.column->is_typed()) {
        EXPECT_FALSE(canonical_equals(left_canonical, right.column->get_value_ref(right_row)));
    }
    EXPECT_NE(left_observation.serialized, right_observation.serialized);
    EXPECT_NE(left_observation.batch_serialized, right_observation.batch_serialized);
    EXPECT_NE(left_observation.arena_serialized, right_observation.arena_serialized);
}

} // namespace

TEST(ColumnVariantV2Test, EmptySkeleton) {
    auto column = ColumnVariantV2::create();
    EXPECT_EQ(column->size(), 0);
    EXPECT_EQ(column->get_name(), "variant_v2");
    EXPECT_TRUE(column->is_variable_length());
    EXPECT_EQ(column->byte_size(), 0);
    EXPECT_LE(column->byte_size(), column->allocated_bytes());
    EXPECT_TRUE(column->structure_equals(*ColumnVariantV2::create()));
    EXPECT_FALSE(column->structure_equals(*ColumnString::create()));
    static_cast<void>(column->has_enough_capacity(*ColumnVariantV2::create()));
    column->sanity_check();
    column->finalize();
    EXPECT_EQ(column->size(), 0);
}

TEST(ColumnVariantV2Test, EncodedScalarObjectAndArray) {
    auto column = ColumnVariantV2::create();
    const VariantField scalar = encode_json("7");
    const VariantField object_a = encode_json(R"({"a":1})");
    const VariantField array = encode_json(R"([true,null,"x"])");
    const VariantField object_a_again = encode_json(R"({"a":[2]})");
    const VariantField object_b = encode_json(R"({"b":3})");

    insert_encoded_field(*column, scalar);
    insert_encoded_field(*column, object_a);
    insert_encoded_field(*column, array);
    insert_encoded_field(*column, object_a_again);
    insert_encoded_field(*column, object_b);

    ASSERT_EQ(column->size(), 5);
    EXPECT_EQ(column->get_value_ref(0).get_int(), 7);

    VariantRef a;
    ASSERT_TRUE(column->get_value_ref(1).object_find(StringRef("a"), &a));
    EXPECT_EQ(a.get_int(), 1);
    const VariantRef array_ref = column->get_value_ref(2);
    ASSERT_EQ(array_ref.num_elements(), 3);
    EXPECT_TRUE(array_ref.array_at(0).get_bool());
    EXPECT_TRUE(array_ref.array_at(1).is_null());
    EXPECT_EQ(array_ref.array_at(2).get_string(), StringRef("x"));

    const std::vector<ColumnPtr> children = subcolumns(*column);
    ASSERT_EQ(children.size(), 3);
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*children[1]).get_data();
    ASSERT_EQ(metadata_ids.size(), 5);
    EXPECT_EQ(metadata_ids[0], metadata_ids[2]);
    EXPECT_EQ(metadata_ids[1], metadata_ids[3]);
    EXPECT_NE(metadata_ids[0], metadata_ids[1]);
    EXPECT_NE(metadata_ids[1], metadata_ids[4]);
    EXPECT_EQ(assert_cast<const ColumnString&>(*children[0]).size(), 3);

    EXPECT_GT(column->byte_size(), 0);
    EXPECT_LE(column->byte_size(), column->allocated_bytes());
    column->sanity_check();
}

TEST(ColumnVariantV2Test, PreservesLegalNoncanonicalBytes) {
    const std::string raw = noncanonical_object_field_bytes();
    const VariantField noncanonical = VariantField::decode({raw.data(), raw.size()});
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, noncanonical);
    insert_encoded_field(*column, noncanonical);

    VariantField round_trip = VariantField::encode(column->get_value_ref(0));
    EXPECT_EQ(as_view(round_trip.bytes()), raw);
    VariantRef a;
    VariantRef b;
    ASSERT_TRUE(column->get_value_ref(0).object_find(StringRef("a"), &a));
    ASSERT_TRUE(column->get_value_ref(0).object_find(StringRef("b"), &b));
    EXPECT_TRUE(a.get_bool());
    EXPECT_TRUE(b.is_null());

    const std::vector<ColumnPtr> children = subcolumns(*column);
    const auto& metadata_ids = assert_cast<const MetaIdsColumn&>(*children[1]).get_data();
    ASSERT_EQ(metadata_ids.size(), 2);
    EXPECT_EQ(metadata_ids[0], metadata_ids[1]);
    EXPECT_EQ(assert_cast<const ColumnString&>(*children[0]).size(), 1);
}

TEST(ColumnVariantV2Test, InsertRejectsMalformedMetadataAndTrailingValueBytes) {
    OwnedEncodedData invalid_metadata;
    invalid_metadata.metadata_bytes.assign(1, static_cast<char>(VARIANT_ENCODING_VERSION));
    invalid_metadata.metadata_offsets = {0, 1};
    invalid_metadata.meta_ids = {0};
    invalid_metadata.value_bytes.assign(
            1, static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                 << VARIANT_VALUE_HEADER_SHIFT));
    invalid_metadata.value_offsets = {0, 1};
    auto metadata_column = ColumnVariantV2::create();
    EXPECT_THROW(metadata_column->insert_encoded_rows(invalid_metadata.view()), Exception);

    OwnedEncodedData trailing_value;
    trailing_value.metadata_bytes = empty_metadata_bytes();
    trailing_value.metadata_offsets = {0,
                                       static_cast<uint32_t>(trailing_value.metadata_bytes.size())};
    trailing_value.meta_ids = {0};
    trailing_value.value_bytes.assign(
            2, static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                 << VARIANT_VALUE_HEADER_SHIFT));
    trailing_value.value_offsets = {0, 2};
    auto value_column = ColumnVariantV2::create();
    EXPECT_THROW(value_column->insert_encoded_rows(trailing_value.view()), Exception);
}

TEST(ColumnVariantV2Test, ReadViewBorrowsValidatedEncodedState) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(R"({"a":1})"));
    insert_encoded_field(*column, encode_json(R"({"a":2})"));
    const std::vector<ColumnPtr> original_children = subcolumns(*column);

    const auto view = column->read_view();

    EXPECT_FALSE(view.is_typed());
    EXPECT_EQ(view.size(), column->size());
    ASSERT_EQ(view.metadata_count(), 1);
    EXPECT_EQ(view.metadata_id_at(0), 0);
    EXPECT_EQ(view.metadata_id_at(1), 0);
    const VariantMetadataRef first_metadata = view.metadata_at(0);
    const VariantMetadataRef second_metadata = view.metadata_at(0);
    EXPECT_EQ(first_metadata.data, second_metadata.data);
    EXPECT_EQ(first_metadata.size, second_metadata.size);
    VariantRef value;
    ASSERT_TRUE(view.value_at(1).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 2);
    EXPECT_FALSE(column->is_typed());
    const std::vector<ColumnPtr> final_children = subcolumns(*column);
    EXPECT_EQ(original_children, final_children);
}

TEST(ColumnVariantV2Test, CopyInterfacesUseSharedMetadataFastPath) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json(R"({"a":1})"));
    insert_encoded_field(*source, encode_json(R"({"b":2})"));
    insert_encoded_field(*source, encode_json(R"({"a":3})"));

    auto destination = ColumnVariantV2::create();
    destination->insert_range_from(*source, 0, source->size());
    EXPECT_EQ(subcolumn_address(*destination, 0), subcolumn_address(*source, 0));
    const size_t dictionaries = metadata_count(*destination);

    destination->insert_from(*source, 1);
    std::array<uint32_t, 2> selected {2, 0};
    destination->insert_indices_from(*source, selected.data(), selected.data() + selected.size());
    EXPECT_EQ(metadata_count(*destination), dictionaries);
    ASSERT_EQ(destination->size(), 6);

    VariantRef value;
    ASSERT_TRUE(destination->get_value_ref(3).object_find(StringRef("b"), &value));
    EXPECT_EQ(value.get_int(), 2);
    ASSERT_TRUE(destination->get_value_ref(4).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 3);

    auto gathered = ColumnVariantV2::create();
    std::vector<const IColumn*> sources {source.get(), source.get()};
    std::vector<size_t> positions {1, 0};
    gathered->insert_from_multi_column(sources, positions);
    IColumn::Selector selector;
    selector.push_back(2);
    selector.push_back(0);
    MutableColumnPtr selected_rows = ColumnVariantV2::create();
    source->append_data_by_selector(selected_rows, selector);
    ASSERT_EQ(gathered->size(), 2);
    ASSERT_EQ(selected_rows->size(), 2);
    auto& selected_variant = assert_cast<ColumnVariantV2&>(*selected_rows);
    ASSERT_TRUE(gathered->get_value_ref(0).object_find(StringRef("b"), &value));
    EXPECT_EQ(value.get_int(), 2);
    ASSERT_TRUE(gathered->get_value_ref(1).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 1);
    ASSERT_TRUE(selected_variant.get_value_ref(0).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 3);
    ASSERT_TRUE(selected_variant.get_value_ref(1).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 1);

    auto different_source = ColumnVariantV2::create();
    insert_encoded_field(*different_source, encode_json(R"({"c":4})"));
    destination->insert_from(*different_source, 0);
    EXPECT_NE(subcolumn_address(*destination, 0), subcolumn_address(*source, 0));
    EXPECT_EQ(metadata_count(*source), dictionaries);
    EXPECT_EQ(metadata_count(*destination), dictionaries + 1);
    ASSERT_TRUE(destination->get_value_ref(destination->size() - 1)
                        .object_find(StringRef("c"), &value));
    EXPECT_EQ(value.get_int(), 4);

    destination->clear();
    EXPECT_EQ(destination->size(), 0);
    EXPECT_EQ(metadata_count(*destination), 0);
    EXPECT_EQ(source->size(), 3);
    EXPECT_EQ(metadata_count(*source), dictionaries);
}

TEST(ColumnVariantV2Test, RemapsDistinctAndDuplicateMetadataBlobs) {
    const VariantField object_a_one = encode_json(R"({"a":1})");
    const VariantField object_a_two = encode_json(R"({"a":2})");
    const VariantField object_b = encode_json(R"({"b":3})");

    auto destination = ColumnVariantV2::create();
    insert_encoded_field(*destination, object_a_one);

    OwnedEncodedData encoded;
    const uint32_t b_first = encoded.add_metadata(object_b.ref().metadata);
    const uint32_t a = encoded.add_metadata(object_a_one.ref().metadata);
    const uint32_t b_duplicate = encoded.add_metadata(object_b.ref().metadata);
    encoded.add_value(object_b.ref(), b_first);
    encoded.add_value(object_a_two.ref(), a);
    encoded.add_value(object_b.ref(), b_duplicate);
    destination->insert_encoded_rows(encoded.view());

    ASSERT_EQ(destination->size(), 4);
    EXPECT_EQ(metadata_count(*destination), 2);
    VariantRef value;
    ASSERT_TRUE(destination->get_value_ref(1).object_find(StringRef("b"), &value));
    EXPECT_EQ(value.get_int(), 3);
    ASSERT_TRUE(destination->get_value_ref(2).object_find(StringRef("a"), &value));
    EXPECT_EQ(value.get_int(), 2);
}

TEST(ColumnVariantV2Test, BulkAppendSharedMetadataAtLeast64KRows) {
    constexpr uint32_t ROWS = 65'536;
    const VariantField null_value = encode_json("null");
    OwnedEncodedData encoded;
    encoded.add_metadata(null_value.ref().metadata);
    encoded.value_bytes.reserve(ROWS * null_value.ref().value.size);
    encoded.value_offsets.reserve(static_cast<size_t>(ROWS) + 1);
    for (uint32_t row = 0; row < ROWS; ++row) {
        encoded.value_bytes.append(null_value.ref().value.data, null_value.ref().value.size);
        encoded.value_offsets.push_back(static_cast<uint32_t>(encoded.value_bytes.size()));
    }

    auto destination = ColumnVariantV2::create();
    destination->insert_encoded_rows(encoded.view(true));
    ASSERT_EQ(destination->size(), ROWS);
    EXPECT_EQ(metadata_count(*destination), 1);
    EXPECT_TRUE(destination->get_value_ref(0).is_null());
    EXPECT_TRUE(destination->get_value_ref(ROWS / 2).is_null());
    EXPECT_TRUE(destination->get_value_ref(ROWS - 1).is_null());
}

TEST(ColumnVariantV2Test, InsertsCodecOwnedBatchesDirectly) {
    {
        JsonStringToVariantEncoder encoder;
        VariantBatchBuilder block = encoder.finish_batch();
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(block);
        EXPECT_EQ(column->size(), 0);
        EXPECT_EQ(metadata_count(*column), 0);
    }

    {
        JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                            .throw_on_invalid_json = true,
                                            .check_duplicate_json_path = false});
        encoder.add_json(StringRef("7", 1));
        VariantBatchBuilder block = encoder.finish_batch();
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(block);
        ASSERT_EQ(column->size(), 1);
        EXPECT_EQ(column->get_value_ref(0).get_int(), 7);
    }

    {
        JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                            .throw_on_invalid_json = true,
                                            .check_duplicate_json_path = false});
        constexpr std::string_view FIRST = R"({"a":[1,{"b":true}]})";
        constexpr std::string_view SECOND = R"({"b":2,"a":[]})";
        encoder.add_json({FIRST.data(), FIRST.size()});
        encoder.add_json({SECOND.data(), SECOND.size()});
        VariantBatchBuilder block = encoder.finish_batch();
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(block);
        ASSERT_EQ(column->size(), 2);
        EXPECT_EQ(metadata_count(*column), 1);
        VariantRef nested;
        ASSERT_TRUE(column->get_value_ref(0).object_find(StringRef("a", 1), &nested));
        ASSERT_EQ(nested.num_elements(), 2);
        EXPECT_EQ(nested.array_at(0).get_int(), 1);
        ASSERT_TRUE(nested.array_at(1).object_find(StringRef("b", 1), &nested));
        EXPECT_TRUE(nested.get_bool());
        ASSERT_TRUE(column->get_value_ref(1).object_find(StringRef("b", 1), &nested));
        EXPECT_EQ(nested.get_int(), 2);
    }

    {
        JsonbWriter writer;
        ASSERT_TRUE(writer.writeStartObject());
        ASSERT_TRUE(writer.writeKey("jsonb", 5));
        ASSERT_TRUE(writer.writeStartArray());
        ASSERT_TRUE(writer.writeNull());
        ASSERT_TRUE(writer.writeInt32(5));
        ASSERT_TRUE(writer.writeEndArray());
        ASSERT_TRUE(writer.writeEndObject());
        const std::string document(writer.getOutput()->getBuffer(),
                                   static_cast<size_t>(writer.getOutput()->getSize()));
        JsonbToVariantEncoder encoder;
        encoder.add_jsonb(StringRef(document));
        VariantBatchBuilder block = encoder.finish_batch();
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(block);
        ASSERT_EQ(column->size(), 1);
        VariantRef array;
        ASSERT_TRUE(column->get_value_ref(0).object_find(StringRef("jsonb", 5), &array));
        ASSERT_EQ(array.num_elements(), 2);
        EXPECT_TRUE(array.array_at(0).is_null());
        EXPECT_EQ(array.array_at(1).get_int(), 5);
    }
}

TEST(ColumnVariantV2Test, InsertsCodecOwnedBatchAtLeast64KRows) {
    constexpr uint32_t ROWS = 65'536;
    VariantBatchBuilder builder({.rows = ROWS, .nodes = ROWS});
    for (uint32_t index = 0; index < ROWS; ++index) {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    ASSERT_EQ(block.value_offsets().size(), static_cast<size_t>(ROWS) + 1);
    ASSERT_EQ(block.value_offsets().back(), ROWS);

    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(block);
    ASSERT_EQ(column->size(), ROWS);
    EXPECT_TRUE(column->get_value_ref(0).is_null());
    EXPECT_TRUE(column->get_value_ref(ROWS / 2).is_null());
    EXPECT_TRUE(column->get_value_ref(ROWS - 1).is_null());
}

TEST(ColumnVariantV2Test, CowCloneForEachAndClear) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(R"({"a":1})"));
    insert_encoded_field(*column, encode_json(R"({"b":2})"));
    constexpr size_t ORIGINAL_METADATA_COUNT = 2;

    ColumnPtr original = column->get_ptr();
    MutableColumnPtr detached_base = IColumn::mutate(original);
    auto& detached = assert_cast<ColumnVariantV2&>(*detached_base);
    insert_encoded_field(detached, encode_json(R"({"a":3})"));
    EXPECT_EQ(metadata_count(detached), ORIGINAL_METADATA_COUNT);
    EXPECT_EQ(original->size(), 2);

    insert_encoded_field(detached, encode_json(R"({"b":4})"));
    EXPECT_EQ(metadata_count(detached), ORIGINAL_METADATA_COUNT);
    detached.clear();
    insert_encoded_field(detached, encode_json(R"({"c":5})"));
    EXPECT_EQ(detached.size(), 1);
    EXPECT_EQ(metadata_count(detached), 1);
}

TEST(ColumnVariantV2Test, SelfInsertRangeAndIndicesSnapshotAliases) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json("1"));
    insert_encoded_field(*column, encode_json("2"));
    insert_encoded_field(*column, encode_json("3"));

    column->insert_from(*column, 1);
    column->insert_range_from(*column, 0, 3);
    std::array<uint32_t, 2> selected {2, 0};
    column->insert_indices_from(*column, selected.data(), selected.data() + selected.size());

    const std::array<int64_t, 9> expected {1, 2, 3, 2, 1, 2, 3, 3, 1};
    ASSERT_EQ(column->size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(column->get_value_ref(row).get_int(), expected[row]);
    }
}

TEST(ColumnVariantV2Test, BulkNoOpAndCanonicalInsertData) {
    const VariantField object = encode_json(R"({"number":42,"array":[1,2]})");
    OwnedEncodedData empty;
    empty.add_metadata(object.ref().metadata);
    auto column = ColumnVariantV2::create();
    column->insert_encoded_rows(empty.view(true));
    EXPECT_EQ(column->size(), 0);
    EXPECT_EQ(metadata_count(*column), 0);

    std::string canonical;
    canonical_serialize(object.ref(), canonical);
    column->insert_data(canonical.data(), canonical.size());
    ASSERT_EQ(column->size(), 1);
    EXPECT_TRUE(canonical_equals(object.ref(), column->get_value_ref(0)));

    std::string malformed = canonical;
    malformed.push_back('\0');
    const size_t old_size = column->size();
    EXPECT_THROW(column->insert_data(malformed.data(), malformed.size()), Exception);
    EXPECT_EQ(column->size(), old_size);
    column->sanity_check();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(ColumnVariantV2Test, DefaultRowsUseCanonicalEmptyObjectAndReuseMetadata) {
    constexpr std::string_view EMPTY_METADATA("\x11\x00\x00", 3);
    constexpr std::string_view EMPTY_OBJECT("\x02\x00\x00", 3);

    auto column = ColumnVariantV2::create();
    column->insert_default();
    column->insert_many_defaults(3);
    column->insert_many_defaults(0);

    ASSERT_EQ(column->size(), 4);
    EXPECT_EQ(metadata_count(*column), 1);
    for (size_t row = 0; row < column->size(); ++row) {
        const VariantRef value = column->get_value_ref(row);
        EXPECT_EQ(std::string_view(value.metadata.data, value.metadata.size), EMPTY_METADATA);
        EXPECT_EQ(std::string_view(value.value.data, value.value.size), EMPTY_OBJECT);
        EXPECT_EQ(json_at(*column, row), "{}");
    }
    column->sanity_check();

    auto shared = column->clone();
    EXPECT_DEATH(shared->insert_default(), "metadata ids must be COW-detached");
    EXPECT_EQ(shared->size(), column->size());
    EXPECT_EQ(metadata_count(*column), 1);
}

TEST(ColumnVariantV2Test, ConstFiltersMatchColumnStringAndKeepMetadataReadOnly) {
    auto source = ColumnVariantV2::create();
    auto reference = ColumnString::create();
    fill_row_transform_columns(*source, *reference);
    const auto source_subcolumns = subcolumns(*source);
    const size_t metadata_bytes = source_subcolumns[0]->byte_size();
    const size_t source_allocated = source->allocated_bytes();

    IColumn::Filter mixed {0, 1, 0, 1, 0};
    ColumnPtr filtered = std::as_const(*source).filter(mixed, 2);
    ColumnPtr filtered_reference = std::as_const(*reference).filter(mixed, 2);
    const auto& filtered_variant = assert_cast<const ColumnVariantV2&>(*filtered);
    expect_values_match(filtered_variant, assert_cast<const ColumnString&>(*filtered_reference));
    EXPECT_EQ(filtered_variant.get_value_ref(0).metadata.data,
              source->get_value_ref(1).metadata.data);
    EXPECT_EQ(source->allocated_bytes(), source_allocated);

    IColumn::Filter none(source->size(), 0);
    ColumnPtr empty = std::as_const(*source).filter(none, 0);
    EXPECT_EQ(empty->size(), 0);
    EXPECT_EQ(empty->byte_size(), metadata_bytes);

    IColumn::Filter all(source->size(), 1);
    ColumnPtr all_rows = std::as_const(*source).filter(all, -1);
    expect_values_match(assert_cast<const ColumnVariantV2&>(*all_rows), *reference);

    IColumn::Filter invalid(source->size() - 1, 1);
    EXPECT_THROW(std::as_const(*source).filter(invalid, -1), Exception);
    expect_values_match(*source, *reference);
    EXPECT_EQ(source->allocated_bytes(), source_allocated);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(ColumnVariantV2Test, InPlaceFilterPrevalidatesInputs) {
    auto column = ColumnVariantV2::create();
    auto reference = ColumnString::create();
    fill_row_transform_columns(*column, *reference);

    IColumn::Filter mixed {1, 0, 1, 0, 1};
    EXPECT_EQ(column->filter(mixed), 3);
    EXPECT_EQ(reference->filter(mixed), 3);
    expect_values_match(*column, *reference);

    auto all_column = ColumnVariantV2::create();
    auto all_reference = ColumnString::create();
    fill_row_transform_columns(*all_column, *all_reference);
    IColumn::Filter all(all_column->size(), 1);
    EXPECT_EQ(all_column->filter(all), all_reference->filter(all));
    expect_values_match(*all_column, *all_reference);

    auto none_column = ColumnVariantV2::create();
    auto none_reference = ColumnString::create();
    fill_row_transform_columns(*none_column, *none_reference);
    const size_t none_metadata_count = metadata_count(*none_column);
    IColumn::Filter none(none_column->size(), 0);
    EXPECT_EQ(none_column->filter(none), none_reference->filter(none));
    EXPECT_EQ(none_column->size(), 0);
    EXPECT_EQ(metadata_count(*none_column), none_metadata_count);

    auto invalid_column = ColumnVariantV2::create();
    auto invalid_reference = ColumnString::create();
    fill_row_transform_columns(*invalid_column, *invalid_reference);
    IColumn::Filter invalid(invalid_column->size() - 1, 1);
    EXPECT_THROW(invalid_column->filter(invalid), Exception);
    expect_values_match(*invalid_column, *invalid_reference);

    auto shared = invalid_column->clone();
    IColumn::Filter keep_first {1, 0, 0, 0, 0};
    EXPECT_DEATH(shared->filter(keep_first), "metadata ids must be COW-detached");
    expect_values_match(*invalid_column, *invalid_reference);
    expect_values_match(assert_cast<const ColumnVariantV2&>(*shared), *invalid_reference);

    ColumnPtr shared_ptr = std::move(shared);
    MutableColumnPtr detached = IColumn::mutate(std::move(shared_ptr));
    auto& detached_variant = assert_cast<ColumnVariantV2&>(*detached);
    EXPECT_EQ(detached_variant.filter(keep_first), 1);
    EXPECT_EQ(detached_variant.size(), 1);
    EXPECT_EQ(json_at(detached_variant, 0), R"({"a":1})");
    expect_values_match(*invalid_column, *invalid_reference);
}

TEST(ColumnVariantV2Test, PermuteMatchesColumnStringAndRejectsInvalidInputs) {
    auto source = ColumnVariantV2::create();
    auto reference = ColumnString::create();
    fill_row_transform_columns(*source, *reference);
    const size_t metadata_bytes = subcolumns(*source)[0]->byte_size();
    IColumn::Permutation permutation {4, 2, 0, 3, 1};

    MutableColumnPtr full = source->permute(permutation, 0);
    MutableColumnPtr full_reference = reference->permute(permutation, 0);
    expect_values_match(assert_cast<const ColumnVariantV2&>(*full),
                        assert_cast<const ColumnString&>(*full_reference));

    MutableColumnPtr limited = source->permute(permutation, 3);
    MutableColumnPtr limited_reference = reference->permute(permutation, 3);
    auto& limited_variant = assert_cast<ColumnVariantV2&>(*limited);
    expect_values_match(limited_variant, assert_cast<const ColumnString&>(*limited_reference));
    EXPECT_EQ(subcolumns(limited_variant)[0]->byte_size(), metadata_bytes);
    EXPECT_EQ(limited_variant.get_value_ref(0).metadata.data,
              source->get_value_ref(4).metadata.data);

    IColumn::Permutation too_short {0, 1};
    EXPECT_THROW(source->permute(too_short, 3), Exception);
    IColumn::Permutation out_of_range {0, 1, 2, 3, 5};
    EXPECT_THROW(source->permute(out_of_range, 0), Exception);
    expect_values_match(*source, *reference);
}

TEST(ColumnVariantV2Test, PopBackAndResizeCoverBoundsShrinkAndGrowth) {
    auto pop_column = ColumnVariantV2::create();
    auto pop_reference = ColumnString::create();
    fill_row_transform_columns(*pop_column, *pop_reference);
    const size_t metadata_bytes = subcolumns(*pop_column)[0]->byte_size();

    pop_column->pop_back(0);
    pop_reference->pop_back(0);
    pop_column->pop_back(2);
    pop_reference->pop_back(2);
    expect_values_match(*pop_column, *pop_reference);

    const size_t size_before_invalid_pop = pop_column->size();
    EXPECT_DEATH(pop_column->pop_back(size_before_invalid_pop + 1),
                 "pop_back length exceeds the column size");
    EXPECT_EQ(pop_column->size(), size_before_invalid_pop);
    expect_values_match(*pop_column, *pop_reference);

    pop_column->pop_back(pop_column->size());
    pop_reference->pop_back(pop_reference->size());
    EXPECT_EQ(pop_column->size(), 0);
    EXPECT_EQ(pop_column->byte_size(), metadata_bytes);

    auto shared_pop_source = ColumnVariantV2::create();
    auto shared_pop_reference = ColumnString::create();
    fill_row_transform_columns(*shared_pop_source, *shared_pop_reference);
    auto shared_pop = shared_pop_source->clone();
    EXPECT_DEATH(shared_pop->pop_back(1), "metadata ids must be COW-detached");
    expect_values_match(*shared_pop_source, *shared_pop_reference);
    expect_values_match(assert_cast<const ColumnVariantV2&>(*shared_pop), *shared_pop_reference);

    auto resize_column = ColumnVariantV2::create();
    auto resize_reference = ColumnString::create();
    fill_row_transform_columns(*resize_column, *resize_reference);
    resize_column->resize(3);
    resize_reference->resize(3);
    expect_values_match(*resize_column, *resize_reference);
    resize_column->resize(3);
    expect_values_match(*resize_column, *resize_reference);

    resize_column->resize(6);
    resize_reference->insert_data("{}", 2);
    resize_reference->insert_data("{}", 2);
    resize_reference->insert_data("{}", 2);
    expect_values_match(*resize_column, *resize_reference);

    auto empty = ColumnVariantV2::create();
    empty->resize(3);
    ASSERT_EQ(empty->size(), 3);
    EXPECT_EQ(metadata_count(*empty), 1);
    for (size_t row = 0; row < empty->size(); ++row) {
        EXPECT_EQ(json_at(*empty, row), "{}");
    }

    auto shared_resize_source = ColumnVariantV2::create();
    auto shared_resize_reference = ColumnString::create();
    fill_row_transform_columns(*shared_resize_source, *shared_resize_reference);
    auto shared_resize = shared_resize_source->clone();
    EXPECT_DEATH(shared_resize->resize(shared_resize->size() - 1),
                 "metadata ids must be COW-detached");
    expect_values_match(*shared_resize_source, *shared_resize_reference);
    expect_values_match(assert_cast<const ColumnVariantV2&>(*shared_resize),
                        *shared_resize_reference);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(ColumnVariantV2Test, CloneFamilyPreservesCowAndMetadataContracts) {
    auto source = ColumnVariantV2::create();
    auto reference = ColumnString::create();
    fill_row_transform_columns(*source, *reference);
    const size_t source_metadata_count = metadata_count(*source);

    auto shallow = source->clone();
    EXPECT_EQ(source->allocated_bytes(), shallow->allocated_bytes());
    for (size_t index = 0; index < 3; ++index) {
        EXPECT_EQ(subcolumn_address(*source, index),
                  subcolumn_address(assert_cast<ColumnVariantV2&>(*shallow), index));
    }

    ColumnPtr shallow_ptr = std::move(shallow);
    MutableColumnPtr detached = IColumn::mutate(std::move(shallow_ptr));
    auto& detached_variant = assert_cast<ColumnVariantV2&>(*detached);
    for (size_t index = 0; index < 3; ++index) {
        EXPECT_NE(subcolumn_address(*source, index), subcolumn_address(detached_variant, index));
    }
    detached_variant.pop_back(1);
    EXPECT_EQ(detached_variant.size(), source->size() - 1);
    expect_values_match(*source, *reference);

    MutableColumnPtr empty = source->clone_empty();
    auto& empty_variant = assert_cast<ColumnVariantV2&>(*empty);
    EXPECT_EQ(empty_variant.size(), 0);
    EXPECT_EQ(metadata_count(empty_variant), 0);
    EXPECT_EQ(empty_variant.byte_size(), 0);

    MutableColumnPtr resized_zero = source->clone_resized(0);
    auto& zero_variant = assert_cast<ColumnVariantV2&>(*resized_zero);
    EXPECT_EQ(zero_variant.size(), 0);
    EXPECT_EQ(metadata_count(zero_variant), 0);

    MutableColumnPtr shrunk = source->clone_resized(3);
    auto& shrunk_variant = assert_cast<ColumnVariantV2&>(*shrunk);
    auto shrunk_reference = std::as_const(*reference).clone_resized(3);
    expect_values_match(shrunk_variant, assert_cast<const ColumnString&>(*shrunk_reference));
    EXPECT_EQ(metadata_count(shrunk_variant), source_metadata_count);
    EXPECT_EQ(subcolumn_address(shrunk_variant, 0), subcolumn_address(*source, 0));
    EXPECT_NE(subcolumn_address(shrunk_variant, 1), subcolumn_address(*source, 1));
    EXPECT_NE(subcolumn_address(shrunk_variant, 2), subcolumn_address(*source, 2));

    MutableColumnPtr equal = source->clone_resized(source->size());
    auto& equal_variant = assert_cast<ColumnVariantV2&>(*equal);
    expect_values_match(equal_variant, *reference);
    EXPECT_EQ(metadata_count(equal_variant), source_metadata_count);

    MutableColumnPtr grown = source->clone_resized(source->size() + 2);
    auto& grown_variant = assert_cast<ColumnVariantV2&>(*grown);
    ASSERT_EQ(grown_variant.size(), source->size() + 2);
    for (size_t row = 0; row < source->size(); ++row) {
        EXPECT_EQ(json_at(grown_variant, row), as_view(reference->get_data_at(row)));
    }
    EXPECT_EQ(json_at(grown_variant, source->size()), "{}");
    EXPECT_EQ(json_at(grown_variant, source->size() + 1), "{}");
    EXPECT_EQ(metadata_count(grown_variant), source_metadata_count);
    grown_variant.sanity_check();
    expect_values_match(*source, *reference);

    auto fresh_source = ColumnVariantV2::create();
    MutableColumnPtr fresh_grown = fresh_source->clone_resized(3);
    auto& fresh_grown_variant = assert_cast<ColumnVariantV2&>(*fresh_grown);
    EXPECT_EQ(fresh_source->size(), 0);
    EXPECT_EQ(metadata_count(*fresh_source), 0);
    ASSERT_EQ(fresh_grown_variant.size(), 3);
    EXPECT_EQ(metadata_count(fresh_grown_variant), 1);
    for (size_t row = 0; row < fresh_grown_variant.size(); ++row) {
        EXPECT_EQ(json_at(fresh_grown_variant, row), "{}");
    }
    fresh_grown_variant.sanity_check();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(ColumnVariantV2Test, CanonicalHashFamiliesRespectEncodingsSeedsAndMasks) {
    auto canonical = ColumnVariantV2::create();
    auto external = ColumnVariantV2::create();
    fill_equivalent_columns(*canonical, *external);
    ASSERT_EQ(canonical->size(), 5);
    ASSERT_EQ(external->size(), canonical->size());

    for (size_t row = 0; row < canonical->size(); ++row) {
        SCOPED_TRACE(row);
        ASSERT_TRUE(canonical_equals(canonical->get_value_ref(row), external->get_value_ref(row)));
        SipHash expected;
        canonical_hash(canonical->get_value_ref(row), expected);
        SipHash canonical_hash_value;
        canonical->update_hash_with_value(row, canonical_hash_value);
        SipHash external_hash_value;
        external->update_hash_with_value(row, external_hash_value);
        const uint64_t expected_digest = expected.get64();
        EXPECT_EQ(canonical_hash_value.get64(), expected_digest);
        EXPECT_EQ(external_hash_value.get64(), expected_digest);
    }

    constexpr std::array<uint8_t, 5> MASK {0, 1, 0, 1, 0};
    constexpr std::array<uint64_t, 5> XX_SEEDS {0x0123456789ABCDEFULL, 0x1111111111111111ULL,
                                                0xFEDCBA9876543210ULL, 0x2222222222222222ULL,
                                                0x13579BDF2468ACE0ULL};
    auto expected_xx = XX_SEEDS;
    for (size_t row = 0; row < expected_xx.size(); ++row) {
        if (MASK[row] == 0) {
            expected_xx[row] = canonical_xx_hash(canonical->get_value_ref(row), expected_xx[row]);
        }
    }
    auto canonical_xx = XX_SEEDS;
    auto external_xx = XX_SEEDS;
    canonical->update_hashes_with_value(canonical_xx.data(), MASK.data());
    external->update_hashes_with_value(external_xx.data(), MASK.data());
    EXPECT_EQ(canonical_xx, expected_xx);
    EXPECT_EQ(external_xx, expected_xx);
    EXPECT_EQ(canonical_xx[1], XX_SEEDS[1]);

    auto expected_xx_all = XX_SEEDS;
    for (size_t row = 0; row < expected_xx_all.size(); ++row) {
        expected_xx_all[row] =
                canonical_xx_hash(canonical->get_value_ref(row), expected_xx_all[row]);
    }
    auto actual_xx_all = XX_SEEDS;
    canonical->update_hashes_with_value(actual_xx_all.data(), nullptr);
    EXPECT_EQ(actual_xx_all, expected_xx_all);

    constexpr uint64_t XX_RANGE_SEED = 0xA5A5A5A5A5A5A5A5ULL;
    uint64_t expected_xx_range = XX_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        if (MASK[row] == 0) {
            expected_xx_range = canonical_xx_hash(canonical->get_value_ref(row), expected_xx_range);
        }
    }
    uint64_t canonical_xx_range = XX_RANGE_SEED;
    uint64_t external_xx_range = XX_RANGE_SEED;
    canonical->update_xxHash_with_value(1, canonical->size(), canonical_xx_range, MASK.data());
    external->update_xxHash_with_value(1, external->size(), external_xx_range, MASK.data());
    EXPECT_EQ(canonical_xx_range, expected_xx_range);
    EXPECT_EQ(external_xx_range, expected_xx_range);

    uint64_t expected_xx_range_all = XX_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        expected_xx_range_all =
                canonical_xx_hash(canonical->get_value_ref(row), expected_xx_range_all);
    }
    uint64_t actual_xx_range_all = XX_RANGE_SEED;
    canonical->update_xxHash_with_value(1, canonical->size(), actual_xx_range_all, nullptr);
    EXPECT_EQ(actual_xx_range_all, expected_xx_range_all);

    constexpr std::array<uint32_t, 5> CRC_SEEDS {0x12345678U, 0x23456789U, 0x3456789AU, 0x456789ABU,
                                                 0x56789ABCU};
    auto expected_crc = CRC_SEEDS;
    for (size_t row = 0; row < expected_crc.size(); ++row) {
        if (MASK[row] == 0) {
            expected_crc[row] =
                    canonical_crc_hash(canonical->get_value_ref(row), expected_crc[row]);
        }
    }
    auto canonical_crc = CRC_SEEDS;
    auto external_crc = CRC_SEEDS;
    canonical->update_crcs_with_value(canonical_crc.data(), PrimitiveType::TYPE_VARIANT,
                                      canonical->size(), 0, MASK.data());
    external->update_crcs_with_value(external_crc.data(), PrimitiveType::TYPE_VARIANT,
                                     external->size(), 0, MASK.data());
    EXPECT_EQ(canonical_crc, expected_crc);
    EXPECT_EQ(external_crc, expected_crc);
    EXPECT_EQ(canonical_crc[1], CRC_SEEDS[1]);

    auto expected_crc_all = CRC_SEEDS;
    for (size_t row = 0; row < expected_crc_all.size(); ++row) {
        expected_crc_all[row] =
                canonical_crc_hash(canonical->get_value_ref(row), expected_crc_all[row]);
    }
    auto actual_crc_all = CRC_SEEDS;
    canonical->update_crcs_with_value(actual_crc_all.data(), PrimitiveType::TYPE_VARIANT,
                                      canonical->size(), 0, nullptr);
    EXPECT_EQ(actual_crc_all, expected_crc_all);

    constexpr uint32_t CRC_RANGE_SEED = 0x456789ABU;
    uint32_t expected_crc_range = CRC_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        if (MASK[row] == 0) {
            expected_crc_range =
                    canonical_crc_hash(canonical->get_value_ref(row), expected_crc_range);
        }
    }
    uint32_t canonical_crc_range = CRC_RANGE_SEED;
    uint32_t external_crc_range = CRC_RANGE_SEED;
    canonical->update_crc_with_value(1, canonical->size(), canonical_crc_range, MASK.data());
    external->update_crc_with_value(1, external->size(), external_crc_range, MASK.data());
    EXPECT_EQ(canonical_crc_range, expected_crc_range);
    EXPECT_EQ(external_crc_range, expected_crc_range);

    uint32_t expected_crc_range_all = CRC_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        expected_crc_range_all =
                canonical_crc_hash(canonical->get_value_ref(row), expected_crc_range_all);
    }
    uint32_t actual_crc_range_all = CRC_RANGE_SEED;
    canonical->update_crc_with_value(1, canonical->size(), actual_crc_range_all, nullptr);
    EXPECT_EQ(actual_crc_range_all, expected_crc_range_all);

    constexpr std::array<uint32_t, 5> CRC32C_SEEDS {0x56789ABCU, 0x6789ABCDU, 0x789ABCDEU,
                                                    0x89ABCDEFU, 0x9ABCDEF0U};
    auto expected_crc32c = CRC32C_SEEDS;
    for (size_t row = 0; row < expected_crc32c.size(); ++row) {
        if (MASK[row] == 0) {
            expected_crc32c[row] =
                    canonical_crc32c_hash(canonical->get_value_ref(row), expected_crc32c[row]);
        }
    }
    auto canonical_crc32c = CRC32C_SEEDS;
    auto external_crc32c = CRC32C_SEEDS;
    canonical->update_crc32c_batch(canonical_crc32c.data(), MASK.data());
    external->update_crc32c_batch(external_crc32c.data(), MASK.data());
    EXPECT_EQ(canonical_crc32c, expected_crc32c);
    EXPECT_EQ(external_crc32c, expected_crc32c);
    EXPECT_EQ(canonical_crc32c[1], CRC32C_SEEDS[1]);

    auto expected_crc32c_all = CRC32C_SEEDS;
    for (size_t row = 0; row < expected_crc32c_all.size(); ++row) {
        expected_crc32c_all[row] =
                canonical_crc32c_hash(canonical->get_value_ref(row), expected_crc32c_all[row]);
    }
    auto actual_crc32c_all = CRC32C_SEEDS;
    canonical->update_crc32c_batch(actual_crc32c_all.data(), nullptr);
    EXPECT_EQ(actual_crc32c_all, expected_crc32c_all);

    constexpr uint32_t CRC32C_RANGE_SEED = 0x89ABCDEFU;
    uint32_t expected_crc32c_range = CRC32C_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        if (MASK[row] == 0) {
            expected_crc32c_range =
                    canonical_crc32c_hash(canonical->get_value_ref(row), expected_crc32c_range);
        }
    }
    uint32_t canonical_crc32c_range = CRC32C_RANGE_SEED;
    uint32_t external_crc32c_range = CRC32C_RANGE_SEED;
    canonical->update_crc32c_single(1, canonical->size(), canonical_crc32c_range, MASK.data());
    external->update_crc32c_single(1, external->size(), external_crc32c_range, MASK.data());
    EXPECT_EQ(canonical_crc32c_range, expected_crc32c_range);
    EXPECT_EQ(external_crc32c_range, expected_crc32c_range);

    uint32_t expected_crc32c_range_all = CRC32C_RANGE_SEED;
    for (size_t row = 1; row < canonical->size(); ++row) {
        expected_crc32c_range_all =
                canonical_crc32c_hash(canonical->get_value_ref(row), expected_crc32c_range_all);
    }
    uint32_t actual_crc32c_range_all = CRC32C_RANGE_SEED;
    canonical->update_crc32c_single(1, canonical->size(), actual_crc32c_range_all, nullptr);
    EXPECT_EQ(actual_crc32c_range_all, expected_crc32c_range_all);

    constexpr std::array<uint8_t, 5> ALL_MASKED {1, 1, 1, 1, 1};
    uint64_t unchanged_xx = XX_RANGE_SEED;
    canonical->update_xxHash_with_value(0, canonical->size(), unchanged_xx, ALL_MASKED.data());
    EXPECT_EQ(unchanged_xx, XX_RANGE_SEED);
    uint32_t unchanged_crc = CRC_RANGE_SEED;
    canonical->update_crc_with_value(0, canonical->size(), unchanged_crc, ALL_MASKED.data());
    EXPECT_EQ(unchanged_crc, CRC_RANGE_SEED);
    uint32_t unchanged_crc32c = CRC32C_RANGE_SEED;
    canonical->update_crc32c_single(0, canonical->size(), unchanged_crc32c, ALL_MASKED.data());
    EXPECT_EQ(unchanged_crc32c, CRC32C_RANGE_SEED);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(ColumnVariantV2Test, CanonicalArenaAndSerializedKeyAdaptersRoundTrip) {
    auto column = ColumnVariantV2::create();
    auto equivalent = ColumnVariantV2::create();
    fill_equivalent_columns(*column, *equivalent);
    ASSERT_EQ(column->size(), 5);

    std::vector<std::string> expected(column->size());
    size_t maximum_size = 0;
    for (size_t row = 0; row < column->size(); ++row) {
        canonical_serialize(column->get_value_ref(row), expected[row]);
        std::string equivalent_bytes;
        canonical_serialize(equivalent->get_value_ref(row), equivalent_bytes);
        EXPECT_EQ(equivalent_bytes, expected[row]);
        EXPECT_EQ(column->serialize_size_at(row), expected[row].size());
        maximum_size = std::max(maximum_size, expected[row].size());

        std::vector<char> caller_owned(expected[row].size());
        EXPECT_EQ(column->serialize_impl(caller_owned.data(), row), expected[row].size());
        EXPECT_EQ(std::string(caller_owned.begin(), caller_owned.end()), expected[row]);
    }
    EXPECT_EQ(column->get_max_row_byte_size(), maximum_size);

    const std::array<std::string, 5> prefixes {"p0", "prefix-one", "x", "four", "last-prefix"};
    std::vector<std::vector<char>> prefixed_buffers(column->size());
    std::vector<StringRef> prefixed_keys(column->size());
    for (size_t row = 0; row < column->size(); ++row) {
        prefixed_buffers[row].assign(prefixes[row].size() + maximum_size + 1,
                                     static_cast<char>(0x5A));
        std::copy(prefixes[row].begin(), prefixes[row].end(), prefixed_buffers[row].begin());
        prefixed_keys[row] = {prefixed_buffers[row].data(), prefixes[row].size()};
    }
    column->serialize(prefixed_keys.data(), prefixed_keys.size());
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(prefixed_keys[row].size, prefixes[row].size() + expected[row].size());
        EXPECT_EQ(std::string_view(prefixed_keys[row].data, prefixes[row].size()), prefixes[row]);
        EXPECT_EQ(std::string_view(prefixed_keys[row].data + prefixes[row].size(),
                                   expected[row].size()),
                  expected[row]);
        EXPECT_EQ(prefixed_buffers[row][prefixed_keys[row].size], static_cast<char>(0x5A));
    }

    MethodSerialized<StringHashMap<IColumn::ColumnIndex>> method;
    ColumnRawPtrs key_columns {column.get(), equivalent.get()};
    method.init_serialized_keys(key_columns, column->size());
    ASSERT_NE(method.keys, nullptr);
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(as_view(method.keys[row]), expected[row] + expected[row]);
    }

    auto batch_round_trip_first = ColumnVariantV2::create();
    auto batch_round_trip_second = ColumnVariantV2::create();
    std::vector<StringRef> serialized_keys(method.keys, method.keys + column->size());
    batch_round_trip_first->deserialize(serialized_keys.data(), serialized_keys.size());
    batch_round_trip_second->deserialize(serialized_keys.data(), serialized_keys.size());
    ASSERT_EQ(batch_round_trip_first->size(), column->size());
    ASSERT_EQ(batch_round_trip_second->size(), column->size());
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(serialized_keys[row].size, 0);
        EXPECT_TRUE(canonical_equals(batch_round_trip_first->get_value_ref(row),
                                     column->get_value_ref(row)));
        EXPECT_TRUE(canonical_equals(batch_round_trip_second->get_value_ref(row),
                                     equivalent->get_value_ref(row)));
    }

    std::vector<std::string> sentinel_cells = expected;
    std::vector<StringRef> sentinel_keys;
    sentinel_keys.reserve(sentinel_cells.size());
    for (std::string& cell : sentinel_cells) {
        cell.push_back(static_cast<char>(0x6B));
        sentinel_keys.emplace_back(cell.data(), cell.size());
    }
    auto sentinel_round_trip = ColumnVariantV2::create();
    sentinel_round_trip->deserialize(sentinel_keys.data(), sentinel_keys.size());
    ASSERT_EQ(sentinel_round_trip->size(), column->size());
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(sentinel_keys[row].data, sentinel_cells[row].data() + expected[row].size());
        ASSERT_EQ(sentinel_keys[row].size, 1);
        EXPECT_EQ(static_cast<uint8_t>(sentinel_keys[row].data[0]), 0x6B);
    }

    auto impl_round_trip = ColumnVariantV2::create();
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(impl_round_trip->deserialize_impl(expected[row].data()), expected[row].size());
    }
    ASSERT_EQ(impl_round_trip->size(), column->size());

    Arena arena(16);
    const char* begin = nullptr;
    std::vector<size_t> cell_sizes;
    size_t total_size = 0;
    for (size_t row = 0; row < column->size(); ++row) {
        const StringRef cell = column->serialize_value_into_arena(row, arena, begin);
        EXPECT_EQ(cell.size, expected[row].size());
        cell_sizes.push_back(cell.size);
        total_size += cell.size;
    }
    ASSERT_NE(begin, nullptr);

    auto arena_round_trip = ColumnVariantV2::create();
    const char* cursor = begin;
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_EQ(std::string_view(cursor, cell_sizes[row]), expected[row]);
        cursor = arena_round_trip->deserialize_and_insert_from_arena(cursor);
    }
    EXPECT_EQ(cursor, begin + total_size);
    ASSERT_EQ(arena_round_trip->size(), column->size());
    for (size_t row = 0; row < column->size(); ++row) {
        EXPECT_TRUE(
                canonical_equals(arena_round_trip->get_value_ref(row), column->get_value_ref(row)));
    }

    std::string malformed = expected.front();
    ASSERT_GT(malformed.size(), sizeof(uint32_t));
    malformed[sizeof(uint32_t)] = static_cast<char>(
            (static_cast<uint8_t>(malformed[sizeof(uint32_t)]) & ~VARIANT_METADATA_VERSION_MASK) |
            2);
    const size_t old_size = arena_round_trip->size();
    EXPECT_THROW(arena_round_trip->deserialize_and_insert_from_arena(malformed.data()), Exception);
    EXPECT_EQ(arena_round_trip->size(), old_size);
    arena_round_trip->sanity_check();
}

TEST(ColumnVariantV2Test, RejectsInvalidBulkOffsetsAndMetadataIds) {
    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 1);
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view());
            },
            "metadata id");

    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 0);
                invalid.value_offsets[0] = 1;
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view());
            },
            "value offsets");

    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 0);
                invalid.metadata_offsets[0] = 1;
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view());
            },
            "metadata offsets");

    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 0);
                invalid.metadata_offsets[1] = invalid.metadata_offsets[2];
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view());
            },
            "metadata offsets");

    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 0);
                --invalid.metadata_offsets.back();
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view());
            },
            "metadata offsets");

    EXPECT_DEATH(
            {
                const VariantField one = encode_json("1");
                OwnedEncodedData invalid;
                invalid.add_metadata(one.ref().metadata);
                invalid.add_metadata(one.ref().metadata);
                invalid.add_value(one.ref(), 0);
                auto column = ColumnVariantV2::create();
                column->insert_encoded_rows(invalid.view(true));
            },
            "omitted metadata ids");
}

TEST(ColumnVariantV2Test, CowDetachAndClear) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(R"({"a":[1,2]})"));
    ColumnPtr original = column->get_ptr();

    MutableColumnPtr detached = IColumn::mutate(original);
    auto& detached_variant = assert_cast<ColumnVariantV2&>(*detached);
    detached_variant.clear();

    EXPECT_EQ(detached_variant.size(), 0);
    EXPECT_EQ(original->size(), 1);
    VariantRef a;
    ASSERT_TRUE(assert_cast<const ColumnVariantV2&>(*original).get_value_ref(0).object_find(
            StringRef("a"), &a));
    EXPECT_EQ(a.num_elements(), 2);
}

TEST(ColumnVariantV2Test, EncodedRowCountInvariant) {
    EXPECT_DEATH(
            {
                auto column = ColumnVariantV2::create();
                insert_encoded_field(*column, encode_json("1"));
                replace_subcolumn(*column, 2, ColumnString::create());
                column->sanity_check();
            },
            "encoded row counts differ");
}

TEST(ColumnVariantV2Test, MetadataIdInvariant) {
    EXPECT_DEATH(
            {
                auto column = ColumnVariantV2::create();
                insert_encoded_field(*column, encode_json("1"));
                auto invalid_ids = MetaIdsColumn::create();
                invalid_ids->insert_value(9);
                replace_subcolumn(*column, 1, invalid_ids->get_ptr());
                column->sanity_check();
            },
            "metadata id is out of range");
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- construction invariant matrix.
TEST(ColumnVariantV2Test, TypedConstructionAndNullableInvariant) {
    constexpr std::array<int32_t, 3> VALUES {7, 0, -9};
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    auto column = typed_int32(VALUES, NULLS);
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(column->size(), VALUES.size());
    EXPECT_EQ(column->get_name(), "variant_v2(typed=INT)");
    EXPECT_EQ(subcolumns(*column).size(), 1);
    EXPECT_EQ(&column->typed_column(), subcolumns(*column).front().get());
    EXPECT_EQ(column->typed_type()->get_primitive_type(), PrimitiveType::TYPE_INT);
    EXPECT_EQ(column->byte_size(), subcolumns(*column).front()->byte_size());
    column->sanity_check();
    const IColumn* typed_storage = &column->typed_column();
    column->clear();
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(column->size(), 0);
    EXPECT_EQ(&column->typed_column(), typed_storage);

    auto state_api = typed_int32(VALUES, NULLS);
    auto same_structure = typed_int32(VALUES, NULLS);
    const std::array<std::string_view, 3> TEXT_VALUES {"a", "b", "c"};
    auto different_structure = typed_strings(TEXT_VALUES, NULLS);
    auto encoded_structure = ColumnVariantV2::create();
    EXPECT_TRUE(state_api->structure_equals(*same_structure));
    EXPECT_TRUE(state_api->structure_equals(*different_structure));
    EXPECT_TRUE(state_api->structure_equals(*encoded_structure));
    EXPECT_FALSE(state_api->structure_equals(*ColumnString::create()));
    EXPECT_EQ(state_api->byte_size(), state_api->typed_column().byte_size());
    EXPECT_EQ(state_api->allocated_bytes(), state_api->typed_column().allocated_bytes());
    EXPECT_EQ(state_api->has_enough_capacity(*same_structure),
              state_api->typed_column().has_enough_capacity(same_structure->typed_column()));
    state_api->finalize();
    EXPECT_TRUE(state_api->is_typed());
    EXPECT_EQ(state_api->size(), VALUES.size());

    EXPECT_DEATH(
            {
                auto non_nullable = ColumnInt32::create();
                non_nullable->insert_value(1);
                static_cast<void>(ColumnVariantV2::create_typed(std::move(non_nullable),
                                                                std::make_shared<DataTypeInt32>()));
            },
            "ColumnNullable");
    EXPECT_DEATH(
            {
                static_cast<void>(ColumnVariantV2::create_typed(nullable_int32(VALUES, NULLS),
                                                                std::make_shared<DataTypeInt64>()));
            },
            "does not match");
    constexpr std::array<int32_t, 1> CONST_VALUE {7};
    constexpr std::array<uint8_t, 1> CONST_NULL {0};
    EXPECT_DEATH(
            {
                ColumnPtr nullable = nullable_int32(CONST_VALUE, CONST_NULL);
                ColumnPtr constant = ColumnConst::create(nullable, 2);
                static_cast<void>(ColumnVariantV2::create_typed(std::move(constant),
                                                                std::make_shared<DataTypeInt32>()));
            },
            "ColumnConst");

    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(
                         ColumnPtr {}, std::make_shared<DataTypeInt32>())),
                 "column must not be null");
    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(nullable_int32(VALUES, NULLS),
                                                                 DataTypePtr {})),
                 "type must not be null");
    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(
                         nullable_int32(VALUES, NULLS),
                         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()))),
                 "non-nullable scalar type");

    auto time_nested = ColumnFloat64::create();
    time_nested->insert_value(1.0);
    constexpr std::array<uint8_t, 1> ONE_NOT_NULL {0};
    ColumnPtr time_nullable = wrap_nullable(std::move(time_nested), ONE_NOT_NULL);
    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(
                         time_nullable, std::make_shared<DataTypeTimeV2>(6))),
                 "unsupported typed identity");
    const std::array<std::string_view, 1> JSONB_VALUE {"{}"};
    ColumnPtr jsonb_nullable = nullable_strings(JSONB_VALUE, ONE_NOT_NULL);
    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(
                         jsonb_nullable, std::make_shared<DataTypeJsonb>())),
                 "unsupported typed identity");

    MutableColumnPtr mismatched = IColumn::mutate(nullable_int32(VALUES, NULLS));
    auto& mismatched_nullable = assert_cast<ColumnNullable&>(*mismatched);
    auto short_null_map = ColumnUInt8::create();
    short_null_map->insert_value(0);
    EXPECT_THROW(mismatched_nullable.replace_columns(mismatched_nullable.get_nested_column_ptr(),
                                                     std::move(short_null_map)),
                 Exception);
    EXPECT_EQ(mismatched_nullable.size(), VALUES.size());

    ColumnPtr decimal_scale_two =
            nullable_decimal<ColumnDecimal32, Decimal32>(2, {Decimal32 {123}}, {0});
    EXPECT_DEATH(static_cast<void>(ColumnVariantV2::create_typed(
                         decimal_scale_two, std::make_shared<DataTypeDecimal32>(9, 3))),
                 "decimal scale");
}

TEST(ColumnVariantV2Test, ReadViewBorrowsTypedStateWithoutMaterializingIt) {
    constexpr std::array<int32_t, 3> VALUES {7, 0, -9};
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    auto column = typed_int32(VALUES, NULLS);
    const IColumn* const typed_address = &column->typed_column();

    const auto view = column->read_view();

    EXPECT_TRUE(view.is_typed());
    EXPECT_EQ(view.size(), column->size());
    EXPECT_EQ(&view.typed_column(), typed_address);
    EXPECT_EQ(view.typed_type().get(), column->typed_type().get());
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(&column->typed_column(), typed_address);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- exhaustive typed mapping matrix.
TEST(ColumnVariantV2Test, TypedEnsureEncodedAllScalarMappings) {
    auto booleans = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnUInt8, UInt8>({0, 1}, {0, 0}), std::make_shared<DataTypeBool>());
    booleans->ensure_encoded();
    EXPECT_EQ(booleans->get_value_ref(0).primitive_id(), VariantPrimitiveId::FALSE_VALUE);
    EXPECT_EQ(booleans->get_value_ref(1).primitive_id(), VariantPrimitiveId::TRUE_VALUE);
    validate_encoded_column(*booleans);
    booleans->ensure_encoded();
    validate_encoded_column(*booleans);

    expect_single_typed_encoding(nullable_fixed<ColumnInt8, Int8>({-7}, {0}),
                                 std::make_shared<DataTypeInt8>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::INT8);
                                     EXPECT_EQ(value.value_size(), 2);
                                     EXPECT_EQ(value.get_int(), -7);
                                 });
    expect_single_typed_encoding(nullable_fixed<ColumnInt16, Int16>({-129}, {0}),
                                 std::make_shared<DataTypeInt16>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::INT16);
                                     EXPECT_EQ(value.value_size(), 3);
                                     EXPECT_EQ(value.get_int(), -129);
                                 });
    expect_single_typed_encoding(nullable_fixed<ColumnInt32, Int32>({32768}, {0}),
                                 std::make_shared<DataTypeInt32>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::INT32);
                                     EXPECT_EQ(value.value_size(), 5);
                                     EXPECT_EQ(value.get_int(), 32768);
                                 });
    expect_single_typed_encoding(nullable_fixed<ColumnInt64, Int64>({int64_t {1} << 40}, {0}),
                                 std::make_shared<DataTypeInt64>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::INT64);
                                     EXPECT_EQ(value.value_size(), 9);
                                     EXPECT_EQ(value.get_int(), int64_t {1} << 40);
                                 });

    const __int128 decimal38_max = power_of_ten_i128(38) - 1;
    const __int128 outside_decimal38 = power_of_ten_i128(38);
    auto largeints = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnInt128, Int128>(
                    {decimal38_max, outside_decimal38, -outside_decimal38}, {0, 0, 0}),
            std::make_shared<DataTypeInt128>());
    largeints->ensure_encoded();
    EXPECT_EQ(largeints->get_value_ref(0).primitive_id(), VariantPrimitiveId::DECIMAL16);
    EXPECT_EQ(largeints->get_value_ref(0).get_decimal(), (VariantDecimal {decimal38_max, 0, 16}));
    EXPECT_EQ(largeints->get_value_ref(1).get_string(),
              StringRef("100000000000000000000000000000000000000"));
    EXPECT_EQ(largeints->get_value_ref(2).get_string(),
              StringRef("-100000000000000000000000000000000000000"));
    validate_encoded_column(*largeints);

    expect_single_typed_encoding(nullable_fixed<ColumnFloat32, Float32>({-1.25F}, {0}),
                                 std::make_shared<DataTypeFloat32>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::FLOAT);
                                     EXPECT_EQ(value.value_size(), sizeof(float) + 1);
                                     EXPECT_EQ(value.get_float(), -1.25F);
                                 });
    expect_single_typed_encoding(nullable_fixed<ColumnFloat64, Float64>({123.5}, {0}),
                                 std::make_shared<DataTypeFloat64>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DOUBLE);
                                     EXPECT_EQ(value.value_size(), sizeof(double) + 1);
                                     EXPECT_EQ(value.get_double(), 123.5);
                                 });

    const DecimalV2Value decimal_v2(static_cast<int128_t>(12'340'000'000LL));
    expect_single_typed_encoding(
            nullable_decimal<ColumnDecimal128V2, DecimalV2Value>(9, {decimal_v2}, {0}),
            std::make_shared<DataTypeDecimalV2>(27, 9), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DECIMAL16);
                EXPECT_EQ(value.get_decimal(), (VariantDecimal {12'340'000'000LL, 9, 16}));
            });
    expect_single_typed_encoding(
            nullable_decimal<ColumnDecimal32, Decimal32>(2, {Decimal32 {12345}}, {0}),
            std::make_shared<DataTypeDecimal32>(9, 2), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DECIMAL4);
                EXPECT_EQ(value.get_decimal(), (VariantDecimal {12345, 2, 4}));
            });
    expect_single_typed_encoding(
            nullable_decimal<ColumnDecimal64, Decimal64>(3, {Decimal64 {-123456}}, {0}),
            std::make_shared<DataTypeDecimal64>(18, 3), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DECIMAL8);
                EXPECT_EQ(value.get_decimal(), (VariantDecimal {-123456, 3, 8}));
            });
    expect_single_typed_encoding(
            nullable_decimal<ColumnDecimal128V3, Decimal128V3>(
                    4, {Decimal128V3 {static_cast<Int128>(1'234'567'890'123'456'789LL)}}, {0}),
            std::make_shared<DataTypeDecimal128>(38, 4), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DECIMAL16);
                EXPECT_EQ(value.get_decimal(),
                          (VariantDecimal {1'234'567'890'123'456'789LL, 4, 16}));
            });
    const VecDateTimeValue date =
            VecDateTimeValue::create_from_olap_date(pack_olap_date(1970, 1, 2));
    expect_single_typed_encoding(nullable_fixed<ColumnDate, VecDateTimeValue>({date}, {0}),
                                 std::make_shared<DataTypeDate>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DATE);
                                     EXPECT_EQ(value.get_date(), 1);
                                 });
    const DateV2Value<DateV2ValueType> date_v2 =
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2));
    expect_single_typed_encoding(
            nullable_fixed<ColumnDateV2, DateV2Value<DateV2ValueType>>({date_v2}, {0}),
            std::make_shared<DataTypeDateV2>(), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::DATE);
                EXPECT_EQ(value.get_date(), 1);
            });
    const VecDateTimeValue datetime =
            VecDateTimeValue::create_from_olap_datetime(19700101000001ULL);
    expect_single_typed_encoding(nullable_fixed<ColumnDateTime, VecDateTimeValue>({datetime}, {0}),
                                 std::make_shared<DataTypeDateTime>(), [](VariantRef value) {
                                     EXPECT_EQ(value.primitive_id(),
                                               VariantPrimitiveId::TIMESTAMP_NTZ_MICROS);
                                     EXPECT_EQ(value.get_timestamp_ntz_micros(), 1'000'000);
                                 });
    auto datetime_v2 =
            DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL);
    datetime_v2.set_microsecond(234567);
    expect_single_typed_encoding(
            nullable_fixed<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>({datetime_v2}, {0}),
            std::make_shared<DataTypeDateTimeV2>(6), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::TIMESTAMP_NTZ_MICROS);
                EXPECT_EQ(value.get_timestamp_ntz_micros(), 1'234'567);
            });
    TimestampTzValue timestamp_tz;
    timestamp_tz.unchecked_set_time(1970, 1, 1, 0, 0, 2, 345678);
    expect_single_typed_encoding(
            nullable_fixed<ColumnTimeStampTz, TimestampTzValue>({timestamp_tz}, {0}),
            std::make_shared<DataTypeTimeStampTz>(6), [](VariantRef value) {
                EXPECT_EQ(value.primitive_id(), VariantPrimitiveId::TIMESTAMP_MICROS);
                EXPECT_EQ(value.get_timestamp_micros(), 2'345'678);
            });

    const std::string short_text(63, 's');
    const std::string long_text(64, 'L');
    const std::array<std::string_view, 2> STRINGS {short_text, long_text};
    constexpr std::array<uint8_t, 2> STRING_NULLS {0, 0};
    for (PrimitiveType primitive :
         {PrimitiveType::TYPE_CHAR, PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_STRING}) {
        auto strings =
                ColumnVariantV2::create_typed(nullable_strings(STRINGS, STRING_NULLS),
                                              std::make_shared<DataTypeString>(64, primitive));
        strings->ensure_encoded();
        EXPECT_EQ(strings->get_value_ref(0).basic_type(), VariantBasicType::SHORT_STRING);
        EXPECT_EQ(strings->get_value_ref(0).get_string(), StringRef(short_text));
        EXPECT_EQ(strings->get_value_ref(1).primitive_id(), VariantPrimitiveId::STRING);
        EXPECT_EQ(strings->get_value_ref(1).get_string(), StringRef(long_text));
        validate_encoded_column(*strings);
    }

    IPv4 ipv4 {};
    ASSERT_TRUE(IPv4Value::from_string(ipv4, "192.0.2.1"));
    expect_single_typed_encoding(
            nullable_fixed<ColumnIPv4, IPv4>({ipv4}, {0}), std::make_shared<DataTypeIPv4>(),
            [](VariantRef value) { EXPECT_EQ(value.get_string(), StringRef("192.0.2.1")); });
    IPv6 ipv6 {};
    ASSERT_TRUE(IPv6Value::from_string(ipv6, "2001:db8::1"));
    expect_single_typed_encoding(
            nullable_fixed<ColumnIPv6, IPv6>({ipv6}, {0}), std::make_shared<DataTypeIPv6>(),
            [](VariantRef value) { EXPECT_EQ(value.get_string(), StringRef("2001:db8::1")); });

    constexpr std::array<int32_t, 3> NULL_VALUES {1, 0, -2};
    constexpr std::array<uint8_t, 3> INNER_NULLS {0, 0, 1};
    auto nulls = typed_int32(NULL_VALUES, INNER_NULLS);
    nulls->ensure_encoded();
    EXPECT_EQ(nulls->get_value_ref(0).get_int(), 1);
    EXPECT_EQ(nulls->get_value_ref(1).get_int(), 0);
    EXPECT_TRUE(nulls->get_value_ref(2).is_null());
    validate_encoded_column(*nulls);

    constexpr std::array<int32_t, 0> NO_VALUES {};
    constexpr std::array<uint8_t, 0> NO_NULLS {};
    auto empty = typed_int32(NO_VALUES, NO_NULLS);
    empty->ensure_encoded();
    EXPECT_FALSE(empty->is_typed());
    EXPECT_EQ(empty->size(), 0);
    EXPECT_EQ(metadata_count(*empty), 0);
    empty->ensure_encoded();

    auto zero_date_nested = ColumnDateV2::create();
    const ColumnDateV2::value_type valid_date =
            ColumnDateV2::value_type::create_from_olap_date(pack_olap_date(1970, 1, 1));
    const ColumnDateV2::value_type invalid_date {};
    zero_date_nested->insert_value(valid_date);
    zero_date_nested->insert_value(invalid_date);
    auto zero_date_nulls = ColumnUInt8::create();
    zero_date_nulls->insert_value(0);
    zero_date_nulls->insert_value(0);
    auto zero_date = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(zero_date_nested), std::move(zero_date_nulls)),
            std::make_shared<DataTypeDateV2>());
    const IColumn* zero_date_storage = &zero_date->typed_column();
    EXPECT_THROW(zero_date->ensure_encoded(), Exception);
    EXPECT_TRUE(zero_date->is_typed());
    EXPECT_EQ(zero_date->size(), 2);
    EXPECT_EQ(&zero_date->typed_column(), zero_date_storage);
    const auto& zero_date_nullable = assert_cast<const ColumnNullable&>(zero_date->typed_column());
    const auto& zero_dates =
            assert_cast<const ColumnDateV2&>(zero_date_nullable.get_nested_column());
    ASSERT_EQ(zero_dates.size(), 2);
    EXPECT_EQ(zero_dates.get_data()[0], valid_date);
    EXPECT_EQ(zero_dates.get_data()[1], invalid_date);
}

TEST(ColumnVariantV2Test, TypedRowTransformsCloneAndCow) {
    constexpr std::array<int32_t, 5> VALUES {1, 2, 3, 4, 5};
    constexpr std::array<uint8_t, 5> NULLS {0, 1, 0, 0, 0};
    auto source = typed_int32(VALUES, NULLS);

    IColumn::Filter mixed {1, 0, 1, 0, 1};
    ColumnPtr filtered = std::as_const(*source).filter(mixed, 3);
    const auto& filtered_variant = assert_cast<const ColumnVariantV2&>(*filtered);
    EXPECT_TRUE(filtered_variant.is_typed());
    EXPECT_EQ(filtered_variant.size(), 3);
    constexpr std::array<int32_t, 3> MIXED_VALUES {1, 3, 5};
    constexpr std::array<uint8_t, 3> MIXED_NULLS {0, 0, 0};
    expect_int32_rows(filtered_variant, MIXED_VALUES, MIXED_NULLS);

    auto inplace_mixed = typed_int32(VALUES, NULLS);
    EXPECT_EQ(inplace_mixed->filter(mixed), 3);
    EXPECT_TRUE(inplace_mixed->is_typed());
    expect_int32_rows(*inplace_mixed, MIXED_VALUES, MIXED_NULLS);
    auto inplace_all = typed_int32(VALUES, NULLS);
    IColumn::Filter all(VALUES.size(), 1);
    EXPECT_EQ(inplace_all->filter(all), VALUES.size());
    EXPECT_TRUE(inplace_all->is_typed());
    expect_int32_rows(*inplace_all, VALUES, NULLS);
    auto inplace_none = typed_int32(VALUES, NULLS);
    IColumn::Filter none(VALUES.size(), 0);
    EXPECT_EQ(inplace_none->filter(none), 0);
    EXPECT_TRUE(inplace_none->is_typed());
    constexpr std::array<int32_t, 0> EMPTY_VALUES {};
    constexpr std::array<uint8_t, 0> EMPTY_NULLS {};
    expect_int32_rows(*inplace_none, EMPTY_VALUES, EMPTY_NULLS);

    IColumn::Permutation permutation {4, 2, 0, 3, 1};
    MutableColumnPtr permuted = source->permute(permutation, 3);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*permuted).is_typed());
    EXPECT_EQ(permuted->size(), 3);
    constexpr std::array<int32_t, 3> PERMUTED_VALUES {5, 3, 1};
    expect_int32_rows(assert_cast<const ColumnVariantV2&>(*permuted), PERMUTED_VALUES, MIXED_NULLS);
    IColumn::Permutation invalid {0, 5, 1, 2, 3};
    EXPECT_THROW(source->permute(invalid, 0), Exception);
    EXPECT_TRUE(source->is_typed());
    EXPECT_EQ(source->size(), VALUES.size());

    MutableColumnPtr shrunk = source->clone_resized(2);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*shrunk).is_typed());
    constexpr std::array<int32_t, 2> SHRUNK_VALUES {1, 2};
    constexpr std::array<uint8_t, 2> SHRUNK_NULLS {0, 1};
    expect_int32_rows(assert_cast<const ColumnVariantV2&>(*shrunk), SHRUNK_VALUES, SHRUNK_NULLS);
    MutableColumnPtr empty = source->clone_empty();
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*empty).is_typed());
    MutableColumnPtr grown = source->clone_resized(VALUES.size() + 1);
    const auto& grown_variant = assert_cast<const ColumnVariantV2&>(*grown);
    EXPECT_FALSE(grown_variant.is_typed());
    for (size_t row = 0; row < VALUES.size(); ++row) {
        if (NULLS[row] != 0) {
            EXPECT_TRUE(grown_variant.get_value_ref(row).is_null());
        } else {
            EXPECT_EQ(grown_variant.get_value_ref(row).get_int(), VALUES[row]);
        }
    }
    EXPECT_EQ(json_at(grown_variant, VALUES.size()), "{}");
    EXPECT_TRUE(source->is_typed());

    const IColumn* source_storage = &source->typed_column();
    source->pop_back(2);
    EXPECT_TRUE(source->is_typed());
    EXPECT_EQ(&source->typed_column(), source_storage);
    EXPECT_EQ(source->size(), 3);
    constexpr std::array<int32_t, 3> POPPED_VALUES {1, 2, 3};
    constexpr std::array<uint8_t, 3> POPPED_NULLS {0, 1, 0};
    expect_int32_rows(*source, POPPED_VALUES, POPPED_NULLS);
    source->resize(2);
    EXPECT_TRUE(source->is_typed());
    EXPECT_EQ(&source->typed_column(), source_storage);
    expect_int32_rows(*source, SHRUNK_VALUES, SHRUNK_NULLS);
    source->resize(4);
    EXPECT_FALSE(source->is_typed());
    EXPECT_EQ(source->get_value_ref(0).get_int(), 1);
    EXPECT_TRUE(source->get_value_ref(1).is_null());
    EXPECT_EQ(json_at(*source, 2), "{}");
    EXPECT_EQ(json_at(*source, 3), "{}");

    auto cow_source = typed_int32(VALUES, NULLS);
    ColumnPtr shared = cow_source->get_ptr();
    MutableColumnPtr detached = IColumn::mutate(shared);
    auto& detached_variant = assert_cast<ColumnVariantV2&>(*detached);
    detached_variant.pop_back(1);
    EXPECT_EQ(cow_source->size(), VALUES.size());
    EXPECT_EQ(detached_variant.size(), VALUES.size() - 1);
    expect_int32_rows(*cow_source, VALUES, NULLS);
    constexpr std::array<int32_t, 4> DETACHED_VALUES {1, 2, 3, 4};
    constexpr std::array<uint8_t, 4> DETACHED_NULLS {0, 1, 0, 0};
    expect_int32_rows(detached_variant, DETACHED_VALUES, DETACHED_NULLS);

    auto shared_owner = typed_int32(VALUES, NULLS);
    MutableColumnPtr shared_alias_base = shared_owner->clone();
    auto& shared_alias = assert_cast<ColumnVariantV2&>(*shared_alias_base);
    EXPECT_DEATH(shared_owner->pop_back(1), "typed column must be COW-detached");
    EXPECT_TRUE(shared_owner->is_typed());
    EXPECT_TRUE(shared_alias.is_typed());
    expect_int32_rows(*shared_owner, VALUES, NULLS);
    expect_int32_rows(shared_alias, VALUES, NULLS);
}

TEST(ColumnVariantV2Test, TypedEncodedInsertMatrixKeepsConstSource) {
    constexpr std::array<int32_t, 3> SOURCE_VALUES {1, 2, 3};
    constexpr std::array<uint8_t, 3> SOURCE_NULLS {0, 1, 0};
    constexpr std::array<int32_t, 1> DESTINATION_VALUES {9};
    constexpr std::array<uint8_t, 1> DESTINATION_NULLS {0};
    auto source = typed_int32(SOURCE_VALUES, SOURCE_NULLS);
    const std::vector<ColumnPtr> source_children = subcolumns(*source);

    auto same_type = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    const IColumn* same_type_storage = &same_type->typed_column();
    same_type->insert_range_from(*source, 0, source->size());
    EXPECT_TRUE(same_type->is_typed());
    EXPECT_EQ(&same_type->typed_column(), same_type_storage);
    EXPECT_EQ(same_type->size(), 4);
    constexpr std::array<int32_t, 4> RANGE_VALUES {9, 1, 2, 3};
    constexpr std::array<uint8_t, 4> RANGE_NULLS {0, 0, 1, 0};
    expect_int32_rows(*same_type, RANGE_VALUES, RANGE_NULLS);
    const size_t same_type_size = same_type->size();
    EXPECT_DEATH(same_type->insert_range_from(*source, source->size(), 1),
                 "range exceeds source size");
    EXPECT_TRUE(same_type->is_typed());
    EXPECT_EQ(same_type->size(), same_type_size);

    auto from = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    from->insert_from(*source, 1);
    constexpr std::array<int32_t, 2> FROM_VALUES {9, 2};
    constexpr std::array<uint8_t, 2> FROM_NULLS {0, 1};
    EXPECT_TRUE(from->is_typed());
    expect_int32_rows(*from, FROM_VALUES, FROM_NULLS);

    auto indices = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    constexpr std::array<uint32_t, 2> SELECTED {2, 0};
    indices->insert_indices_from(*source, SELECTED.data(), SELECTED.data() + SELECTED.size());
    constexpr std::array<int32_t, 3> INDICES_VALUES {9, 3, 1};
    constexpr std::array<uint8_t, 3> INDICES_NULLS {0, 0, 0};
    EXPECT_TRUE(indices->is_typed());
    expect_int32_rows(*indices, INDICES_VALUES, INDICES_NULLS);
    constexpr std::array<uint32_t, 2> INVALID_SELECTED {0, 3};
    EXPECT_DEATH(indices->insert_indices_from(*source, INVALID_SELECTED.data(),
                                              INVALID_SELECTED.data() + INVALID_SELECTED.size()),
                 "source index is out of range");
    EXPECT_TRUE(indices->is_typed());
    expect_int32_rows(*indices, INDICES_VALUES, INDICES_NULLS);

    auto self = typed_int32(SOURCE_VALUES, SOURCE_NULLS);
    self->insert_range_from(*self, 0, 2);
    constexpr std::array<uint32_t, 2> SELF_SELECTED {2, 0};
    self->insert_indices_from(*self, SELF_SELECTED.data(),
                              SELF_SELECTED.data() + SELF_SELECTED.size());
    constexpr std::array<int32_t, 7> SELF_VALUES {1, 2, 3, 1, 2, 3, 1};
    constexpr std::array<uint8_t, 7> SELF_NULLS {0, 1, 0, 0, 1, 0, 0};
    EXPECT_TRUE(self->is_typed());
    expect_int32_rows(*self, SELF_VALUES, SELF_NULLS);

    auto zero_length = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    zero_length->insert_range_from(*source, source->size(), 0);
    zero_length->insert_indices_from(*source, SELECTED.data(), SELECTED.data());
    EXPECT_TRUE(zero_length->is_typed());
    expect_int32_rows(*zero_length, DESTINATION_VALUES, DESTINATION_NULLS);

    auto encoded_destination = ColumnVariantV2::create();
    insert_encoded_field(*encoded_destination, encode_json("7"));
    encoded_destination->insert_range_from(*source, 0, source->size());
    EXPECT_FALSE(encoded_destination->is_typed());
    EXPECT_EQ(encoded_destination->size(), 4);
    constexpr std::array<int32_t, 4> ENCODED_DESTINATION_VALUES {7, 1, 2, 3};
    constexpr std::array<uint8_t, 4> ENCODED_DESTINATION_NULLS {0, 0, 1, 0};
    expect_int32_rows(*encoded_destination, ENCODED_DESTINATION_VALUES, ENCODED_DESTINATION_NULLS);

    auto typed_destination = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    typed_destination->insert_range_from(*encoded_destination, 0, 1);
    EXPECT_FALSE(typed_destination->is_typed());
    EXPECT_EQ(typed_destination->size(), 2);
    constexpr std::array<int32_t, 2> TYPED_DESTINATION_VALUES {9, 7};
    constexpr std::array<uint8_t, 2> TYPED_DESTINATION_NULLS {0, 0};
    expect_int32_rows(*typed_destination, TYPED_DESTINATION_VALUES, TYPED_DESTINATION_NULLS);

    const std::array<std::string_view, 1> TEXT {"x"};
    auto strings = typed_strings(TEXT, DESTINATION_NULLS);
    auto different_type = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    different_type->insert_range_from(*strings, 0, 1);
    EXPECT_FALSE(different_type->is_typed());
    EXPECT_EQ(different_type->get_value_ref(0).get_int(), 9);
    EXPECT_EQ(different_type->get_value_ref(1).get_string(), StringRef("x"));

    auto chars = ColumnVariantV2::create_typed(
            nullable_strings(TEXT, DESTINATION_NULLS),
            std::make_shared<DataTypeString>(1, PrimitiveType::TYPE_CHAR));
    auto string_identity = typed_strings(TEXT, DESTINATION_NULLS);
    string_identity->insert_range_from(*chars, 0, 1);
    EXPECT_FALSE(string_identity->is_typed());
    EXPECT_EQ(string_identity->get_value_ref(0).get_string(), StringRef("x"));
    EXPECT_EQ(string_identity->get_value_ref(1).get_string(), StringRef("x"));

    auto defaults = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    defaults->insert_many_defaults(0);
    EXPECT_TRUE(defaults->is_typed());
    defaults->insert_default();
    EXPECT_FALSE(defaults->is_typed());
    EXPECT_EQ(json_at(*defaults, defaults->size() - 1), "{}");

    const VariantField inserted_field = encoded_integer(11, 4);
    std::string inserted_cell;
    canonical_serialize(inserted_field.ref(), inserted_cell);
    auto inserted_data = typed_int32(DESTINATION_VALUES, DESTINATION_NULLS);
    inserted_data->insert_data(inserted_cell.data(), inserted_cell.size());
    EXPECT_FALSE(inserted_data->is_typed());
    constexpr std::array<int32_t, 2> INSERTED_DATA_VALUES {9, 11};
    constexpr std::array<uint8_t, 2> INSERTED_DATA_NULLS {0, 0};
    expect_int32_rows(*inserted_data, INSERTED_DATA_VALUES, INSERTED_DATA_NULLS);

    EXPECT_TRUE(source->is_typed());
    EXPECT_EQ(source->size(), SOURCE_VALUES.size());
    EXPECT_EQ(subcolumns(*source).front().get(), source_children.front().get());
    expect_int32_rows(*source, SOURCE_VALUES, SOURCE_NULLS);
}

TEST(ColumnVariantV2Test, MixedEncodedTypedFilterAndRangePreserveCanonicalRows) {
    constexpr std::array<int32_t, 4> VALUES {1, 2, 3, 4};
    constexpr std::array<uint8_t, 4> NULLS {0, 1, 0, 0};
    auto [typed, encoded] = mixed_numeric_columns();
    expect_canonical_rows_equal(*typed, *encoded);

    IColumn::Filter keep {0, 1, 1, 1};
    ColumnPtr typed_filtered = std::as_const(*typed).filter(keep, 3);
    ColumnPtr encoded_filtered = std::as_const(*encoded).filter(keep, 3);
    const auto& typed_filtered_variant = assert_cast<const ColumnVariantV2&>(*typed_filtered);
    const auto& encoded_filtered_variant = assert_cast<const ColumnVariantV2&>(*encoded_filtered);
    EXPECT_TRUE(typed_filtered_variant.is_typed());
    EXPECT_FALSE(encoded_filtered_variant.is_typed());
    expect_canonical_rows_equal(typed_filtered_variant, encoded_filtered_variant);

    auto typed_inplace = typed_int32(VALUES, NULLS);
    auto encoded_inplace = mixed_numeric_columns().encoded;
    EXPECT_EQ(typed_inplace->filter(keep), 3);
    EXPECT_EQ(encoded_inplace->filter(keep), 3);
    expect_canonical_rows_equal(*typed_inplace, *encoded_inplace);

    auto encoded_range = ColumnVariantV2::create();
    encoded_range->insert_range_from(*typed, 0, typed->size());
    EXPECT_FALSE(encoded_range->is_typed());
    expect_canonical_rows_equal(*encoded_range, *encoded);

    constexpr std::array<int32_t, 0> NO_VALUES {};
    constexpr std::array<uint8_t, 0> NO_NULLS {};
    auto typed_range = typed_int32(NO_VALUES, NO_NULLS);
    typed_range->insert_range_from(*encoded, 0, encoded->size());
    EXPECT_FALSE(typed_range->is_typed());
    expect_canonical_rows_equal(*typed_range, *encoded);

    EXPECT_TRUE(typed->is_typed());
    EXPECT_FALSE(encoded->is_typed());
    expect_canonical_rows_equal(*typed, *encoded);
}

TEST(ColumnVariantV2Test, MixedEncodedTypedInsertAndGatherPreserveCanonicalRows) {
    auto [typed, encoded] = mixed_numeric_columns();

    constexpr std::array<uint32_t, 4> SELECTED {3, 1, 0, 3};
    const std::array<VariantField, 1> PREFIX {encoded_integer(9, sizeof(int8_t))};
    auto encoded_indices = encoded_rows(PREFIX);
    encoded_indices->insert_from(*typed, 1);
    encoded_indices->insert_indices_from(*typed, SELECTED.data(),
                                         SELECTED.data() + SELECTED.size());
    constexpr std::array<int32_t, 6> EXPECTED_VALUES {9, 0, 4, 0, 1, 4};
    constexpr std::array<uint8_t, 6> EXPECTED_NULLS {0, 1, 0, 1, 0, 0};
    expect_int32_rows(*encoded_indices, EXPECTED_VALUES, EXPECTED_NULLS);

    constexpr std::array<int32_t, 1> PREFIX_VALUE {9};
    constexpr std::array<uint8_t, 1> PREFIX_NULL {0};
    auto typed_indices = typed_int32(PREFIX_VALUE, PREFIX_NULL);
    typed_indices->insert_from(*encoded, 1);
    typed_indices->insert_indices_from(*encoded, SELECTED.data(),
                                       SELECTED.data() + SELECTED.size());
    EXPECT_FALSE(typed_indices->is_typed());
    expect_canonical_rows_equal(*typed_indices, *encoded_indices);

    auto gathered = ColumnVariantV2::create();
    const std::vector<const IColumn*> sources {typed.get(), encoded.get(), typed.get(),
                                               encoded.get()};
    const std::vector<size_t> positions {3, 1, 0, 2};
    gathered->insert_from_multi_column(sources, positions);
    constexpr std::array<int32_t, 4> GATHERED_VALUES {4, 0, 1, 3};
    constexpr std::array<uint8_t, 4> GATHERED_NULLS {0, 1, 0, 0};
    auto gathered_expected = typed_int32(GATHERED_VALUES, GATHERED_NULLS);
    expect_canonical_rows_equal(*gathered, *gathered_expected);

    EXPECT_TRUE(typed->is_typed());
    EXPECT_FALSE(encoded->is_typed());
    expect_canonical_rows_equal(*typed, *encoded);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- exhaustive E/T adapter matrix.
TEST(ColumnVariantV2Test, TypedCanonicalHashCrcAndArenaMatchEncoded) {
    constexpr std::array<int32_t, 3> VALUES {42, 0, -7};
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    auto typed = typed_int32(VALUES, NULLS);
    auto encoded = ColumnVariantV2::create();
    insert_encoded_field(*encoded, encoded_integer(42, 4));
    insert_encoded_field(*encoded, encode_json("null"));
    insert_encoded_field(*encoded, encoded_integer(-7, 4));

    for (size_t row = 0; row < typed->size(); ++row) {
        SipHash typed_sip;
        typed->update_hash_with_value(row, typed_sip);
        SipHash encoded_sip;
        encoded->update_hash_with_value(row, encoded_sip);
        EXPECT_EQ(typed_sip.get64(), encoded_sip.get64());

        const size_t typed_size = typed->serialize_size_at(row);
        const size_t encoded_size = encoded->serialize_size_at(row);
        EXPECT_EQ(typed_size, encoded_size);
        std::string typed_cell(typed_size, '\0');
        std::string encoded_cell(encoded_size, '\0');
        EXPECT_EQ(typed->serialize_impl(typed_cell.data(), row), typed_size);
        EXPECT_EQ(encoded->serialize_impl(encoded_cell.data(), row), encoded_size);
        EXPECT_EQ(typed_cell, encoded_cell);
    }

    constexpr std::array<uint8_t, 3> MASK {0, 1, 0};
    constexpr std::array<uint64_t, 3> XX_SEEDS {1, 2, 3};
    std::array<uint64_t, 3> typed_xx = XX_SEEDS;
    std::array<uint64_t, 3> encoded_xx = XX_SEEDS;
    typed->update_hashes_with_value(typed_xx.data(), MASK.data());
    encoded->update_hashes_with_value(encoded_xx.data(), MASK.data());
    EXPECT_EQ(typed_xx, encoded_xx);

    uint64_t typed_xx_range = 7;
    uint64_t encoded_xx_range = 7;
    typed->update_xxHash_with_value(0, typed->size(), typed_xx_range, MASK.data());
    encoded->update_xxHash_with_value(0, encoded->size(), encoded_xx_range, MASK.data());
    EXPECT_EQ(typed_xx_range, encoded_xx_range);

    constexpr std::array<uint32_t, 3> CRC_SEEDS {4, 5, 6};
    std::array<uint32_t, 3> typed_crc = CRC_SEEDS;
    std::array<uint32_t, 3> encoded_crc = CRC_SEEDS;
    typed->update_crcs_with_value(typed_crc.data(), PrimitiveType::TYPE_VARIANT, typed->size(), 0,
                                  MASK.data());
    encoded->update_crcs_with_value(encoded_crc.data(), PrimitiveType::TYPE_VARIANT,
                                    encoded->size(), 0, MASK.data());
    EXPECT_EQ(typed_crc, encoded_crc);

    uint32_t typed_crc_range = 8;
    uint32_t encoded_crc_range = 8;
    typed->update_crc_with_value(0, typed->size(), typed_crc_range, MASK.data());
    encoded->update_crc_with_value(0, encoded->size(), encoded_crc_range, MASK.data());
    EXPECT_EQ(typed_crc_range, encoded_crc_range);

    std::array<uint32_t, 3> typed_crc32c = CRC_SEEDS;
    std::array<uint32_t, 3> encoded_crc32c = CRC_SEEDS;
    typed->update_crc32c_batch(typed_crc32c.data(), MASK.data());
    encoded->update_crc32c_batch(encoded_crc32c.data(), MASK.data());
    EXPECT_EQ(typed_crc32c, encoded_crc32c);

    uint32_t typed_crc32c_range = 9;
    uint32_t encoded_crc32c_range = 9;
    typed->update_crc32c_single(0, typed->size(), typed_crc32c_range, MASK.data());
    encoded->update_crc32c_single(0, encoded->size(), encoded_crc32c_range, MASK.data());
    EXPECT_EQ(typed_crc32c_range, encoded_crc32c_range);

    auto typed_xx_all = XX_SEEDS;
    auto encoded_xx_all = XX_SEEDS;
    typed->update_hashes_with_value(typed_xx_all.data(), nullptr);
    encoded->update_hashes_with_value(encoded_xx_all.data(), nullptr);
    EXPECT_EQ(typed_xx_all, encoded_xx_all);
    auto typed_crc_all = CRC_SEEDS;
    auto encoded_crc_all = CRC_SEEDS;
    typed->update_crc32c_batch(typed_crc_all.data(), nullptr);
    encoded->update_crc32c_batch(encoded_crc_all.data(), nullptr);
    EXPECT_EQ(typed_crc_all, encoded_crc_all);

    std::vector<std::string> expected_cells(typed->size());
    size_t expected_maximum = 0;
    for (size_t row = 0; row < typed->size(); ++row) {
        expected_cells[row].resize(encoded->serialize_size_at(row));
        encoded->serialize_impl(expected_cells[row].data(), row);
        expected_maximum = std::max(expected_maximum, expected_cells[row].size());
    }
    EXPECT_EQ(typed->get_max_row_byte_size(), expected_maximum);
    std::vector<std::vector<char>> batch_buffers(
            typed->size(), std::vector<char>(expected_maximum + 1, static_cast<char>(0x5A)));
    std::vector<StringRef> batch_keys;
    batch_keys.reserve(typed->size());
    for (std::vector<char>& buffer : batch_buffers) {
        batch_keys.emplace_back(buffer.data(), 0);
    }
    typed->serialize(batch_keys.data(), batch_keys.size());
    for (size_t row = 0; row < typed->size(); ++row) {
        EXPECT_EQ(as_view(batch_keys[row]), expected_cells[row]);
        EXPECT_EQ(batch_buffers[row][batch_keys[row].size], static_cast<char>(0x5A));
    }

    constexpr std::array<int32_t, 0> NO_VALUES {};
    constexpr std::array<uint8_t, 0> NO_NULLS {};
    auto batch_round_trip = typed_int32(NO_VALUES, NO_NULLS);
    std::vector<StringRef> batch_input;
    batch_input.reserve(expected_cells.size());
    for (std::string& cell : expected_cells) {
        batch_input.emplace_back(cell.data(), cell.size());
    }
    batch_round_trip->deserialize(batch_input.data(), batch_input.size());
    EXPECT_FALSE(batch_round_trip->is_typed());
    expect_int32_rows(*batch_round_trip, VALUES, NULLS);

    auto impl_round_trip = typed_int32(NO_VALUES, NO_NULLS);
    for (const std::string& cell : expected_cells) {
        EXPECT_EQ(impl_round_trip->deserialize_impl(cell.data()), cell.size());
    }
    EXPECT_FALSE(impl_round_trip->is_typed());
    expect_int32_rows(*impl_round_trip, VALUES, NULLS);

    Arena arena(16);
    const char* begin = nullptr;
    std::vector<StringRef> arena_cells;
    arena_cells.reserve(typed->size());
    for (size_t row = 0; row < typed->size(); ++row) {
        const StringRef cell = typed->serialize_value_into_arena(row, arena, begin);
        EXPECT_EQ(as_view(cell), expected_cells[row]);
        arena_cells.push_back(cell);
    }
    auto arena_round_trip = typed_int32(NO_VALUES, NO_NULLS);
    const char* cursor = begin;
    for (StringRef cell : arena_cells) {
        cursor = arena_round_trip->deserialize_and_insert_from_arena(cursor);
        EXPECT_EQ(cursor, cell.data + cell.size);
    }
    EXPECT_FALSE(arena_round_trip->is_typed());
    expect_int32_rows(*arena_round_trip, VALUES, NULLS);

    EXPECT_TRUE(typed->is_typed());
}

TEST(ColumnVariantV2Test, ETCrossCheckNumericCrossKindMatrix) {
    constexpr std::array<Int8, 6> INTEGERS {-128, -1, 0, 1, 42, 127};
    constexpr std::array<double, 6> DOUBLES {-128.0, -1.0, 0.0, 1.0, 42.0, 127.0};
    const std::array<Decimal32, 6> DECIMALS {Decimal32 {-12'800}, Decimal32 {-100},
                                             Decimal32 {0},       Decimal32 {100},
                                             Decimal32 {4'200},   Decimal32 {12'700}};
    constexpr std::array<uint8_t, 6> NOT_NULL {};

    std::vector<VariantField> int64_fields;
    std::vector<VariantField> decimal16_fields;
    std::vector<VariantField> double_fields;
    for (Int8 value : INTEGERS) {
        int64_fields.push_back(encoded_integer(value, sizeof(int64_t)));
        decimal16_fields.push_back(encoded_decimal(static_cast<__int128>(value) * 100, 2));
        double_fields.push_back(encoded_double(value));
    }

    std::vector<ETCrossCheckRepresentation> representations;
    representations.push_back({"E/int64-width8", encoded_rows(int64_fields)});
    representations.push_back({"E/decimal16-scale2", encoded_rows(decimal16_fields)});
    representations.push_back({"E/integral-double", encoded_rows(double_fields)});
    representations.push_back(
            {"T/int8", ColumnVariantV2::create_typed(
                               nullable_fixed_values<ColumnInt8, Int8>(INTEGERS, NOT_NULL),
                               std::make_shared<DataTypeInt8>())});
    representations.push_back(
            {"T/decimal32-scale2",
             ColumnVariantV2::create_typed(
                     nullable_decimal_values<ColumnDecimal32, Decimal32>(2, DECIMALS, NOT_NULL),
                     std::make_shared<DataTypeDecimal32>(9, 2))});
    representations.push_back(
            {"T/float64", ColumnVariantV2::create_typed(
                                  nullable_fixed_values<ColumnFloat64, double>(DOUBLES, NOT_NULL),
                                  std::make_shared<DataTypeFloat64>())});

    ETCrossCheckCoverage coverage;
    expect_et_cross_check_group("numeric-cross-kind", representations, coverage);
    EXPECT_TRUE(coverage.encoded_encoded);
    EXPECT_TRUE(coverage.encoded_typed);
    EXPECT_TRUE(coverage.typed_typed);
}

TEST(ColumnVariantV2Test, ETCrossCheckFloatingStringAndNullMatrix) {
    const auto negative_zero = std::bit_cast<double>(uint64_t {0x8000000000000000ULL});
    const auto positive_zero = std::bit_cast<double>(uint64_t {0});
    const auto nan_a = std::bit_cast<double>(uint64_t {0x7FF8000000000001ULL});
    const auto nan_b = std::bit_cast<double>(uint64_t {0xFFF8000000000042ULL});
    const auto float_nan = std::bit_cast<float>(uint32_t {0x7FC01234U});
    const auto double_nan = std::bit_cast<double>(uint64_t {0x7FF8000000005678ULL});
    const std::array<float, 2> FLOAT_VALUES {-0.0F, float_nan};
    const std::array<double, 2> DOUBLE_VALUES {positive_zero, double_nan};
    constexpr std::array<uint8_t, 2> FLOAT_NOT_NULL {};
    const std::array<VariantField, 2> encoded_floating_a {encoded_double(negative_zero),
                                                          encoded_double(nan_a)};
    const std::array<VariantField, 2> encoded_floating_b {encoded_double(positive_zero),
                                                          encoded_double(nan_b)};

    std::vector<ETCrossCheckRepresentation> floating_representations;
    floating_representations.push_back(
            {"E/double-negative-zero-nan-a", encoded_rows(encoded_floating_a)});
    floating_representations.push_back(
            {"E/double-positive-zero-nan-b", encoded_rows(encoded_floating_b)});
    floating_representations.push_back(
            {"T/float32-negative-zero-nan",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnFloat32, float>(FLOAT_VALUES, FLOAT_NOT_NULL),
                     std::make_shared<DataTypeFloat32>())});
    floating_representations.push_back(
            {"T/float64-positive-zero-nan",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnFloat64, double>(DOUBLE_VALUES, FLOAT_NOT_NULL),
                     std::make_shared<DataTypeFloat64>())});
    ETCrossCheckCoverage floating_coverage;
    expect_et_cross_check_group("signed-zero-and-nan", floating_representations, floating_coverage);

    const std::string short_63(63, 's');
    const std::string long_64(64, 'L');
    const std::string long_257(257, 'x');
    const std::array<std::string_view, 5> TEXTS {"", "short", short_63, long_64, long_257};
    constexpr std::array<uint8_t, 5> STRING_NOT_NULL {};
    std::vector<VariantField> encoded_strings;
    encoded_strings.reserve(TEXTS.size());
    for (std::string_view text : TEXTS) {
        encoded_strings.push_back(encoded_string(text));
    }

    std::vector<ETCrossCheckRepresentation> string_representations;
    string_representations.push_back({"E/raw-short-long-string", encoded_rows(encoded_strings)});
    string_representations.push_back(
            {"T/string-scan",
             ColumnVariantV2::create_typed(nullable_strings(TEXTS, STRING_NOT_NULL),
                                           std::make_shared<DataTypeString>())});
    string_representations.push_back(
            {"T/varchar-cast",
             ColumnVariantV2::create_typed(
                     nullable_strings(TEXTS, STRING_NOT_NULL),
                     std::make_shared<DataTypeString>(257, PrimitiveType::TYPE_VARCHAR))});
    ETCrossCheckCoverage string_coverage;
    expect_et_cross_check_group("short-and-long-string", string_representations, string_coverage);

    constexpr std::array<Int32, 1> IGNORED_INTEGER {777};
    constexpr std::array<uint8_t, 1> IS_NULL {1};
    const std::array<std::string_view, 1> IGNORED_STRING {"ignored"};
    const std::array<VariantField, 1> encoded_nulls {encoded_null()};
    std::vector<ETCrossCheckRepresentation> null_representations;
    null_representations.push_back({"E/json-null", encoded_rows(encoded_nulls)});
    null_representations.push_back(
            {"T/null-int32",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnInt32, Int32>(IGNORED_INTEGER, IS_NULL),
                     std::make_shared<DataTypeInt32>())});
    null_representations.push_back(
            {"T/null-string",
             ColumnVariantV2::create_typed(nullable_strings(IGNORED_STRING, IS_NULL),
                                           std::make_shared<DataTypeString>())});
    ETCrossCheckCoverage null_coverage;
    expect_et_cross_check_group("json-null", null_representations, null_coverage);
}

TEST(ColumnVariantV2Test, ETCrossCheckTemporalClassMatrix) {
    const std::array<VariantField, 2> encoded_dates {encoded_date(1), encoded_date(2)};
    const std::array<VecDateTimeValue, 2> legacy_dates {
            VecDateTimeValue::create_from_olap_date(pack_olap_date(1970, 1, 2)),
            VecDateTimeValue::create_from_olap_date(pack_olap_date(1970, 1, 3))};
    const std::array<DateV2Value<DateV2ValueType>, 2> date_v2_values {
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2)),
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 3))};
    constexpr std::array<uint8_t, 2> NOT_NULL {};

    std::vector<ETCrossCheckRepresentation> date_representations;
    date_representations.push_back({"E/date", encoded_rows(encoded_dates)});
    date_representations.push_back(
            {"T/legacy-date",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnDate, VecDateTimeValue>(legacy_dates, NOT_NULL),
                     std::make_shared<DataTypeDate>())});
    date_representations.push_back(
            {"T/date-v2", ColumnVariantV2::create_typed(
                                  nullable_fixed_values<ColumnDateV2, DateV2Value<DateV2ValueType>>(
                                          date_v2_values, NOT_NULL),
                                  std::make_shared<DataTypeDateV2>())});
    ETCrossCheckCoverage date_coverage;
    expect_et_cross_check_group("date", date_representations, date_coverage);

    constexpr std::array<int64_t, 2> TIMESTAMP_MICROS {1'000'000, 2'000'000};
    std::array<VariantField, 2> encoded_ntz_micros {
            encoded_timestamp(TIMESTAMP_MICROS[0], false, false),
            encoded_timestamp(TIMESTAMP_MICROS[1], false, false)};
    std::array<VariantField, 2> encoded_ntz_nanos {
            encoded_timestamp(TIMESTAMP_MICROS[0] * 1000, false, true),
            encoded_timestamp(TIMESTAMP_MICROS[1] * 1000, false, true)};
    const std::array<VecDateTimeValue, 2> legacy_datetimes {
            VecDateTimeValue::create_from_olap_datetime(19700101000001ULL),
            VecDateTimeValue::create_from_olap_datetime(19700101000002ULL)};
    const std::array<DateV2Value<DateTimeV2ValueType>, 2> datetime_v2_values {
            DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL),
            DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000002ULL)};

    std::vector<ETCrossCheckRepresentation> ntz_representations;
    ntz_representations.push_back({"E/timestamp-ntz-micros", encoded_rows(encoded_ntz_micros)});
    ntz_representations.push_back({"E/timestamp-ntz-nanos", encoded_rows(encoded_ntz_nanos)});
    ntz_representations.push_back(
            {"T/legacy-datetime",
             ColumnVariantV2::create_typed(nullable_fixed_values<ColumnDateTime, VecDateTimeValue>(
                                                   legacy_datetimes, NOT_NULL),
                                           std::make_shared<DataTypeDateTime>())});
    ntz_representations.push_back(
            {"T/datetime-v2",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>(
                             datetime_v2_values, NOT_NULL),
                     std::make_shared<DataTypeDateTimeV2>(6))});
    ETCrossCheckCoverage ntz_coverage;
    expect_et_cross_check_group("timestamp-ntz", ntz_representations, ntz_coverage);

    TimestampTzValue timestamp_tz_one;
    timestamp_tz_one.unchecked_set_time(1970, 1, 1, 0, 0, 1, 0);
    TimestampTzValue timestamp_tz_two;
    timestamp_tz_two.unchecked_set_time(1970, 1, 1, 0, 0, 2, 0);
    const std::array<TimestampTzValue, 2> timestamp_tz_values {timestamp_tz_one, timestamp_tz_two};
    const std::array<VariantField, 2> encoded_tz_micros {
            encoded_timestamp(TIMESTAMP_MICROS[0], true, false),
            encoded_timestamp(TIMESTAMP_MICROS[1], true, false)};
    const std::array<VariantField, 2> encoded_tz_nanos {
            encoded_timestamp(TIMESTAMP_MICROS[0] * 1000, true, true),
            encoded_timestamp(TIMESTAMP_MICROS[1] * 1000, true, true)};

    std::vector<ETCrossCheckRepresentation> tz_representations;
    tz_representations.push_back({"E/timestamp-tz-micros", encoded_rows(encoded_tz_micros)});
    tz_representations.push_back({"E/timestamp-tz-nanos", encoded_rows(encoded_tz_nanos)});
    tz_representations.push_back(
            {"T/timestamp-tz-scan",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnTimeStampTz, TimestampTzValue>(timestamp_tz_values,
                                                                                NOT_NULL),
                     std::make_shared<DataTypeTimeStampTz>(6))});
    tz_representations.push_back(
            {"T/timestamp-tz-cast",
             ColumnVariantV2::create_typed(
                     nullable_fixed_values<ColumnTimeStampTz, TimestampTzValue>(timestamp_tz_values,
                                                                                NOT_NULL),
                     std::make_shared<DataTypeTimeStampTz>(6))});
    ETCrossCheckCoverage tz_coverage;
    expect_et_cross_check_group("timestamp-tz", tz_representations, tz_coverage);

    const std::array<VariantField, 1> encoded_integer_one {encoded_integer(1, sizeof(int64_t))};
    const std::array<VariantField, 1> encoded_ntz_one {encoded_timestamp(1, false, true)};
    ETCrossCheckRepresentation integer_one {.name = "E/exact-integer-one",
                                            .column = encoded_rows(encoded_integer_one)};
    ETCrossCheckRepresentation ntz_one {.name = "E/timestamp-ntz-payload-one",
                                        .column = encoded_rows(encoded_ntz_one)};
    expect_et_cross_check_distinct("date-vs-exact-integer", date_representations[0], 0, integer_one,
                                   0);
    expect_et_cross_check_distinct("date-vs-timestamp-ntz", date_representations[0], 0, ntz_one, 0);
    expect_et_cross_check_distinct("timestamp-tz-vs-ntz", tz_representations[2], 0,
                                   ntz_representations[2], 0);
}

TEST(ColumnVariantV2Test, TypedUnsupportedInterfacesStayUnsupported) {
    constexpr std::array<int32_t, 1> VALUES {1};
    constexpr std::array<uint8_t, 1> NULLS {0};
    auto typed = typed_int32(VALUES, NULLS);
    Field field;
    expect_not_implemented([&] { static_cast<void>((*typed)[0]); }, "T1.7b");
    expect_not_implemented([&] { typed->get(0, field); }, "T1.7b");
    expect_not_implemented([&] { typed->insert(field); }, "T1.7b");
    expect_not_implemented([&] { static_cast<void>(typed->get_data_at(0)); },
                           "intentionally unsupported");
    HybridSorter sorter;
    IColumn::Permutation result;
    expect_not_implemented([&] { typed->get_permutation(false, 0, 0, sorter, result); },
                           "intentionally unsupported");
    expect_not_implemented([&] { typed->replace_column_data(*typed, 0); },
                           "intentionally unsupported");
    EXPECT_TRUE(typed->is_typed());
}

TEST(ColumnVariantV2Test, ReplaceNullPayloadsWithCanonicalDefault) {
    auto encoded = ColumnVariantV2::create();
    insert_encoded_field(*encoded, encode_json("1"));
    insert_encoded_field(*encoded, encode_json(R"({"hidden":2})"));
    insert_encoded_field(*encoded, encode_json("3"));
    constexpr std::array<uint8_t, 3> ENCODED_NULLS {0, 1, 0};
    encoded->replace_column_null_data(ENCODED_NULLS.data());
    EXPECT_EQ(json_at(*encoded, 0), "1");
    EXPECT_EQ(json_at(*encoded, 1), "{}");
    EXPECT_EQ(json_at(*encoded, 2), "3");

    constexpr std::array<int32_t, 3> VALUES {7, 8, 9};
    constexpr std::array<uint8_t, 3> NOT_NULL {0, 0, 0};
    constexpr std::array<uint8_t, 3> TYPED_NULLS {1, 0, 1};
    auto typed = typed_int32(VALUES, NOT_NULL);
    typed->replace_column_null_data(TYPED_NULLS.data());
    EXPECT_FALSE(typed->is_typed());
    EXPECT_EQ(json_at(*typed, 0), "{}");
    EXPECT_EQ(json_at(*typed, 1), "8");
    EXPECT_EQ(json_at(*typed, 2), "{}");
}

TEST(ColumnVariantV2Test, DeferredAndUnsupportedInterfaces) {
    auto column = ColumnVariantV2::create();
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json("1"));
    Field field;

    expect_not_implemented([&] { static_cast<void>((*column)[0]); }, "T1.7b");
    expect_not_implemented([&] { column->get(0, field); }, "T1.7b");
    expect_not_implemented([&] { column->insert(field); }, "T1.7b");
    expect_not_implemented([&] { column->insert_duplicate_fields(field, 1); }, "T1.7b");

    expect_not_implemented([&] { static_cast<void>(column->get_data_at(0)); },
                           "intentionally unsupported");
    HybridSorter sorter;
    IColumn::Permutation result;
    expect_not_implemented([&] { column->get_permutation(false, 0, 0, sorter, result); },
                           "intentionally unsupported");
    expect_not_implemented([&] { column->replace_column_data(*source, 0); },
                           "intentionally unsupported");
}

} // namespace doris
