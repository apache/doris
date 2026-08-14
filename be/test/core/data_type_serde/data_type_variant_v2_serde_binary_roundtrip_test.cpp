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

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/decimalv2_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

constexpr size_t VARIANT_COLUMN_HEADER_BYTES = sizeof(bool) + sizeof(size_t) * 2;
constexpr size_t VARIANT_REPRESENTATION_OFFSET = VARIANT_COLUMN_HEADER_BYTES;
constexpr size_t SHREDDED_WIRE_VERSION_OFFSET = VARIANT_REPRESENTATION_OFFSET + sizeof(uint8_t);
constexpr size_t SHREDDED_PAYLOAD_LENGTH_OFFSET = SHREDDED_WIRE_VERSION_OFFSET + sizeof(uint8_t);
constexpr size_t SHREDDED_PAYLOAD_OFFSET = SHREDDED_PAYLOAD_LENGTH_OFFSET + sizeof(uint64_t);

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder block = encoder.finish_batch();
    return VariantField::from_ref(block.value_at(0));
}

ColumnVariantV2::MutablePtr encoded(std::string_view json) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(json));
    return column;
}

void append_unsigned(std::string& output, unsigned __int128 value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

VariantField noncanonical_object() {
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
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 2, 1);
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));

    std::string field;
    append_unsigned(field, metadata.size(), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return VariantField::from_bytes({field.data(), field.size()});
}

std::vector<char> serialize(const IColumn& source,
                            int be_exec_version = BeExecVersionManager::get_newest_version()) {
    const DataTypeVariantV2 type;
    const int64_t max_size = type.get_uncompressed_serialized_bytes(source, be_exec_version);
    EXPECT_GT(max_size, 0);
    std::vector<char> bytes(max_size);
    char* end = type.serialize(source, bytes.data(), be_exec_version);
    EXPECT_LE(end - bytes.data(), max_size);
    bytes.resize(end - bytes.data());
    return bytes;
}

struct DecodedVariant {
    MutableColumnPtr column;

    bool is_constant() const { return check_and_get_column<ColumnConst>(column.get()) != nullptr; }

    ColumnVariantV2* operator->() {
        IColumn* physical = column.get();
        if (auto* constant = check_and_get_column<ColumnConst>(physical)) {
            physical = const_cast<IColumn*>(&constant->get_data_column());
        }
        return &assert_cast<ColumnVariantV2&>(*physical);
    }
    const ColumnVariantV2* operator->() const {
        const IColumn* physical = column.get();
        if (const auto* constant = check_and_get_column<ColumnConst>(physical)) {
            physical = &constant->get_data_column();
        }
        return &assert_cast<const ColumnVariantV2&>(*physical);
    }
    const ColumnVariantV2& operator*() const { return *operator->(); }
};

DecodedVariant deserialize(const std::vector<char>& bytes,
                           int be_exec_version = BeExecVersionManager::get_newest_version()) {
    const DataTypeVariantV2 type;
    MutableColumnPtr destination = type.create_column();
    EXPECT_EQ(DataTypeVariantV2SerDe::deserialize(bytes.data(), bytes.data() + bytes.size(),
                                                  &destination, be_exec_version),
              bytes.data() + bytes.size());
    return {.column = std::move(destination)};
}

DecodedVariant round_trip(const IColumn& source,
                          int be_exec_version = BeExecVersionManager::get_newest_version()) {
    return deserialize(serialize(source, be_exec_version), be_exec_version);
}

MutableColumnPtr wrap_nullable(MutableColumnPtr nested, std::span<const uint8_t> nullmap) {
    EXPECT_EQ(nested->size(), nullmap.size());
    auto null_column = ColumnUInt8::create();
    null_column->get_data().insert(nullmap.begin(), nullmap.end());
    return ColumnNullable::create(std::move(nested), std::move(null_column));
}

template <typename Column, typename Value>
MutableColumnPtr fixed_column(std::initializer_list<Value> values) {
    auto column = Column::create();
    for (const Value& value : values) {
        column->insert_value(value);
    }
    return column;
}

template <typename Column, typename Value>
MutableColumnPtr decimal_column(uint32_t scale, std::initializer_list<Value> values) {
    auto column = Column::create(0, scale);
    for (const Value& value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnVariantV2::MutablePtr typed(MutableColumnPtr nested, DataTypePtr type,
                                  std::span<const uint8_t> nullmap) {
    return ColumnVariantV2::create_typed(wrap_nullable(std::move(nested), nullmap),
                                         std::move(type));
}

ColumnVariantV2::MutablePtr encoded_rows(std::initializer_list<std::string_view> rows) {
    auto column = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        insert_encoded_field(*column, encode_json(row));
    }
    return column;
}

ColumnUInt8::MutablePtr presence(std::initializer_list<uint8_t> values) {
    auto column = ColumnUInt8::create();
    for (uint8_t value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnVariantV2::MutablePtr conflict_shredded() {
    auto residual = encoded_rows({R"({})", R"({"shape":{"value_leaf_unique":"typed-conflict"}})",
                                  R"({"shape":"object-conflict"})"});
    constexpr std::array<uint8_t, 3> TYPED_NULLS {0, 1, 1};
    auto typed_values = typed(fixed_column<ColumnInt32, Int32>({7, 0, 0}),
                              std::make_shared<DataTypeInt32>(), TYPED_NULLS);
    auto encoded_values = encoded_rows({"1", R"("one")", "null"});

    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"shape", "value_leaf_unique"}),
                        std::move(typed_values), presence({1, 0, 0}));
    fields.emplace_back(PathInData(std::vector<std::string> {"wire_presence_unique"}),
                        std::move(encoded_values), presence({1, 1, 1}));
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

ColumnVariantV2::MutablePtr simple_shredded(const std::vector<std::vector<std::string>>& paths) {
    auto residual = encoded_rows({R"({})"});
    ColumnVariantV2::ShreddedFields fields;
    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    for (const auto& path : paths) {
        auto values = typed(fixed_column<ColumnInt32, Int32>({1}),
                            std::make_shared<DataTypeInt32>(), NOT_NULL);
        fields.emplace_back(PathInData(path), std::move(values), presence({1}));
    }
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

void expect_materialized_equal(const ColumnVariantV2& expected, const ColumnVariantV2& actual) {
    const auto expected_encoded = expected.materialize_encoded_range(0, expected.size());
    const auto actual_encoded = actual.materialize_encoded_range(0, actual.size());
    ASSERT_EQ(actual_encoded->size(), expected_encoded->size());
    for (size_t row = 0; row < expected_encoded->size(); ++row) {
        EXPECT_TRUE(canonical_equals(expected_encoded->read_view().value_at(row),
                                     actual_encoded->read_view().value_at(row)))
                << row;
    }
}

size_t path_part_offset(const std::vector<char>& bytes, std::string_view key) {
    std::vector<char> needle(sizeof(uint32_t) + key.size() + sizeof(uint8_t) * 2, 0);
    const auto key_size = static_cast<uint32_t>(key.size());
    std::memcpy(needle.data(), &key_size, sizeof(key_size));
    std::memcpy(needle.data() + sizeof(key_size), key.data(), key.size());
    const auto found = std::ranges::search(bytes, needle).begin();
    if (found == bytes.end()) {
        ADD_FAILURE() << "Could not find serialized path part " << key;
        return 0;
    }
    return static_cast<size_t>(found - bytes.begin());
}

size_t presence_values_offset(const std::vector<char>& bytes, std::string_view last_path_part) {
    return path_part_offset(bytes, last_path_part) + sizeof(uint32_t) + last_path_part.size() +
           sizeof(uint8_t) * 2 + sizeof(uint64_t);
}

size_t child_length_offset(const std::vector<char>& bytes, std::string_view last_path_part,
                           size_t saved_rows) {
    return presence_values_offset(bytes, last_path_part) + saved_rows;
}

void write_u64(std::vector<char>& bytes, size_t offset, uint64_t value) {
    ASSERT_LE(offset + sizeof(value), bytes.size());
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
}

void expect_corrupt_wire_rejected_without_publish(const std::vector<char>& bytes) {
    auto sentinel = encoded(R"({"destination":"unchanged"})");
    const VariantField sentinel_value = VariantField::from_ref(sentinel->read_view().value_at(0));
    MutableColumnPtr destination = std::move(sentinel);
    IColumn* const original_destination = destination.get();
    bool rejected = false;
    try {
        static_cast<void>(DataTypeVariantV2SerDe::deserialize(
                bytes.data(), bytes.data() + bytes.size(), &destination,
                BeExecVersionManager::get_newest_version()));
    } catch (const Exception& exception) {
        rejected = true;
        EXPECT_EQ(exception.code(), ErrorCode::CORRUPTION) << exception.what();
    }
    EXPECT_TRUE(rejected);
    ASSERT_EQ(destination.get(), original_destination);
    const auto& unchanged = assert_cast<const ColumnVariantV2&>(*destination);
    ASSERT_TRUE(unchanged.is_encoded());
    ASSERT_EQ(unchanged.size(), 1);
    EXPECT_EQ(VariantField::from_ref(unchanged.read_view().value_at(0)).bytes(),
              sentinel_value.bytes());
}

void expect_type_identity(const DataTypePtr& expected, const DataTypePtr& actual) {
    ASSERT_NE(expected, nullptr);
    ASSERT_NE(actual, nullptr);
    EXPECT_EQ(actual->get_primitive_type(), expected->get_primitive_type());
    EXPECT_EQ(actual->get_precision(), expected->get_precision());
    EXPECT_EQ(actual->get_scale(), expected->get_scale());
    if (is_string_type(expected->get_primitive_type())) {
        EXPECT_EQ(assert_cast<const DataTypeString&>(*actual).len(),
                  assert_cast<const DataTypeString&>(*expected).len());
    }
    if (expected->get_primitive_type() == TYPE_DECIMALV2) {
        const auto& expected_decimal = assert_cast<const DataTypeDecimalV2&>(*expected);
        const auto& actual_decimal = assert_cast<const DataTypeDecimalV2&>(*actual);
        EXPECT_EQ(actual_decimal.get_original_precision(),
                  expected_decimal.get_original_precision());
        EXPECT_EQ(actual_decimal.get_original_scale(), expected_decimal.get_original_scale());
    }
}

void expect_typed_equal(const ColumnVariantV2& expected, const ColumnVariantV2& actual) {
    ASSERT_TRUE(expected.is_typed());
    ASSERT_TRUE(actual.is_typed());
    ASSERT_EQ(actual.size(), expected.size());
    expect_type_identity(expected.typed_type(), actual.typed_type());
    const auto& expected_nullable = assert_cast<const ColumnNullable&>(expected.typed_column());
    const auto& actual_nullable = assert_cast<const ColumnNullable&>(actual.typed_column());
    ASSERT_EQ(actual_nullable.size(), expected_nullable.size());
    for (size_t row = 0; row < expected_nullable.size(); ++row) {
        EXPECT_EQ(actual_nullable.get_null_map_data()[row],
                  expected_nullable.get_null_map_data()[row])
                << row;
        const StringRef expected_value = expected_nullable.get_nested_column().get_data_at(row);
        const StringRef actual_value = actual_nullable.get_nested_column().get_data_at(row);
        ASSERT_EQ(actual_value.size, expected_value.size) << row;
        EXPECT_EQ(std::string_view(actual_value.data, actual_value.size),
                  std::string_view(expected_value.data, expected_value.size))
                << row;
    }
}

void expect_typed_round_trip(const ColumnVariantV2& source) {
    const auto destination = round_trip(source);
    expect_typed_equal(source, *destination);
}

} // namespace

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedAndTypedWireDoesNotDependOnBeExecVersion) {
    const int newest_version = BeExecVersionManager::get_newest_version();
    const int previous_version = newest_version - 1;
    constexpr size_t REPRESENTATION_OFFSET = sizeof(bool) + sizeof(size_t) * 2;

    auto encoded_source = encoded(R"({"legacy":1})");
    const auto encoded_bytes = serialize(*encoded_source, newest_version);
    EXPECT_EQ(encoded_bytes, serialize(*encoded_source, previous_version));
    ASSERT_GT(encoded_bytes.size(), REPRESENTATION_OFFSET);
    EXPECT_EQ(static_cast<uint8_t>(encoded_bytes[REPRESENTATION_OFFSET]), 0);
    const auto decoded_encoded = deserialize(encoded_bytes, previous_version);
    ASSERT_TRUE(decoded_encoded->is_encoded());
    EXPECT_TRUE(canonical_equals(encoded_source->read_view().value_at(0),
                                 decoded_encoded->read_view().value_at(0)));

    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    auto typed_source = typed(fixed_column<ColumnInt32, Int32>({123}),
                              std::make_shared<DataTypeInt32>(), NOT_NULL);
    const auto typed_bytes = serialize(*typed_source, newest_version);
    EXPECT_EQ(typed_bytes, serialize(*typed_source, previous_version));
    ASSERT_GT(typed_bytes.size(), REPRESENTATION_OFFSET);
    EXPECT_EQ(static_cast<uint8_t>(typed_bytes[REPRESENTATION_OFFSET]), 1);
    const auto decoded_typed = deserialize(typed_bytes, previous_version);
    expect_typed_equal(*typed_source, *decoded_typed);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ShreddedWireDoesNotDependOnBeExecVersion) {
    const int newest_version = BeExecVersionManager::get_newest_version();
    const int previous_version = newest_version - 1;
    auto source = conflict_shredded();
    ASSERT_TRUE(source->is_shredded());

    const auto current_bytes = serialize(*source, newest_version);
    EXPECT_EQ(current_bytes, serialize(*source, previous_version));
    ASSERT_TRUE(source->is_shredded());
    const auto decoded = deserialize(current_bytes, previous_version);
    ASSERT_TRUE(decoded->is_shredded());
    expect_materialized_equal(*source, *decoded);
    EXPECT_TRUE(source->is_shredded());
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     DecodedShreddedLayoutStaysFrozenAcrossAppendCloneAndClear) {
    auto decoded = round_trip(*conflict_shredded());
    ASSERT_TRUE(decoded->is_shredded());
    ASSERT_EQ(decoded->shredded_field_count(), 2);
    auto& decoded_column = *decoded.operator->();

    const auto append_new_layout_and_expect_residual = [](ColumnVariantV2& destination,
                                                          std::string path) {
        auto source = simple_shredded({{std::move(path)}});
        auto expected = source->materialize_encoded_range(0, 1);
        const size_t destination_row = destination.size();
        destination.insert_range_from(*source, 0, 1);

        EXPECT_EQ(destination.shredded_field_count(), 2);
        EXPECT_EQ(ColumnVariantV2::TestAccess::shredded_union_rebuilds(destination), 0);
        auto actual = destination.materialize_encoded_range(destination_row, 1);
        EXPECT_TRUE(canonical_equals(expected->get_value_ref(0), actual->get_value_ref(0)));
    };

    append_new_layout_and_expect_residual(decoded_column, "decoded_new");

    MutableColumnPtr cloned_column = decoded->clone_resized(decoded->size());
    auto& cloned = assert_cast<ColumnVariantV2&>(*cloned_column);
    append_new_layout_and_expect_residual(cloned, "clone_new");

    cloned.clear();
    EXPECT_TRUE(cloned.is_shredded());
    EXPECT_EQ(cloned.shredded_field_count(), 2);
    append_new_layout_and_expect_residual(cloned, "clear_new");
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the wire conflict matrix.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWirePreservesTypeAndStructureConflictRows) {
    constexpr size_t REPRESENTATION_OFFSET = sizeof(bool) + sizeof(size_t) * 2;
    auto source = conflict_shredded();
    const auto bytes = serialize(*source);
    ASSERT_GT(bytes.size(), REPRESENTATION_OFFSET);
    EXPECT_EQ(static_cast<uint8_t>(bytes[REPRESENTATION_OFFSET]), 2);
    const auto decoded = deserialize(bytes);
    ASSERT_TRUE(source->is_shredded());
    ASSERT_TRUE(decoded->is_shredded());
    ASSERT_EQ(decoded->shredded_field_count(), 2);

    const auto& conflict_path = decoded->shredded_field_path(0).get_parts();
    ASSERT_EQ(conflict_path.size(), 2);
    EXPECT_EQ(conflict_path[0].key, "shape");
    EXPECT_FALSE(conflict_path[0].is_nested);
    EXPECT_EQ(conflict_path[0].anonymous_array_level, 0);
    EXPECT_EQ(conflict_path[1].key, "value_leaf_unique");
    EXPECT_FALSE(conflict_path[1].is_nested);
    EXPECT_EQ(conflict_path[1].anonymous_array_level, 0);
    EXPECT_TRUE(decoded->shredded_field_values(0).is_typed());
    EXPECT_TRUE(decoded->shredded_field_values(1).is_encoded());
    const auto& conflict_presence = decoded->shredded_field_presence(0).get_data();
    ASSERT_EQ(conflict_presence.size(), 3);
    EXPECT_EQ(conflict_presence[0], 1);
    EXPECT_EQ(conflict_presence[1], 0);
    EXPECT_EQ(conflict_presence[2], 0);
    expect_materialized_equal(*source, *decoded);

    const auto& mixed_encoded_child = decoded->shredded_field_values(1);
    const std::array<std::string_view, 3> expected_child_rows {"1", R"("one")", "null"};
    for (size_t row = 0; row < expected_child_rows.size(); ++row) {
        const VariantField expected = encode_json(expected_child_rows[row]);
        EXPECT_TRUE(canonical_equals(expected.ref(), mixed_encoded_child.read_view().value_at(row)))
                << row;
    }

    const auto materialized = decoded->materialize_encoded_range(0, decoded->size());
    const std::array<std::string_view, 3> expected_rows {
            R"({"shape":{"value_leaf_unique":7},"wire_presence_unique":1})",
            R"({"shape":{"value_leaf_unique":"typed-conflict"},"wire_presence_unique":"one"})",
            R"({"shape":"object-conflict","wire_presence_unique":null})"};
    for (size_t row = 0; row < expected_rows.size(); ++row) {
        const VariantField expected = encode_json(expected_rows[row]);
        EXPECT_TRUE(canonical_equals(expected.ref(), materialized->read_view().value_at(row)))
                << row;
    }
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, NativeShreddedWirePreservesEmptyAndConstColumns) {
    auto empty_residual = ColumnVariantV2::create();
    constexpr std::array<uint8_t, 0> NO_NULLS {};
    auto empty_values = typed(ColumnInt32::create(), std::make_shared<DataTypeInt32>(), NO_NULLS);
    ColumnVariantV2::ShreddedFields empty_fields;
    empty_fields.emplace_back(PathInData(std::vector<std::string> {"empty_path"}),
                              std::move(empty_values), ColumnUInt8::create());
    auto empty_source =
            ColumnVariantV2::create_shredded(std::move(empty_residual), std::move(empty_fields));
    const auto decoded_empty = round_trip(*empty_source);
    ASSERT_TRUE(decoded_empty->is_shredded());
    EXPECT_EQ(decoded_empty->size(), 0);
    EXPECT_EQ(decoded_empty->shredded_field_count(), 1);

    auto one_source = simple_shredded({{"const_path_unique"}});
    ColumnPtr one_data = std::move(one_source);
    const auto& one_source_ref = assert_cast<const ColumnVariantV2&>(*one_data);
    ColumnPtr const_source = ColumnConst::create(one_data, 4);
    const auto decoded_const = round_trip(*const_source);
    ASSERT_TRUE(decoded_const.is_constant());
    ASSERT_EQ(decoded_const.column->size(), 4);
    ASSERT_TRUE(decoded_const->is_shredded());
    ASSERT_EQ(decoded_const->size(), 1);
    expect_materialized_equal(one_source_ref, *decoded_const);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRoundTripsNullablePresentMissingVariantNullAndSqlNull) {
    auto residual = encoded_rows({R"({})", R"({})", R"({})", R"({})"});
    auto values = encoded_rows({"1", "null", "null", "null"});
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"nullable_wire_unique"}),
                        std::move(values), presence({1, 0, 1, 0}));
    auto shredded = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    constexpr std::array<uint8_t, 4> OUTER_NULLS {0, 0, 0, 1};
    MutableColumnPtr source = wrap_nullable(std::move(shredded), OUTER_NULLS);
    const auto& source_nullable = assert_cast<const ColumnNullable&>(*source);
    const auto& source_nested =
            assert_cast<const ColumnVariantV2&>(source_nullable.get_nested_column());
    ASSERT_TRUE(source_nested.is_shredded());

    const DataTypePtr nullable_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    const int be_exec_version = BeExecVersionManager::get_newest_version();
    const int64_t max_size =
            nullable_type->get_uncompressed_serialized_bytes(*source, be_exec_version);
    ASSERT_GT(max_size, 0);
    std::vector<char> bytes(max_size);
    char* const serialized_end = nullable_type->serialize(*source, bytes.data(), be_exec_version);
    ASSERT_LE(serialized_end - bytes.data(), max_size);
    bytes.resize(serialized_end - bytes.data());

    MutableColumnPtr decoded = nullable_type->create_column();
    EXPECT_EQ(nullable_type->deserialize(bytes.data(), &decoded, be_exec_version),
              bytes.data() + bytes.size());
    const auto& decoded_nullable = assert_cast<const ColumnNullable&>(*decoded);
    ASSERT_EQ(decoded_nullable.size(), OUTER_NULLS.size());
    for (size_t row = 0; row < OUTER_NULLS.size(); ++row) {
        EXPECT_EQ(decoded_nullable.get_null_map_data()[row], OUTER_NULLS[row]) << row;
    }
    const auto& decoded_nested =
            assert_cast<const ColumnVariantV2&>(decoded_nullable.get_nested_column());
    ASSERT_TRUE(decoded_nested.is_shredded());
    expect_materialized_equal(source_nested, decoded_nested);

    const auto materialized = decoded_nested.materialize_encoded_range(0, decoded_nested.size());
    const std::array<std::string_view, 4> expected_rows {
            R"({"nullable_wire_unique":1})", R"({})", R"({"nullable_wire_unique":null})", R"({})"};
    for (size_t row = 0; row < expected_rows.size(); ++row) {
        const VariantField expected = encode_json(expected_rows[row]);
        EXPECT_TRUE(canonical_equals(expected.ref(), materialized->read_view().value_at(row)))
                << row;
    }
    EXPECT_TRUE(source_nested.is_shredded());
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     BlockRoundTripsShreddedFirstAndNullableLastWithAndWithoutCompression) {
    for (segment_v2::CompressionTypePB compression :
         {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
        auto first = conflict_shredded();
        auto nullable_nested = conflict_shredded();
        const auto& expected_first = *first;
        const auto& expected_nullable = *nullable_nested;

        auto middle = ColumnInt32::create();
        middle->insert_many_vals(7, first->size());
        auto outer_nulls = ColumnUInt8::create(first->size(), 0);
        outer_nulls->get_data().back() = 1;
        MutableColumnPtr nullable =
                ColumnNullable::create(std::move(nullable_nested), std::move(outer_nulls));

        const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
        const DataTypePtr nullable_variant_type = make_nullable(variant_type);
        Block source({{std::move(first), variant_type, "first"},
                      {std::move(middle), std::make_shared<DataTypeInt32>(), "middle"},
                      {std::move(nullable), nullable_variant_type, "last"}});

        PBlock wire;
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t compress_time = 0;
        ASSERT_TRUE(source.serialize(BeExecVersionManager::get_newest_version(), &wire,
                                     &uncompressed_bytes, &compressed_bytes, &compress_time,
                                     compression)
                            .ok());

        Block decoded;
        size_t decoded_bytes = 0;
        int64_t decompress_time = 0;
        ASSERT_TRUE(decoded.deserialize(wire, &decoded_bytes, &decompress_time).ok());
        ASSERT_EQ(decoded.columns(), 3);
        const auto& decoded_first =
                assert_cast<const ColumnVariantV2&>(*decoded.get_by_position(0).column);
        ASSERT_TRUE(decoded_first.is_shredded());
        expect_materialized_equal(expected_first, decoded_first);

        const auto& decoded_nullable =
                assert_cast<const ColumnNullable&>(*decoded.get_by_position(2).column);
        EXPECT_EQ(decoded_nullable.get_null_map_data(), (NullMap {0, 0, 1}));
        const auto& decoded_nested =
                assert_cast<const ColumnVariantV2&>(decoded_nullable.get_nested_column());
        ASSERT_TRUE(decoded_nested.is_shredded());
        expect_materialized_equal(expected_nullable, decoded_nested);
    }
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, BlockRoundTripsNestedShreddedArray) {
    auto nested = conflict_shredded();
    const auto& expected_nested = *nested;
    const size_t element_count = nested->size();
    auto element_nulls = ColumnUInt8::create(element_count, 0);
    auto elements = ColumnNullable::create(std::move(nested), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(element_count);
    auto array = ColumnArray::create(std::move(elements), std::move(offsets));
    const DataTypePtr array_type =
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariantV2>());
    Block source({{std::move(array), array_type, "nested_shredded"}});

    PBlock wire;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compress_time = 0;
    ASSERT_TRUE(source.serialize(BeExecVersionManager::get_newest_version(), &wire,
                                 &uncompressed_bytes, &compressed_bytes, &compress_time,
                                 segment_v2::NO_COMPRESSION)
                        .ok());

    Block decoded;
    size_t decoded_bytes = 0;
    int64_t decompress_time = 0;
    ASSERT_TRUE(decoded.deserialize(wire, &decoded_bytes, &decompress_time).ok());
    const auto& decoded_array = assert_cast<const ColumnArray&>(*decoded.get_by_position(0).column);
    const auto& decoded_elements = assert_cast<const ColumnNullable&>(decoded_array.get_data());
    const auto& decoded_nested =
            assert_cast<const ColumnVariantV2&>(decoded_elements.get_nested_column());
    ASSERT_TRUE(decoded_nested.is_shredded());
    expect_materialized_equal(expected_nested, decoded_nested);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, BlockRoundTripsNestedShreddedStructAndMap) {
    const auto round_trip_block = [](const Block& source, PBlock* wire, Block* decoded) {
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t compress_time = 0;
        EXPECT_TRUE(source.serialize(BeExecVersionManager::get_newest_version(), wire,
                                     &uncompressed_bytes, &compressed_bytes, &compress_time,
                                     segment_v2::NO_COMPRESSION)
                            .ok());
        size_t decoded_bytes = 0;
        int64_t decompress_time = 0;
        EXPECT_TRUE(decoded->deserialize(*wire, &decoded_bytes, &decompress_time).ok());
    };
    auto struct_variant = conflict_shredded();
    const auto& expected_struct_variant = *struct_variant;
    MutableColumns struct_children;
    struct_children.emplace_back(std::move(struct_variant));
    struct_children.emplace_back(fixed_column<ColumnInt32, Int32>({11, 12, 13}));
    auto struct_column = ColumnStruct::create(std::move(struct_children));
    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    const DataTypePtr struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {variant_type, std::make_shared<DataTypeInt32>()},
            Strings {"nested_variant", "number"});
    Block struct_source({{std::move(struct_column), struct_type, "shredded_struct"}});
    PBlock struct_wire;
    Block decoded_struct_block;
    round_trip_block(struct_source, &struct_wire, &decoded_struct_block);
    const auto& decoded_struct =
            assert_cast<const ColumnStruct&>(*decoded_struct_block.get_by_position(0).column);
    const auto& decoded_struct_variant =
            assert_cast<const ColumnVariantV2&>(decoded_struct.get_column(0));
    ASSERT_TRUE(decoded_struct_variant.is_shredded());
    expect_materialized_equal(expected_struct_variant, decoded_struct_variant);

    auto map_variant = conflict_shredded();
    const auto& expected_map_variant = *map_variant;
    auto keys = ColumnString::create();
    keys->insert_data("a", 1);
    keys->insert_data("b", 1);
    keys->insert_data("c", 1);
    auto map_offsets = ColumnArray::ColumnOffsets::create();
    map_offsets->get_data().push_back(keys->size());
    auto map_column =
            ColumnMap::create(std::move(keys), std::move(map_variant), std::move(map_offsets));
    const DataTypePtr map_type =
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), variant_type);
    Block map_source({{std::move(map_column), map_type, "shredded_map"}});
    PBlock map_wire;
    Block decoded_map_block;
    round_trip_block(map_source, &map_wire, &decoded_map_block);
    const auto& decoded_map =
            assert_cast<const ColumnMap&>(*decoded_map_block.get_by_position(0).column);
    const auto& decoded_map_variant = assert_cast<const ColumnVariantV2&>(decoded_map.get_values());
    ASSERT_TRUE(decoded_map_variant.is_shredded());
    expect_materialized_equal(expected_map_variant, decoded_map_variant);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRejectsCorruptTagRowsPresenceAndNestedChildAtomically) {
    constexpr size_t LOGICAL_ROWS_OFFSET = sizeof(bool);
    constexpr size_t SAVED_ROWS_OFFSET = sizeof(bool) + sizeof(size_t);
    auto source = conflict_shredded();
    const auto valid = serialize(*source);

    auto bad_tag = valid;
    bad_tag[VARIANT_REPRESENTATION_OFFSET] = static_cast<char>(0x7f);
    expect_corrupt_wire_rejected_without_publish(bad_tag);

    auto bad_rows = valid;
    const size_t wrong_saved_rows = source->size() + 1;
    std::memcpy(bad_rows.data() + SAVED_ROWS_OFFSET, &wrong_saved_rows, sizeof(wrong_saved_rows));
    expect_corrupt_wire_rejected_without_publish(bad_rows);

    auto self_consistent_bad_rows = valid;
    std::memcpy(self_consistent_bad_rows.data() + LOGICAL_ROWS_OFFSET, &wrong_saved_rows,
                sizeof(wrong_saved_rows));
    std::memcpy(self_consistent_bad_rows.data() + SAVED_ROWS_OFFSET, &wrong_saved_rows,
                sizeof(wrong_saved_rows));
    expect_corrupt_wire_rejected_without_publish(self_consistent_bad_rows);

    const size_t conflict_presence = presence_values_offset(valid, "value_leaf_unique");
    auto bad_presence = valid;
    bad_presence[conflict_presence + 1] = 2;
    expect_corrupt_wire_rejected_without_publish(bad_presence);

    auto nested_child = valid;
    const size_t encoded_child_payload =
            child_length_offset(valid, "wire_presence_unique", source->size()) + sizeof(uint64_t);
    nested_child[encoded_child_payload] =
            static_cast<char>(ColumnVariantV2::Representation::SHREDDED);
    expect_corrupt_wire_rejected_without_publish(nested_child);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRejectsTruncatedAndBadFramingAtomically) {
    auto source = conflict_shredded();
    const auto valid = serialize(*source);
    ASSERT_GT(valid.size(), SHREDDED_PAYLOAD_OFFSET);

    auto bad_const_flag = valid;
    bad_const_flag[0] = 2;
    expect_corrupt_wire_rejected_without_publish(bad_const_flag);

    auto truncated_column_header = valid;
    truncated_column_header.resize(VARIANT_COLUMN_HEADER_BYTES - 1);
    expect_corrupt_wire_rejected_without_publish(truncated_column_header);

    auto truncated_wire_version = valid;
    truncated_wire_version.resize(SHREDDED_WIRE_VERSION_OFFSET);
    expect_corrupt_wire_rejected_without_publish(truncated_wire_version);

    auto bad_wire_version = valid;
    bad_wire_version[SHREDDED_WIRE_VERSION_OFFSET] = 2;
    expect_corrupt_wire_rejected_without_publish(bad_wire_version);

    uint64_t residual_size = 0;
    std::memcpy(&residual_size, valid.data() + SHREDDED_PAYLOAD_OFFSET, sizeof(residual_size));
    const size_t field_count_offset =
            SHREDDED_PAYLOAD_OFFSET + sizeof(residual_size) + residual_size;
    ASSERT_LE(field_count_offset + sizeof(uint32_t), valid.size());

    auto empty_fields = valid;
    constexpr uint32_t ZERO_FIELDS = 0;
    std::memcpy(empty_fields.data() + field_count_offset, &ZERO_FIELDS, sizeof(ZERO_FIELDS));
    expect_corrupt_wire_rejected_without_publish(empty_fields);

    auto excessive_fields = valid;
    constexpr uint32_t EXCESSIVE_FIELDS = std::numeric_limits<uint32_t>::max();
    std::memcpy(excessive_fields.data() + field_count_offset, &EXCESSIVE_FIELDS,
                sizeof(EXCESSIVE_FIELDS));
    expect_corrupt_wire_rejected_without_publish(excessive_fields);

    auto truncated_payload_length = valid;
    truncated_payload_length.resize(SHREDDED_PAYLOAD_LENGTH_OFFSET + sizeof(uint64_t) - 1);
    expect_corrupt_wire_rejected_without_publish(truncated_payload_length);

    auto oversized_payload_length = valid;
    write_u64(oversized_payload_length, SHREDDED_PAYLOAD_LENGTH_OFFSET,
              std::numeric_limits<uint64_t>::max());
    expect_corrupt_wire_rejected_without_publish(oversized_payload_length);

    auto truncated_payload = valid;
    truncated_payload.pop_back();
    expect_corrupt_wire_rejected_without_publish(truncated_payload);

    auto oversized_residual_length = valid;
    write_u64(oversized_residual_length, SHREDDED_PAYLOAD_OFFSET,
              std::numeric_limits<uint64_t>::max());
    expect_corrupt_wire_rejected_without_publish(oversized_residual_length);

    const size_t child_size_offset =
            child_length_offset(valid, "value_leaf_unique", source->size());
    auto oversized_child_length = valid;
    write_u64(oversized_child_length, child_size_offset, std::numeric_limits<uint64_t>::max());
    expect_corrupt_wire_rejected_without_publish(oversized_child_length);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     ProductionDeserializeRejectsUnsupportedShreddedWireVersionAtomically) {
    auto unsupported = serialize(*conflict_shredded());
    unsupported[SHREDDED_WIRE_VERSION_OFFSET] = 2;

    auto sentinel = encoded(R"({"destination":"unchanged"})");
    const VariantField sentinel_value = VariantField::from_ref(sentinel->read_view().value_at(0));
    MutableColumnPtr destination = std::move(sentinel);
    IColumn* const original_destination = destination.get();
    const DataTypeVariantV2 type;
    bool rejected = false;
    try {
        static_cast<void>(type.deserialize(unsupported.data(), &destination,
                                           BeExecVersionManager::get_newest_version()));
    } catch (const Exception& exception) {
        rejected = true;
        EXPECT_EQ(exception.code(), ErrorCode::CORRUPTION) << exception.what();
    }

    EXPECT_TRUE(rejected);
    ASSERT_EQ(destination.get(), original_destination);
    const auto& unchanged = assert_cast<const ColumnVariantV2&>(*destination);
    ASSERT_TRUE(unchanged.is_encoded());
    ASSERT_EQ(unchanged.size(), 1);
    EXPECT_EQ(VariantField::from_ref(unchanged.read_view().value_at(0)).bytes(),
              sentinel_value.bytes());
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWirePreservesNonCanonicalNullableNullByte) {
    auto residual = encoded_rows({R"({})"});
    constexpr std::array<uint8_t, 1> NON_CANONICAL_NULL {2};
    auto values = typed(fixed_column<ColumnInt32, Int32>({7}), std::make_shared<DataTypeInt32>(),
                        NON_CANONICAL_NULL);
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"noncanonical_null_unique"}),
                        std::move(values), presence({1}));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));

    const auto decoded = round_trip(*source);
    ASSERT_TRUE(decoded->is_shredded());
    ASSERT_EQ(decoded->shredded_field_count(), 1);
    EXPECT_EQ(decoded->shredded_field_path(0).get_parts().back().key, "noncanonical_null_unique");
    const auto& typed_values = decoded->shredded_field_values(0);
    const auto& nullable = assert_cast<const ColumnNullable&>(typed_values.typed_column());
    EXPECT_EQ(nullable.get_null_map_data()[0], 2);
    EXPECT_TRUE(nullable.is_null_at(0));
    expect_materialized_equal(*source, *decoded);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRejectsCorruptPathOrderPrefixAndFlagsAtomically) {
    auto ordered_source = simple_shredded({{"path_order_a"}, {"path_order_b"}});
    const auto ordered = serialize(*ordered_source);
    auto duplicate_path = ordered;
    const size_t duplicate_offset = path_part_offset(duplicate_path, "path_order_b");
    duplicate_path[duplicate_offset + sizeof(uint32_t) + std::string_view("path_order_b").size() -
                   1] = 'a';
    expect_corrupt_wire_rejected_without_publish(duplicate_path);

    auto prefix_source = simple_shredded({{"prefix_a"}, {"prefix_b", "prefix_leaf"}});
    const auto prefix = serialize(*prefix_source);
    auto overlapping_paths = prefix;
    const size_t prefix_offset = path_part_offset(overlapping_paths, "prefix_b");
    overlapping_paths[prefix_offset + sizeof(uint32_t) + std::string_view("prefix_b").size() - 1] =
            'a';
    expect_corrupt_wire_rejected_without_publish(overlapping_paths);

    auto invalid_flag = ordered;
    const size_t flag_offset = path_part_offset(invalid_flag, "path_order_a") + sizeof(uint32_t) +
                               std::string_view("path_order_a").size();
    invalid_flag[flag_offset] = 2;
    expect_corrupt_wire_rejected_without_publish(invalid_flag);

    auto utf8_source = simple_shredded({{"utf8_path_unique"}});
    auto invalid_utf8 = serialize(*utf8_source);
    const size_t key_offset = path_part_offset(invalid_utf8, "utf8_path_unique") + sizeof(uint32_t);
    invalid_utf8[key_offset] = static_cast<char>(0xFF);
    expect_corrupt_wire_rejected_without_publish(invalid_utf8);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRejectsResidualShreddedFieldExactAndAncestorOverlapAtomically) {
    auto source = conflict_shredded();
    const auto valid = serialize(*source);
    const size_t conflict_presence = presence_values_offset(valid, "value_leaf_unique");

    auto exact_overlap = valid;
    exact_overlap[conflict_presence + 1] = 1;
    expect_corrupt_wire_rejected_without_publish(exact_overlap);

    auto ancestor_overlap = valid;
    ancestor_overlap[conflict_presence + 2] = 1;
    expect_corrupt_wire_rejected_without_publish(ancestor_overlap);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest,
     NativeShreddedWireRejectsActiveEncodedObjectAndArrayChildAtomically) {
    auto residual = encoded_rows({R"({})", R"({})"});
    auto container_values = encoded_rows({R"({"nested":1})", R"([1,2])"});
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"container_child_unique"}),
                        std::move(container_values), presence({0, 0}));
    auto source = ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
    const auto valid = serialize(*source);
    const size_t child_presence = presence_values_offset(valid, "container_child_unique");

    auto active_object = valid;
    active_object[child_presence] = 1;
    expect_corrupt_wire_rejected_without_publish(active_object);

    auto active_array = valid;
    active_array[child_presence + 1] = 1;
    expect_corrupt_wire_rejected_without_publish(active_array);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ComputeVariantTypeRoundTripsEncodedColumn) {
    auto source = encoded(R"({"adapter":[1,true,null]})");
    const auto destination = round_trip(*source);
    ASSERT_EQ(destination->size(), source->size());
    EXPECT_TRUE(canonical_equals(source->read_view().value_at(0),
                                 destination->read_view().value_at(0)));
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ExecutionTypeSelectsPhysicalColumn) {
    DataTypeVariant legacy;
    DataTypeVariantV2 compute_v2;

    MutableColumnPtr legacy_column = legacy.create_column();
    MutableColumnPtr compute_v2_column = compute_v2.create_column();
    EXPECT_NE(dynamic_cast<ColumnVariant*>(legacy_column.get()), nullptr);
    EXPECT_EQ(dynamic_cast<ColumnVariantV2*>(legacy_column.get()), nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(compute_v2_column.get()), nullptr);
    EXPECT_TRUE(legacy.check_column(*legacy_column).ok());
    EXPECT_FALSE(legacy.check_column(*compute_v2_column).ok());
    EXPECT_TRUE(compute_v2.check_column(*compute_v2_column).ok());
    EXPECT_FALSE(compute_v2.check_column(*legacy_column).ok());
    EXPECT_FALSE(legacy.equals(compute_v2));
    EXPECT_FALSE(compute_v2.equals(legacy));
    EXPECT_TRUE(compute_v2.equals(DataTypeVariantV2 {}));
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ExecutionMarkerRoundTripsThroughDescriptors) {
    DataTypeVariantV2 compute_v2(12, true);

    PColumnMeta column_meta;
    compute_v2.to_pb_column_meta(&column_meta);
    DataTypePtr from_protobuf = DataTypeFactory::instance().create_data_type(column_meta);
    ASSERT_NE(from_protobuf, nullptr);
    const auto* protobuf_variant = dynamic_cast<const DataTypeVariantV2*>(from_protobuf.get());
    ASSERT_NE(protobuf_variant, nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(from_protobuf->create_column().get()), nullptr);

    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::VARIANT);
    scalar_type.__set_variant_max_subcolumns_count(12);
    scalar_type.__set_variant_enable_doc_mode(true);
    scalar_type.__set_variant_is_v2(true);
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc type_desc;
    type_desc.types.push_back(type_node);
    DataTypePtr from_thrift = DataTypeFactory::instance().create_data_type(type_desc);
    ASSERT_NE(from_thrift, nullptr);
    const auto* thrift_variant = dynamic_cast<const DataTypeVariantV2*>(from_thrift.get());
    ASSERT_NE(thrift_variant, nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(from_thrift->create_column().get()), nullptr);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedRowsPreserveStateOrderAndRowBytes) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json("null"));
    insert_encoded_field(*source, noncanonical_object());
    insert_encoded_field(*source, encode_json(R"({"z":1,"a":[true,null]})"));
    insert_encoded_field(*source, encode_json(R"("text")"));
    insert_encoded_field(*source, encode_json(R"({"z":2,"a":[]})"));

    const auto destination = round_trip(*source);
    ASSERT_FALSE(destination->is_typed());
    ASSERT_EQ(destination->size(), source->size());
    for (size_t row = 0; row < source->size(); ++row) {
        const VariantField expected = VariantField::from_ref(source->read_view().value_at(row));
        const VariantField actual = VariantField::from_ref(destination->read_view().value_at(row));
        EXPECT_EQ(std::string_view(actual.bytes().data, actual.bytes().size),
                  std::string_view(expected.bytes().data, expected.bytes().size))
                << row;
    }
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedColumnsPreserveDictionaryEntries) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json(R"({"a":1})"));
    insert_encoded_field(*source, encode_json(R"({"b":2})"));
    insert_encoded_field(*source, encode_json(R"({"c":3})"));
    const size_t source_metadata_count = source->read_view().metadata_count();
    ASSERT_GT(source_metadata_count, 1);

    const IColumn::Filter filter {0, 1, 0};
    ColumnPtr filtered = source->filter(filter, 1);
    const IColumn::Permutation permutation {0};
    MutableColumnPtr selected_base = filtered->permute(permutation, 0);
    const auto& selected = assert_cast<const ColumnVariantV2&>(*selected_base);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected.read_view().metadata_count(), source_metadata_count);

    const auto decoded = round_trip(selected);
    ASSERT_FALSE(decoded->is_typed());
    EXPECT_EQ(decoded->read_view().metadata_count(), source_metadata_count);
    EXPECT_EQ(source->read_view().metadata_count(), source_metadata_count);
    EXPECT_FALSE(source->is_typed());
    EXPECT_EQ(selected.read_view().metadata_count(), source_metadata_count);
}

// NOLINTNEXTLINE(readability-function-size) -- one row matrix covers every fixed scalar identity.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, TypedNumericAndDecimalPhysicalBits) {
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    expect_typed_round_trip(*typed(fixed_column<ColumnUInt8, UInt8>({0, 255, 1}),
                                   std::make_shared<DataTypeBool>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnInt8, Int8>({-128, 17, 127}),
                                   std::make_shared<DataTypeInt8>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnInt16, Int16>({-32768, 123, 32767}),
                                   std::make_shared<DataTypeInt16>(), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnInt32, Int32>({std::numeric_limits<Int32>::min(), 0x12345678,
                                                     std::numeric_limits<Int32>::max()}),
                   std::make_shared<DataTypeInt32>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnInt64, Int64>({std::numeric_limits<Int64>::min(), 0x123456789ABCDEF,
                                              std::numeric_limits<Int64>::max()}),
            std::make_shared<DataTypeInt64>(), NULLS));
    const Int128 large_positive = (static_cast<Int128>(1) << 120) + 0x1234;
    expect_typed_round_trip(
            *typed(fixed_column<ColumnInt128, Int128>(
                           {-large_positive, static_cast<Int128>(-1), large_positive}),
                   std::make_shared<DataTypeInt128>(), NULLS));

    expect_typed_round_trip(
            *typed(fixed_column<ColumnFloat32, Float32>({std::bit_cast<Float32>(0x80000000U),
                                                         std::bit_cast<Float32>(0x7FC12345U),
                                                         std::bit_cast<Float32>(0x00000000U)}),
                   std::make_shared<DataTypeFloat32>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnFloat64, Float64>({std::bit_cast<Float64>(0x8000000000000000ULL),
                                                  std::bit_cast<Float64>(0x7FF8123456789ABCULL),
                                                  std::bit_cast<Float64>(0x0000000000000000ULL)}),
            std::make_shared<DataTypeFloat64>(), NULLS));

    const auto decimal_v2_type = std::make_shared<DataTypeDecimalV2>(DecimalV2Value::PRECISION,
                                                                     DecimalV2Value::SCALE, 12, 4);
    expect_typed_round_trip(*typed(
            decimal_column<ColumnDecimal128V2, DecimalV2Value>(
                    DecimalV2Value::SCALE, {DecimalV2Value(-static_cast<Int128>(123456789)),
                                            DecimalV2Value(static_cast<Int128>(0x123456789ABCDEF)),
                                            DecimalV2Value(static_cast<Int128>(987654321))}),
            decimal_v2_type, NULLS));
    expect_typed_round_trip(
            *typed(decimal_column<ColumnDecimal32, Decimal32>(
                           3, {Decimal32(-12345), Decimal32(678), Decimal32(99999)}),
                   std::make_shared<DataTypeDecimal32>(9, 3), NULLS));
    expect_typed_round_trip(*typed(
            decimal_column<ColumnDecimal64, Decimal64>(
                    6, {Decimal64(static_cast<Int64>(-1234567890123LL)), Decimal64(Int64 {7}),
                        Decimal64(static_cast<Int64>(999999999999LL))}),
            std::make_shared<DataTypeDecimal64>(18, 6), NULLS));
    expect_typed_round_trip(*typed(decimal_column<ColumnDecimal128V3, Decimal128V3>(
                                           9, {Decimal128V3(-large_positive), Decimal128V3(-1),
                                               Decimal128V3(large_positive)}),
                                   std::make_shared<DataTypeDecimal128>(38, 9), NULLS));
}

// NOLINTNEXTLINE(readability-function-size) -- one row matrix covers every remaining identity.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, TypedTemporalStringAndIpPhysicalBits) {
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    const VecDateTimeValue old_date_a =
            binary_cast<Int64, VecDateTimeValue>(static_cast<Int64>(0x0123456789ABCDEFULL));
    const VecDateTimeValue old_date_b =
            binary_cast<Int64, VecDateTimeValue>(static_cast<Int64>(0xFEDCBA9876543210ULL));
    expect_typed_round_trip(*typed(fixed_column<ColumnDate, VecDateTimeValue>(
                                           {old_date_a, old_date_b, VecDateTimeValue()}),
                                   std::make_shared<DataTypeDate>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnDateTime, VecDateTimeValue>(
                                           {old_date_b, old_date_a, VecDateTimeValue()}),
                                   std::make_shared<DataTypeDateTime>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnDateV2, DateV2Value<DateV2ValueType>>(
                    {DateV2Value<DateV2ValueType>(0xFFFFFFFFU),
                     DateV2Value<DateV2ValueType>(0x12345678U), DateV2Value<DateV2ValueType>(0U)}),
            std::make_shared<DataTypeDateV2>(), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>(
                           {DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {
                                            0xFFFFFFFFFFFFFFFFULL}),
                            DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {
                                            0x0123456789ABCDEFULL}),
                            DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {0})}),
                   std::make_shared<DataTypeDateTimeV2>(6), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnTimeStampTz, TimestampTzValue>(
                           {TimestampTzValue(0xFEDCBA9876543210ULL),
                            TimestampTzValue(0x0123456789ABCDEFULL), TimestampTzValue(0ULL)}),
                   std::make_shared<DataTypeTimeStampTz>(4), NULLS));

    const std::array<std::string_view, 3> strings {std::string_view("", 0),
                                                   std::string_view("a\0b", 3),
                                                   std::string_view("under-null", 10)};
    const std::array<DataTypePtr, 3> string_types {
            std::make_shared<DataTypeString>(8, TYPE_CHAR),
            std::make_shared<DataTypeString>(32, TYPE_VARCHAR), std::make_shared<DataTypeString>()};
    for (const DataTypePtr& type : string_types) {
        auto nested = ColumnString::create();
        for (std::string_view value : strings) {
            nested->insert_data(value.data(), value.size());
        }
        expect_typed_round_trip(*typed(std::move(nested), type, NULLS));
    }

    expect_typed_round_trip(*typed(fixed_column<ColumnIPv4, IPv4>({0x01020304U, 0xAABBCCDDU, 0U}),
                                   std::make_shared<DataTypeIPv4>(), NULLS));
    const IPv6 ipv6_a = (static_cast<IPv6>(0x0123456789ABCDEFULL) << 64) |
                        static_cast<IPv6>(0xFEDCBA9876543210ULL);
    const IPv6 ipv6_b = (static_cast<IPv6>(0xFFEEDDCCBBAA9988ULL) << 64) |
                        static_cast<IPv6>(0x7766554433221100ULL);
    expect_typed_round_trip(*typed(fixed_column<ColumnIPv6, IPv6>({ipv6_a, ipv6_b, 0}),
                                   std::make_shared<DataTypeIPv6>(), NULLS));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EmptyAndConstColumnsPreserveWholeColumnState) {
    auto empty_encoded = ColumnVariantV2::create();
    const auto decoded_empty_encoded = round_trip(*empty_encoded);
    EXPECT_FALSE(decoded_empty_encoded->is_typed());
    EXPECT_EQ(decoded_empty_encoded->size(), 0);

    const std::array<uint8_t, 0> NO_NULLS {};
    auto empty_strings = typed(ColumnString::create(),
                               std::make_shared<DataTypeString>(17, TYPE_VARCHAR), NO_NULLS);
    const auto decoded_empty_typed = round_trip(*empty_strings);
    ASSERT_TRUE(decoded_empty_typed->is_typed());
    EXPECT_EQ(decoded_empty_typed->size(), 0);
    expect_type_identity(empty_strings->typed_type(), decoded_empty_typed->typed_type());

    auto empty_fixed = typed(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), NO_NULLS);
    const auto decoded_empty_fixed = round_trip(*empty_fixed);
    ASSERT_TRUE(decoded_empty_fixed->is_typed());
    EXPECT_EQ(decoded_empty_fixed->size(), 0);
    expect_type_identity(empty_fixed->typed_type(), decoded_empty_fixed->typed_type());

    auto one_encoded = ColumnVariantV2::create();
    insert_encoded_field(*one_encoded, encode_json(R"({"const":true})"));
    ColumnPtr one_encoded_ptr = std::move(one_encoded);
    const auto& one_encoded_ref = assert_cast<const ColumnVariantV2&>(*one_encoded_ptr);
    ColumnPtr encoded_const = ColumnConst::create(one_encoded_ptr, 4);
    const auto decoded_encoded_const = round_trip(*encoded_const);
    ASSERT_TRUE(decoded_encoded_const.is_constant());
    ASSERT_EQ(decoded_encoded_const.column->size(), 4);
    ASSERT_FALSE(decoded_encoded_const->is_typed());
    ASSERT_EQ(decoded_encoded_const->size(), 1);
    EXPECT_EQ(VariantField::from_ref(decoded_encoded_const->read_view().value_at(0)).bytes(),
              VariantField::from_ref(one_encoded_ref.read_view().value_at(0)).bytes());

    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    auto one_typed = typed(fixed_column<ColumnInt32, Int32>({0x12345678}),
                           std::make_shared<DataTypeInt32>(), NOT_NULL);
    ColumnPtr one_typed_ptr = std::move(one_typed);
    const auto& one_typed_ref = assert_cast<const ColumnVariantV2&>(*one_typed_ptr);
    ColumnPtr typed_const = ColumnConst::create(one_typed_ptr, 5);
    const auto decoded_typed_const = round_trip(*typed_const);
    ASSERT_TRUE(decoded_typed_const.is_constant());
    ASSERT_EQ(decoded_typed_const.column->size(), 5);
    ASSERT_TRUE(decoded_typed_const->is_typed());
    ASSERT_EQ(decoded_typed_const->size(), 1);
    const auto& decoded_nullable =
            assert_cast<const ColumnNullable&>(decoded_typed_const->typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(decoded_nullable.get_nested_column()).get_data()[0],
              0x12345678);

    ColumnPtr encoded_const_zero = ColumnConst::create(one_encoded_ptr, 0);
    const auto decoded_encoded_const_zero = round_trip(*encoded_const_zero);
    EXPECT_TRUE(decoded_encoded_const_zero.is_constant());
    EXPECT_EQ(decoded_encoded_const_zero.column->size(), 0);
    EXPECT_FALSE(decoded_encoded_const_zero->is_typed());
    EXPECT_EQ(decoded_encoded_const_zero->size(), 1);
    ColumnPtr typed_const_zero = ColumnConst::create(one_typed_ptr, 0);
    const auto decoded_typed_const_zero = round_trip(*typed_const_zero);
    EXPECT_TRUE(decoded_typed_const_zero.is_constant());
    EXPECT_EQ(decoded_typed_const_zero.column->size(), 0);
    EXPECT_TRUE(decoded_typed_const_zero->is_typed());
    EXPECT_EQ(decoded_typed_const_zero->size(), 1);
    expect_type_identity(one_typed_ref.typed_type(), decoded_typed_const_zero->typed_type());
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedAndTypedDecodeToCanonicalEquality) {
    auto encoded_source = encoded(R"(42)");
    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    auto typed_source = typed(fixed_column<ColumnInt32, Int32>({42}),
                              std::make_shared<DataTypeInt32>(), NOT_NULL);
    const auto decoded_encoded = round_trip(*encoded_source);
    auto decoded_typed = round_trip(*typed_source);
    decoded_typed->ensure_encoded();
    ASSERT_FALSE(decoded_typed->is_typed());
    EXPECT_TRUE(canonical_equals(decoded_encoded->read_view().value_at(0),
                                 decoded_typed->read_view().value_at(0)));
}

} // namespace doris
