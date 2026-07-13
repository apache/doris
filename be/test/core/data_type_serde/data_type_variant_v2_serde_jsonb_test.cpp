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
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr typed_int(int32_t value, bool is_null = false) {
    auto nested = ColumnInt32::create();
    nested->insert_value(value);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(is_null ? 1 : 0);
    return ColumnVariantV2::create_typed_from_cast(
            ColumnNullable::create(std::move(nested), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

ColumnPtr encoded_copy(const ColumnVariantV2& typed) {
    ColumnPtr copy = typed.clone();
    MutableColumnPtr mutable_copy = IColumn::mutate(std::move(copy));
    assert_cast<ColumnVariantV2&>(*mutable_copy).ensure_encoded();
    return mutable_copy;
}

std::string write_direct(const DataTypeSerDe& serde, const IColumn& column, size_t row,
                         int32_t col_id = 3) {
    JsonbWriter writer;
    EXPECT_TRUE(writer.writeStartObject());
    Arena arena;
    DataTypeSerDe::FormatOptions options;
    serde.write_one_cell_to_jsonb(column, writer, arena, col_id, row, options);
    EXPECT_TRUE(writer.writeEndObject());
    return {writer.getOutput()->getBuffer(), static_cast<size_t>(writer.getOutput()->getSize())};
}

const JsonbValue* first_value(const std::string& bytes) {
    const JsonbDocument* document = nullptr;
    const Status status =
            JsonbDocument::checkAndCreateDocument(bytes.data(), bytes.size(), &document);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok() || (*document)->begin() == (*document)->end()) {
        return nullptr;
    }
    return (*document)->begin()->value();
}

std::string binary_document(const JsonbValue* value) {
    EXPECT_NE(value, nullptr);
    EXPECT_TRUE(value != nullptr && value->isBinary());
    if (value == nullptr || !value->isBinary()) {
        return {};
    }
    const auto* binary = value->unpack<JsonbBinaryVal>();
    return {binary->getBlob(), binary->getBlobLen()};
}

std::string json_at(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row) {
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

std::string object_with_value(const std::function<void(JsonbWriter&)>& write_value) {
    JsonbWriter writer;
    EXPECT_TRUE(writer.writeStartObject());
    EXPECT_TRUE(writer.writeKey(JsonbKeyValue::keyid_type {1}));
    write_value(writer);
    EXPECT_TRUE(writer.writeEndObject());
    return {writer.getOutput()->getBuffer(), static_cast<size_t>(writer.getOutput()->getSize())};
}

ColumnVariantV2::MutablePtr trailing_value_column() {
    constexpr std::array<char, 3> metadata {static_cast<char>(0x11), 0, 0};
    constexpr std::array<uint32_t, 2> metadata_offsets {0, 3};
    constexpr std::array<uint32_t, 1> ids {0};
    constexpr std::array<char, 2> values {0, 0};
    constexpr std::array<uint32_t, 2> value_offsets {0, 2};
    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows({.metadata_bytes = {metadata.data(), metadata.size()},
                                 .metadata_offsets = metadata_offsets,
                                 .meta_ids = ids,
                                 .value_bytes = {values.data(), values.size()},
                                 .value_offsets = value_offsets});
    return result;
}

} // namespace

TEST(DataTypeVariantV2SerdeJsonbTest, EncodedAndTypedWriteIdenticalBinaryDocumentsAndRoundTrip) {
    DataTypeVariantV2SerDe serde;
    auto typed = typed_int(42);
    ColumnPtr encoded = encoded_copy(*typed);
    const std::string typed_row = write_direct(serde, *typed, 0);
    const std::string encoded_row = write_direct(serde, *encoded, 0);
    const std::string typed_document = binary_document(first_value(typed_row));
    const std::string encoded_document = binary_document(first_value(encoded_row));
    EXPECT_EQ(typed_document, encoded_document);
    EXPECT_TRUE(typed->is_typed());

    auto typed_round_trip = ColumnVariantV2::create();
    serde.read_one_cell_from_jsonb(*typed_round_trip, first_value(typed_row));
    auto encoded_round_trip = ColumnVariantV2::create();
    serde.read_one_cell_from_jsonb(*encoded_round_trip, first_value(encoded_row));
    EXPECT_EQ(json_at(serde, *typed_round_trip, 0), "42");
    EXPECT_EQ(json_at(serde, *encoded_round_trip, 0), "42");
}

TEST(DataTypeVariantV2SerdeJsonbTest, PreKeyedAndNullableWriterShapesBothSucceed) {
    auto source = typed_int(7);
    DataTypeVariantV2SerDe serde;
    ColumnPtr constant = ColumnConst::create(source->get_ptr(), 3);
    const std::string constant_bytes = write_direct(serde, *constant, 2, 8);
    ASSERT_NE(first_value(constant_bytes), nullptr);
    EXPECT_TRUE(first_value(constant_bytes)->isBinary());

    JsonbWriter pre_keyed;
    ASSERT_TRUE(pre_keyed.writeStartObject());
    ASSERT_TRUE(pre_keyed.writeKey(JsonbKeyValue::keyid_type {9}));
    Arena arena;
    DataTypeSerDe::FormatOptions options;
    EXPECT_NO_THROW(serde.write_one_cell_to_jsonb(*source, pre_keyed, arena, 9, 0, options));
    ASSERT_TRUE(pre_keyed.writeEndObject());
    const std::string pre_keyed_bytes(pre_keyed.getOutput()->getBuffer(),
                                      pre_keyed.getOutput()->getSize());
    ASSERT_NE(first_value(pre_keyed_bytes), nullptr);
    EXPECT_TRUE(first_value(pre_keyed_bytes)->isBinary());

    MutableColumnPtr nested = typed_int(7);
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(0);
    auto outer = ColumnNullable::create(std::move(nested), std::move(outer_nulls));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    const std::string nullable_bytes = write_direct(nullable_serde, *outer, 0, 11);
    const JsonbValue* nullable_value = first_value(nullable_bytes);
    ASSERT_NE(nullable_value, nullptr);
    EXPECT_TRUE(nullable_value->isBinary());

    MutableColumnPtr round_trip_nested = ColumnVariantV2::create();
    auto round_trip_nulls = ColumnUInt8::create();
    auto round_trip =
            ColumnNullable::create(std::move(round_trip_nested), std::move(round_trip_nulls));
    nullable_serde.read_one_cell_from_jsonb(*round_trip, nullable_value);
    ASSERT_EQ(round_trip->size(), 1);
    EXPECT_FALSE(round_trip->is_null_at(0));
    EXPECT_EQ(json_at(serde, round_trip->get_nested_column(), 0), "7");
}

TEST(DataTypeVariantV2SerdeJsonbTest, OuterSqlNullAndVariantNullUseDifferentRowStoreTags) {
    DataTypeVariantV2SerDe serde;
    auto variant_null = typed_int(0, true);
    const std::string variant_bytes = write_direct(serde, *variant_null, 0);
    EXPECT_TRUE(first_value(variant_bytes)->isBinary());

    MutableColumnPtr nested = ColumnVariantV2::create();
    nested->insert_default();
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(1);
    auto outer = ColumnNullable::create(std::move(nested), std::move(outer_nulls));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    const std::string outer_bytes = write_direct(nullable_serde, *outer, 0);
    const JsonbValue* outer_value = first_value(outer_bytes);
    ASSERT_NE(outer_value, nullptr);
    EXPECT_TRUE(outer_value->isNull());

    MutableColumnPtr round_trip_nested = ColumnVariantV2::create();
    auto round_trip_nulls = ColumnUInt8::create();
    auto round_trip =
            ColumnNullable::create(std::move(round_trip_nested), std::move(round_trip_nulls));
    nullable_serde.read_one_cell_from_jsonb(*round_trip, outer_value);
    ASSERT_EQ(round_trip->size(), 1);
    EXPECT_TRUE(round_trip->is_null_at(0));
    EXPECT_EQ(round_trip->get_nested_column().size(), 1);
}

// GTest writer and throw macros inflate this two-case malformed-input matrix.
TEST(DataTypeVariantV2SerdeJsonbTest, // NOLINT(readability-function-cognitive-complexity)
     StringFallbackAndMalformedBinaryAreRejectedAtomically) {
    DataTypeVariantV2SerDe serde;
    auto destination = ColumnVariantV2::create();
    destination->insert_default();
    const size_t before = destination->size();

    const std::string string_bytes = object_with_value([](JsonbWriter& writer) {
        ASSERT_TRUE(writer.writeStartString());
        ASSERT_TRUE(writer.writeString("legacy", 6));
        ASSERT_TRUE(writer.writeEndString());
    });
    EXPECT_THROW(serde.read_one_cell_from_jsonb(*destination, first_value(string_bytes)),
                 Exception);
    EXPECT_EQ(destination->size(), before);

    const std::string malformed_bytes = object_with_value([](JsonbWriter& writer) {
        ASSERT_TRUE(writer.writeStartBinary());
        const std::array<char, 2> malformed {static_cast<char>(JSONB_VER), static_cast<char>(0x7F)};
        ASSERT_TRUE(writer.writeBinary(malformed.data(), malformed.size()));
        ASSERT_TRUE(writer.writeEndBinary());
    });
    EXPECT_THROW(serde.read_one_cell_from_jsonb(*destination, first_value(malformed_bytes)),
                 Exception);
    EXPECT_EQ(destination->size(), before);
    EXPECT_EQ(json_at(serde, *destination, 0), "{}");
}

TEST(DataTypeVariantV2SerdeJsonbTest, BadEncodedInputDoesNotTouchResultWriter) {
    DataTypeVariantV2SerDe serde;
    auto bad = trailing_value_column();
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    const size_t before = writer.getOutput()->getSize();
    Arena arena;
    DataTypeSerDe::FormatOptions options;
    EXPECT_THROW(serde.write_one_cell_to_jsonb(*bad, writer, arena, 1, 0, options), Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), before);

    auto valid = typed_int(1);
    EXPECT_THROW(serde.write_one_cell_to_jsonb(*valid, writer, arena, 1,
                                               std::numeric_limits<int64_t>::max(), options),
                 Exception);
    EXPECT_EQ(writer.getOutput()->getSize(), before);
}

} // namespace doris
