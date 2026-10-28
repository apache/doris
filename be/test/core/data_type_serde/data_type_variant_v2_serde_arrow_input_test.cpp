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

// Arrow Stream Load sends a Variant column as JSON text or as Parquet Variant binary in
// struct<metadata, value>, whose metadata child may be dictionary encoded. Every shape must load
// the same Variant for the same document.

#include <arrow/api.h>
#include <arrow/buffer_builder.h>
#include <arrow/extension/parquet_variant.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace {

const std::vector<std::string>& documents() {
    static const std::vector<std::string> values = {
            R"({"b":1,"a":{"z":[1,2,{"k":null}],"y":"s"},"c":[true,false,null]})",
            R"({"a":{"y":"t"},"d":1.5})",
            "[]",
            "42",
            R"("text")",
            "null",
    };
    return values;
}

std::string json_at(const IColumn& column, size_t row) {
    DataTypeVariantV2SerDe serde;
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(column, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    writer.commit();
    return output->get_data_at(0).to_string();
}

std::vector<std::string> json_rows(const IColumn& column) {
    std::vector<std::string> rows;
    for (size_t row = 0; row < column.size(); ++row) {
        rows.push_back(json_at(column, row));
    }
    return rows;
}

Status read_arrow(IColumn& column, const arrow::Array& array, int64_t start, int64_t end) {
    return DataTypeVariantV2SerDe().read_column_from_arrow(column, &array, start, end,
                                                           cctz::utc_time_zone());
}

Status read_arrow(IColumn& column, const arrow::Array& array) {
    return read_arrow(column, array, 0, array.length());
}

// One encoder batch, so every row shares one metadata dictionary, as a producer that encodes a
// record batch at a time sends it.
ColumnVariantV2::MutablePtr encode_batch(const std::vector<std::string>& values) {
    JsonStringToVariantEncoder encoder;
    for (const auto& value : values) {
        encoder.add_json({value.data(), value.size()});
    }
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    return column;
}

// One encoder batch per row, so every row has its own metadata dictionary.
ColumnVariantV2::MutablePtr encode_rows(const std::vector<std::string>& values) {
    auto column = ColumnVariantV2::create();
    for (const auto& value : values) {
        JsonStringToVariantEncoder encoder;
        encoder.add_json({value.data(), value.size()});
        column->insert_encoded_batch(encoder.finish_batch());
    }
    return column;
}

std::string_view metadata_at(const ColumnVariantV2& column, size_t row) {
    const auto ref = column.get_value_ref(row);
    return {ref.metadata.data, ref.metadata.size};
}

std::string_view value_at(const ColumnVariantV2& column, size_t row) {
    const auto ref = column.get_value_ref(row);
    return {ref.value.data, ref.value.size};
}

template <typename Builder>
std::shared_ptr<arrow::Array> finish(Builder& builder) {
    return builder.Finish().ValueOrDie();
}

template <typename Builder>
std::shared_ptr<arrow::Array> binary_array(const std::vector<std::string_view>& values,
                                           const std::vector<bool>& nulls = {}) {
    Builder builder;
    for (size_t row = 0; row < values.size(); ++row) {
        if (!nulls.empty() && nulls[row]) {
            EXPECT_TRUE(builder.AppendNull().ok());
        } else {
            EXPECT_TRUE(builder.Append(values[row]).ok());
        }
    }
    return finish(builder);
}

std::shared_ptr<arrow::Array> text_array(const std::shared_ptr<arrow::DataType>& type,
                                         const std::vector<std::string_view>& values,
                                         const std::vector<bool>& nulls = {}) {
    switch (type->id()) {
    case arrow::Type::STRING:
        return binary_array<arrow::StringBuilder>(values, nulls);
    case arrow::Type::LARGE_STRING:
        return binary_array<arrow::LargeStringBuilder>(values, nulls);
    case arrow::Type::BINARY:
        return binary_array<arrow::BinaryBuilder>(values, nulls);
    default:
        return binary_array<arrow::LargeBinaryBuilder>(values, nulls);
    }
}

std::shared_ptr<arrow::Array> make_struct(const std::shared_ptr<arrow::Array>& metadata,
                                          const std::shared_ptr<arrow::Array>& value,
                                          bool value_first = false,
                                          std::shared_ptr<arrow::Buffer> validity = nullptr) {
    auto metadata_field = arrow::field("metadata", metadata->type(), false);
    auto value_field = arrow::field("value", value->type(), false);
    if (value_first) {
        return arrow::StructArray::Make({value, metadata}, {value_field, metadata_field},
                                        std::move(validity))
                .ValueOrDie();
    }
    return arrow::StructArray::Make({metadata, value}, {metadata_field, value_field},
                                    std::move(validity))
            .ValueOrDie();
}

// struct<metadata, value> holding the rows of an encoded column, each with its own metadata cell.
std::shared_ptr<arrow::Array> binary_struct(const ColumnVariantV2& column, bool large = false,
                                            bool value_first = false) {
    std::vector<std::string_view> metadata;
    std::vector<std::string_view> values;
    for (size_t row = 0; row < column.size(); ++row) {
        metadata.push_back(metadata_at(column, row));
        values.push_back(value_at(column, row));
    }
    auto type = large ? arrow::large_binary() : arrow::binary();
    return make_struct(text_array(type, metadata), text_array(type, values), value_first);
}

std::shared_ptr<arrow::Buffer> validity(const std::vector<bool>& nulls) {
    arrow::TypedBufferBuilder<bool> builder;
    for (bool is_null : nulls) {
        EXPECT_TRUE(builder.Append(!is_null).ok());
    }
    return builder.Finish().ValueOrDie();
}

template <typename IndexType>
std::shared_ptr<arrow::Array> dictionary_metadata(const std::vector<std::string_view>& entries,
                                                  const std::vector<int64_t>& indices,
                                                  const std::vector<bool>& nulls = {}) {
    using IndexBuilder = typename arrow::TypeTraits<IndexType>::BuilderType;
    using CType = typename IndexType::c_type;
    IndexBuilder builder;
    for (size_t row = 0; row < indices.size(); ++row) {
        if (!nulls.empty() && nulls[row]) {
            EXPECT_TRUE(builder.AppendNull().ok());
        } else {
            EXPECT_TRUE(builder.Append(static_cast<CType>(indices[row])).ok());
        }
    }
    auto dictionary = binary_array<arrow::BinaryBuilder>(entries);
    auto type = arrow::dictionary(arrow::TypeTraits<IndexType>::type_singleton(), arrow::binary());
    // The constructor skips FromArrays' index bounds check, so a test can send an out-of-range
    // index as a client could.
    return std::make_shared<arrow::DictionaryArray>(type, finish(builder), dictionary);
}

std::vector<std::string_view> values_of(const ColumnVariantV2& column) {
    std::vector<std::string_view> values;
    for (size_t row = 0; row < column.size(); ++row) {
        values.push_back(value_at(column, row));
    }
    return values;
}

} // namespace

TEST(DataTypeVariantV2SerdeArrowInputTest, JsonTextLoadsLikeJsonParse) {
    const auto expected = json_rows(*encode_rows(documents()));
    std::vector<std::string_view> texts(documents().begin(), documents().end());
    for (const auto& type :
         {arrow::utf8(), arrow::large_utf8(), arrow::binary(), arrow::large_binary()}) {
        auto column = ColumnVariantV2::create();
        auto status = read_arrow(*column, *text_array(type, texts));
        ASSERT_TRUE(status.ok()) << type->ToString() << ": " << status;
        EXPECT_EQ(json_rows(*column), expected) << type->ToString();
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, JsonTextNullRowIsVariantNull) {
    auto column = ColumnVariantV2::create();
    auto array = text_array(arrow::utf8(), {R"({"a":1})", "", "7"}, {false, true, false});
    ASSERT_TRUE(read_arrow(*column, *array).ok());
    EXPECT_EQ(json_rows(*column), (std::vector<std::string> {R"({"a":1})", "null", "7"}));
}

TEST(DataTypeVariantV2SerdeArrowInputTest, JsonTextParsesLikeAJsonLoad) {
    // A JSON load reads a Variant column through deserialize_column_from_json_vector: text that is
    // not JSON stays a string, and keys longer than parse_to_variant's limit are kept.
    const std::string long_key_json = R"({")" + std::string(300, 'k') + R"(":1})";
    const std::vector<std::string> texts = {R"({"b":2})", "not-json", long_key_json,
                                            "18446744073709551616"};
    auto expected = ColumnVariantV2::create();
    std::vector<Slice> slices;
    for (const auto& text : texts) {
        slices.emplace_back(text.data(), text.size());
    }
    uint64_t deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(
            DataTypeVariantV2SerDe()
                    .deserialize_column_from_json_vector(*expected, slices, &deserialized, options)
                    .ok());

    auto column = ColumnVariantV2::create();
    auto array = text_array(arrow::utf8(), {texts[0], texts[1], texts[2], texts[3]});
    ASSERT_TRUE(read_arrow(*column, *array).ok());
    EXPECT_EQ(json_rows(*column), json_rows(*expected));
    EXPECT_EQ(json_at(*column, 1), R"("not-json")");
}

TEST(DataTypeVariantV2SerdeArrowInputTest, BinaryStructShapesMatchJsonText) {
    const auto expected = json_rows(*encode_rows(documents()));
    for (bool shared : {true, false}) {
        auto encoded = shared ? encode_batch(documents()) : encode_rows(documents());
        for (bool large : {false, true}) {
            for (bool value_first : {false, true}) {
                auto column = ColumnVariantV2::create();
                auto status = read_arrow(*column, *binary_struct(*encoded, large, value_first));
                ASSERT_TRUE(status.ok()) << status;
                EXPECT_EQ(json_rows(*column), expected) << "shared=" << shared << " large=" << large
                                                        << " value_first=" << value_first;
            }
        }
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, SharedMetadataIsStoredOnce) {
    auto encoded = encode_batch(documents());
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *binary_struct(*encoded)).ok());
    for (size_t row = 1; row < column->size(); ++row) {
        EXPECT_EQ(column->get_value_ref(row).metadata.data, column->get_value_ref(0).metadata.data);
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, ParquetVariantExtensionIsUnwrapped) {
    auto encoded = encode_batch(documents());
    auto storage = binary_struct(*encoded);
    auto extension_type = arrow::extension::variant(storage->type());
    auto array = arrow::ExtensionType::WrapArray(extension_type, storage);
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *array).ok());
    EXPECT_EQ(json_rows(*column), json_rows(*encoded));
}

TEST(DataTypeVariantV2SerdeArrowInputTest, NullStructRowsAreVariantNull) {
    auto encoded = encode_batch({R"({"a":1})", R"({"a":2})", R"({"a":3})"});
    auto plain = binary_struct(*encoded);
    const auto& storage = static_cast<const arrow::StructArray&>(*plain);
    // The hidden child row of a NULL struct row keeps its bytes; the reader must not load them.
    auto with_null =
            make_struct(storage.field(0), storage.field(1), false, validity({false, true, false}));
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *with_null).ok());
    EXPECT_EQ(json_rows(*column), (std::vector<std::string> {R"({"a":1})", "null", R"({"a":3})"}));

    auto dictionary = make_struct(
            dictionary_metadata<arrow::Int32Type>({metadata_at(*encoded, 0)}, {0, 0, 0}),
            storage.field(1), false, validity({true, false, true}));
    column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *dictionary).ok());
    EXPECT_EQ(json_rows(*column), (std::vector<std::string> {"null", R"({"a":2})", "null"}));
}

TEST(DataTypeVariantV2SerdeArrowInputTest, DictionaryMetadataWithOneEntry) {
    auto encoded = encode_batch(documents());
    const auto expected = json_rows(*encoded);
    const std::vector<int64_t> indices(encoded->size(), 0);
    auto values = binary_array<arrow::BinaryBuilder>(values_of(*encoded));
    for (const auto& metadata :
         {dictionary_metadata<arrow::Int8Type>({metadata_at(*encoded, 0)}, indices),
          dictionary_metadata<arrow::UInt64Type>({metadata_at(*encoded, 0)}, indices)}) {
        auto column = ColumnVariantV2::create();
        ASSERT_TRUE(read_arrow(*column, *make_struct(metadata, values)).ok());
        EXPECT_EQ(json_rows(*column), expected);
        for (size_t row = 1; row < column->size(); ++row) {
            EXPECT_EQ(column->get_value_ref(row).metadata.data,
                      column->get_value_ref(0).metadata.data);
        }
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, DictionaryMetadataWithSeveralEntries) {
    auto first = encode_batch({R"({"a":1})", R"({"a":{"b":2}})"});
    auto second = encode_batch({R"({"x":"y"})", "[1]"});
    auto values = binary_array<arrow::BinaryBuilder>(
            {value_at(*first, 0), value_at(*second, 0), value_at(*first, 1), value_at(*second, 1)});
    auto metadata = dictionary_metadata<arrow::Int16Type>(
            {metadata_at(*first, 0), metadata_at(*second, 0)}, {0, 1, 0, 1});
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *make_struct(metadata, values)).ok());
    EXPECT_EQ(json_rows(*column),
              (std::vector<std::string> {R"({"a":1})", R"({"x":"y"})", R"({"a":{"b":2}})", "[1]"}));
}

TEST(DataTypeVariantV2SerdeArrowInputTest, SlicedArraysAndRangesUseLogicalRows) {
    auto encoded = encode_batch(documents());
    const auto expected = json_rows(*encoded);
    std::vector<std::string_view> texts(documents().begin(), documents().end());
    const std::vector<int64_t> indices(encoded->size(), 0);
    auto values = binary_array<arrow::BinaryBuilder>(values_of(*encoded));
    for (const auto& array :
         {text_array(arrow::utf8(), texts), binary_struct(*encoded),
          make_struct(dictionary_metadata<arrow::Int32Type>({metadata_at(*encoded, 0)}, indices),
                      values)}) {
        auto column = ColumnVariantV2::create();
        ASSERT_TRUE(read_arrow(*column, *array, 1, 4).ok()) << array->type()->ToString();
        ASSERT_TRUE(read_arrow(*column, *array->Slice(2), 1, 3).ok());
        EXPECT_EQ(json_rows(*column),
                  (std::vector<std::string> {expected[1], expected[2], expected[3], expected[3],
                                             expected[4]}))
                << array->type()->ToString();
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, InvalidRowsFailWithoutAppending) {
    auto encoded = encode_batch({R"({"a":1})", R"({"a":2})"});
    auto column = encode_batch({R"({"sentinel":1})"});
    const auto values = values_of(*encoded);
    auto value_array = binary_array<arrow::BinaryBuilder>(values);
    const std::string_view metadata = metadata_at(*encoded, 0);

    // Malformed value bytes in the second row.
    auto bad_value = make_struct(binary_array<arrow::BinaryBuilder>({metadata, metadata}),
                                 binary_array<arrow::BinaryBuilder>({values[0], "\xff"}));
    // A non-NULL row whose metadata child is NULL.
    auto null_child = make_struct(
            binary_array<arrow::BinaryBuilder>({metadata, metadata}, {false, true}), value_array);
    // A dictionary index past the dictionary, and a NULL index on a non-NULL row.
    auto bad_index =
            make_struct(dictionary_metadata<arrow::Int32Type>({metadata}, {0, 1}), value_array);
    auto null_index = make_struct(
            dictionary_metadata<arrow::Int32Type>({metadata}, {0, 0}, {false, true}), value_array);
    // A dictionary entry that is not Variant metadata. The second row's value is a scalar, which
    // references no key, so only the metadata validation can reject it.
    auto scalar = encode_batch({"1"});
    auto bad_entry =
            make_struct(dictionary_metadata<arrow::Int32Type>({metadata, "bad"}, {0, 1}),
                        binary_array<arrow::BinaryBuilder>({values[0], value_at(*scalar, 0)}));
    // Empty Variant bytes behind a dictionary: insert_encoded_rows asserts strictly increasing
    // offsets, so the reader must reject these itself rather than abort.
    auto empty_value = make_struct(dictionary_metadata<arrow::Int32Type>({metadata}, {0, 0}),
                                   binary_array<arrow::BinaryBuilder>({values[0], ""}));
    auto empty_entry =
            make_struct(dictionary_metadata<arrow::Int32Type>({""}, {0, 0}), value_array);
    auto empty_unused_entry = make_struct(
            dictionary_metadata<arrow::Int32Type>({metadata, "", metadata}, {0, 2}), value_array);
    arrow::Int32Builder index_builder;
    ASSERT_TRUE(index_builder.AppendValues({0, 1}).ok());
    auto null_entry = make_struct(
            std::make_shared<arrow::DictionaryArray>(
                    arrow::dictionary(arrow::int32(), arrow::binary()), finish(index_builder),
                    binary_array<arrow::BinaryBuilder>({metadata, metadata}, {false, true})),
            value_array);
    for (const auto& array : {bad_value, null_child, bad_index, null_index, bad_entry, empty_value,
                              empty_entry, empty_unused_entry, null_entry}) {
        EXPECT_FALSE(read_arrow(*column, *array).ok()) << array->ToString();
        ASSERT_EQ(column->size(), 1);
        EXPECT_EQ(json_at(*column, 0), R"({"sentinel":1})");
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, UnsupportedShapesAreInvalidArgument) {
    auto binary = binary_array<arrow::BinaryBuilder>({"x"});
    std::shared_ptr<arrow::Array> three_fields =
            arrow::StructArray::Make({binary, binary, binary},
                                     std::vector<std::string> {"metadata", "value", "x"})
                    .ValueOrDie();
    std::shared_ptr<arrow::Array> renamed =
            arrow::StructArray::Make({binary, binary}, std::vector<std::string> {"meta", "value"})
                    .ValueOrDie();
    arrow::Int32Builder builder;
    ASSERT_TRUE(builder.Append(0).ok());
    auto ints = finish(builder);
    auto string_dictionary = make_struct(
            arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                               ints, text_array(arrow::utf8(), {"x"}))
                    .ValueOrDie(),
            binary);
    for (const auto& array : {three_fields, renamed, string_dictionary, ints}) {
        auto column = ColumnVariantV2::create();
        EXPECT_EQ(read_arrow(*column, *array).code(), ErrorCode::INVALID_ARGUMENT)
                << array->type()->ToString();
        EXPECT_FALSE(DataTypeVariantV2SerDe::is_arrow_input_type(*array->type()));
    }
}

TEST(DataTypeVariantV2SerdeArrowInputTest, NullableColumnKeepsSqlNulls) {
    auto type = make_nullable(std::make_shared<DataTypeVariantV2>());
    auto column = type->create_column();
    auto array = text_array(arrow::utf8(), {R"({"a":1})", "", "null"}, {false, true, false});
    ASSERT_TRUE(type->get_serde()
                        ->read_column_from_arrow(*column, array.get(), 0, 3, cctz::utc_time_zone())
                        .ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_FALSE(nullable.is_null_at(2));
    EXPECT_EQ(json_at(nullable.get_nested_column(), 0), R"({"a":1})");
    EXPECT_EQ(json_at(nullable.get_nested_column(), 2), "null");
}

} // namespace doris
