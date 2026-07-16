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
#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "core/custom_allocator.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/column_reader.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "util/coding.h"

namespace doris::format::parquet::native {
namespace {

class RejectFixedConsumer final : public ParquetFixedValueConsumer {
public:
    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        return Status::InternalError("Unexpected fixed dictionary");
    }
};

class CaptureBinaryConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef* values, size_t num_values) override {
        refs.assign(values, values + num_values);
        return Status::OK();
    }

    std::vector<StringRef> refs;
};

class CaptureFixedConsumer final : public ParquetFixedValueConsumer {
public:
    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        if (width == 0) {
            width = value_width;
        }
        DORIS_CHECK_EQ(width, value_width);
        bytes.insert(bytes.end(), values, values + num_values * value_width);
        return Status::OK();
    }

    template <typename T>
    std::vector<T> values() const {
        DORIS_CHECK_EQ(width, sizeof(T));
        DORIS_CHECK_EQ(bytes.size() % sizeof(T), 0);
        std::vector<T> result(bytes.size() / sizeof(T));
        memcpy(result.data(), bytes.data(), bytes.size());
        return result;
    }

    size_t width = 0;
    std::vector<uint8_t> bytes;
};

std::shared_ptr<::parquet::ColumnDescriptor> descriptor(::parquet::Type::type physical_type) {
    auto node = ::parquet::schema::PrimitiveNode::Make("value", ::parquet::Repetition::REQUIRED,
                                                       physical_type);
    return std::make_shared<::parquet::ColumnDescriptor>(node, 0, 0);
}

DorisUniqueBufferPtr<uint8_t> make_byte_array_dictionary(const std::vector<std::string>& values,
                                                         int32_t* length) {
    size_t total_size = 0;
    for (const auto& value : values) {
        total_size += sizeof(uint32_t) + value.size();
    }
    *length = static_cast<int32_t>(total_size);
    auto dictionary = make_unique_buffer<uint8_t>(total_size);
    size_t offset = 0;
    for (const auto& value : values) {
        encode_fixed32_le(dictionary.get() + offset, static_cast<uint32_t>(value.size()));
        offset += sizeof(uint32_t);
        memcpy(dictionary.get() + offset, value.data(), value.size());
        offset += value.size();
    }
    return dictionary;
}

std::vector<uint8_t> encode_plain_byte_arrays(const std::vector<std::string>& values) {
    size_t total_size = 0;
    for (const auto& value : values) {
        total_size += sizeof(uint32_t) + value.size();
    }
    std::vector<uint8_t> encoded(total_size);
    size_t offset = 0;
    for (const auto& value : values) {
        encode_fixed32_le(encoded.data() + offset, static_cast<uint32_t>(value.size()));
        offset += sizeof(uint32_t);
        memcpy(encoded.data() + offset, value.data(), value.size());
        offset += value.size();
    }
    return encoded;
}

TEST(ParquetV2NativeDecoderTest, ByteArrayDictionaryReferencesOwnedPageAndValidatesIndices) {
    int32_t dictionary_length = 0;
    auto dictionary = make_byte_array_dictionary({"alpha", "beta"}, &dictionary_length);
    const uint8_t* dictionary_address = dictionary.get();
    ByteArrayDictDecoder decoder;

    ASSERT_TRUE(decoder.set_dict(dictionary, dictionary_length, 2).ok());
    EXPECT_EQ(dictionary.get(), nullptr);

    RejectFixedConsumer fixed_consumer;
    CaptureBinaryConsumer binary_consumer;
    ASSERT_TRUE(decoder.decode_dictionary(fixed_consumer, binary_consumer).ok());
    ASSERT_EQ(binary_consumer.refs.size(), 2);
    EXPECT_EQ(binary_consumer.refs[0].to_string_view(), "alpha");
    EXPECT_EQ(binary_consumer.refs[1].to_string_view(), "beta");
    EXPECT_EQ(binary_consumer.refs[0].data,
              reinterpret_cast<const char*>(dictionary_address + sizeof(uint32_t)));

    // bit width 1, an RLE run of three values (header = 3 << 1), dictionary id 1.
    char valid_indices[] = {1, 6, 1};
    Slice valid_slice(valid_indices, sizeof(valid_indices));
    ASSERT_TRUE(decoder.set_data(&valid_slice).ok());
    std::vector<uint32_t> decoded_indices;
    ASSERT_TRUE(decoder.decode_dictionary_indices(3, &decoded_indices).ok());
    EXPECT_EQ(decoded_indices, std::vector<uint32_t>({1, 1, 1}));

    // bit width 2, one RLE value with dictionary id 3. Skipping still validates the encoded id so
    // filter selection cannot hide a corrupt dictionary stream.
    char invalid_indices[] = {2, 2, 3};
    Slice invalid_slice(invalid_indices, sizeof(invalid_indices));
    ASSERT_TRUE(decoder.set_data(&invalid_slice).ok());
    ParquetDecodeSource& source = decoder;
    EXPECT_TRUE(source.skip_values(1).is<ErrorCode::CORRUPTION>());

    // Sparse decode must validate filtered dictionary ids too. Predicate selection must never
    // turn a corrupt page into a successful read merely because the bad row was not selected.
    ASSERT_TRUE(decoder.set_data(&invalid_slice).ok());
    ParquetSelection filtered_corrupt {.total_values = 1, .selected_values = 0, .ranges = {}};
    EXPECT_TRUE(decoder.decode_selected_dictionary_indices(filtered_corrupt, &decoded_indices)
                        .is<ErrorCode::CORRUPTION>());

    Slice empty_indices;
    EXPECT_TRUE(decoder.set_data(&empty_indices).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, SparsePlainAndBooleanDecodeOnceAndPreserveCursor) {
    const ParquetSelection selection {
            .total_values = 7,
            .selected_values = 3,
            .ranges = {{.first = 1, .count = 2}, {.first = 6, .count = 1}}};

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    std::vector<int32_t> integers {10, 11, 12, 13, 14, 15, 16, 17};
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(), std::vector<int32_t>({11, 12, 16}));
    CaptureFixedConsumer trailing_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_integer).ok());
    EXPECT_EQ(trailing_integer.values<int32_t>(), std::vector<int32_t>({17}));

    const std::vector<std::string> strings {"zero", "one",  "two", "three",
                                            "four", "five", "six", "seven"};
    auto encoded_strings = encode_plain_byte_arrays(strings);
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, decoder)
                        .ok());
    Slice string_slice(encoded_strings.data(), encoded_strings.size());
    ASSERT_TRUE(decoder->set_data(&string_slice).ok());
    CaptureBinaryConsumer selected_strings;
    ASSERT_TRUE(decoder->decode_selected_binary_values(selection, selected_strings).ok());
    ASSERT_EQ(selected_strings.refs.size(), 3);
    EXPECT_EQ(selected_strings.refs[0].to_string_view(), "one");
    EXPECT_EQ(selected_strings.refs[1].to_string_view(), "two");
    EXPECT_EQ(selected_strings.refs[2].to_string_view(), "six");
    CaptureBinaryConsumer trailing_string;
    ASSERT_TRUE(decoder->decode_binary_values(1, trailing_string).ok());
    ASSERT_EQ(trailing_string.refs.size(), 1);
    EXPECT_EQ(trailing_string.refs[0].to_string_view(), "seven");

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char booleans[] = {static_cast<char>(0b10001101)};
    Slice boolean_slice(booleans, sizeof(booleans));
    ASSERT_TRUE(decoder->set_data(&boolean_slice).ok());
    CaptureFixedConsumer selected_booleans;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_booleans).ok());
    EXPECT_EQ(selected_booleans.values<uint8_t>(), std::vector<uint8_t>({0, 1, 0}));
    CaptureFixedConsumer trailing_boolean;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_boolean).ok());
    EXPECT_EQ(trailing_boolean.values<uint8_t>(), std::vector<uint8_t>({1}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    char rle_booleans[] = {0x02, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0x8D)};
    Slice rle_boolean_slice(rle_booleans, sizeof(rle_booleans));
    ASSERT_TRUE(decoder->set_data(&rle_boolean_slice).ok());
    CaptureFixedConsumer selected_rle_booleans;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_rle_booleans).ok());
    EXPECT_EQ(selected_rle_booleans.values<uint8_t>(), std::vector<uint8_t>({0, 1, 0}));
    CaptureFixedConsumer trailing_rle_boolean;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_rle_boolean).ok());
    EXPECT_EQ(trailing_rle_boolean.values<uint8_t>(), std::vector<uint8_t>({1}));
}

TEST(ParquetV2NativeDecoderTest, PlainAndBooleanRleExposeRawValuesAndPreserveCursor) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    std::vector<int32_t> integers {11, 22, 33};
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer integer_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(2, integer_consumer).ok());
    EXPECT_EQ(integer_consumer.values<int32_t>(), std::vector<int32_t>({22, 33}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char plain_boolean[] = {static_cast<char>(0b10001101)};
    Slice plain_boolean_slice(plain_boolean, sizeof(plain_boolean));
    ASSERT_TRUE(decoder->set_data(&plain_boolean_slice).ok());
    CaptureFixedConsumer plain_boolean_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(8, plain_boolean_consumer).ok());
    EXPECT_EQ(plain_boolean_consumer.values<uint8_t>(),
              std::vector<uint8_t>({1, 0, 1, 1, 0, 0, 0, 1}));

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    char rle_boolean[] = {0x02, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0x8D)};
    Slice rle_boolean_slice(rle_boolean, sizeof(rle_boolean));
    ASSERT_TRUE(decoder->set_data(&rle_boolean_slice).ok());
    ASSERT_TRUE(decoder->skip_values(3).ok());
    CaptureFixedConsumer rle_boolean_consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(5, rle_boolean_consumer).ok());
    EXPECT_EQ(rle_boolean_consumer.values<uint8_t>(), std::vector<uint8_t>({1, 0, 0, 0, 1}));
}

TEST(ParquetV2NativeDecoderTest, DeltaEncodingsExposeValuesAfterSkip) {
    const std::vector<int32_t> integers {100, 101, 99, 1000};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto int_encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    int_encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto int_buffer = int_encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(int32_t));
    Slice int_slice(int_buffer->data(), int_buffer->size());
    ASSERT_TRUE(decoder->set_data(&int_slice).ok());
    CaptureFixedConsumer first_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, first_integer).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer remaining_integers;
    ASSERT_TRUE(decoder->decode_fixed_values(2, remaining_integers).ok());
    EXPECT_EQ(first_integer.values<int32_t>(), std::vector<int32_t>({100}));
    EXPECT_EQ(remaining_integers.values<int32_t>(), std::vector<int32_t>({99, 1000}));

    const std::vector<std::string> strings {"prefix-a", "prefix-b", "other", "other-tail"};
    std::vector<::parquet::ByteArray> byte_arrays;
    byte_arrays.reserve(strings.size());
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
        auto buffer = encoder->FlushValues();
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         encoding == ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 ? tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 : tparquet::Encoding::DELTA_BYTE_ARRAY,
                                         decoder)
                            .ok());
        Slice slice(buffer->data(), buffer->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        ASSERT_TRUE(decoder->skip_values(1).ok());
        CaptureBinaryConsumer consumer;
        ASSERT_TRUE(decoder->decode_binary_values(3, consumer).ok());
        ASSERT_EQ(consumer.refs.size(), 3);
        EXPECT_EQ(consumer.refs[0].to_string_view(), "prefix-b");
        EXPECT_EQ(consumer.refs[1].to_string_view(), "other");
        EXPECT_EQ(consumer.refs[2].to_string_view(), "other-tail");
    }
}

TEST(ParquetV2NativeDecoderTest, SparseStatefulEncodingsBatchDecodeAndCompact) {
    const ParquetSelection selection {
            .total_values = 3,
            .selected_values = 2,
            .ranges = {{.first = 0, .count = 1}, {.first = 2, .count = 1}}};
    const std::vector<int32_t> integers {100, 101, 99, 1000};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto int_encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    int_encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto int_buffer = int_encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(int32_t));
    Slice int_slice(int_buffer->data(), int_buffer->size());
    ASSERT_TRUE(decoder->set_data(&int_slice).ok());
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(), std::vector<int32_t>({100, 99}));
    CaptureFixedConsumer trailing_integer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_integer).ok());
    EXPECT_EQ(trailing_integer.values<int32_t>(), std::vector<int32_t>({1000}));

    const std::vector<std::string> strings {"prefix-a", "prefix-b", "other", "other-tail"};
    std::vector<::parquet::ByteArray> byte_arrays;
    byte_arrays.reserve(strings.size());
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
        auto buffer = encoder->FlushValues();
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         encoding == ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 ? tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
                                                 : tparquet::Encoding::DELTA_BYTE_ARRAY,
                                         decoder)
                            .ok());
        Slice slice(buffer->data(), buffer->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        CaptureBinaryConsumer selected_strings;
        ASSERT_TRUE(decoder->decode_selected_binary_values(selection, selected_strings).ok());
        ASSERT_EQ(selected_strings.refs.size(), 2);
        EXPECT_EQ(selected_strings.refs[0].to_string_view(), "prefix-a");
        EXPECT_EQ(selected_strings.refs[1].to_string_view(), "other");
        CaptureBinaryConsumer trailing_string;
        ASSERT_TRUE(decoder->decode_binary_values(1, trailing_string).ok());
        ASSERT_EQ(trailing_string.refs.size(), 1);
        EXPECT_EQ(trailing_string.refs[0].to_string_view(), "other-tail");
    }

    const std::vector<float> floats {1.0F, -2.5F, 3.25F, 9.5F};
    std::vector<uint8_t> encoded_floats(floats.size() * sizeof(float));
    for (size_t row = 0; row < floats.size(); ++row) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&floats[row]);
        for (size_t byte = 0; byte < sizeof(float); ++byte) {
            encoded_floats[byte * floats.size() + row] = bytes[byte];
        }
    }
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice float_slice(encoded_floats.data(), encoded_floats.size());
    ASSERT_TRUE(decoder->set_data(&float_slice).ok());
    CaptureFixedConsumer selected_floats;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_floats).ok());
    EXPECT_EQ(selected_floats.values<float>(), std::vector<float>({1.0F, 3.25F}));
    CaptureFixedConsumer trailing_float;
    ASSERT_TRUE(decoder->decode_fixed_values(1, trailing_float).ok());
    EXPECT_EQ(trailing_float.values<float>(), std::vector<float>({9.5F}));
}

TEST(ParquetV2NativeDecoderTest, ByteStreamSplitRestoresFixedWidthRows) {
    const std::vector<float> values {1.0F, -2.5F, 3.25F};
    std::vector<uint8_t> encoded(values.size() * sizeof(float));
    for (size_t row = 0; row < values.size(); ++row) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&values[row]);
        for (size_t byte = 0; byte < sizeof(float); ++byte) {
            encoded[byte * values.size() + row] = bytes[byte];
        }
    }

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    ASSERT_TRUE(decoder->skip_values(1).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(2, consumer).ok());
    EXPECT_EQ(consumer.values<float>(), std::vector<float>({-2.5F, 3.25F}));
}

TEST(ParquetV2NativeDecoderTest, OversizedNestedBatchScratchIsReleased) {
    ::doris::RowRanges row_ranges;
    tparquet::ColumnChunk chunk;
    ScalarColumnReader<true, false> reader(row_ranges, 1, chunk, nullptr, nullptr, nullptr);

    constexpr size_t max_retained_bytes = 64UL << 10;
    reader.reserve_batch_scratch_for_test(1UL << 16);
    const size_t oversized_bytes = reader.retained_batch_scratch_bytes_for_test();
    ASSERT_GT(oversized_bytes, max_retained_bytes);

    reader.release_batch_scratch(max_retained_bytes);
    const size_t released_bytes = reader.retained_batch_scratch_bytes_for_test();
    EXPECT_LT(released_bytes, oversized_bytes);
    EXPECT_LE(released_bytes, max_retained_bytes + sizeof(void*));
}

} // namespace
} // namespace doris::format::parquet::native
