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
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/column_reader.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "format_v2/parquet/reader/native/page_reader.h"
#include "io/fs/buffered_reader.h"
#include "util/coding.h"
#include "util/thrift_util.h"

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

const RowRanges& scripted_row_ranges() {
    static const RowRanges ranges;
    return ranges;
}

class ScriptedColumnReader final : public ColumnReader {
public:
    ScriptedColumnReader(size_t rows, size_t values, bool eof, std::vector<level_t> rep_levels,
                         std::vector<level_t> def_levels)
            : ColumnReader(scripted_row_ranges(), rows, nullptr, nullptr),
              _rows(rows),
              _values(values),
              _eof(eof),
              _rep_levels(std::move(rep_levels)),
              _def_levels(std::move(def_levels)) {}

    Status read_column_data(ColumnPtr& column, const DataTypePtr&,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>&, FilterMap&,
                            size_t, size_t* read_rows, bool* eof, bool, int64_t = -1) override {
        if (_used) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
        column = IColumn::mutate(std::move(column));
        column->assert_mutable()->insert_many_defaults(_values);
        *read_rows = _rows;
        *eof = _eof;
        _used = true;
        return Status::OK();
    }

    Status read_column_levels(FilterMap&, size_t, size_t* read_rows, bool* eof) override {
        *read_rows = _rows;
        *eof = _eof;
        return Status::OK();
    }

    const std::vector<level_t>& get_rep_level() const override { return _rep_levels; }
    const std::vector<level_t>& get_def_level() const override { return _def_levels; }
    ColumnStatistics column_statistics() override { return {}; }
    void close() override {}
    void release_batch_scratch(size_t) override {}
    void reset_filter_map_index() override {}

private:
    size_t _rows;
    size_t _values;
    bool _eof;
    bool _used = false;
    std::vector<level_t> _rep_levels;
    std::vector<level_t> _def_levels;
};

class MemoryBufferedReader final : public io::BufferedStreamReader {
public:
    explicit MemoryBufferedReader(std::vector<uint8_t> data) : _data(std::move(data)) {}

    Status read_bytes(const uint8_t** buf, uint64_t offset, size_t bytes_to_read,
                      const io::IOContext*) override {
        if (offset > _data.size() || bytes_to_read > _data.size() - offset) {
            return Status::IOError("out of bounds");
        }
        *buf = _data.data() + offset;
        return Status::OK();
    }
    Status read_bytes(Slice& slice, uint64_t offset, const io::IOContext*) override {
        if (offset > _data.size() || slice.size > _data.size() - offset) {
            return Status::IOError("out of bounds");
        }
        slice.data = reinterpret_cast<char*>(_data.data() + offset);
        return Status::OK();
    }
    std::string path() override { return "memory.parquet"; }
    int64_t mtime() const override { return 0; }

private:
    std::vector<uint8_t> _data;
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

    // Dictionary indices are uint32_t. Wider external bit widths would make the RLE decoder copy
    // a five-byte repeated value into four-byte state before it can validate an index.
    char oversized_bit_width[] = {33, 2, 0};
    Slice oversized_bit_width_slice(oversized_bit_width, sizeof(oversized_bit_width));
    EXPECT_TRUE(decoder.set_data(&oversized_bit_width_slice).is<ErrorCode::CORRUPTION>());
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

TEST(ParquetV2NativeDecoderTest, BitPackedLevelCursorOperationsPreservePosition) {
    char encoded[] = {static_cast<char>(0b00001101)};
    Slice levels(encoded, sizeof(encoded));
    LevelDecoder decoder;
    ASSERT_TRUE(decoder.init(&levels, tparquet::Encoding::BIT_PACKED, 1, 4).ok());

    level_t value = -1;
    EXPECT_EQ(decoder.get_next_run(&value, 4), 1);
    EXPECT_EQ(value, 1);
    EXPECT_EQ(decoder.get_next(), 0);
    decoder.rewind_one();
    EXPECT_EQ(decoder.get_next(), 0);
    level_t tail[2] = {-1, -1};
    EXPECT_EQ(decoder.get_levels(tail, 2), 2);
    EXPECT_EQ(tail[0], 1);
    EXPECT_EQ(tail[1], 1);

    char truncated_rle[] = {0x01, 0x00, 0x00, 0x00, 0x03};
    Slice truncated_levels(truncated_rle, sizeof(truncated_rle));
    LevelDecoder rle_decoder;
    ASSERT_TRUE(rle_decoder.init(&truncated_levels, tparquet::Encoding::RLE, 1, 1).ok());
    level_t truncated_value = -1;
    EXPECT_EQ(rle_decoder.get_levels(&truncated_value, 1), 0);
}

TEST(ParquetV2NativeDecoderTest, TruncatedBooleanStreamsFailWhileSkipping) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, decoder).ok());
    char plain_boolean[] = {static_cast<char>(0xFF)};
    Slice plain_slice(plain_boolean, sizeof(plain_boolean));
    ASSERT_TRUE(decoder->set_data(&plain_slice).ok());
    EXPECT_FALSE(decoder->skip_values(9).ok());

    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, decoder).ok());
    // A bit-packed run header for eight values without its required payload byte.
    char truncated_rle[] = {0x01, 0x00, 0x00, 0x00, 0x03};
    Slice rle_slice(truncated_rle, sizeof(truncated_rle));
    ASSERT_TRUE(decoder->set_data(&rle_slice).ok());
    EXPECT_FALSE(decoder->skip_values(1).ok());

    // The first literal group is complete but the second has only its header. Exact-count checks
    // must reject both dense and selected decodes instead of exposing the retained vector tail.
    char partially_truncated_rle[] = {0x03, 0x00, 0x00, 0x00, 0x03, static_cast<char>(0xFF), 0x03};
    Slice partially_truncated_slice(partially_truncated_rle, sizeof(partially_truncated_rle));
    ASSERT_TRUE(decoder->set_data(&partially_truncated_slice).ok());
    CaptureFixedConsumer dense_consumer;
    EXPECT_FALSE(decoder->decode_fixed_values(9, dense_consumer).ok());
    ASSERT_TRUE(decoder->set_data(&partially_truncated_slice).ok());
    CaptureFixedConsumer selected_consumer;
    const ParquetSelection select_tail {
            .total_values = 9, .selected_values = 1, .ranges = {{.first = 8, .count = 1}}};
    EXPECT_FALSE(decoder->decode_selected_fixed_values(select_tail, selected_consumer).ok());
}

TEST(ParquetV2NativeDecoderTest, DeltaFixedWidthValidatesFilteredAndSkippedValues) {
    const std::vector<std::string> values {"good", "bad"};
    std::vector<::parquet::ByteArray> byte_arrays;
    for (const auto& value : values) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_type_length(4);
    Slice slice(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer selected_consumer;
    const ParquetSelection select_good {
            .total_values = 2, .selected_values = 1, .ranges = {{.first = 0, .count = 1}}};
    EXPECT_TRUE(decoder->decode_selected_fixed_values(select_good, selected_consumer)
                        .is<ErrorCode::CORRUPTION>());

    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_TRUE(decoder->skip_values(2).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, DeltaSkipRequiresTheRequestedValueCount) {
    const std::vector<int32_t> integers {7};
    auto int_descriptor = descriptor(::parquet::Type::INT32);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, int_descriptor.get());
    encoder->Put(integers.data(), static_cast<int>(integers.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    Slice slice(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_FALSE(decoder->skip_values(2).ok());
}

TEST(ParquetV2NativeDecoderTest, DeltaHeadersAndLengthsAreBoundedBeforeAllocation) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_expected_values(1);
    // block_size=128, miniblocks=4, total_values=INT_MAX, first_value=0.
    char oversized_count[] = {static_cast<char>(0x80),
                              0x01,
                              0x04,
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              static_cast<char>(0xFF),
                              0x07,
                              0x00};
    Slice oversized_count_slice(oversized_count, sizeof(oversized_count));
    EXPECT_TRUE(decoder->set_data(&oversized_count_slice).is<ErrorCode::CORRUPTION>());

    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_expected_values(1);
    // A single decoded length of 1 GiB with no following payload bytes must fail before the
    // decoder resizes its backing data buffer.
    char oversized_length[] = {static_cast<char>(0x80),
                               0x01,
                               0x04,
                               0x01,
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               static_cast<char>(0x80),
                               0x08};
    Slice oversized_length_slice(oversized_length, sizeof(oversized_length));
    ASSERT_TRUE(decoder->set_data(&oversized_length_slice).ok());
    CaptureBinaryConsumer consumer;
    EXPECT_TRUE(decoder->decode_binary_values(1, consumer).is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(consumer.refs.empty());
}

TEST(ParquetV2NativeDecoderTest, EmptyDeltaLengthPageResetsDecoderState) {
    const std::vector<std::string> strings {"old", "state"};
    std::vector<::parquet::ByteArray> byte_arrays;
    for (const auto& value : strings) {
        byte_arrays.emplace_back(static_cast<uint32_t>(value.size()),
                                 reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(
            ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, false, byte_descriptor.get());
    encoder->Put(byte_arrays.data(), static_cast<int>(byte_arrays.size()));
    auto buffer = encoder->FlushValues();

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, decoder)
                        .ok());
    Slice valid(buffer->data(), buffer->size());
    ASSERT_TRUE(decoder->set_data(&valid).ok());
    Slice empty;
    EXPECT_TRUE(decoder->set_data(&empty).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ByteStreamSplitRejectsPartialRowsAtPageBoundary) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    char partial_row[] = {0, 1, 2, 3, 4};
    Slice slice(partial_row, sizeof(partial_row));
    EXPECT_TRUE(decoder->set_data(&slice).is<ErrorCode::CORRUPTION>());
}

Status read_scripted_map(const DataTypePtr& key_type, size_t key_rows, size_t value_rows,
                         size_t key_values, size_t value_values,
                         std::vector<level_t> key_rep_levels, std::vector<level_t> value_rep_levels,
                         bool value_eof) {
    auto value_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    ColumnPtr column = map_type->create_column();
    FieldSchema field;
    field.name = "m";
    field.data_type = map_type;
    field.definition_level = 1;
    field.repetition_level = 1;
    field.repeated_parent_def_level = 0;

    auto key_reader = std::make_unique<ScriptedColumnReader>(key_rows, key_values, true,
                                                             std::move(key_rep_levels),
                                                             std::vector<level_t>(key_values, 1));
    auto value_reader = std::make_unique<ScriptedColumnReader>(
            value_rows, value_values, value_eof, std::move(value_rep_levels),
            std::vector<level_t>(value_values, 1));
    MapColumnReader reader(scripted_row_ranges(), key_rows, nullptr, nullptr);
    RETURN_IF_ERROR(reader.init(std::move(key_reader), std::move(value_reader), &field));

    auto root = std::make_shared<TableSchemaChangeHelper::MapNode>(
            TableSchemaChangeHelper::ConstNode::get_instance(),
            TableSchemaChangeHelper::ConstNode::get_instance());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    return reader.read_column_data(column, map_type, root, filter, key_rows, &read_rows, &eof,
                                   false);
}

TEST(ParquetV2NativeDecoderTest, ComplexReadersRejectMalformedSiblingShape) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 1, 1, 2, 2, {1, 0}, {1, 0}, true)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(read_scripted_map(int_type, 1, 1, 2, 2, {0, 1}, {0, 0}, true)
                        .is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(
            read_scripted_map(int_type, 2, 1, 2, 1, {0, 0}, {0}, true).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, MapReaderRejectsNullKeys) {
    auto nullable_key = make_nullable(std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(read_scripted_map(nullable_key, 1, 1, 1, 1, {0}, {0}, true)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShortSibling) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    FieldSchema field;
    field.name = "s";
    field.data_type = struct_type;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {0, 0});
    children["b"] = std::make_unique<ScriptedColumnReader>(1, 1, true, std::vector<level_t> {0},
                                                           std::vector<level_t> {0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<TableSchemaChangeHelper::StructNode>();
    root->add_children("a", "a", TableSchemaChangeHelper::ConstNode::get_instance());
    root->add_children("b", "b", TableSchemaChangeHelper::ConstNode::get_instance());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, DecoderOwnedHighWaterScratchIsReleased) {
    constexpr size_t value_count = 1UL << 20;
    std::vector<uint8_t> encoded(value_count * sizeof(float), 0);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FLOAT, tparquet::Encoding::BYTE_STREAM_SPLIT,
                                     decoder)
                        .ok());
    decoder->set_type_length(sizeof(float));
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(value_count, consumer).ok());
    ASSERT_GT(decoder->retained_scratch_bytes(), 1UL << 20);
    decoder->release_scratch(64UL << 10);
    EXPECT_LE(decoder->retained_scratch_bytes(), 64UL << 10);
}

TEST(ParquetV2NativeDecoderTest, PageHeaderRejectsSignedAndV2LevelSizeCorruption) {
    auto parse_header = [](tparquet::PageHeader header) {
        std::vector<uint8_t> bytes;
        ThriftSerializer serializer(/*compact=*/true, 128);
        DORIS_CHECK(serializer.serialize(&header, &bytes).ok());
        if (header.compressed_page_size > 0) {
            bytes.resize(bytes.size() + header.compressed_page_size);
        }
        MemoryBufferedReader reader(bytes);
        tparquet::ColumnMetaData metadata;
        ParquetPageReadContext context(false, "");
        PageReader<false, false> page_reader(&reader, nullptr, 0, bytes.size(), 1, metadata,
                                             context);
        return page_reader.parse_page_header();
    };

    tparquet::PageHeader negative;
    negative.type = tparquet::PageType::DATA_PAGE;
    negative.__set_compressed_page_size(-1);
    negative.__set_uncompressed_page_size(1);
    negative.__isset.data_page_header = true;
    negative.data_page_header.__set_num_values(1);
    EXPECT_TRUE(parse_header(negative).is<ErrorCode::CORRUPTION>());

    tparquet::PageHeader oversized_levels;
    oversized_levels.type = tparquet::PageType::DATA_PAGE_V2;
    oversized_levels.__set_compressed_page_size(1);
    oversized_levels.__set_uncompressed_page_size(1);
    oversized_levels.__isset.data_page_header_v2 = true;
    oversized_levels.data_page_header_v2.__set_num_values(1);
    oversized_levels.data_page_header_v2.__set_num_rows(1);
    oversized_levels.data_page_header_v2.__set_repetition_levels_byte_length(2);
    oversized_levels.data_page_header_v2.__set_definition_levels_byte_length(0);
    EXPECT_TRUE(parse_header(oversized_levels).is<ErrorCode::CORRUPTION>());

    tparquet::PageHeader impossible_counts = oversized_levels;
    impossible_counts.__set_compressed_page_size(0);
    impossible_counts.__set_uncompressed_page_size(0);
    impossible_counts.data_page_header_v2.__set_repetition_levels_byte_length(0);
    impossible_counts.data_page_header_v2.__set_num_nulls(2);
    EXPECT_TRUE(parse_header(impossible_counts).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, EmptyOffsetIndexCannotSelectIndexedPageReader) {
    MemoryBufferedReader reader(std::vector<uint8_t> {0});
    tparquet::ColumnMetaData metadata;
    tparquet::OffsetIndex empty_index;
    ParquetPageReadContext context(false, "");
    PageReader<false, true> page_reader(&reader, nullptr, 0, 1, 1, metadata, context, &empty_index);
    EXPECT_FALSE(page_reader.has_next_page());
    EXPECT_TRUE(page_reader.next_page().is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ComplexPageStatisticsPreservePerLeafCrossings) {
    ColumnChunkReaderStatistics first_chunk;
    first_chunk.page_read_counter = 1;
    first_chunk.data_page_read_counter = 1;
    ColumnChunkReaderStatistics second_chunk;
    second_chunk.page_read_counter = 1;
    second_chunk.data_page_read_counter = 1;
    ColumnReader::ColumnStatistics combined;
    ColumnReader::ColumnStatistics first(first_chunk, 0, 0);
    ColumnReader::ColumnStatistics second(second_chunk, 0, 0);
    combined.merge(first);
    combined.merge(second);

    EXPECT_EQ(combined.page_read_counter, 2);
    ASSERT_EQ(combined.leaf_page_read_counters.size(), 2);
    EXPECT_EQ(combined.leaf_page_read_counters[0], 1);
    EXPECT_EQ(combined.leaf_page_read_counters[1], 1);
}

TEST(ParquetV2NativeDecoderTest, NativePageCacheUsesStableFileDescriptionIdentity) {
    ParquetPageCacheKeyBuilder first;
    ParquetPageCacheKeyBuilder replaced;
    first.init("s3://bucket/object::etag-v1::1234");
    replaced.init("s3://bucket/object::etag-v2::1234");
    EXPECT_EQ(first.make_key(4096, 128).fname, "s3://bucket/object::etag-v1::1234");
    EXPECT_NE(first.make_key(4096, 128).encode(), replaced.make_key(4096, 128).encode());
}

TEST(ParquetV2NativeDecoderTest, UncompressedV2PageCachePayloadIsAlwaysDecompressed) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(100);
    header.__set_uncompressed_page_size(1000);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    tparquet::ColumnMetaData metadata;
    metadata.__set_codec(tparquet::CompressionCodec::SNAPPY);

    // The representation is explicit in V2; a codec and compression ratio cannot override it on
    // a warm cache hit.
    EXPECT_TRUE(should_cache_decompressed(&header, metadata));
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
