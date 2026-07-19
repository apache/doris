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
#include <numeric>
#include <string>
#include <vector>

#include "core/custom_allocator.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/parquet_timestamp.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/column_reader.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "format_v2/parquet/reader/native/delta_bit_pack_decoder.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "format_v2/parquet/reader/native/page_reader.h"
#include "io/fs/buffered_reader.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/thrift_util.h"
#include "util/timezone_utils.h"

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

class CapturePlainBinaryLayoutConsumer final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef*, size_t) override {
        legacy_consume_called = true;
        return Status::InternalError("PLAIN BYTE_ARRAY used the legacy StringRef path");
    }

    Status consume_plain_byte_array(
            const char* encoded_data, const uint32_t* payload_offsets,
            const uint32_t* value_offsets, size_t num_values,
            const std::vector<ParquetSelectionRange>& value_spans) override {
        base = encoded_data;
        source_offsets.assign(payload_offsets, payload_offsets + num_values);
        output_offsets.assign(value_offsets, value_offsets + num_values + 1);
        spans = value_spans;
        return Status::OK();
    }

    const char* base = nullptr;
    bool legacy_consume_called = false;
    std::vector<uint32_t> source_offsets;
    std::vector<uint32_t> output_offsets;
    std::vector<ParquetSelectionRange> spans;
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
                            const std::shared_ptr<NativeSchemaNode>&, FilterMap&, size_t,
                            size_t* read_rows, bool* eof, bool, int64_t = -1) override {
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

std::vector<uint8_t> serialize_page(tparquet::PageHeader header,
                                    const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 128);
    DORIS_CHECK(serializer.serialize(&header, &bytes).ok());
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

Status load_scripted_page(tparquet::PageHeader header, const std::vector<uint8_t>& payload,
                          tparquet::CompressionCodec::type codec, bool preload_page_cache = false) {
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 128);
    RETURN_IF_ERROR(serializer.serialize(&header, &bytes));
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    const size_t chunk_size = bytes.size();
    const std::string page_cache_file_key = "native-scripted-page-" +
                                            std::to_string(static_cast<int>(header.type)) + "-" +
                                            std::to_string(header.compressed_page_size) + "-" +
                                            std::to_string(header.uncompressed_page_size);
    if (preload_page_cache) {
        auto* page = new DataPage(bytes.size(), true, segment_v2::DATA_PAGE);
        memcpy(page->data(), bytes.data(), bytes.size());
        page->reset_size(bytes.size());
        PageCacheHandle handle;
        StoragePageCache::instance()->insert(
                StoragePageCache::CacheKey(page_cache_file_key, chunk_size, 0), page, &handle,
                segment_v2::DATA_PAGE);
    }
    MemoryBufferedReader reader(std::move(bytes));

    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(codec);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(chunk_size);
    if (header.type == tparquet::PageType::DICTIONARY_PAGE) {
        chunk.meta_data.__set_dictionary_page_offset(0);
        chunk.meta_data.__set_data_page_offset(0);
    } else {
        chunk.meta_data.__set_data_page_offset(0);
    }
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 0;
    field.definition_level = 0;
    ParquetPageReadContext context(preload_page_cache, page_cache_file_key);
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    RETURN_IF_ERROR(chunk_reader.init());
    return chunk_reader.load_page_data();
}

Status load_malformed_nested_page(tparquet::PageHeader header, const std::vector<uint8_t>& payload,
                                  int64_t metadata_values = std::numeric_limits<int32_t>::max(),
                                  level_t max_repetition_level = 1) {
    auto bytes = serialize_page(header, payload);
    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(metadata_values);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = max_repetition_level;
    field.definition_level = 0;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());
    std::vector<level_t> rep_levels;
    size_t result_rows = 0;
    bool cross_page = false;
    return chunk_reader.load_page_nested_rows(rep_levels, 1, &result_rows, &cross_page);
}

Status materialize_plain_int96(const std::vector<ParquetInt96Timestamp>& values,
                               const std::vector<uint8_t>& filter_values = {}) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(values.size() * sizeof(ParquetInt96Timestamp));
    header.__set_uncompressed_page_size(values.size() * sizeof(ParquetInt96Timestamp));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(values.size());
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    auto bytes = serialize_page(
            header, std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(values.data()),
                                         reinterpret_cast<const uint8_t*>(values.data()) +
                                                 values.size() * sizeof(ParquetInt96Timestamp)));
    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT96);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(values.size());
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT96;
    ParquetPageReadContext page_context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, values.size(),
                                                 nullptr, page_context);
    RETURN_IF_ERROR(chunk_reader.init());
    RETURN_IF_ERROR(chunk_reader.load_page_data());

    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();
    ParquetDecodeContext decode_context;
    decode_context.physical_type = ParquetPhysicalType::INT96;
    static const auto utc = cctz::utc_time_zone();
    decode_context.timezone = &utc;
    ParquetMaterializationState state;
    state.enable_strict_mode = true;
    FilterMap filter;
    RETURN_IF_ERROR(filter.init(filter_values.empty() ? nullptr : filter_values.data(),
                                filter_values.size(), false));
    ColumnSelectVector select_vector;
    const std::vector<uint16_t> run_length_null_map {static_cast<uint16_t>(values.size()), 0};
    RETURN_IF_ERROR(select_vector.init(run_length_null_map, values.size(), nullptr, &filter, 0));
    return chunk_reader.materialize_values(column, *type.get_serde(), decode_context, state,
                                           select_vector);
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

TEST(ParquetV2NativeDecoderTest, ByteArrayDictionaryBoundsEntryCountBeforeReserve) {
    auto dictionary = make_unique_buffer<uint8_t>(1);
    dictionary.get()[0] = 0;
    ByteArrayDictDecoder decoder;
    EXPECT_TRUE(decoder.set_dict(dictionary, 1, std::numeric_limits<int32_t>::max())
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, LegacyConvertedTimestampsRemainUtcAdjusted) {
    TimezoneUtils::load_timezones_to_cache();
    for (const auto converted_type :
         {tparquet::ConvertedType::TIMESTAMP_MILLIS, tparquet::ConvertedType::TIMESTAMP_MICROS}) {
        NativeFieldSchema field;
        field.physical_type = tparquet::Type::INT64;
        field.parquet_schema.__set_converted_type(converted_type);
        ParquetDecodeContext context;
        ASSERT_TRUE(init_decode_context_for_test(field, nullptr, &context).ok());
        EXPECT_EQ(context.logical_type, ParquetLogicalType::TIMESTAMP);
        EXPECT_TRUE(context.timestamp_is_adjusted_to_utc);

        cctz::time_zone shanghai;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai));
        ASSERT_TRUE(init_decode_context_for_test(field, &shanghai, &context).ok());
        int64_t epoch = 0;
        Slice epoch_slice(reinterpret_cast<uint8_t*>(&epoch), sizeof(epoch));
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT64, tparquet::Encoding::PLAIN, decoder)
                            .ok());
        decoder->set_type_length(sizeof(epoch));
        ASSERT_TRUE(decoder->set_data(&epoch_slice).ok());
        ParquetMaterializationState state;
        DataTypeDateTimeV2 type(0);
        auto column = type.create_column();
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*column, *decoder, context, 1, state)
                            .ok());
        EXPECT_EQ(type.to_string(*column, 0), "1970-01-01 08:00:00");
    }
}

TEST(ParquetV2NativeDecoderTest, GeospatialByteArrayAnnotationsDecodeAsRawBytes) {
    const std::string wkb("\x01\x01\x00\x00\x00", 5);
    for (const bool geometry : {true, false}) {
        tparquet::LogicalType logical;
        if (geometry) {
            logical.__set_GEOMETRY(tparquet::GeometryType());
        } else {
            logical.__set_GEOGRAPHY(tparquet::GeographyType());
        }

        NativeFieldSchema field;
        field.name = geometry ? "geometry" : "geography";
        field.physical_type = tparquet::Type::BYTE_ARRAY;
        field.parquet_schema.__set_logicalType(logical);
        ParquetDecodeContext context;
        ASSERT_TRUE(init_decode_context_for_test(field, nullptr, &context).ok());
        EXPECT_EQ(context.physical_type, ParquetPhysicalType::BYTE_ARRAY);
        EXPECT_EQ(context.logical_type, ParquetLogicalType::NONE);

        DataTypeString type;
        auto plain_column = type.create_column();
        auto encoded = encode_plain_byte_arrays({wkb});
        Slice plain_slice(encoded.data(), encoded.size());
        std::unique_ptr<Decoder> plain_decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN,
                                         plain_decoder)
                            .ok());
        ASSERT_TRUE(plain_decoder->set_data(&plain_slice).ok());
        ParquetMaterializationState plain_state;
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*plain_column, *plain_decoder, context, 1,
                                                       plain_state)
                            .ok());
        ASSERT_EQ(plain_column->size(), 1);
        EXPECT_EQ(plain_column->get_data_at(0).to_string_view(), wkb);

        int32_t dictionary_length = 0;
        auto dictionary = make_byte_array_dictionary({wkb, "unused"}, &dictionary_length);
        ByteArrayDictDecoder dictionary_decoder;
        ASSERT_TRUE(dictionary_decoder.set_dict(dictionary, dictionary_length, 2).ok());
        char dictionary_index[] = {1, 2, 0};
        Slice dictionary_slice(dictionary_index, sizeof(dictionary_index));
        ASSERT_TRUE(dictionary_decoder.set_data(&dictionary_slice).ok());
        context.encoding = ParquetValueEncoding::DICTIONARY;
        auto dictionary_column = type.create_column();
        ParquetMaterializationState dictionary_state;
        ASSERT_TRUE(type.get_serde()
                            ->read_column_from_parquet(*dictionary_column, dictionary_decoder,
                                                       context, 1, dictionary_state)
                            .ok());
        ASSERT_EQ(dictionary_column->size(), 1);
        EXPECT_EQ(dictionary_column->get_data_at(0).to_string_view(), wkb);

        field.physical_type = tparquet::Type::INT32;
        EXPECT_TRUE(
                init_decode_context_for_test(field, nullptr, &context).is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, InvalidLogicalPhysicalPairsFailBeforeDecode) {
    auto invalid = [](tparquet::Type::type physical, const tparquet::LogicalType& logical) {
        NativeFieldSchema field;
        field.name = "bad";
        field.physical_type = physical;
        field.parquet_schema.__set_logicalType(logical);
        ParquetDecodeContext context;
        return init_decode_context_for_test(field, nullptr, &context);
    };

    tparquet::LogicalType date;
    date.__set_DATE(tparquet::DateType());
    EXPECT_FALSE(invalid(tparquet::Type::BOOLEAN, date).ok());

    tparquet::LogicalType timestamp;
    timestamp.__set_TIMESTAMP(tparquet::TimestampType());
    timestamp.TIMESTAMP.__set_unit(tparquet::TimeUnit());
    timestamp.TIMESTAMP.unit.__set_MICROS(tparquet::MicroSeconds());
    EXPECT_FALSE(invalid(tparquet::Type::INT32, timestamp).ok());

    tparquet::LogicalType integer;
    integer.__set_INTEGER(tparquet::IntType());
    integer.INTEGER.__set_bitWidth(64);
    integer.INTEGER.__set_isSigned(true);
    EXPECT_FALSE(invalid(tparquet::Type::INT32, integer).ok());

    tparquet::LogicalType string;
    string.__set_STRING(tparquet::StringType());
    EXPECT_FALSE(invalid(tparquet::Type::INT32, string).ok());

    tparquet::LogicalType uuid;
    uuid.__set_UUID(tparquet::UUIDType());
    EXPECT_FALSE(invalid(tparquet::Type::BYTE_ARRAY, uuid).ok());

    auto invalid_converted = [](tparquet::Type::type physical,
                                tparquet::ConvertedType::type converted, int type_length = -1,
                                int precision = -1, int scale = -1) {
        NativeFieldSchema field;
        field.name = "bad";
        field.physical_type = physical;
        field.parquet_schema.__set_converted_type(converted);
        if (type_length >= 0) {
            field.parquet_schema.__set_type_length(type_length);
        }
        if (precision >= 0) {
            field.parquet_schema.__set_precision(precision);
        }
        if (scale >= 0) {
            field.parquet_schema.__set_scale(scale);
        }
        ParquetDecodeContext context;
        return init_decode_context_for_test(field, nullptr, &context);
    };
    EXPECT_FALSE(invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::UTF8).ok());
    EXPECT_FALSE(
            invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::TIME_MICROS).ok());
    EXPECT_FALSE(invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::UINT_64).ok());
    EXPECT_FALSE(invalid_converted(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                   tparquet::ConvertedType::INTERVAL, 8)
                         .ok());
    EXPECT_FALSE(
            invalid_converted(tparquet::Type::INT32, tparquet::ConvertedType::DECIMAL, -1, 10, 2)
                    .ok());

    NativeFieldSchema fixed;
    fixed.name = "fixed";
    fixed.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    ParquetDecodeContext context;
    EXPECT_FALSE(init_decode_context_for_test(fixed, nullptr, &context).ok());
    fixed.parquet_schema.__set_type_length(4);
    EXPECT_TRUE(init_decode_context_for_test(fixed, nullptr, &context).ok());
}

TEST(ParquetV2NativeDecoderTest, NonStrictLegacyTimestampsKeepDefaultOnOverflow) {
    DataTypePtr datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    NativeFieldSchema int96_field;
    int96_field.physical_type = tparquet::Type::INT96;
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(int96_field, datetime_type, false));
    EXPECT_FALSE(preserves_timestamp_conversion_default_for_test(int96_field, datetime_type, true));

    NativeFieldSchema utc_field;
    utc_field.physical_type = tparquet::Type::INT64;
    utc_field.parquet_schema.__set_logicalType(tparquet::LogicalType());
    utc_field.parquet_schema.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    utc_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(true);
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(utc_field, datetime_type, false));

    NativeFieldSchema converted_utc_field;
    converted_utc_field.physical_type = tparquet::Type::INT64;
    converted_utc_field.parquet_schema.__set_converted_type(
            tparquet::ConvertedType::TIMESTAMP_MICROS);
    EXPECT_TRUE(preserves_timestamp_conversion_default_for_test(converted_utc_field, datetime_type,
                                                                false));

    NativeFieldSchema local_field = utc_field;
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(false);
    EXPECT_FALSE(
            preserves_timestamp_conversion_default_for_test(local_field, datetime_type, false));

    NativeFieldSchema integer_field;
    integer_field.physical_type = tparquet::Type::INT64;
    EXPECT_FALSE(preserves_timestamp_conversion_default_for_test(
            integer_field, make_nullable(std::make_shared<DataTypeInt64>()), false));
}

TEST(ParquetV2NativeDecoderTest, FastInt96NormalizesLegacyOutOfDayNanos) {
    EXPECT_TRUE(materialize_plain_int96({{-1, 2440588}}).ok());
    EXPECT_TRUE(materialize_plain_int96({{86400000000000LL, 2440588}}).ok());
    EXPECT_TRUE(materialize_plain_int96({{0, 2440588}, {-1, 2440588}}, {0, 1}).ok());
}

TEST(ParquetV2NativeDecoderTest, NonStrictLocalTimestampDefaultsBecomeNull) {
    DataTypePtr datetime_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    NativeFieldSchema local_field;
    local_field.physical_type = tparquet::Type::INT64;
    local_field.parquet_schema.__set_logicalType(tparquet::LogicalType());
    local_field.parquet_schema.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(false);

    DataTypeDateTimeV2 type(6);
    auto column = type.create_column();
    const uint64_t invalid_datetime = 0;
    column->insert_data(reinterpret_cast<const char*>(&invalid_datetime), sizeof(invalid_datetime));
    IColumn::Filter null_map;
    null_map.resize_fill(1, 0);
    mark_local_timestamp_defaults_for_test(local_field, datetime_type, false, *column, &null_map,
                                           0);
    EXPECT_EQ(null_map[0], 1);

    null_map[0] = 0;
    local_field.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(true);
    mark_local_timestamp_defaults_for_test(local_field, datetime_type, false, *column, &null_map,
                                           0);
    EXPECT_EQ(null_map[0], 0);
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

TEST(ParquetV2NativeDecoderTest, PlainByteArrayPublishesOffsetsAndCoalescedSelectionSpans) {
    const std::vector<std::string> strings {"zero", "one",  "two", "three",
                                            "four", "five", "six", "seven"};
    auto encoded = encode_plain_byte_arrays(strings);
    Slice slice(encoded.data(), encoded.size());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, decoder)
                        .ok());
    ASSERT_TRUE(decoder->set_data(&slice).ok());

    const ParquetSelection selection {
            .total_values = 7,
            .selected_values = 4,
            .ranges = {{.first = 1, .count = 2}, {.first = 5, .count = 2}}};
    CapturePlainBinaryLayoutConsumer consumer;
    ASSERT_TRUE(decoder->decode_selected_binary_values(selection, consumer).ok());
    EXPECT_FALSE(consumer.legacy_consume_called);
    EXPECT_EQ(consumer.output_offsets, std::vector<uint32_t>({0, 3, 6, 10, 13}));
    ASSERT_EQ(consumer.source_offsets.size(), selection.selected_values);
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[0], 3), "one");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[1], 3), "two");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[2], 4), "five");
    EXPECT_EQ(std::string_view(consumer.base + consumer.source_offsets[3], 3), "six");
    ASSERT_EQ(consumer.spans.size(), 2);
    EXPECT_EQ(consumer.spans[0].first, 0);
    EXPECT_EQ(consumer.spans[0].count, 2);
    EXPECT_EQ(consumer.spans[1].first, 2);
    EXPECT_EQ(consumer.spans[1].count, 2);

    CaptureBinaryConsumer trailing;
    ASSERT_TRUE(decoder->decode_binary_values(1, trailing).ok());
    ASSERT_EQ(trailing.refs.size(), 1);
    EXPECT_EQ(trailing.refs[0].to_string_view(), "seven");
}

TEST(ParquetV2NativeDecoderTest, SparsePlainFixedDecodeDoesNotRetainGatherBuffer) {
    constexpr size_t value_count = 1UL << 18;
    std::vector<int32_t> integers(value_count);
    std::iota(integers.begin(), integers.end(), 0);
    Slice integer_slice(reinterpret_cast<const uint8_t*>(integers.data()),
                        integers.size() * sizeof(int32_t));

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int32_t));
    ASSERT_TRUE(decoder->set_data(&integer_slice).ok());
    const ParquetSelection selection {
            .total_values = value_count,
            .selected_values = 2,
            .ranges = {{.first = 1, .count = 1}, {.first = value_count - 1, .count = 1}}};
    CaptureFixedConsumer selected_integers;
    ASSERT_TRUE(decoder->decode_selected_fixed_values(selection, selected_integers).ok());
    EXPECT_EQ(selected_integers.values<int32_t>(),
              std::vector<int32_t>({1, static_cast<int32_t>(value_count - 1)}));
    // Sparse PLAIN spans can be consumed directly from the encoded page. Retaining a gather buffer
    // makes a highly selective batch allocate in proportion to its selected width for no benefit.
    EXPECT_EQ(decoder->retained_scratch_bytes(), 0);
}

TEST(ParquetV2NativeDecoderTest, FixedPlainLargeSkipCannotWrapPageOffset) {
    std::vector<int64_t> values {11, 22};
    Slice slice(reinterpret_cast<const uint8_t*>(values.data()), values.size() * sizeof(int64_t));
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::INT64, tparquet::Encoding::PLAIN, decoder).ok());
    decoder->set_type_length(sizeof(int64_t));
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    EXPECT_FALSE(decoder->skip_values(536870913).ok());
    CaptureFixedConsumer consumer;
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());
    EXPECT_EQ(consumer.values<int64_t>(), std::vector<int64_t>({11}));
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

TEST(ParquetV2NativeDecoderTest, BitPackedLevelByteCountDoesNotWrapAtLargeCounts) {
    char placeholder[8] = {};
    constexpr uint32_t num_levels = 1'500'000'000;
    constexpr size_t expected_bytes = 562'500'000;
    Slice levels(placeholder, expected_bytes);
    LevelDecoder decoder;

    ASSERT_TRUE(decoder.init(&levels, tparquet::Encoding::BIT_PACKED, 4, num_levels).ok());
    EXPECT_EQ(levels.size, 0);
}

TEST(ParquetV2NativeDecoderTest, RejectsLevelsAboveSchemaMaximumOnEveryDecodePath) {
    auto make_decoder = [](tparquet::Encoding::type encoding) {
        static char encoded[] = {0x03}; // width=2 value=3, while the schema maximum is 2.
        static char rle_encoded[] = {0x02, 0x00, 0x00, 0x00, 0x02, 0x03};
        Slice levels = encoding == tparquet::Encoding::BIT_PACKED
                               ? Slice(encoded, sizeof(encoded))
                               : Slice(rle_encoded, sizeof(rle_encoded));
        LevelDecoder decoder;
        EXPECT_TRUE(decoder.init(&levels, encoding, 2, 1).ok());
        return decoder;
    };

    for (auto encoding : {tparquet::Encoding::BIT_PACKED, tparquet::Encoding::RLE}) {
        {
            auto decoder = make_decoder(encoding);
            level_t value = -1;
            EXPECT_EQ(decoder.get_levels(&value, 1), 0);
        }
        {
            auto decoder = make_decoder(encoding);
            level_t value = -1;
            EXPECT_EQ(decoder.get_next_run(&value, 1), 0);
        }
        {
            auto decoder = make_decoder(encoding);
            EXPECT_EQ(decoder.get_next(), -1);
        }
    }
}

TEST(ParquetV2NativeDecoderTest, NestedReadersRejectRleAndBitPackedLevelsAboveMaximum) {
    for (auto encoding : {tparquet::Encoding::RLE, tparquet::Encoding::BIT_PACKED}) {
        const std::vector<uint8_t> payload = encoding == tparquet::Encoding::RLE
                                                     ? std::vector<uint8_t> {2, 0, 0, 0, 2, 3}
                                                     : std::vector<uint8_t> {3};
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(1);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_repetition_level_encoding(encoding);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);

        EXPECT_TRUE(load_malformed_nested_page(header, payload, 1, 2).is<ErrorCode::CORRUPTION>());
    }
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

TEST(ParquetV2NativeDecoderTest, CompactRleSkipsKeepScratchBounded) {
    constexpr uint32_t value_count = 1U << 20;
    uint8_t header[16];
    uint8_t* header_end = encode_varint32(header, value_count << 1);

    int32_t dictionary_length = 0;
    auto dictionary = make_byte_array_dictionary({"only"}, &dictionary_length);
    ByteArrayDictDecoder dictionary_decoder;
    ASSERT_TRUE(dictionary_decoder.set_dict(dictionary, dictionary_length, 1).ok());
    std::vector<char> dictionary_indices {0};
    dictionary_indices.insert(dictionary_indices.end(), reinterpret_cast<char*>(header),
                              reinterpret_cast<char*>(header_end));
    Slice dictionary_slice(dictionary_indices.data(), dictionary_indices.size());
    ASSERT_TRUE(dictionary_decoder.set_data(&dictionary_slice).ok());
    ParquetDecodeSource& dictionary_source = dictionary_decoder;
    ASSERT_TRUE(dictionary_source.skip_values(value_count).ok());
    EXPECT_LE(dictionary_decoder.retained_scratch_bytes(), 4096 * sizeof(uint32_t));

    std::vector<char> boolean_payload(reinterpret_cast<char*>(header),
                                      reinterpret_cast<char*>(header_end));
    boolean_payload.push_back(0);
    std::vector<char> boolean_page(sizeof(uint32_t) + boolean_payload.size());
    encode_fixed32_le(reinterpret_cast<uint8_t*>(boolean_page.data()), boolean_payload.size());
    memcpy(boolean_page.data() + sizeof(uint32_t), boolean_payload.data(), boolean_payload.size());
    Slice boolean_slice(boolean_page.data(), boolean_page.size());
    std::unique_ptr<Decoder> boolean_decoder;
    ASSERT_TRUE(
            Decoder::get_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE, boolean_decoder)
                    .ok());
    ASSERT_TRUE(boolean_decoder->set_data(&boolean_slice).ok());
    ASSERT_TRUE(boolean_decoder->skip_values(value_count).ok());
    EXPECT_LE(boolean_decoder->retained_scratch_bytes(), 4096);
}

TEST(ParquetV2NativeDecoderTest, CompactDeltaSkipKeepsScratchBounded) {
    constexpr uint32_t delta_count = 1U << 20;
    std::vector<uint8_t> encoded(64);
    uint8_t* cursor = encoded.data();
    cursor = encode_varint32(cursor, delta_count);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, delta_count + 1);
    cursor = encode_varint32(cursor, 0); // first value, zig-zag encoded
    cursor = encode_varint32(cursor, 0); // minimum delta
    *cursor++ = 0;                       // zero-width miniblock
    encoded.resize(cursor - encoded.data());

    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED,
                                     decoder)
                        .ok());
    decoder->set_expected_values(delta_count + 1);
    Slice slice(encoded.data(), encoded.size());
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    ASSERT_TRUE(decoder->skip_values(delta_count + 1).ok());
    EXPECT_LE(decoder->retained_scratch_bytes(), 4096 * sizeof(int32_t) + 1);
}

TEST(ParquetV2NativeDecoderTest, DeltaPaddingBitCountIsWidenedBeforeCursorAdvance) {
    int64_t padding_bits = 0;
    ASSERT_TRUE(detail::checked_delta_padding_bits(64, std::numeric_limits<uint32_t>::max(),
                                                   &padding_bits)
                        .ok());
    EXPECT_EQ(padding_bits, 64LL * std::numeric_limits<uint32_t>::max());
    EXPECT_GT(padding_bits, std::numeric_limits<uint32_t>::max());
}

TEST(ParquetV2NativeDecoderTest, DeltaByteArraySkipsKeepScratchBounded) {
    constexpr size_t value_count = 1U << 16;
    std::vector<::parquet::ByteArray> values(value_count);
    auto byte_descriptor = descriptor(::parquet::Type::BYTE_ARRAY);
    for (const auto encoding :
         {::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, ::parquet::Encoding::DELTA_BYTE_ARRAY}) {
        auto encoder = ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false,
                                                                             byte_descriptor.get());
        encoder->Put(values.data(), static_cast<int>(values.size()));
        auto encoded = encoder->FlushValues();

        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                         static_cast<tparquet::Encoding::type>(encoding), decoder)
                            .ok());
        decoder->set_expected_values(value_count);
        Slice slice(encoded->data(), encoded->size());
        ASSERT_TRUE(decoder->set_data(&slice).ok());
        ASSERT_TRUE(decoder->skip_values(value_count).ok());
        EXPECT_LT(decoder->retained_scratch_bytes(), 1UL << 20);
    }
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

TEST(ParquetV2NativeDecoderTest, TruncatedFixedWidthPlainKeepsPublicErrorKeyword) {
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                                     tparquet::Encoding::PLAIN, decoder)
                        .ok());
    decoder->set_type_length(4);
    char truncated[] = {'a', 'b', 'c'};
    Slice slice(truncated, sizeof(truncated));
    ASSERT_TRUE(decoder->set_data(&slice).ok());
    CaptureFixedConsumer consumer;
    const auto status = decoder->decode_fixed_values(1, consumer);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Unexpected end of stream"), std::string::npos);
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
    CaptureBinaryConsumer consumer;
    EXPECT_TRUE(decoder->set_data(&oversized_length_slice).is<ErrorCode::CORRUPTION>());
    EXPECT_TRUE(consumer.refs.empty());

    ASSERT_TRUE(Decoder::get_decoder(tparquet::Type::BYTE_ARRAY,
                                     tparquet::Encoding::DELTA_BYTE_ARRAY, decoder)
                        .ok());
    decoder->set_expected_values(1);
    std::vector<uint8_t> huge_prefix(64);
    uint8_t* cursor = huge_prefix.data();
    cursor = encode_varint32(cursor, 128);
    cursor = encode_varint32(cursor, 4);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, std::numeric_limits<uint32_t>::max() - 1);
    cursor = encode_varint32(cursor, 128);
    cursor = encode_varint32(cursor, 4);
    cursor = encode_varint32(cursor, 1);
    cursor = encode_varint32(cursor, 0); // one empty suffix
    huge_prefix.resize(cursor - huge_prefix.data());
    Slice huge_prefix_slice(huge_prefix.data(), huge_prefix.size());
    ASSERT_TRUE(decoder->set_data(&huge_prefix_slice).ok());
    EXPECT_TRUE(decoder->decode_binary_values(1, consumer).is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_LT(decoder->retained_scratch_bytes(), 1UL << 20);
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
                         bool value_eof, std::vector<level_t> key_def_levels = {},
                         std::vector<level_t> value_def_levels = {}) {
    auto value_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    ColumnPtr column = map_type->create_column();
    NativeFieldSchema field;
    field.name = "m";
    field.data_type = map_type;
    field.definition_level = 1;
    field.repetition_level = 1;
    field.repeated_parent_def_level = 0;

    if (key_def_levels.empty()) {
        key_def_levels.assign(key_rep_levels.size(), 1);
    }
    if (value_def_levels.empty()) {
        value_def_levels.assign(value_rep_levels.size(), 1);
    }
    auto key_reader = std::make_unique<ScriptedColumnReader>(
            key_rows, key_values, true, std::move(key_rep_levels), std::move(key_def_levels));
    auto value_reader = std::make_unique<ScriptedColumnReader>(value_rows, value_values, value_eof,
                                                               std::move(value_rep_levels),
                                                               std::move(value_def_levels));
    MapColumnReader reader(scripted_row_ranges(), key_rows, nullptr, nullptr);
    RETURN_IF_ERROR(reader.init(std::move(key_reader), std::move(value_reader), &field));

    auto root = std::make_shared<NativeMapSchemaNode>(std::make_shared<NativeScalarSchemaNode>(),
                                                      std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    return reader.read_column_data(column, map_type, root, filter, key_rows, &read_rows, &eof,
                                   false);
}

TEST(ParquetV2NativeDecoderTest, MapReaderUsesKeyShapeForNestedValues) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 1, 1, 2, 2, {0, 1}, {0, 2, 1}, true).ok());
}

TEST(ParquetV2NativeDecoderTest, MapReaderRejectsShiftedEntryDistribution) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 2, 2, 4, 4, {0, 1, 0, 1}, {0, 1, 1, 0}, true)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, MapReaderRejectsShiftedEntryPresence) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(read_scripted_map(int_type, 2, 2, 2, 2, {0, 0}, {0, 0}, true, {0, 1}, {1, 0})
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, ComplexReadersRejectMalformedSiblingCounts) {
    auto int_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(
            read_scripted_map(int_type, 2, 1, 2, 1, {0, 0}, {0}, true).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, MapReaderPreservesNullableKeysWrittenByDoris) {
    auto nullable_key = make_nullable(std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(read_scripted_map(nullable_key, 1, 1, 1, 1, {0}, {0}, true).ok());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShortSibling) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
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
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShiftedRepeatedParentShape) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
    field.name = "s";
    field.repetition_level = 1;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(
            2, 3, true, std::vector<level_t> {0, 1, 0}, std::vector<level_t> {0, 0, 0});
    children["b"] = std::make_unique<ScriptedColumnReader>(
            2, 3, true, std::vector<level_t> {0, 0, 1}, std::vector<level_t> {0, 0, 0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
    FilterMap filter;
    size_t read_rows = 0;
    bool eof = false;
    EXPECT_TRUE(
            reader.read_column_data(column, struct_type, root, filter, 2, &read_rows, &eof, false)
                    .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, StructReaderRejectsShiftedOptionalParentPresence) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, int_type}, Strings {"a", "b"});
    ColumnPtr column = struct_type->create_column();
    NativeFieldSchema field;
    field.name = "s";
    field.data_type = struct_type;
    field.definition_level = 1;
    field.children.resize(2);
    field.children[0].name = "a";
    field.children[1].name = "b";

    std::unordered_map<std::string, std::unique_ptr<ColumnReader>> children;
    children["a"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {0, 1});
    children["b"] = std::make_unique<ScriptedColumnReader>(2, 2, true, std::vector<level_t> {0, 0},
                                                           std::vector<level_t> {1, 0});
    StructColumnReader reader(scripted_row_ranges(), 2, nullptr, nullptr);
    ASSERT_TRUE(reader.init(std::move(children), &field).ok());
    auto root = std::make_shared<NativeStructSchemaNode>();
    root->add_child("a", "a", std::make_shared<NativeScalarSchemaNode>());
    root->add_child("b", "b", std::make_shared<NativeScalarSchemaNode>());
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
    EXPECT_EQ(decoder->active_scratch_bytes(), value_count * sizeof(float));

    std::vector<uint8_t> ordinary_encoded(sizeof(float), 0);
    Slice ordinary_slice(ordinary_encoded.data(), ordinary_encoded.size());
    ASSERT_TRUE(decoder->set_data(&ordinary_slice).ok());
    ASSERT_TRUE(decoder->decode_fixed_values(1, consumer).ok());
    // Capacity records the high-water allocation while size records this batch. The reader needs
    // both to distinguish stable large batches from a one-off outlier before releasing capacity.
    EXPECT_EQ(decoder->active_scratch_bytes(), sizeof(float));
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

    tparquet::PageHeader missing_layout;
    missing_layout.type = tparquet::PageType::DATA_PAGE;
    missing_layout.__set_compressed_page_size(0);
    missing_layout.__set_uncompressed_page_size(0);
    EXPECT_TRUE(parse_header(missing_layout).is<ErrorCode::CORRUPTION>());

    auto swapped_layout = oversized_levels;
    swapped_layout.type = tparquet::PageType::DATA_PAGE;
    EXPECT_TRUE(parse_header(swapped_layout).is<ErrorCode::CORRUPTION>());

    auto competing_layouts = negative;
    competing_layouts.__set_compressed_page_size(0);
    competing_layouts.__set_uncompressed_page_size(0);
    competing_layouts.__isset.data_page_header_v2 = true;
    competing_layouts.data_page_header_v2.__set_num_values(0);
    competing_layouts.data_page_header_v2.__set_num_rows(0);
    competing_layouts.data_page_header_v2.__set_num_nulls(0);
    competing_layouts.data_page_header_v2.__set_repetition_levels_byte_length(0);
    competing_layouts.data_page_header_v2.__set_definition_levels_byte_length(0);
    EXPECT_TRUE(parse_header(competing_layouts).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, FlatPagesRejectLogicalAndPhysicalCardinalityMismatch) {
    auto init_chunk = [](tparquet::PageHeader header, bool with_offset_index) {
        std::vector<uint8_t> payload(static_cast<size_t>(header.compressed_page_size), 0);
        auto bytes = serialize_page(header, payload);
        MemoryBufferedReader reader(bytes);
        tparquet::ColumnChunk chunk;
        chunk.meta_data.__set_type(tparquet::Type::INT32);
        chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        chunk.meta_data.__set_num_values(2);
        chunk.meta_data.__set_total_compressed_size(bytes.size());
        chunk.meta_data.__set_data_page_offset(0);
        NativeFieldSchema field;
        field.physical_type = tparquet::Type::INT32;
        field.repetition_level = 0;
        field.definition_level = 0;
        ParquetPageReadContext context(false, "");
        tparquet::OffsetIndex offset_index;
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(bytes.size());
        location.__set_first_row_index(0);
        offset_index.__set_page_locations({location});
        if (with_offset_index) {
            ColumnChunkReader<false, true> reader_with_index(&reader, &chunk, &field, &offset_index,
                                                             1, nullptr, context);
            return reader_with_index.init();
        }
        ColumnChunkReader<false, false> sequential_reader(&reader, &chunk, &field, nullptr, 1,
                                                          nullptr, context);
        return sequential_reader.init();
    };

    tparquet::PageHeader v2;
    v2.type = tparquet::PageType::DATA_PAGE_V2;
    v2.__set_compressed_page_size(8);
    v2.__set_uncompressed_page_size(8);
    v2.__isset.data_page_header_v2 = true;
    v2.data_page_header_v2.__set_num_values(2);
    v2.data_page_header_v2.__set_num_rows(1);
    v2.data_page_header_v2.__set_num_nulls(0);
    v2.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    v2.data_page_header_v2.__set_repetition_levels_byte_length(0);
    v2.data_page_header_v2.__set_definition_levels_byte_length(0);
    v2.data_page_header_v2.__set_is_compressed(false);
    auto status = init_chunk(v2, false);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    status = init_chunk(v2, true);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;

    tparquet::PageHeader v1;
    v1.type = tparquet::PageType::DATA_PAGE;
    v1.__set_compressed_page_size(8);
    v1.__set_uncompressed_page_size(8);
    v1.__isset.data_page_header = true;
    v1.data_page_header.__set_num_values(2);
    v1.data_page_header.__set_encoding(tparquet::Encoding::RLE_DICTIONARY);
    v1.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    v1.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    status = init_chunk(v1, true);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}

TEST(ParquetV2NativeDecoderTest, NestedV2PageRejectsMissingAdvertisedRowStarts) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(12);
    header.__set_uncompressed_page_size(12);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_num_values(2);
    header.data_page_header_v2.__set_num_rows(2);
    header.data_page_header_v2.__set_num_nulls(0);
    header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header_v2.__set_repetition_levels_byte_length(4);
    header.data_page_header_v2.__set_definition_levels_byte_length(0);
    header.data_page_header_v2.__set_is_compressed(false);
    // Two values [0, 1] contain only one repetition-level zero, so they describe one row.
    EXPECT_TRUE(load_malformed_nested_page(header, {2, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0}, 2)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, FirstNestedV1PageRejectsOrphanContinuation) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    // A two-value RLE run at repetition level 1 has no level-0 row start.
    const std::vector<uint8_t> payload {2, 0, 0, 0, 4, 1, 0, 0, 0, 0, 0, 0, 0, 0};
    header.__set_compressed_page_size(payload.size());
    header.__set_uncompressed_page_size(payload.size());
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(2);
    header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    EXPECT_TRUE(load_malformed_nested_page(header, payload, 2).is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, NestedV1ContinuationRemainsValidAfterFirstRowStart) {
    auto make_page = [](int values, std::vector<uint8_t> payload) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(payload.size());
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(values);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        return serialize_page(header, payload);
    };
    auto first_page = make_page(2, {2, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0});
    auto second_page = make_page(1, {2, 0, 0, 0, 2, 1, 0, 0, 0, 0});
    first_page.insert(first_page.end(), second_page.begin(), second_page.end());

    MemoryBufferedReader reader(first_page);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(3);
    chunk.meta_data.__set_total_compressed_size(first_page.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    field.repetition_level = 1;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<true, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                context);
    ASSERT_TRUE(chunk_reader.init().ok());
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
    std::vector<level_t> levels;
    size_t rows = 0;
    bool cross_page = false;
    ASSERT_TRUE(chunk_reader.load_page_nested_rows(levels, 1, &rows, &cross_page).ok());
    ASSERT_TRUE(cross_page);
    ASSERT_TRUE(chunk_reader.load_cross_page_nested_row(levels, &cross_page).ok());
    EXPECT_FALSE(cross_page);
    EXPECT_EQ(levels, std::vector<level_t>({0, 1, 1}));
}

TEST(ParquetV2NativeDecoderTest, HugeNestedPageCountsDoNotPreallocateFromHeaders) {
    for (auto page_type : {tparquet::PageType::DATA_PAGE, tparquet::PageType::DATA_PAGE_V2}) {
        tparquet::PageHeader header;
        header.type = page_type;
        header.__set_compressed_page_size(4);
        header.__set_uncompressed_page_size(4);
        if (page_type == tparquet::PageType::DATA_PAGE) {
            header.__isset.data_page_header = true;
            header.data_page_header.__set_num_values(std::numeric_limits<int32_t>::max());
            header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
            header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        } else {
            header.__isset.data_page_header_v2 = true;
            header.data_page_header_v2.__set_num_values(std::numeric_limits<int32_t>::max());
            header.data_page_header_v2.__set_num_rows(1);
            header.data_page_header_v2.__set_num_nulls(0);
            header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
            header.data_page_header_v2.__set_repetition_levels_byte_length(0);
            header.data_page_header_v2.__set_definition_levels_byte_length(0);
            header.data_page_header_v2.__set_is_compressed(false);
        }
        // The four-byte V1 level length (or V2 value payload) contains no advertised levels. The
        // reader must report corruption without reserving INT_MAX level slots first.
        EXPECT_TRUE(load_malformed_nested_page(header, {0, 0, 0, 0}).is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, PageDecompressionRejectsBothSizeMismatchDirections) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(tparquet::CompressionCodec::SNAPPY, &codec).ok());
    std::vector<uint8_t> input(8, 7);
    faststring compressed;
    ASSERT_TRUE(codec->compress(Slice(input.data(), input.size()), &compressed).ok());
    std::vector<uint8_t> payload(compressed.data(), compressed.data() + compressed.size());

    for (const int32_t advertised_size : {4, 12}) {
        tparquet::PageHeader header;
        header.type = tparquet::PageType::DATA_PAGE;
        header.__set_compressed_page_size(payload.size());
        header.__set_uncompressed_page_size(advertised_size);
        header.__isset.data_page_header = true;
        header.data_page_header.__set_num_values(1);
        header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        EXPECT_FALSE(load_scripted_page(header, payload, tparquet::CompressionCodec::SNAPPY).ok());
    }
}

TEST(ParquetV2NativeDecoderTest, UncompressedDictionaryRequiresEqualPhysicalAndLogicalSizes) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DICTIONARY_PAGE;
    header.__set_compressed_page_size(8);
    header.__set_uncompressed_page_size(1);
    header.__isset.dictionary_page_header = true;
    header.dictionary_page_header.__set_num_values(1);
    header.dictionary_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    EXPECT_TRUE(load_scripted_page(header, std::vector<uint8_t>(8),
                                   tparquet::CompressionCodec::UNCOMPRESSED)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(ParquetV2NativeDecoderTest, UncompressedDataPagesRequireEqualPhysicalAndLogicalSizes) {
    for (const auto page_type : {tparquet::PageType::DATA_PAGE, tparquet::PageType::DATA_PAGE_V2}) {
        tparquet::PageHeader header;
        header.type = page_type;
        header.__set_compressed_page_size(8);
        header.__set_uncompressed_page_size(4);
        if (page_type == tparquet::PageType::DATA_PAGE) {
            header.__isset.data_page_header = true;
        } else {
            header.__isset.data_page_header_v2 = true;
            header.data_page_header_v2.__set_is_compressed(false);
        }
        const std::vector<uint8_t> payload(8);
        EXPECT_TRUE(load_scripted_page(header, payload, tparquet::CompressionCodec::UNCOMPRESSED)
                            .is<ErrorCode::CORRUPTION>());
        EXPECT_TRUE(
                load_scripted_page(header, payload, tparquet::CompressionCodec::UNCOMPRESSED, true)
                        .is<ErrorCode::CORRUPTION>());
    }
}

TEST(ParquetV2NativeDecoderTest, LegacyV2CompressedOverrideAllowsDifferentSizes) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(8);
    header.__set_uncompressed_page_size(16);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    EXPECT_TRUE(validate_uncompressed_page_sizes(header, tparquet::CompressionCodec::SNAPPY, true)
                        .ok());
    EXPECT_TRUE(validate_uncompressed_page_sizes(header, tparquet::CompressionCodec::SNAPPY, false)
                        .is<ErrorCode::CORRUPTION>());
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

TEST(ParquetV2NativeDecoderTest, ColumnChunkRangeRejectsSignedOverflowAndBoundsLegacyPadding) {
    tparquet::ColumnMetaData metadata;
    metadata.__set_data_page_offset(-1);
    metadata.__set_total_compressed_size(10);
    ColumnChunkRange range;
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_data_page_offset(95);
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_data_page_offset(10);
    metadata.__set_total_compressed_size(std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(
            compute_column_chunk_range(metadata, 100, false, &range).is<ErrorCode::CORRUPTION>());

    metadata.__set_total_compressed_size(20);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, true, &range).ok());
    EXPECT_EQ(range.offset, 10);
    EXPECT_EQ(range.length, 25);

    metadata.__set_dictionary_page_offset(0);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, false, &range).ok());
    EXPECT_EQ(range.offset, 10);
    EXPECT_EQ(range.length, 20);

    metadata.__set_dictionary_page_offset(5);
    ASSERT_TRUE(compute_column_chunk_range(metadata, 35, false, &range).ok());
    EXPECT_EQ(range.offset, 5);
    EXPECT_EQ(range.length, 20);
}

TEST(ParquetV2NativeDecoderTest, OffsetIndexValidationRejectsBackwardAndOverlappingLocations) {
    ColumnChunkRange range {.offset = 100, .length = 100};
    tparquet::OffsetIndex index;
    tparquet::PageLocation first;
    first.__set_offset(110);
    first.__set_compressed_page_size(20);
    first.__set_first_row_index(0);
    tparquet::PageLocation second;
    second.__set_offset(120);
    second.__set_compressed_page_size(20);
    second.__set_first_row_index(10);
    index.page_locations = {first, second};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));

    second.__set_offset(140);
    second.__set_first_row_index(0);
    index.page_locations = {first, second};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));

    second.__set_first_row_index(10);
    index.page_locations = {first, second};
    EXPECT_TRUE(validate_offset_index(index, range, 110, 20));

    range = {.offset = std::numeric_limits<size_t>::max(), .length = 2};
    EXPECT_FALSE(validate_offset_index(index, range, 110, 20));
}

TEST(ParquetV2NativeDecoderTest, OffsetIndexValidationRejectsShiftedFirstDataPage) {
    ColumnChunkRange range {.offset = 100, .length = 100};
    tparquet::OffsetIndex index;
    tparquet::PageLocation first;
    first.__set_offset(120);
    first.__set_compressed_page_size(20);
    first.__set_first_row_index(0);
    tparquet::PageLocation second;
    second.__set_offset(140);
    second.__set_compressed_page_size(20);
    second.__set_first_row_index(10);
    index.page_locations = {first, second};

    EXPECT_FALSE(validate_offset_index(index, range, 100, 20));
}

TEST(ParquetV2NativeDecoderTest, ColumnChunkSkipsIndexPageBeforeInitializingDataDecoder) {
    tparquet::PageHeader index_header;
    index_header.type = tparquet::PageType::INDEX_PAGE;
    index_header.__set_compressed_page_size(3);
    index_header.__set_uncompressed_page_size(3);
    index_header.__set_index_page_header(tparquet::IndexPageHeader());
    auto bytes = serialize_page(index_header, {1, 2, 3});

    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(sizeof(int32_t));
    data_header.__set_uncompressed_page_size(sizeof(int32_t));
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(1);
    data_header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    const int32_t value = 42;
    auto data_bytes = serialize_page(
            data_header,
            std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(&value),
                                 reinterpret_cast<const uint8_t*>(&value) + sizeof(value)));
    bytes.insert(bytes.end(), data_bytes.begin(), data_bytes.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    const auto init_status = chunk_reader.init();
    ASSERT_TRUE(init_status.ok()) << init_status;
    EXPECT_EQ(chunk_reader.remaining_num_values(), 1);
    ASSERT_TRUE(chunk_reader.load_page_data().ok());
}

TEST(ParquetV2NativeDecoderTest, ColumnChunkSkipsUnknownAuxiliaryPage) {
    tparquet::PageHeader unknown_header;
    unknown_header.type = static_cast<tparquet::PageType::type>(127);
    unknown_header.__set_compressed_page_size(3);
    unknown_header.__set_uncompressed_page_size(3);
    auto bytes = serialize_page(unknown_header, {1, 2, 3});

    tparquet::PageHeader data_header;
    data_header.type = tparquet::PageType::DATA_PAGE;
    data_header.__set_compressed_page_size(sizeof(int32_t));
    data_header.__set_uncompressed_page_size(sizeof(int32_t));
    data_header.__isset.data_page_header = true;
    data_header.data_page_header.__set_num_values(1);
    data_header.data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
    data_header.data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
    const int32_t value = 42;
    auto data_bytes = serialize_page(
            data_header,
            std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(&value),
                                 reinterpret_cast<const uint8_t*>(&value) + sizeof(value)));
    bytes.insert(bytes.end(), data_bytes.begin(), data_bytes.end());

    MemoryBufferedReader reader(bytes);
    tparquet::ColumnChunk chunk;
    chunk.meta_data.__set_type(tparquet::Type::INT32);
    chunk.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    chunk.meta_data.__set_num_values(1);
    chunk.meta_data.__set_total_compressed_size(bytes.size());
    chunk.meta_data.__set_data_page_offset(0);
    NativeFieldSchema field;
    field.physical_type = tparquet::Type::INT32;
    ParquetPageReadContext context(false, "");
    ColumnChunkReader<false, false> chunk_reader(&reader, &chunk, &field, nullptr, 1, nullptr,
                                                 context);
    const auto init_status = chunk_reader.init();
    ASSERT_TRUE(init_status.ok()) << init_status;
    EXPECT_EQ(chunk_reader.remaining_num_values(), 1);
}

TEST(ParquetV2NativeDecoderTest, LegacyDataPageV2OverridesFalseCompressedFlag) {
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    header.__set_compressed_page_size(4);
    header.__set_uncompressed_page_size(1024);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_is_compressed(false);
    tparquet::ColumnMetaData metadata;
    metadata.__set_codec(tparquet::CompressionCodec::SNAPPY);
    EXPECT_TRUE(should_cache_decompressed(&header, metadata, false));
    EXPECT_FALSE(should_cache_decompressed(&header, metadata, true));

    EXPECT_TRUE(parquet_reader_compat("parquet-cpp version 1.5.0").data_page_v2_always_compressed);
    EXPECT_FALSE(parquet_reader_compat("parquet-cpp version 2.0.0").data_page_v2_always_compressed);
    EXPECT_TRUE(parquet_reader_compat("parquet-mr version 1.2.8").parquet_816_padding);
    EXPECT_FALSE(parquet_reader_compat("parquet-mr version 1.2.9").parquet_816_padding);
}

TEST(ParquetV2NativeDecoderTest, NullableNumericOverflowIsNullOnlyOutsideStrictMode) {
    const int32_t overflow = 1000;
    auto decode = [&](bool strict, MutableColumnPtr* column, IColumn::Filter* null_map) {
        std::unique_ptr<Decoder> decoder;
        RETURN_IF_ERROR(
                Decoder::get_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN, decoder));
        decoder->set_type_length(sizeof(overflow));
        Slice slice(reinterpret_cast<const uint8_t*>(&overflow), sizeof(overflow));
        RETURN_IF_ERROR(decoder->set_data(&slice));
        ParquetDecodeContext context;
        context.physical_type = ParquetPhysicalType::INT32;
        ParquetMaterializationState state;
        state.enable_strict_mode = strict;
        state.conversion_failure_null_map = null_map;
        DataTypeInt8 type;
        *column = type.create_column();
        return type.get_serde()->read_column_from_parquet(**column, *decoder, context, 1, state);
    };

    MutableColumnPtr column;
    IColumn::Filter null_map;
    null_map.resize_fill(1, 0);
    ASSERT_TRUE(decode(false, &column, &null_map).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(null_map[0], 1);

    null_map.clear();
    null_map.resize_fill(1, 0);
    EXPECT_FALSE(decode(true, &column, &null_map).ok());
    EXPECT_EQ(null_map[0], 0);
}

TEST(ParquetV2NativeDecoderTest, FixedLengthStringsAppendAsOneContiguousSpan) {
    ColumnString column;
    const std::string values = "aaabbbccc";
    column.insert_many_fixed_length_data(values.data(), 3, 3);
    ASSERT_EQ(column.size(), 3);
    EXPECT_EQ(column.get_data_at(0).to_string_view(), "aaa");
    EXPECT_EQ(column.get_data_at(1).to_string_view(), "bbb");
    EXPECT_EQ(column.get_data_at(2).to_string_view(), "ccc");
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

TEST(ParquetV2NativeDecoderTest, CacheDisabledPagesDoNotPrepareCopiedPayload) {
    EXPECT_FALSE(can_prepare_page_cache_payload(false, false, true, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, true, true, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, false, false, true));
    EXPECT_FALSE(can_prepare_page_cache_payload(true, false, true, false));
    EXPECT_TRUE(can_prepare_page_cache_payload(true, false, true, true));
}

TEST(ParquetV2NativeDecoderTest, OversizedNestedBatchScratchUsesIdleBatchHysteresis) {
    ::doris::RowRanges row_ranges;
    tparquet::ColumnChunk chunk;
    ScalarColumnReader<true, false> reader(row_ranges, 1, chunk, nullptr, nullptr, nullptr);

    constexpr size_t max_retained_bytes = 64UL << 10;
    reader.reserve_batch_scratch_for_test(1UL << 16);
    const size_t oversized_bytes = reader.retained_batch_scratch_bytes_for_test();
    ASSERT_GT(oversized_bytes, max_retained_bytes);

    reader.release_batch_scratch(max_retained_bytes);
    EXPECT_EQ(reader.retained_batch_scratch_bytes_for_test(), oversized_bytes);
    reader.release_batch_scratch(max_retained_bytes);
    EXPECT_EQ(reader.retained_batch_scratch_bytes_for_test(), oversized_bytes);
    reader.release_batch_scratch(max_retained_bytes);
    const size_t released_bytes = reader.retained_batch_scratch_bytes_for_test();
    EXPECT_LT(released_bytes, oversized_bytes);
    EXPECT_LE(released_bytes, max_retained_bytes + sizeof(void*));
}

} // namespace
} // namespace doris::format::parquet::native
