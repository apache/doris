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
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/parquet/vparquet_column_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

class CountingBufferedReader final : public io::BufferedStreamReader {
public:
    explicit CountingBufferedReader(std::vector<uint8_t> data) : _data(std::move(data)) {}

    Status read_bytes(const uint8_t** buf, uint64_t offset, size_t bytes_to_read,
                      const io::IOContext* io_ctx) override {
        ++_read_count;
        if (_read_count == _failed_read) {
            return Status::IOError("Injected read failure");
        }
        if (offset + bytes_to_read > _data.size()) {
            return Status::IOError("Out of bounds");
        }
        *buf = _data.data() + offset;
        return Status::OK();
    }

    Status read_bytes(Slice& slice, uint64_t offset, const io::IOContext* io_ctx) override {
        ++_read_count;
        if (_read_count == _failed_read) {
            return Status::IOError("Injected read failure");
        }
        if (offset + slice.size > _data.size()) {
            return Status::IOError("Out of bounds");
        }
        slice.data = reinterpret_cast<char*>(_data.data() + offset);
        return Status::OK();
    }

    std::string path() override { return "parquet_column_chunk_reader_test"; }
    int64_t mtime() const override { return 0; }
    size_t read_count() const { return _read_count; }
    void fail_on_read(size_t read_count) { _failed_read = read_count; }

private:
    std::vector<uint8_t> _data;
    size_t _read_count = 0;
    size_t _failed_read = 0;
};

class CountingFileReader final : public io::FileReader {
public:
    explicit CountingFileReader(std::vector<uint8_t> data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }
    size_t read_count() const { return _read_count; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        ++_read_count;
        if (offset > _data.size()) {
            return Status::IOError("Out of bounds");
        }
        *bytes_read = std::min(result.size, _data.size() - offset);
        memcpy(result.data, _data.data() + offset, *bytes_read);
        return Status::OK();
    }

private:
    std::vector<uint8_t> _data;
    io::Path _path = "parquet_scalar_column_reader_test";
    size_t _read_count = 0;
    bool _closed = false;
};

struct ColumnChunkFixture {
    std::vector<uint8_t> data;
    tparquet::ColumnChunk chunk;
    tparquet::OffsetIndex offset_index;
    FieldSchema field_schema;
};

Status append_page(tparquet::PageHeader* header, const std::vector<uint8_t>& payload,
                   std::vector<uint8_t>& data, int64_t* page_offset, int32_t* page_size) {
    std::vector<uint8_t> header_bytes;
    ThriftSerializer serializer(/*compact=*/true, /*initial_buffer_size=*/256);
    RETURN_IF_ERROR(serializer.serialize(header, &header_bytes));

    *page_offset = data.size();
    data.insert(data.end(), header_bytes.begin(), header_bytes.end());
    data.insert(data.end(), payload.begin(), payload.end());
    *page_size = cast_set<int32_t>(header_bytes.size() + payload.size());
    return Status::OK();
}

std::vector<uint8_t> encode_byte_array_dictionary(const std::vector<std::string>& values) {
    size_t size = 0;
    for (const auto& value : values) {
        size += sizeof(uint32_t) + value.size();
    }

    std::vector<uint8_t> data(size);
    size_t offset = 0;
    for (const auto& value : values) {
        encode_fixed32_le(data.data() + offset, cast_set<uint32_t>(value.size()));
        offset += sizeof(uint32_t);
        memcpy(data.data() + offset, value.data(), value.size());
        offset += value.size();
    }
    return data;
}

tparquet::PageHeader make_data_page_header(tparquet::Encoding::type encoding) {
    tparquet::DataPageHeader data_header;
    data_header.__set_num_values(1);
    data_header.__set_encoding(encoding);
    data_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
    data_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);

    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(1);
    header.__set_uncompressed_page_size(1);
    header.__set_data_page_header(data_header);
    return header;
}

Status make_dictionary_fixture(ColumnChunkFixture* fixture) {
    constexpr size_t PREFIX_SIZE = 16;
    fixture->data.resize(PREFIX_SIZE, 0);

    const std::vector<std::string> dictionary = {"alice", "bob", "carol"};
    std::vector<uint8_t> dictionary_data = encode_byte_array_dictionary(dictionary);

    tparquet::DictionaryPageHeader dictionary_header;
    dictionary_header.__set_num_values(cast_set<int32_t>(dictionary.size()));
    dictionary_header.__set_encoding(tparquet::Encoding::PLAIN);

    tparquet::PageHeader header;
    header.type = tparquet::PageType::DICTIONARY_PAGE;
    header.__set_compressed_page_size(cast_set<int32_t>(dictionary_data.size()));
    header.__set_uncompressed_page_size(cast_set<int32_t>(dictionary_data.size()));
    header.__set_dictionary_page_header(dictionary_header);

    int64_t dictionary_offset = 0;
    int32_t dictionary_page_size = 0;
    RETURN_IF_ERROR(append_page(&header, dictionary_data, fixture->data, &dictionary_offset,
                                &dictionary_page_size));

    std::vector<int64_t> data_page_offsets;
    std::vector<int32_t> data_page_sizes;
    for (int i = 0; i < 2; ++i) {
        header = make_data_page_header(tparquet::Encoding::RLE_DICTIONARY);
        int64_t page_offset = 0;
        int32_t page_size = 0;
        RETURN_IF_ERROR(append_page(&header, {0}, fixture->data, &page_offset, &page_size));
        data_page_offsets.push_back(page_offset);
        data_page_sizes.push_back(page_size);
    }

    auto& metadata = fixture->chunk.meta_data;
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    metadata.__set_num_values(2);
    metadata.__set_dictionary_page_offset(dictionary_offset);
    metadata.__set_data_page_offset(data_page_offsets[0]);
    metadata.__set_total_compressed_size(
            cast_set<int64_t>(fixture->data.size() - dictionary_offset));

    for (int i = 0; i < 2; ++i) {
        tparquet::PageLocation location;
        location.__set_offset(data_page_offsets[i]);
        location.__set_compressed_page_size(data_page_sizes[i]);
        location.__set_first_row_index(i);
        fixture->offset_index.page_locations.push_back(location);
    }

    fixture->field_schema.physical_type = tparquet::Type::BYTE_ARRAY;
    return Status::OK();
}

Status make_plain_fixture(ColumnChunkFixture* fixture, int page_count = 1) {
    constexpr size_t PREFIX_SIZE = 16;
    fixture->data.resize(PREFIX_SIZE, 0);

    std::vector<int64_t> data_page_offsets;
    for (int i = 0; i < page_count; ++i) {
        tparquet::PageHeader header = make_data_page_header(tparquet::Encoding::PLAIN);
        int64_t data_page_offset = 0;
        int32_t data_page_size = 0;
        RETURN_IF_ERROR(
                append_page(&header, {0}, fixture->data, &data_page_offset, &data_page_size));
        data_page_offsets.push_back(data_page_offset);

        tparquet::PageLocation location;
        location.__set_offset(data_page_offset);
        location.__set_compressed_page_size(data_page_size);
        location.__set_first_row_index(i);
        fixture->offset_index.page_locations.push_back(location);
    }

    auto& metadata = fixture->chunk.meta_data;
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    metadata.__set_num_values(page_count);
    metadata.__set_data_page_offset(data_page_offsets.front());
    metadata.__set_total_compressed_size(
            cast_set<int64_t>(fixture->data.size() - data_page_offsets.front()));

    fixture->field_schema.physical_type = tparquet::Type::BYTE_ARRAY;
    return Status::OK();
}

void expect_dictionary_values(ColumnChunkReader<false, false>* reader) {
    MutableColumnPtr column = ColumnString::create();
    ASSERT_TRUE(reader->read_dict_values_to_column(column).ok());
    const auto& strings = assert_cast<const ColumnString&>(*column);
    ASSERT_EQ(strings.size(), 3);
    EXPECT_EQ(std::string(strings.get_data_at(0)), "alice");
    EXPECT_EQ(std::string(strings.get_data_at(1)), "bob");
    EXPECT_EQ(std::string(strings.get_data_at(2)), "carol");
}

TEST(ParquetColumnChunkReaderTest, DictionaryProbeDoesNotParseDataPageHeader) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, false> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                           nullptr, 2, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    EXPECT_EQ(buffered_reader.read_count(), 0);

    bool has_dict = false;
    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_TRUE(has_dict);
    const size_t dictionary_read_count = buffered_reader.read_count();
    EXPECT_GT(dictionary_read_count, 0);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 1);
    expect_dictionary_values(&reader);

    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_EQ(buffered_reader.read_count(), dictionary_read_count);

    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_GT(buffered_reader.read_count(), dictionary_read_count);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 2);
    EXPECT_EQ(reader.page_start_row(), 0);
    EXPECT_EQ(reader.page_end_row(), 1);

    const size_t data_header_read_count = buffered_reader.read_count();
    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_EQ(buffered_reader.read_count(), data_header_read_count);
}

TEST(ParquetColumnChunkReaderTest, ParsePageHeaderLoadsDictionaryOnFirstUse) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, false> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                           nullptr, 2, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    EXPECT_EQ(buffered_reader.read_count(), 0);

    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_GT(buffered_reader.read_count(), 0);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 2);
    EXPECT_EQ(reader.page_start_row(), 0);
    EXPECT_EQ(reader.page_end_row(), 1);

    const size_t read_count = buffered_reader.read_count();
    bool has_dict = false;
    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_TRUE(has_dict);
    EXPECT_EQ(buffered_reader.read_count(), read_count);
    expect_dictionary_values(&reader);
}

TEST(ParquetColumnChunkReaderTest, SequentialReaderAdvancesAfterLazyDictionaryLoad) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, false> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                           nullptr, 2, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_EQ(reader.page_start_row(), 0);
    EXPECT_EQ(reader.page_end_row(), 1);

    ASSERT_TRUE(reader.next_page().ok());
    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_EQ(reader.page_start_row(), 1);
    EXPECT_EQ(reader.page_end_row(), 2);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 3);
}

TEST(ParquetColumnChunkReaderTest, PlainPageHeaderIsReusedAfterDictionaryCheck) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_plain_fixture(&fixture).ok());
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, false> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                           nullptr, 1, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    EXPECT_EQ(buffered_reader.read_count(), 0);

    bool has_dict = true;
    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_FALSE(has_dict);
    const size_t dictionary_probe_read_count = buffered_reader.read_count();
    EXPECT_GT(dictionary_probe_read_count, 0);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 1);

    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_EQ(buffered_reader.read_count(), dictionary_probe_read_count);
    EXPECT_EQ(reader.page_start_row(), 0);
    EXPECT_EQ(reader.page_end_row(), 1);
}

TEST(ParquetColumnChunkReaderTest, FailedDictionaryCheckCanBeRetried) {
    for (size_t failed_read : {1, 2}) {
        ColumnChunkFixture fixture;
        ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
        CountingBufferedReader buffered_reader(std::move(fixture.data));
        ParquetPageReadContext page_read_ctx(false);
        ColumnChunkReader<false, false> reader(&buffered_reader, &fixture.chunk,
                                               &fixture.field_schema, nullptr, 2, nullptr,
                                               page_read_ctx);

        ASSERT_TRUE(reader.init().ok());
        buffered_reader.fail_on_read(failed_read);

        bool has_dict = false;
        EXPECT_FALSE(reader.load_dictionary_page(&has_dict).ok());
        EXPECT_FALSE(has_dict);

        ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
        EXPECT_TRUE(has_dict);
        EXPECT_GT(buffered_reader.read_count(), failed_read);
    }
}

TEST(ParquetColumnChunkReaderTest, ScalarDictionaryReadUsesExplicitProbe) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    auto file_reader = std::make_shared<CountingFileReader>(std::move(fixture.data));

    RowRanges row_ranges;
    row_ranges.add({0, 2});
    ScalarColumnReader<false, false> reader(row_ranges, 2, fixture.chunk, nullptr, nullptr,
                                            nullptr);

    TQueryOptions query_options;
    query_options.__set_enable_parquet_file_page_cache(false);
    RuntimeState runtime_state(query_options, TQueryGlobals());

    ASSERT_TRUE(reader.init(file_reader, &fixture.field_schema,
                            /*max_buf_size=*/1024 * 1024, &runtime_state)
                        .ok());
    EXPECT_EQ(file_reader->read_count(), 0);

    MutableColumnPtr column = ColumnString::create();
    bool has_dict = false;
    ASSERT_TRUE(reader.read_dict_values_to_column(column, &has_dict).ok());
    EXPECT_TRUE(has_dict);
    EXPECT_EQ(reader.column_statistics().parse_page_header_num, 1);

    const auto& strings = assert_cast<const ColumnString&>(*column);
    ASSERT_EQ(strings.size(), 3);
    EXPECT_EQ(std::string(strings.get_data_at(0)), "alice");
    EXPECT_EQ(std::string(strings.get_data_at(1)), "bob");
    EXPECT_EQ(std::string(strings.get_data_at(2)), "carol");
}

void expect_offset_index_skip(ColumnChunkFixture fixture) {
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, true> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                          &fixture.offset_index, 2, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    EXPECT_EQ(buffered_reader.read_count(), 0);

    ASSERT_TRUE(reader.next_page().ok());
    const size_t dictionary_read_count = buffered_reader.read_count();
    EXPECT_GT(dictionary_read_count, 0);

    bool has_dict = false;
    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_TRUE(has_dict);
    EXPECT_EQ(buffered_reader.read_count(), dictionary_read_count);

    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_GT(buffered_reader.read_count(), dictionary_read_count);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 2);
    EXPECT_EQ(reader.page_start_row(), 1);
    EXPECT_EQ(reader.page_end_row(), 2);
}

TEST(ParquetColumnChunkReaderTest, OffsetIndexSkipLoadsDictionaryBeforeMovingPage) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    expect_offset_index_skip(std::move(fixture));
}

TEST(ParquetColumnChunkReaderTest, OffsetIndexSkipFindsDictionaryWithoutMetadataOffset) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_dictionary_fixture(&fixture).ok());
    fixture.chunk.meta_data.__set_data_page_offset(fixture.chunk.meta_data.dictionary_page_offset);
    fixture.chunk.meta_data.__isset.dictionary_page_offset = false;
    expect_offset_index_skip(std::move(fixture));
}

TEST(ParquetColumnChunkReaderTest, OffsetIndexSkipMovesPastPlainFirstPage) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_plain_fixture(&fixture, 2).ok());
    CountingBufferedReader buffered_reader(std::move(fixture.data));
    ParquetPageReadContext page_read_ctx(false);
    ColumnChunkReader<false, true> reader(&buffered_reader, &fixture.chunk, &fixture.field_schema,
                                          &fixture.offset_index, 2, nullptr, page_read_ctx);

    ASSERT_TRUE(reader.init().ok());
    EXPECT_EQ(buffered_reader.read_count(), 0);

    ASSERT_TRUE(reader.next_page().ok());
    const size_t dictionary_probe_read_count = buffered_reader.read_count();
    EXPECT_GT(dictionary_probe_read_count, 0);

    bool has_dict = true;
    ASSERT_TRUE(reader.load_dictionary_page(&has_dict).ok());
    EXPECT_FALSE(has_dict);
    EXPECT_EQ(buffered_reader.read_count(), dictionary_probe_read_count);

    ASSERT_TRUE(reader.parse_page_header().ok());
    EXPECT_GT(buffered_reader.read_count(), dictionary_probe_read_count);
    EXPECT_EQ(reader.statistics().parse_page_header_num, 2);
    EXPECT_EQ(reader.page_start_row(), 1);
    EXPECT_EQ(reader.page_end_row(), 2);
}

} // namespace
} // namespace doris
