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

#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "format/parquet/parquet_common.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_reader.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

// Serves an in-memory byte buffer as the parquet file behind a column reader.
class InMemoryFileReader final : public io::FileReader {
public:
    explicit InMemoryFileReader(std::vector<uint8_t> data) : _data(std::move(data)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }
    size_t size() const override { return _data.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
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
    bool _closed = false;
};

struct ColumnChunkFixture {
    std::vector<uint8_t> data;
    tparquet::ColumnChunk chunk;
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

// One uncompressed BYTE_ARRAY column chunk holding a single plain-encoded data page.
Status make_plain_fixture(ColumnChunkFixture* fixture) {
    constexpr size_t PREFIX_SIZE = 16;
    fixture->data.resize(PREFIX_SIZE, 0);

    tparquet::PageHeader header = make_data_page_header(tparquet::Encoding::PLAIN);
    int64_t data_page_offset = 0;
    int32_t data_page_size = 0;
    RETURN_IF_ERROR(append_page(&header, {0}, fixture->data, &data_page_offset, &data_page_size));

    auto& metadata = fixture->chunk.meta_data;
    metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    metadata.__set_num_values(1);
    metadata.__set_data_page_offset(data_page_offset);
    metadata.__set_total_compressed_size(
            cast_set<int64_t>(fixture->data.size() - data_page_offset));

    fixture->field_schema.physical_type = tparquet::Type::BYTE_ARRAY;
    return Status::OK();
}

} // namespace

// A direct read moves the caller's column into the reader; a read that stops before convert()
// hands it back, so the caller still holds a column instead of a null pointer.
TEST(ParquetScalarColumnReaderTest, NestedReadRestoresColumnWhenStopped) {
    ColumnChunkFixture fixture;
    ASSERT_TRUE(make_plain_fixture(&fixture).ok());
    auto file_reader = std::make_shared<InMemoryFileReader>(std::move(fixture.data));
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    fixture.field_schema.data_type = string_type;
    fixture.field_schema.parquet_schema.__set_type(tparquet::Type::BYTE_ARRAY);

    RowRanges row_ranges;
    row_ranges.add({0, 1});
    io::IOContext io_ctx;
    ScalarColumnReader<true, false> reader(row_ranges, 1, fixture.chunk, nullptr, nullptr, &io_ctx);
    reader.set_column_in_nested();

    TQueryOptions query_options;
    query_options.__set_enable_parquet_file_page_cache(false);
    RuntimeState runtime_state(query_options, TQueryGlobals());
    ASSERT_TRUE(reader.init(file_reader, &fixture.field_schema,
                            /*max_buf_size=*/1024 * 1024, &runtime_state)
                        .ok());

    ColumnPtr column = ColumnString::create();
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 0, false).ok());
    size_t read_rows = 0;
    bool eof = false;
    io_ctx.should_stop = true;

    Status status = reader.read_column_data(column, string_type, nullptr, filter_map, 1, &read_rows,
                                            &eof, false);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
    ASSERT_TRUE(column);
    EXPECT_TRUE(column->empty());
}

} // namespace doris
