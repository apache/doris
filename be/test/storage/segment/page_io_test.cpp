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

#include "storage/segment/page_io.h"

#include <crc32c/crc32c.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "io/fs/file_reader.h"
#include "storage/cache/page_cache.h"
#include "storage/olap_common.h"
#include "storage/segment/page_handle.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace doris::segment_v2 {
namespace {

class BufferFileReader final : public io::FileReader {
public:
    BufferFileReader(std::string path, std::string contents)
            : _path(std::move(path)), _contents(std::move(contents)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _contents.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }
    size_t read_count() const { return _read_count; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        if (offset > _contents.size() || result.size > _contents.size() - offset) {
            return Status::InvalidArgument("read exceeds test buffer");
        }
        ++_read_count;
        memcpy(result.data, _contents.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    io::Path _path;
    std::string _contents;
    size_t _read_count = 0;
    bool _closed = false;
};

PageFooterPB data_page_footer(size_t body_size) {
    PageFooterPB footer;
    footer.set_type(DATA_PAGE);
    footer.set_uncompressed_size(static_cast<uint32_t>(body_size));
    auto* data_footer = footer.mutable_data_page_footer();
    data_footer->set_first_ordinal(0);
    data_footer->set_num_values(1);
    data_footer->set_nullmap_size(0);
    return footer;
}

std::string encode_page(Slice stored_body, const PageFooterPB& footer) {
    std::string encoded(stored_body.data, stored_body.size);
    const std::string footer_bytes = footer.SerializeAsString();
    encoded.append(footer_bytes);
    put_fixed32_le(&encoded, static_cast<uint32_t>(footer_bytes.size()));
    put_fixed32_le(&encoded, crc32c::Crc32c(encoded.data(), encoded.size()));
    return encoded;
}

PageReadOptions page_read_options(BufferFileReader* reader, OlapReaderStatistics* stats,
                                  BlockCompressionCodec* codec = nullptr) {
    io::IOContext io_ctx;
    PageReadOptions options(io_ctx);
    options.file_reader = reader;
    options.page_pointer = PagePointer(0, static_cast<uint32_t>(reader->size()));
    options.codec = codec;
    options.stats = stats;
    options.use_page_cache = false;
    options.pre_decode = false;
    options.type = DATA_PAGE;
    return options;
}

class PageIOSliceDecodeTest : public testing::Test {
protected:
    void assert_slice_matches_file_read(const std::string& body, BlockCompressionCodec* codec,
                                        std::string_view path_prefix) {
        std::string stored_body = body;
        if (codec != nullptr) {
            faststring compressed;
            ASSERT_TRUE(codec->compress(body, &compressed).ok());
            ASSERT_LT(compressed.size(), body.size());
            stored_body.assign(reinterpret_cast<const char*>(compressed.data()), compressed.size());
        }
        const PageFooterPB footer = data_page_footer(body.size());
        const std::string encoded = encode_page(Slice(stored_body), footer);

        BufferFileReader file_reader(std::string(path_prefix) + "_file", encoded);
        OlapReaderStatistics file_stats;
        PageReadOptions file_options = page_read_options(&file_reader, &file_stats, codec);
        PageHandle file_handle;
        Slice file_body;
        PageFooterPB file_footer;
        ASSERT_TRUE(PageIO::read_and_decompress_page(file_options, &file_handle, &file_body,
                                                     &file_footer)
                            .ok());

        std::string range_buffer = encoded;
        BufferFileReader slice_reader(std::string(path_prefix) + "_slice", encoded);
        OlapReaderStatistics slice_stats;
        PageReadOptions slice_options = page_read_options(&slice_reader, &slice_stats, codec);
        PageHandle slice_handle;
        Slice slice_body;
        PageFooterPB slice_footer;
        ASSERT_TRUE(PageIO::decode_page_from_slice(slice_options, Slice(range_buffer),
                                                   &slice_handle, &slice_body, &slice_footer)
                            .ok());

        EXPECT_EQ(std::string(file_body.data, file_body.size), body);
        EXPECT_EQ(std::string(slice_body.data, slice_body.size), body);
        EXPECT_EQ(file_footer.SerializeAsString(), footer.SerializeAsString());
        EXPECT_EQ(slice_footer.SerializeAsString(), footer.SerializeAsString());
        EXPECT_EQ(file_reader.read_count(), 1);
        EXPECT_EQ(slice_reader.read_count(), 0);
        EXPECT_EQ(file_stats.total_pages_num, 1);
        EXPECT_EQ(slice_stats.total_pages_num, 1);
        EXPECT_EQ(file_stats.compressed_bytes_read, encoded.size());
        EXPECT_EQ(slice_stats.compressed_bytes_read, encoded.size());
        EXPECT_EQ(file_stats.uncompressed_bytes_read, body.size());
        EXPECT_EQ(slice_stats.uncompressed_bytes_read, body.size());

        std::fill(range_buffer.begin(), range_buffer.end(), 'z');
        EXPECT_EQ(std::string(slice_body.data, slice_body.size), body);
    }
};

TEST_F(PageIOSliceDecodeTest, uncompressed_slice_matches_file_read_and_owns_page_bytes) {
    assert_slice_matches_file_read("page-prefetch-uncompressed-body", nullptr,
                                   "page_io_slice_uncompressed");
}

TEST_F(PageIOSliceDecodeTest, compressed_slice_matches_file_read_and_owns_page_bytes) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::LZ4, &codec).ok());
    ASSERT_NE(codec, nullptr);
    const std::string body(128 * 1024, 'p');
    assert_slice_matches_file_read(body, codec, "page_io_slice_compressed");
}

TEST_F(PageIOSliceDecodeTest, slice_decode_rejects_size_mismatch) {
    const std::string body = "size-mismatch";
    const std::string encoded = encode_page(Slice(body), data_page_footer(body.size()));
    BufferFileReader reader("page_io_slice_size_mismatch", encoded);
    OlapReaderStatistics stats;
    PageReadOptions options = page_read_options(&reader, &stats);
    PageHandle handle;
    Slice decoded_body;
    PageFooterPB footer;

    Status status = PageIO::decode_page_from_slice(
            options, Slice(encoded.data(), encoded.size() - 1), &handle, &decoded_body, &footer);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(reader.read_count(), 0);
}

TEST_F(PageIOSliceDecodeTest, slice_decode_rejects_checksum_mismatch) {
    const std::string body = "checksum-mismatch";
    std::string encoded = encode_page(Slice(body), data_page_footer(body.size()));
    encoded[0] ^= 1;
    BufferFileReader reader("page_io_slice_checksum_mismatch", encoded);
    OlapReaderStatistics stats;
    PageReadOptions options = page_read_options(&reader, &stats);
    PageHandle handle;
    Slice decoded_body;
    PageFooterPB footer;

    Status status = PageIO::decode_page_from_slice(options, Slice(encoded), &handle, &decoded_body,
                                                   &footer);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(stats.compressed_bytes_read, encoded.size());
    EXPECT_EQ(reader.read_count(), 0);
}

TEST_F(PageIOSliceDecodeTest, slice_decode_rejects_footer_size_out_of_bounds) {
    const std::string body = "footer-size-out-of-bounds";
    std::string encoded = encode_page(Slice(body), data_page_footer(body.size()));
    encode_fixed32_le(reinterpret_cast<uint8_t*>(encoded.data()) + encoded.size() - 8,
                      static_cast<uint32_t>(encoded.size()));
    encode_fixed32_le(reinterpret_cast<uint8_t*>(encoded.data()) + encoded.size() - 4,
                      crc32c::Crc32c(encoded.data(), encoded.size() - 4));
    BufferFileReader reader("page_io_slice_footer_bounds", encoded);
    OlapReaderStatistics stats;
    PageReadOptions options = page_read_options(&reader, &stats);
    PageHandle handle;
    Slice decoded_body;
    PageFooterPB footer;

    Status status = PageIO::decode_page_from_slice(options, Slice(encoded), &handle, &decoded_body,
                                                   &footer);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_EQ(reader.read_count(), 0);
}

TEST_F(PageIOSliceDecodeTest, slice_decode_inserts_page_cache_for_existing_read_path) {
    const std::string body = "page-cache-body";
    const std::string encoded = encode_page(Slice(body), data_page_footer(body.size()));
    BufferFileReader reader("page_io_slice_page_cache", encoded);
    auto* page_cache = StoragePageCache::instance();
    ASSERT_NE(page_cache, nullptr);
    const StoragePageCache::CacheKey cache_key(reader.path().native(), reader.size(), 0);
    page_cache->erase(cache_key, DATA_PAGE);

    OlapReaderStatistics slice_stats;
    PageReadOptions slice_options = page_read_options(&reader, &slice_stats);
    slice_options.use_page_cache = true;
    PageHandle slice_handle;
    Slice slice_body;
    PageFooterPB slice_footer;
    ASSERT_TRUE(PageIO::decode_page_from_slice(slice_options, Slice(encoded), &slice_handle,
                                               &slice_body, &slice_footer)
                        .ok());
    ASSERT_TRUE(PageIO::lookup_page_cache_for_prefetch(slice_options));
    EXPECT_EQ(slice_stats.total_pages_num, 1);
    EXPECT_EQ(slice_stats.cached_pages_num, 0);

    OlapReaderStatistics cached_stats;
    PageReadOptions cached_options = page_read_options(&reader, &cached_stats);
    cached_options.use_page_cache = true;
    PageHandle cached_handle;
    Slice cached_body;
    PageFooterPB cached_footer;
    ASSERT_TRUE(PageIO::read_and_decompress_page(cached_options, &cached_handle, &cached_body,
                                                 &cached_footer)
                        .ok());
    EXPECT_EQ(std::string(cached_body.data, cached_body.size), body);
    EXPECT_EQ(reader.read_count(), 0);
    EXPECT_EQ(cached_stats.total_pages_num, 1);
    EXPECT_EQ(cached_stats.cached_pages_num, 1);
    EXPECT_EQ(cached_stats.compressed_bytes_read, 0);
    EXPECT_EQ(cached_stats.uncompressed_bytes_read, body.size());

    page_cache->erase(cache_key, DATA_PAGE);
}

} // namespace
} // namespace doris::segment_v2
