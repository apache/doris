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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "io/fs/buffered_reader.h"
#include "olap/page_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/thrift_util.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_column_chunk_reader.h"
#include "vec/exec/format/parquet/vparquet_page_reader.h"

using namespace doris;
using namespace doris::vectorized;

class FakeBufferedReader : public io::BufferedStreamReader {
public:
    FakeBufferedReader(std::string path, std::vector<uint8_t> data)
            : _path(std::move(path)), _data(std::move(data)) {}
    Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read,
                      const doris::io::IOContext* io_ctx) override {
        if (offset + bytes_to_read > _data.size()) return Status::IOError("Out of bounds");
        *buf = _data.data() + offset;
        return Status::OK();
    }
    Status read_bytes(Slice& slice, uint64_t offset, const doris::io::IOContext* io_ctx) override {
        if (offset + slice.size > _data.size()) return Status::IOError("Out of bounds");
        slice.data = reinterpret_cast<char*>(_data.data() + offset);
        return Status::OK();
    }
    std::string path() override { return _path; }

    int64_t mtime() const override { return 0; }

private:
    std::string _path;
    std::vector<uint8_t> _data;
};

TEST(ParquetPageCacheTest, CacheHitReturnsDecompressedPayload) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // construct thrift PageHeader (uncompressed payload) and payload
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(4);
    header.__set_uncompressed_page_size(4);
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> payload = {0x11, 0x22, 0x33, 0x44};
    std::vector<uint8_t> cached_data;
    cached_data.insert(cached_data.end(), header_bytes.begin(), header_bytes.end());
    cached_data.insert(cached_data.end(), payload.begin(), payload.end());

    std::string path = "test_parquet_cache_file";
    int64_t header_offset = 128;
    // make file_end_offset consistent with reader/page reader end offset used in test
    int64_t file_end_offset = header_offset + static_cast<int64_t>(cached_data.size());

    // insert into cache
    int64_t mtime = 0;
    StoragePageCache::CacheKey key(fmt::format("{}::{}", path, mtime),
                                   static_cast<size_t>(file_end_offset), header_offset);
    size_t total = cached_data.size();
    auto* page = new DataPage(total, true, segment_v2::DATA_PAGE);
    memcpy(page->data(), cached_data.data(), total);
    page->reset_size(total);
    PageCacheHandle handle;
    StoragePageCache::instance()->insert(key, page, &handle, segment_v2::DATA_PAGE);

    // create fake reader and a ColumnChunkReader to verify cache hit
    // ensure the reader contains the same header+payload at the header offset so header parsing succeeds
    std::vector<uint8_t> backing(256, 0);
    memcpy(backing.data() + header_offset, cached_data.data(), total);
    FakeBufferedReader reader(path, backing);
    // prepare column chunk metadata so ColumnChunkReader uses same offsets
    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(header_offset);
    cc.meta_data.__set_total_compressed_size(total);
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);

    FieldSchema field_schema;
    field_schema.repetition_level = 0;
    field_schema.definition_level = 0;

    ColumnChunkReader<false, false> ccr(&reader, &cc, &field_schema, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr.init().ok());
    // load_page_data should hit the cache and return decompressed payload
    ASSERT_TRUE(ccr.load_page_data().ok());
    Slice s = ccr.get_page_data();
    ASSERT_EQ(s.size, payload.size());
    ASSERT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
    // stats: ensure there was a page read and at least one hit recorded
    auto& statistics = ccr.statistics();
    EXPECT_EQ(statistics.page_read_counter, 1);
    EXPECT_EQ(statistics.page_cache_hit_counter, 1);
    EXPECT_EQ(statistics.page_cache_decompressed_hit_counter, 1);
}

TEST(ParquetPageCacheTest, DecompressedPageInsertedByColumnChunkReader) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;
    // ensure decompressed pages are cached via BE config
    double old_thresh = config::parquet_page_cache_decompress_threshold;
    bool old_enable_compressed = config::enable_parquet_cache_compressed_pages;
    config::parquet_page_cache_decompress_threshold = 100.0;
    config::enable_parquet_cache_compressed_pages = false;

    // construct uncompressed header + payload in file buffer
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(4);
    header.__set_uncompressed_page_size(4);
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> payload = {0x55, 0x66, 0x77, 0x88};
    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), payload.begin(), payload.end());

    std::string path = "test_parquet_insert_file";
    int64_t header_offset = 0;

    FakeBufferedReader reader(path, file_data);

    // prepare column chunk metadata
    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(header_offset);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);

    {
        FieldSchema field_schema;
        field_schema.repetition_level = 0;
        field_schema.definition_level = 0;
        ColumnChunkReader<false, false> ccr(&reader, &cc, &field_schema, nullptr, 0, nullptr, ctx);
        ASSERT_TRUE(ccr.init().ok());
        ASSERT_TRUE(ccr.load_page_data().ok());

        // Now cache should have an entry; verify by creating a fresh ColumnChunkReader and hitting cache
        ColumnChunkReader<false, false> ccr_check(&reader, &cc, &field_schema, nullptr, 0, nullptr,
                                                  ctx);
        ASSERT_TRUE(ccr_check.init().ok());
        // ASSERT_TRUE(ccr_check.next_page().ok());
        ASSERT_TRUE(ccr_check.load_page_data().ok());
        Slice s = ccr_check.get_page_data();
        ASSERT_EQ(s.size, payload.size());
        EXPECT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
        EXPECT_EQ(ccr_check.statistics().page_cache_hit_counter, 1);
    }
    // restore config
    config::parquet_page_cache_decompress_threshold = old_thresh;
    config::enable_parquet_cache_compressed_pages = old_enable_compressed;
}

TEST(ParquetPageCacheTest, V2LevelsPreservedInCache) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;
    // ensure decompressed pages are cached via BE config
    double old_thresh = config::parquet_page_cache_decompress_threshold;
    bool old_enable_compressed = config::enable_parquet_cache_compressed_pages;
    config::parquet_page_cache_decompress_threshold = 100.0;
    config::enable_parquet_cache_compressed_pages = false;

    // construct v2 header + levels + payload in file buffer (uncompressed)
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    int rl = 2;
    int dl = 1;
    int payload_sz = 2;
    header.__set_compressed_page_size(rl + dl + payload_sz);
    header.__set_uncompressed_page_size(rl + dl + payload_sz);
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_repetition_levels_byte_length(rl);
    header.data_page_header_v2.__set_definition_levels_byte_length(dl);
    header.data_page_header_v2.__set_is_compressed(false);
    header.data_page_header_v2.__set_num_values(1);

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> level_bytes = {0x11, 0x22, 0x33};
    std::vector<uint8_t> payload = {0xAA, 0xBB};
    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), level_bytes.begin(), level_bytes.end());
    file_data.insert(file_data.end(), payload.begin(), payload.end());

    std::string path = "test_v2_levels_file";
    FakeBufferedReader reader(path, file_data);

    // prepare column chunk metadata
    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(0);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);

    FieldSchema field_schema;
    field_schema.repetition_level = 0;
    field_schema.definition_level = 0;
    {
        ColumnChunkReader<false, false> ccr(&reader, &cc, &field_schema, nullptr, 0, nullptr, ctx);
        ASSERT_TRUE(ccr.init().ok());
        ASSERT_TRUE(ccr.load_page_data().ok());

        // Now cache should have entry; verify by creating a ColumnChunkReader and hitting cache
        ColumnChunkReader<false, false> ccr_check(&reader, &cc, &field_schema, nullptr, 0, nullptr,
                                                  ctx);
        ASSERT_TRUE(ccr_check.init().ok());
        ASSERT_TRUE(ccr_check.load_page_data().ok());
        Slice s = ccr_check.get_page_data();
        ASSERT_EQ(s.size, payload.size());
        EXPECT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
    }

    // Verify that a fresh ColumnChunkReader reusing cache gets level bytes preserved
    FieldSchema field_schema2;
    field_schema2.repetition_level = 2; // v2 levels present
    field_schema2.definition_level = 1;
    ColumnChunkReader<false, false> ccr2(&reader, &cc, &field_schema2, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr2.init().ok());
    ASSERT_TRUE(ccr2.load_page_data().ok());
    // Level slices should equal the original level bytes
    const Slice& rep = ccr2.v2_rep_levels();
    const Slice& def = ccr2.v2_def_levels();
    auto& statistics = ccr2.statistics();
    EXPECT_GT(statistics.page_cache_hit_counter, 0);
    // because threshold is set to cache decompressed, we should see decompressed hits
    EXPECT_GT(statistics.page_cache_decompressed_hit_counter, 0);
    ASSERT_EQ(def.size, dl);
    EXPECT_EQ(0, memcmp(rep.data, level_bytes.data(), rl));
    EXPECT_EQ(0, memcmp(def.data, level_bytes.data() + rl, dl));
    // restore config
    config::parquet_page_cache_decompress_threshold = old_thresh;
    config::enable_parquet_cache_compressed_pages = old_enable_compressed;
}

TEST(ParquetPageCacheTest, CompressedV1PageCachedAndHit) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // construct compressed v1 header + compressed payload in file buffer
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03, 0x04};

    // compress payload using a block codec
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    faststring compressed_fast;
    std::vector<Slice> inputs;
    inputs.emplace_back(payload.data(), payload.size());
    ASSERT_TRUE(codec->compress(inputs, payload.size(), &compressed_fast).ok());

    header.__set_compressed_page_size(static_cast<int32_t>(compressed_fast.size()));
    header.__set_uncompressed_page_size(static_cast<int32_t>(payload.size()));

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), compressed_fast.data(),
                     compressed_fast.data() + compressed_fast.size());

    std::string path = "test_compressed_v1_file";
    FakeBufferedReader reader(path, file_data);

    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(0);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::SNAPPY);

    FieldSchema field_schema;
    field_schema.repetition_level = 0;
    field_schema.definition_level = 0;

    // Load page to trigger decompression + cache insert
    ColumnChunkReader<false, false> ccr(&reader, &cc, &field_schema, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr.init().ok());
    ASSERT_TRUE(ccr.load_page_data().ok());
    EXPECT_EQ(ccr.statistics().page_cache_write_counter, 1);

    // Now verify a fresh reader hits the cache and returns payload
    ColumnChunkReader<false, false> ccr_check(&reader, &cc, &field_schema, nullptr, 0, nullptr,
                                              ctx);
    ASSERT_TRUE(ccr_check.init().ok());
    // ASSERT_TRUE(ccr_check.next_page().ok());
    ASSERT_TRUE(ccr_check.load_page_data().ok());
    Slice s = ccr_check.get_page_data();
    ASSERT_EQ(s.size, payload.size());
    EXPECT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
    EXPECT_EQ(ccr_check.statistics().page_cache_hit_counter, 1);
}

TEST(ParquetPageCacheTest, CompressedV2LevelsPreservedInCache) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // construct v2 header + levels + compressed payload in file buffer
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE_V2;
    int rl = 2;
    int dl = 1;
    //int payload_sz = 2;
    header.__isset.data_page_header_v2 = true;
    header.data_page_header_v2.__set_repetition_levels_byte_length(rl);
    header.data_page_header_v2.__set_definition_levels_byte_length(dl);
    header.data_page_header_v2.__set_is_compressed(true);
    header.data_page_header_v2.__set_num_values(1);

    std::vector<uint8_t> level_bytes = {0x11, 0x22, 0x33};
    std::vector<uint8_t> payload = {0xAA, 0xBB};

    // compress payload
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    faststring compressed_fast;
    std::vector<Slice> inputs;
    inputs.emplace_back(payload.data(), payload.size());
    ASSERT_TRUE(codec->compress(inputs, payload.size(), &compressed_fast).ok());

    // compressed page: levels (uncompressed) followed by compressed payload
    std::vector<uint8_t> compressed_page;
    compressed_page.insert(compressed_page.end(), level_bytes.begin(), level_bytes.end());
    compressed_page.insert(compressed_page.end(), compressed_fast.data(),
                           compressed_fast.data() + compressed_fast.size());

    header.__set_compressed_page_size(static_cast<int32_t>(compressed_page.size()));
    header.__set_uncompressed_page_size(static_cast<int32_t>(level_bytes.size() + payload.size()));

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), compressed_page.begin(), compressed_page.end());

    std::string path = "test_compressed_v2_file";
    FakeBufferedReader reader(path, file_data);

    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(0);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::SNAPPY);

    FieldSchema field_schema;
    field_schema.repetition_level = 0;
    field_schema.definition_level = 0;

    // Load page to trigger decompression + cache insert
    ColumnChunkReader<false, false> ccr(&reader, &cc, &field_schema, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr.init().ok());
    ASSERT_TRUE(ccr.load_page_data().ok());
    EXPECT_EQ(ccr.statistics().page_cache_write_counter, 1);

    // Now verify a fresh reader hits the cache and v2 levels are preserved
    FieldSchema field_schema2;
    field_schema2.repetition_level = rl;
    field_schema2.definition_level = dl;
    ColumnChunkReader<false, false> ccr_check(&reader, &cc, &field_schema2, nullptr, 0, nullptr,
                                              ctx);
    ASSERT_TRUE(ccr_check.init().ok());
    ASSERT_TRUE(ccr_check.load_page_data().ok());
    Slice s = ccr_check.get_page_data();
    ASSERT_EQ(s.size, payload.size());
    EXPECT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
    const Slice& rep = ccr_check.v2_rep_levels();
    const Slice& def = ccr_check.v2_def_levels();
    ASSERT_EQ(rep.size, rl);
    ASSERT_EQ(def.size, dl);
    // cached v2 page is stored decompressed (threshold=100), make sure counter reflects it
    EXPECT_GT(ccr_check.statistics().page_cache_decompressed_hit_counter, 0);
    EXPECT_EQ(0, memcmp(rep.data, level_bytes.data(), rl));
    EXPECT_EQ(0, memcmp(def.data, level_bytes.data() + rl, dl));
}

TEST(ParquetPageCacheTest, MultiPagesMixedV1V2CacheHit) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // Prepare a v1 uncompressed page and a v2 uncompressed page and insert both into cache
    std::string path = "test_multi_pages_file";

    // v1 page
    tparquet::PageHeader hdr1;
    hdr1.type = tparquet::PageType::DATA_PAGE;
    hdr1.__set_compressed_page_size(4);
    hdr1.__set_uncompressed_page_size(4);
    hdr1.__isset.data_page_header = true;
    hdr1.data_page_header.__set_num_values(1);
    std::vector<uint8_t> header1_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&hdr1, &header1_bytes).ok());
    std::vector<uint8_t> payload1 = {0x10, 0x20, 0x30, 0x40};
    std::vector<uint8_t> cached1;
    cached1.insert(cached1.end(), header1_bytes.begin(), header1_bytes.end());
    cached1.insert(cached1.end(), payload1.begin(), payload1.end());

    // v2 page
    tparquet::PageHeader hdr2;
    hdr2.type = tparquet::PageType::DATA_PAGE_V2;
    int rl = 2;
    int dl = 1;
    int payload2_sz = 2;
    hdr2.__set_compressed_page_size(rl + dl + payload2_sz);
    hdr2.__set_uncompressed_page_size(rl + dl + payload2_sz);
    hdr2.__isset.data_page_header_v2 = true;
    hdr2.data_page_header_v2.__set_repetition_levels_byte_length(rl);
    hdr2.data_page_header_v2.__set_definition_levels_byte_length(dl);
    hdr2.data_page_header_v2.__set_is_compressed(false);
    hdr2.data_page_header_v2.__set_num_values(1);
    std::vector<uint8_t> header2_bytes;
    ASSERT_TRUE(ts.serialize(&hdr2, &header2_bytes).ok());
    std::vector<uint8_t> level_bytes = {0x11, 0x22, 0x33};
    std::vector<uint8_t> payload2 = {0xAA, 0xBB};
    std::vector<uint8_t> cached2;
    cached2.insert(cached2.end(), header2_bytes.begin(), header2_bytes.end());
    cached2.insert(cached2.end(), level_bytes.begin(), level_bytes.end());
    cached2.insert(cached2.end(), payload2.begin(), payload2.end());

    // Insert both pages into cache under different header offsets
    size_t total1 = cached1.size();
    auto* page1 = new DataPage(total1, true, segment_v2::DATA_PAGE);
    memcpy(page1->data(), cached1.data(), total1);
    page1->reset_size(total1);
    PageCacheHandle h1;
    size_t header1_start = 128;
    int64_t mtime = 0;
    StoragePageCache::CacheKey key1(fmt::format("{}::{}", path, mtime),
                                    static_cast<size_t>(header1_start + total1), header1_start);
    StoragePageCache::instance()->insert(key1, page1, &h1, segment_v2::DATA_PAGE);

    size_t total2 = cached2.size();
    auto* page2 = new DataPage(total2, true, segment_v2::DATA_PAGE);
    memcpy(page2->data(), cached2.data(), total2);
    page2->reset_size(total2);
    PageCacheHandle h2;
    size_t header2_start = 256;
    StoragePageCache::CacheKey key2(fmt::format("{}::{}", path, mtime),
                                    static_cast<size_t>(header2_start + total2), header2_start);
    StoragePageCache::instance()->insert(key2, page2, &h2, segment_v2::DATA_PAGE);

    // Now create readers that would lookup those cache keys
    // Reader1 must expose header+page bytes at offset header1_start
    std::vector<uint8_t> reader_backing1(3000, 0);
    memcpy(reader_backing1.data() + header1_start, cached1.data(), total1);
    FakeBufferedReader reader1(path, reader_backing1);
    tparquet::ColumnChunk cc1;
    cc1.meta_data.__set_data_page_offset(128);
    cc1.meta_data.__set_total_compressed_size(total1);
    cc1.meta_data.__set_num_values(1);
    cc1.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    FieldSchema field_schema1;
    field_schema1.repetition_level = 0;
    field_schema1.definition_level = 0;
    ColumnChunkReader<false, false> ccr1(&reader1, &cc1, &field_schema1, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr1.init().ok());
    ASSERT_TRUE(ccr1.load_page_data().ok());
    Slice s1 = ccr1.get_page_data();
    ASSERT_EQ(s1.size, payload1.size());
    EXPECT_EQ(0, memcmp(s1.data, payload1.data(), payload1.size()));

    std::vector<uint8_t> reader_backing2(3000, 0);
    memcpy(reader_backing2.data() + header2_start, cached2.data(), total2);
    FakeBufferedReader reader2(path, reader_backing2);
    tparquet::ColumnChunk cc2;
    cc2.meta_data.__set_data_page_offset(256);
    cc2.meta_data.__set_total_compressed_size(total2);
    cc2.meta_data.__set_num_values(1);
    cc2.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    FieldSchema field_schema2;
    field_schema2.repetition_level = rl;
    field_schema2.definition_level = dl;
    ColumnChunkReader<false, false> ccr2(&reader2, &cc2, &field_schema2, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr2.init().ok());
    ASSERT_TRUE(ccr2.load_page_data().ok());
    Slice s2 = ccr2.get_page_data();
    ASSERT_EQ(s2.size, payload2.size());
    EXPECT_EQ(0, memcmp(s2.data, payload2.data(), payload2.size()));
    const Slice& rep = ccr2.v2_rep_levels();
    const Slice& def = ccr2.v2_def_levels();
    ASSERT_EQ(rep.size, rl);
    ASSERT_EQ(def.size, dl);
    EXPECT_EQ(0, memcmp(rep.data, level_bytes.data(), rl));
    EXPECT_EQ(0, memcmp(def.data, level_bytes.data() + rl, dl));
}

TEST(ParquetPageCacheTest, CacheMissThenHit) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // uncompressed v1 page
    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(4);
    header.__set_uncompressed_page_size(4);
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);
    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());
    std::vector<uint8_t> payload = {0xDE, 0xAD, 0xBE, 0xEF};
    std::vector<uint8_t> backing(256, 0);
    std::vector<uint8_t> cached;
    cached.insert(cached.end(), header_bytes.begin(), header_bytes.end());
    cached.insert(cached.end(), payload.begin(), payload.end());
    int64_t header_offset = 64;
    memcpy(backing.data() + header_offset, cached.data(), cached.size());

    std::string path = "test_miss_then_hit";
    FakeBufferedReader reader(path, backing);

    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(header_offset);
    cc.meta_data.__set_total_compressed_size(cached.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);

    FieldSchema fs;
    fs.repetition_level = 0;
    fs.definition_level = 0;

    // First reader: should not hit cache, but should write cache
    ColumnChunkReader<false, false> ccr(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr.init().ok());
    ASSERT_TRUE(ccr.load_page_data().ok());
    auto& statistics = ccr.statistics();
    EXPECT_EQ(statistics.page_cache_hit_counter, 0);
    EXPECT_EQ(statistics.page_cache_write_counter, 1);

    // Second reader: should hit cache
    ColumnChunkReader<false, false> ccr2(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr2.init().ok());
    ASSERT_TRUE(ccr2.load_page_data().ok());
    auto& statistics2 = ccr2.statistics();
    EXPECT_EQ(statistics2.page_cache_hit_counter, 1);
    EXPECT_EQ(statistics2.page_cache_decompressed_hit_counter, 1);
}

TEST(ParquetPageCacheTest, DecompressThresholdCachesCompressed) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // prepare a compressible payload (lots of zeros)
    std::vector<uint8_t> payload(1024, 0);

    // compress payload using snappy
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    faststring compressed_fast;
    std::vector<Slice> inputs;
    inputs.emplace_back(payload.data(), payload.size());
    ASSERT_TRUE(codec->compress(inputs, payload.size(), &compressed_fast).ok());

    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(static_cast<int32_t>(compressed_fast.size()));
    header.__set_uncompressed_page_size(static_cast<int32_t>(payload.size()));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), compressed_fast.data(),
                     compressed_fast.data() + compressed_fast.size());

    std::string path = "test_threshold_file_compressed";
    FakeBufferedReader reader(path, file_data);

    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(0);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::SNAPPY);

    FieldSchema fs;
    fs.repetition_level = 0;
    fs.definition_level = 0;

    // Case: very small threshold -> cache the compressed payload (smaller footprint)
    double old_thresh = config::parquet_page_cache_decompress_threshold;
    bool old_enable_compressed = config::enable_parquet_cache_compressed_pages;
    config::parquet_page_cache_decompress_threshold = 0.1;
    config::enable_parquet_cache_compressed_pages = true;
    ColumnChunkReader<false, false> ccr_small_thresh(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr_small_thresh.init().ok());
    // ASSERT_TRUE(ccr_small_thresh.next_page().ok());
    ASSERT_TRUE(ccr_small_thresh.load_page_data().ok());
    EXPECT_EQ(ccr_small_thresh.statistics().page_cache_write_counter, 1);

    // Inspect cache entry: payload stored should be compressed size
    PageCacheHandle handle_small;
    size_t file_end = header_bytes.size() + compressed_fast.size();
    int64_t mtime = 0;
    StoragePageCache::CacheKey key_small(fmt::format("{}::{}", path, mtime),
                                         /*file_end_offset*/ file_end, /*header_start*/ 0);
    bool found_small =
            StoragePageCache::instance()->lookup(key_small, &handle_small, segment_v2::DATA_PAGE);
    ASSERT_TRUE(found_small);
    Slice cached_small = handle_small.data();
    size_t header_size = header_bytes.size();
    size_t payload_in_cache_size = cached_small.size - header_size; // no levels here
    ASSERT_EQ(payload_in_cache_size, compressed_fast.size());

    // restore config
    config::parquet_page_cache_decompress_threshold = old_thresh;
    config::enable_parquet_cache_compressed_pages = old_enable_compressed;
}

TEST(ParquetPageCacheTest, DecompressThresholdCachesDecompressed) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;

    // prepare a compressible payload (lots of zeros)
    std::vector<uint8_t> payload(1024, 0);

    // compress payload using snappy
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::CompressionTypePB::SNAPPY, &codec).ok());
    faststring compressed_fast;
    std::vector<Slice> inputs;
    inputs.emplace_back(payload.data(), payload.size());
    ASSERT_TRUE(codec->compress(inputs, payload.size(), &compressed_fast).ok());

    tparquet::PageHeader header;
    header.type = tparquet::PageType::DATA_PAGE;
    header.__set_compressed_page_size(static_cast<int32_t>(compressed_fast.size()));
    header.__set_uncompressed_page_size(static_cast<int32_t>(payload.size()));
    header.__isset.data_page_header = true;
    header.data_page_header.__set_num_values(1);

    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&header, &header_bytes).ok());

    std::vector<uint8_t> file_data;
    file_data.insert(file_data.end(), header_bytes.begin(), header_bytes.end());
    file_data.insert(file_data.end(), compressed_fast.data(),
                     compressed_fast.data() + compressed_fast.size());

    std::string path = "test_threshold_file_decompressed";
    FakeBufferedReader reader(path, file_data);

    tparquet::ColumnChunk cc;
    cc.meta_data.__set_data_page_offset(0);
    cc.meta_data.__set_total_compressed_size(file_data.size());
    cc.meta_data.__set_num_values(1);
    cc.meta_data.__set_codec(tparquet::CompressionCodec::SNAPPY);

    FieldSchema fs;
    fs.repetition_level = 0;
    fs.definition_level = 0;

    // Case: very large threshold -> cache decompressed payload
    double old_thresh = config::parquet_page_cache_decompress_threshold;
    bool old_enable_compressed = config::enable_parquet_cache_compressed_pages;
    config::parquet_page_cache_decompress_threshold = 100.0;
    config::enable_parquet_cache_compressed_pages = false;
    ColumnChunkReader<false, false> ccr_large_thresh(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr_large_thresh.init().ok());
    // ASSERT_TRUE(ccr_large_thresh.next_page().ok());
    ASSERT_TRUE(ccr_large_thresh.load_page_data().ok());
    EXPECT_EQ(ccr_large_thresh.statistics().page_cache_write_counter, 1);

    // Inspect cache entry for large threshold: payload stored should be uncompressed size
    PageCacheHandle handle_large;
    size_t file_end = header_bytes.size() + compressed_fast.size();
    int64_t mtime = 0;
    StoragePageCache::CacheKey key_large(fmt::format("{}::{}", path, mtime),
                                         /*file_end_offset*/ file_end, /*header_start*/ 0);
    bool found_large =
            StoragePageCache::instance()->lookup(key_large, &handle_large, segment_v2::DATA_PAGE);
    ASSERT_TRUE(found_large);
    Slice cached_large = handle_large.data();
    size_t payload_in_cache_size_large = cached_large.size - header_bytes.size();
    ASSERT_EQ(payload_in_cache_size_large, payload.size());

    // Verify cache hit for a new reader (should hit the decompressed entry we just created)
    ColumnChunkReader<false, false> ccr_check(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
    ASSERT_TRUE(ccr_check.init().ok());
    // ASSERT_TRUE(ccr_check.next_page().ok());
    ASSERT_TRUE(ccr_check.load_page_data().ok());
    EXPECT_EQ(ccr_check.statistics().page_cache_hit_counter, 1);
    // restore config
    config::parquet_page_cache_decompress_threshold = old_thresh;
    config::enable_parquet_cache_compressed_pages = old_enable_compressed;
}

TEST(ParquetPageCacheTest, MultipleReadersShareCachedEntry) {
    ParquetPageReadContext ctx;
    ctx.enable_parquet_file_page_cache = true;
    double old_thresh = config::parquet_page_cache_decompress_threshold;
    bool old_enable_compressed = config::enable_parquet_cache_compressed_pages;
    config::parquet_page_cache_decompress_threshold = 100.0;
    config::enable_parquet_cache_compressed_pages = false;

    // Create a v2 cached page and then instantiate multiple readers that hit the cache
    std::string path = "test_shared_handles";
    tparquet::PageHeader hdr;
    hdr.type = tparquet::PageType::DATA_PAGE_V2;
    int rl = 2;
    int dl = 1;
    hdr.__isset.data_page_header_v2 = true;
    hdr.data_page_header_v2.__set_repetition_levels_byte_length(rl);
    hdr.data_page_header_v2.__set_definition_levels_byte_length(dl);
    hdr.data_page_header_v2.__set_is_compressed(false);
    hdr.data_page_header_v2.__set_num_values(1);
    std::vector<uint8_t> header_bytes;
    ThriftSerializer ts(/*compact*/ true, /*initial*/ 256);
    ASSERT_TRUE(ts.serialize(&hdr, &header_bytes).ok());
    std::vector<uint8_t> level_bytes = {0x11, 0x22, 0x33};
    std::vector<uint8_t> payload = {0x0A, 0x0B};
    std::vector<uint8_t> cached;
    cached.insert(cached.end(), header_bytes.begin(), header_bytes.end());
    cached.insert(cached.end(), level_bytes.begin(), level_bytes.end());
    cached.insert(cached.end(), payload.begin(), payload.end());

    size_t total = cached.size();
    auto* page = new DataPage(total, true, segment_v2::DATA_PAGE);
    memcpy(page->data(), cached.data(), total);
    page->reset_size(total);
    PageCacheHandle handle;
    size_t header_start = 512;
    int64_t mtime = 0;
    StoragePageCache::CacheKey key(fmt::format("{}::{}", path, mtime),
                                   static_cast<size_t>(header_start + total), header_start);
    StoragePageCache::instance()->insert(key, page, &handle, segment_v2::DATA_PAGE);

    // Create multiple readers that will hit cache
    const int N = 4;
    for (int i = 0; i < N; ++i) {
        std::vector<uint8_t> reader_backing(5000, 0);
        memcpy(reader_backing.data() + header_start, cached.data(), total);
        FakeBufferedReader reader(path, reader_backing);
        tparquet::ColumnChunk cc;
        cc.meta_data.__set_data_page_offset(512);
        cc.meta_data.__set_total_compressed_size(total);
        cc.meta_data.__set_num_values(1);
        cc.meta_data.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        FieldSchema fs;
        fs.repetition_level = rl;
        fs.definition_level = dl;
        ColumnChunkReader<false, false> ccr(&reader, &cc, &fs, nullptr, 0, nullptr, ctx);
        ASSERT_TRUE(ccr.init().ok());
        ASSERT_TRUE(ccr.load_page_data().ok());
        Slice s = ccr.get_page_data();
        ASSERT_EQ(s.size, payload.size());
        EXPECT_EQ(0, memcmp(s.data, payload.data(), payload.size()));
        const Slice& rep = ccr.v2_rep_levels();
        const Slice& def = ccr.v2_def_levels();
        ASSERT_EQ(rep.size, rl);
        ASSERT_EQ(def.size, dl);
    }
    // restore config
    config::parquet_page_cache_decompress_threshold = old_thresh;
    config::enable_parquet_cache_compressed_pages = old_enable_compressed;
}
