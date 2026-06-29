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

#include "block_file_cache_test_common.h"

namespace doris::io {

TEST_F(BlockFileCacheTest,
       direct_partial_hit_with_downloaded_remainder_should_not_read_remote_again) {
    std::string local_cache_base_path =
            caches_dir / "cache_direct_partial_downloaded_no_remote_read" / "";
    config::enable_read_cache_file_directly = true;
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    fs::create_directories(local_cache_base_path);

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    io::FileCacheSettings settings;
    settings.query_queue_size = 6291456;
    settings.query_queue_elements = 6;
    settings.index_queue_size = 1048576;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1048576;
    settings.disposable_queue_elements = 1;
    settings.capacity = 8388608;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;

    io::CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.query_id = query_id;
    ASSERT_TRUE(
            FileCacheFactory::instance()->create_file_cache(local_cache_base_path, settings).ok());

    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
        auto seed_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
        std::string buffer(64_kb, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(
                seed_reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
        EXPECT_EQ(bytes_read, 64_kb);
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    }

    FileReaderSPtr stale_local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &stale_local_reader));
    auto stale_reader = std::make_shared<CachedRemoteFileReader>(stale_local_reader, opts);
    EXPECT_EQ(stale_reader->_cache_file_readers.size(), 1);

    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
        auto updater_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
        std::string buffer(64_kb, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(updater_reader
                            ->read_at(1_mb + 100, Slice(buffer.data(), buffer.size()), &bytes_read,
                                      &io_ctx)
                            .ok());
        EXPECT_EQ(bytes_read, 64_kb);
        EXPECT_EQ(std::string(64_kb, '1'), buffer);
    }

    EXPECT_EQ(stale_reader->_cache_file_readers.size(), 1);

    std::string cross_block_buffer(64_kb, '\0');
    IOContext io_ctx;
    FileCacheStatistics stats;
    io_ctx.file_cache_stats = &stats;
    size_t bytes_read {0};
    ASSERT_TRUE(stale_reader
                        ->read_at(1_mb - 100,
                                  Slice(cross_block_buffer.data(), cross_block_buffer.size()),
                                  &bytes_read, &io_ctx)
                        .ok());
    EXPECT_EQ(bytes_read, 64_kb);
    EXPECT_EQ(std::string(100, '0') + std::string(64_kb - 100, '1'), cross_block_buffer);
    EXPECT_EQ(stats.bytes_read_from_remote, 0);

    EXPECT_TRUE(stale_reader->close().ok());
    EXPECT_TRUE(stale_reader->closed());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
    config::enable_read_cache_file_directly = false;
}

TEST_F(BlockFileCacheTest, ReadFileCacheFalseReadsExactRemoteBytesAndDoesNotPopulateCache) {
    std::string local_cache_base_path =
            caches_dir / "cache_read_file_cache_false_exact_remote" / "";
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    fs::create_directories(local_cache_base_path);

    io::FileCacheSettings settings;
    settings.query_queue_size = 6291456;
    settings.query_queue_elements = 6;
    settings.index_queue_size = 1048576;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1048576;
    settings.disposable_queue_elements = 1;
    settings.capacity = 8388608;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;
    ASSERT_TRUE(
            FileCacheFactory::instance()->create_file_cache(local_cache_base_path, settings).ok());
    auto cache = FileCacheFactory::instance()->_path_to_cache[local_cache_base_path];
    wait_until_cache_ready(*cache);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    constexpr size_t read_offset = 123;
    constexpr size_t read_size = 4_kb;
    {
        std::string buffer(read_size, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.read_file_cache = false;
        io_ctx.is_inverted_index = true;
        io_ctx.snii_section_type = SNII_SECTION_META;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read = 0;
        ASSERT_TRUE(reader->read_at(read_offset, Slice(buffer.data(), buffer.size()), &bytes_read,
                                    &io_ctx)
                            .ok());
        EXPECT_EQ(bytes_read, read_size);
        EXPECT_EQ(std::string(read_size, '0'), buffer);
        EXPECT_EQ(stats.bytes_read_from_remote, read_size);
        EXPECT_EQ(stats.remote_physical_read_count, 1);
        EXPECT_EQ(stats.remote_physical_read_bytes, read_size);
        EXPECT_EQ(stats.bytes_write_into_cache, 0);
        EXPECT_EQ(stats.file_cache_blocks_total, 0);
        EXPECT_EQ(stats.num_skip_cache_io_total, 1);
    }

    {
        std::string buffer(read_size, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read = 0;
        ASSERT_TRUE(reader->read_at(read_offset, Slice(buffer.data(), buffer.size()), &bytes_read,
                                    &io_ctx)
                            .ok());
        EXPECT_EQ(bytes_read, read_size);
        EXPECT_EQ(std::string(read_size, '0'), buffer);
        EXPECT_EQ(stats.bytes_read_from_remote, read_size);
        EXPECT_EQ(stats.remote_physical_read_count, 1);
        EXPECT_EQ(stats.remote_physical_read_bytes, 1_mb);
        EXPECT_EQ(stats.bytes_write_into_cache, 1_mb);
        EXPECT_EQ(stats.file_cache_blocks_miss, 1);
    }

    EXPECT_TRUE(reader->close().ok());
    EXPECT_TRUE(reader->closed());
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

} // namespace doris::io
