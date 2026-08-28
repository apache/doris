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

#include "io/cache/range_cache_writeback.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/partial_block_writeback_manager.h"
#include "io/fs/path.h"

namespace doris::io {
namespace {

using namespace std::chrono_literals;

constexpr size_t kBlockSize = 4096;

FileCacheSettings range_writeback_cache_settings() {
    FileCacheSettings settings;
    settings.query_queue_size = 4_mb;
    settings.query_queue_elements = 1024;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 256;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 256;
    settings.capacity = 8_mb;
    settings.max_file_block_size = kBlockSize;
    settings.max_query_cache_size = 0;
    return settings;
}

std::string patterned_file(size_t size) {
    std::string content(size, '\0');
    for (size_t index = 0; index < size; ++index) {
        content[index] = static_cast<char>('a' + index % 23);
    }
    return content;
}

class RecordingFileReader final : public FileReader {
public:
    explicit RecordingFileReader(std::string content) : _content(std::move(content)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const Path& path() const override { return _path; }
    size_t size() const override { return _content.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 0; }

    std::vector<FileRange> reads() const {
        std::lock_guard lock(_mutex);
        return _reads;
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext*) override {
        DORIS_CHECK(offset <= _content.size());
        DORIS_CHECK(result.size <= _content.size() - offset);
        {
            std::lock_guard lock(_mutex);
            _reads.push_back({.offset = offset, .size = result.size});
        }
        std::memcpy(result.data, _content.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    const Path _path {"range_cache_writeback_source"};
    const std::string _content;
    mutable std::mutex _mutex;
    std::vector<FileRange> _reads;
    bool _closed {false};
};

template <typename Predicate>
bool wait_until(Predicate predicate) {
    for (int attempt = 0; attempt < 5000; ++attempt) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return false;
}

bool cache_range_downloaded(BlockFileCache* cache, const UInt128Wrapper& hash, size_t offset,
                            size_t size) {
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    FileBlocks blocks;
    bool fully_covered = false;
    DORIS_CHECK(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context, &blocks,
                                                              &fully_covered)
                        .ok());
    return fully_covered;
}

std::string read_cached_range(BlockFileCache* cache, const UInt128Wrapper& hash, size_t offset,
                              size_t size) {
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    FileBlocks blocks;
    bool fully_covered = false;
    DORIS_CHECK(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context, &blocks,
                                                              &fully_covered)
                        .ok());
    DORIS_CHECK(fully_covered);
    DORIS_CHECK(blocks.size() == 1);
    std::string result(size, '\0');
    DORIS_CHECK(blocks.front()->read(Slice(result), offset - blocks.front()->offset()).ok());
    return result;
}

class RangeCacheWritebackTest : public BlockFileCacheTest {
protected:
    std::unique_ptr<BlockFileCache> create_cache(const std::string& name) {
        auto path = caches_dir / name;
        std::error_code error;
        fs::remove_all(path, error);
        fs::create_directories(path);
        _paths.emplace_back(path);
        auto cache =
                std::make_unique<BlockFileCache>(path.string(), range_writeback_cache_settings());
        EXPECT_TRUE(cache->initialize().ok());
        wait_until_cache_ready(*cache);
        auto* manager = cache->async_write_manager();
        EXPECT_TRUE(manager->start().ok());
        auto options = manager->options();
        options.worker_count = 1;
        options.max_pending_bytes = 8 * kBlockSize;
        EXPECT_TRUE(manager->update_options(options).ok());
        return cache;
    }

    std::unique_ptr<PartialBlockWritebackManager> create_partial_manager() {
        std::unique_ptr<PartialBlockWritebackManager> manager;
        EXPECT_TRUE(PartialBlockWritebackManager::create(
                            {.block_size = kBlockSize,
                             .worker_count = 2,
                             .max_pending_bytes = 8 * kBlockSize,
                             .hole_fill_coalesce = {.max_gap_bytes = 128,
                                                    .max_range_bytes = kBlockSize,
                                                    .max_read_amplification_ratio = 2.0}},
                            &manager)
                            .ok());
        return manager;
    }

    RangeCacheWriteback make_writeback(BlockFileCache* cache,
                                       PartialBlockWritebackManager* partial_manager,
                                       const FileReaderSPtr& source_reader,
                                       const UInt128Wrapper& cache_hash) {
        IOContext io_context;
        return RangeCacheWriteback({
                .write_manager = cache->async_write_manager(),
                .partial_block_manager = partial_manager,
                .inflight_index = cache->inflight_write_buffer_index(),
                .source_reader = source_reader,
                .cache_hash = cache_hash,
                .file_size = source_reader->size(),
                .block_size = kBlockSize,
                .admission_ctx = {},
                .io_context = FileRangeReadIOContext::from_caller(io_context),
        });
    }

    void TearDown() override {
        for (const auto& path : _paths) {
            std::error_code error;
            fs::remove_all(path, error);
        }
    }

private:
    std::vector<fs::path> _paths;
};

TEST_F(RangeCacheWritebackTest, RoutesCompleteAndPartialBlocks) {
    const std::string content = patterned_file(3 * kBlockSize);
    auto source_reader = std::make_shared<RecordingFileReader>(content);
    auto cache = create_cache("range_writeback_mixed");
    auto partial_manager = create_partial_manager();
    const auto hash = BlockFileCache::hash("range_writeback_mixed_file");
    auto writeback = make_writeback(cache.get(), partial_manager.get(), source_reader, hash);
    auto epoch = writeback.capture_write_epoch();
    ASSERT_TRUE(epoch.has_value());
    const FileRange range {.offset = 1024, .size = 2 * kBlockSize};
    const std::string range_data = content.substr(range.offset, range.size);

    const auto result = writeback.submit_consumed_range(range, Slice(range_data), *epoch);

    EXPECT_EQ(result.complete_block_count, 1);
    EXPECT_EQ(result.partial_fragment_count, 2);
    EXPECT_EQ(result.submitted_complete_block_count, 1);
    EXPECT_EQ(result.submitted_partial_fragment_count, 2);
    ASSERT_TRUE(wait_until([&]() { return partial_manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache->async_write_manager()->pending_count() == 0; }));
    for (size_t block_offset = 0; block_offset < content.size(); block_offset += kBlockSize) {
        ASSERT_TRUE(cache_range_downloaded(cache.get(), hash, block_offset, kBlockSize));
        EXPECT_EQ(read_cached_range(cache.get(), hash, block_offset, kBlockSize),
                  content.substr(block_offset, kBlockSize));
    }
    auto reads = source_reader->reads();
    std::ranges::sort(reads, {}, &FileRange::offset);
    EXPECT_EQ(reads, (std::vector<FileRange> {
                             {.offset = 0, .size = 1024},
                             {.offset = 2 * kBlockSize + 1024, .size = kBlockSize - 1024}}));
}

TEST_F(RangeCacheWritebackTest, TreatsPhysicalEofPrefixAsCompleteBlock) {
    const std::string content = patterned_file(2500);
    auto source_reader = std::make_shared<RecordingFileReader>(content);
    auto cache = create_cache("range_writeback_eof");
    auto partial_manager = create_partial_manager();
    const auto hash = BlockFileCache::hash("range_writeback_eof_file");
    auto writeback = make_writeback(cache.get(), partial_manager.get(), source_reader, hash);
    auto epoch = writeback.capture_write_epoch();
    ASSERT_TRUE(epoch.has_value());

    const auto result = writeback.submit_consumed_range({.offset = 0, .size = content.size()},
                                                        Slice(content), *epoch);

    EXPECT_EQ(result.complete_block_count, 1);
    EXPECT_EQ(result.partial_fragment_count, 0);
    EXPECT_EQ(result.submitted_complete_block_count, 1);
    ASSERT_TRUE(wait_until([&]() { return cache->async_write_manager()->pending_count() == 0; }));
    ASSERT_TRUE(cache_range_downloaded(cache.get(), hash, 0, content.size()));
    EXPECT_EQ(read_cached_range(cache.get(), hash, 0, content.size()), content);
    EXPECT_TRUE(source_reader->reads().empty());
}

TEST_F(RangeCacheWritebackTest, RejectsEpochInvalidatedAfterForegroundReadStarts) {
    const std::string content = patterned_file(kBlockSize);
    auto source_reader = std::make_shared<RecordingFileReader>(content);
    auto cache = create_cache("range_writeback_stale_epoch");
    auto partial_manager = create_partial_manager();
    const auto hash = BlockFileCache::hash("range_writeback_stale_epoch_file");
    auto writeback = make_writeback(cache.get(), partial_manager.get(), source_reader, hash);
    auto epoch = writeback.capture_write_epoch();
    ASSERT_TRUE(epoch.has_value());
    cache->async_write_manager()->invalidate_pending_writes(hash);

    const auto result = writeback.submit_consumed_range({.offset = 0, .size = content.size()},
                                                        Slice(content), *epoch);

    EXPECT_EQ(result.complete_block_count, 1);
    EXPECT_EQ(result.submitted_complete_block_count, 0);
    EXPECT_EQ(cache->async_write_manager()->pending_count(), 0);
    EXPECT_FALSE(cache_range_downloaded(cache.get(), hash, 0, content.size()));
}

TEST_F(RangeCacheWritebackTest, DoesNotCaptureEpochWhenCacheWriterIsStopped) {
    const std::string content = patterned_file(kBlockSize);
    auto source_reader = std::make_shared<RecordingFileReader>(content);
    auto cache = create_cache("range_writeback_stopped");
    auto partial_manager = create_partial_manager();
    const auto hash = BlockFileCache::hash("range_writeback_stopped_file");
    auto writeback = make_writeback(cache.get(), partial_manager.get(), source_reader, hash);
    cache->async_write_manager()->shutdown();

    EXPECT_FALSE(writeback.capture_write_epoch().has_value());
}

} // namespace
} // namespace doris::io
