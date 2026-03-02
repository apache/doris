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
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/slice.h"

namespace doris::io {

namespace {

class MockPatternFileReader final : public FileReader {
public:
    MockPatternFileReader(std::string path, size_t file_size, uint32_t pattern_id)
            : _path(std::move(path)), _size(file_size), _pattern_id(pattern_id) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* /*io_ctx*/) override {
        if (offset >= _size) {
            *bytes_read = 0;
            return Status::OK();
        }
        const size_t readable = std::min(result.size, _size - offset);
        memset(result.data, static_cast<int>('a' + (_pattern_id % 26)), readable);
        *bytes_read = readable;
        return Status::OK();
    }

private:
    Path _path;
    size_t _size;
    uint32_t _pattern_id;
    bool _closed {false};
};

struct LockWaitSummary {
    int64_t total_ns {0};
    int64_t max_ns {0};
    int64_t p50_ns {0};
    int64_t p95_ns {0};
    int64_t p99_ns {0};
    double avg_ns {0.0};
    size_t non_zero_samples {0};
};

int64_t get_percentile_value(const std::vector<int64_t>& sorted_values, double percentile) {
    if (sorted_values.empty()) {
        return 0;
    }
    const double rank = percentile * static_cast<double>(sorted_values.size() - 1);
    return sorted_values[static_cast<size_t>(rank)];
}

LockWaitSummary summarize_lock_wait(std::vector<int64_t>* values) {
    LockWaitSummary summary;
    if (values == nullptr || values->empty()) {
        return summary;
    }
    std::sort(values->begin(), values->end());
    summary.total_ns = std::accumulate(values->begin(), values->end(), int64_t {0});
    summary.max_ns = values->back();
    summary.p50_ns = get_percentile_value(*values, 0.50);
    summary.p95_ns = get_percentile_value(*values, 0.95);
    summary.p99_ns = get_percentile_value(*values, 0.99);
    summary.avg_ns = static_cast<double>(summary.total_ns) / static_cast<double>(values->size());
    summary.non_zero_samples =
            std::count_if(values->begin(), values->end(), [](int64_t v) { return v > 0; });
    return summary;
}

} // namespace

class CachedRemoteFileReaderLockWaitTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto* exec_env = ExecEnv::GetInstance();
        if (exec_env->file_cache_factory() == nullptr) {
            _suite_factory = std::make_unique<FileCacheFactory>();
            exec_env->_file_cache_factory = _suite_factory.get();
            _owns_factory = true;
        }
        if (!exec_env->_file_cache_open_fd_cache) {
            exec_env->_file_cache_open_fd_cache = std::make_unique<FDCache>();
            _owns_fd_cache = true;
        }
    }

    static void TearDownTestSuite() {
        auto* factory = ExecEnv::GetInstance()->file_cache_factory();
        if (factory != nullptr) {
            factory->clear_file_caches(true);
            factory->_caches.clear();
            factory->_path_to_cache.clear();
            factory->_capacity = 0;
        }
        if (_owns_factory) {
            ExecEnv::GetInstance()->_file_cache_factory = nullptr;
            _suite_factory.reset();
            _owns_factory = false;
        }
        if (_owns_fd_cache) {
            ExecEnv::GetInstance()->_file_cache_open_fd_cache.reset(nullptr);
            _owns_fd_cache = false;
        }
    }

    void SetUp() override {
        auto* factory = FileCacheFactory::instance();
        ASSERT_NE(factory, nullptr);
        factory->clear_file_caches(true);
        factory->_caches.clear();
        factory->_path_to_cache.clear();
        factory->_capacity = 0;

        constexpr size_t kCapacityBytes = 512ULL * 1024ULL * 1024ULL;
        auto settings = get_file_cache_settings(kCapacityBytes, 0, DEFAULT_NORMAL_PERCENT,
                                                DEFAULT_DISPOSABLE_PERCENT, DEFAULT_INDEX_PERCENT,
                                                DEFAULT_TTL_PERCENT, "memory");
        ASSERT_TRUE(factory->create_file_cache("memory", settings).ok());
        _cache = factory->get_by_path(std::string("memory"));
        ASSERT_NE(_cache, nullptr);
        for (int i = 0; i < 100; ++i) {
            if (_cache->get_async_open_success()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(_cache->get_async_open_success());
    }

    void TearDown() override {
        auto* factory = FileCacheFactory::instance();
        if (factory != nullptr) {
            factory->clear_file_caches(true);
            factory->_caches.clear();
            factory->_path_to_cache.clear();
            factory->_capacity = 0;
        }
    }

protected:
    inline static std::unique_ptr<FileCacheFactory> _suite_factory;
    inline static bool _owns_factory = false;
    inline static bool _owns_fd_cache = false;
    BlockFileCache* _cache {nullptr};
};

TEST_F(CachedRemoteFileReaderLockWaitTest,
       HighConcurrencyReadViaCachedRemoteFileReaderLockWaitManyFilesMemoryCache) {
    constexpr size_t kFileCount = 2048;
    constexpr size_t kFileSize = 64 * 1024;
    constexpr size_t kWarmupReadBytes = 4 * 1024;
    constexpr size_t kStressReadBytes = 4 * 1024;
    constexpr size_t kOpsPerThread = 2000;
    const size_t hw_threads =
            std::thread::hardware_concurrency() == 0 ? 16 : std::thread::hardware_concurrency();
    const size_t thread_count = std::min<size_t>(48, std::max<size_t>(16, hw_threads));

    const bool original_direct_read = config::enable_read_cache_file_directly;
    config::enable_read_cache_file_directly = false;

    std::vector<std::shared_ptr<CachedRemoteFileReader>> readers;
    readers.reserve(kFileCount);
    std::vector<UInt128Wrapper> cache_keys;
    cache_keys.reserve(kFileCount);

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;

    for (size_t i = 0; i < kFileCount; ++i) {
        std::string path = "/mock/file_" + std::to_string(i) + ".dat";
        auto remote_reader = std::make_shared<MockPatternFileReader>(path, kFileSize, i);
        auto cached_reader = std::make_shared<CachedRemoteFileReader>(remote_reader, opts);
        cache_keys.emplace_back(BlockFileCache::hash(remote_reader->path().filename().native()));
        readers.emplace_back(std::move(cached_reader));
    }

    for (auto& reader : readers) {
        std::array<char, kWarmupReadBytes> buffer {};
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read = 0;
        ASSERT_TRUE(
                reader->read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        ASSERT_EQ(bytes_read, buffer.size());
    }

    size_t populated_keys = 0;
    for (const auto& hash : cache_keys) {
        if (!_cache->get_blocks_by_key(hash).empty()) {
            ++populated_keys;
        }
    }
    EXPECT_EQ(populated_keys, kFileCount);

    std::atomic<size_t> ready_threads {0};
    std::atomic<bool> start_flag {false};
    std::atomic<size_t> failed_reads {0};
    std::vector<std::vector<int64_t>> thread_samples(thread_count);
    std::vector<std::thread> workers;
    workers.reserve(thread_count);

    for (size_t tid = 0; tid < thread_count; ++tid) {
        workers.emplace_back([&, tid] {
            SCOPED_INIT_THREAD_CONTEXT();
            std::mt19937 gen(static_cast<uint32_t>(tid + 20260228));
            std::uniform_int_distribution<size_t> reader_dist(0, kFileCount - 1);
            std::uniform_int_distribution<size_t> offset_dist(0, kFileSize - kStressReadBytes);
            std::array<char, kStressReadBytes> buffer {};
            auto& samples = thread_samples[tid];
            samples.reserve(kOpsPerThread);

            ready_threads.fetch_add(1, std::memory_order_release);
            while (!start_flag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (size_t op = 0; op < kOpsPerThread; ++op) {
                const size_t reader_idx = reader_dist(gen);
                const size_t offset = offset_dist(gen);

                IOContext io_ctx;
                FileCacheStatistics stats;
                io_ctx.file_cache_stats = &stats;
                size_t bytes_read = 0;
                Status st = readers[reader_idx]->read_at(
                        offset, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx);
                if (!st.ok() || bytes_read != kStressReadBytes) {
                    failed_reads.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                samples.push_back(stats.lock_wait_timer);
            }
        });
    }

    while (ready_threads.load(std::memory_order_acquire) < thread_count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    start_flag.store(true, std::memory_order_release);

    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    config::enable_read_cache_file_directly = original_direct_read;

    EXPECT_EQ(failed_reads.load(std::memory_order_relaxed), 0);

    std::vector<int64_t> merged_samples;
    merged_samples.reserve(thread_count * kOpsPerThread);
    for (auto& per_thread : thread_samples) {
        merged_samples.insert(merged_samples.end(), per_thread.begin(), per_thread.end());
    }
    EXPECT_EQ(merged_samples.size(), thread_count * kOpsPerThread);

    LockWaitSummary summary = summarize_lock_wait(&merged_samples);

    LOG(INFO) << "cached_remote_file_reader lock wait summary: samples=" << merged_samples.size()
              << " total_ns=" << summary.total_ns << " avg_ns=" << summary.avg_ns
              << " p50_ns=" << summary.p50_ns << " p95_ns=" << summary.p95_ns
              << " p99_ns=" << summary.p99_ns << " max_ns=" << summary.max_ns
              << " non_zero_samples=" << summary.non_zero_samples;

    EXPECT_GT(summary.total_ns, 0);
    EXPECT_GT(summary.non_zero_samples, 0);
}

} // namespace doris::io
