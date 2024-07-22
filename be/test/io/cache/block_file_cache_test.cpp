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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/tests/gtest_lru_file_cache.cpp
// and modified by Doris

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>
#if defined(__APPLE__)
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <ranges>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/path.h"
#include "olap/options.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris::io {

extern int disk_used_percentage(const std::string& path, std::pair<int, int>* percent);

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";
std::string tmp_file = caches_dir / "tmp_file";

constexpr unsigned long long operator"" _mb(unsigned long long m) {
    return m * 1024 * 1024;
}

constexpr unsigned long long operator"" _kb(unsigned long long m) {
    return m * 1024;
}

void assert_range([[maybe_unused]] size_t assert_n, io::FileBlockSPtr file_block,
                  const io::FileBlock::Range& expected_range, io::FileBlock::State expected_state) {
    auto range = file_block->range();

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_block->state(), expected_state);
}

std::vector<io::FileBlockSPtr> fromHolder(const io::FileBlocksHolder& holder) {
    return std::vector<io::FileBlockSPtr>(holder.file_blocks.begin(), holder.file_blocks.end());
}

void download(io::FileBlockSPtr file_block, size_t size = 0) {
    const auto& hash = file_block->get_hash_value();
    if (size == 0) {
        size = file_block->range().size();
    }

    std::string data(size, '0');
    Slice result(data.data(), size);
    EXPECT_TRUE(file_block->append(result).ok());
    EXPECT_TRUE(file_block->finalize().ok());
    auto key_str = hash.to_string();
    auto subdir = FSFileCacheStorage::USE_CACHE_VERSION2
                          ? fs::path(cache_base_path) / key_str.substr(0, 3) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()))
                          : fs::path(cache_base_path) /
                                    (key_str + "_" + std::to_string(file_block->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
}

void complete(const io::FileBlocksHolder& holder) {
    for (const auto& file_block : holder.file_blocks) {
        ASSERT_TRUE(file_block->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(file_block);
    }
}

class BlockFileCacheTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        config::enable_ttl_cache_evict_using_lru = false;
        bool exists {false};
        ASSERT_TRUE(global_local_filesystem()->exists(caches_dir, &exists).ok());
        if (!exists) {
            ASSERT_TRUE(global_local_filesystem()->create_directory(caches_dir).ok());
        }
        ASSERT_TRUE(global_local_filesystem()->exists(tmp_file, &exists).ok());
        if (!exists) {
            FileWriterPtr writer;
            ASSERT_TRUE(global_local_filesystem()->create_file(tmp_file, &writer).ok());
            for (int i = 0; i < 10; i++) {
                std::string data(1_mb, '0' + i);
                ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            }
            std::string data(1, '0');
            ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            ASSERT_TRUE(writer->close().ok());
        }
        ExecEnv::GetInstance()->_file_cache_factory = factory.get();
        ExecEnv::GetInstance()->_file_cache_open_fd_cache = std::make_unique<io::FDCache>();
    }
    static void TearDownTestSuite() {
        config::file_cache_enter_disk_resource_limit_mode_percent = 90;
        ExecEnv::GetInstance()->_file_cache_open_fd_cache.reset(nullptr);
    }

private:
    inline static std::unique_ptr<FileCacheFactory> factory = std::make_unique<FileCacheFactory>();
};

TEST_F(BlockFileCacheTest, init) {
    std::string string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        },
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : 193273528320,
            "query_limit" : 38654705664
        }
        ]
        )");
    config::enable_file_cache_query_limit = true;
    std::vector<CachePath> cache_paths;
    EXPECT_TRUE(parse_conf_cache_paths(string, cache_paths));
    EXPECT_EQ(cache_paths.size(), 2);
    for (const auto& cache_path : cache_paths) {
        io::FileCacheSettings settings = cache_path.init_settings();
        EXPECT_EQ(settings.capacity, 193273528320);
        EXPECT_EQ(settings.max_query_cache_size, 38654705664);
    }

    // err normal
    std::string err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "total_size" : "193273528320",
            "query_limit" : 38654705664
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));

    // err query_limit
    err_string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "normal" : 193273528320,
            "persistent" : 193273528320,
            "query_limit" : "38654705664"
        }
        ]
        )");
    cache_paths.clear();
    EXPECT_FALSE(parse_conf_cache_paths(err_string, cache_paths));
}

void test_file_cache(io::FileCacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
    case io::FileCacheType::INDEX:
        settings.index_queue_elements = 5;
        settings.index_queue_size = 30;
        break;
    case io::FileCacheType::NORMAL:
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        break;
    case io::FileCacheType::DISPOSABLE:
        settings.disposable_queue_size = 30;
        settings.disposable_queue_elements = 5;
        break;
    default:
        break;
    }
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key = io::BlockFileCache::hash("key1");
    {
        io::BlockFileCache mgr(cache_base_path, settings);
        ASSERT_TRUE(mgr.initialize().ok());

        for (int i = 0; i < 100; i++) {
            if (mgr.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            auto holder = mgr.get_or_set(key, 0, 10, context); /// Add range [0, 9]
            auto blocks = fromHolder(holder);
            /// Range was not present in mgr. It should be added in mgr as one while file block.
            ASSERT_EQ(blocks.size(), 1);
            assert_range(1, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            assert_range(2, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADING);
            download(blocks[0]);
            assert_range(3, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        /// Current mgr:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 1);
        ASSERT_EQ(mgr.get_used_cache_size(cache_type), 10);
        {
            /// Want range [5, 14], but [0, 9] already in mgr, so only [10, 14] will be put in mgr.
            auto holder = mgr.get_or_set(key, 5, 10, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 2);

            assert_range(4, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(5, blocks[1], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[1]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[1]);
            assert_range(6, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }

        /// Current mgr:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 2);
        ASSERT_EQ(mgr.get_used_cache_size(cache_type), 15);

        {
            auto holder = mgr.get_or_set(key, 9, 1, context); /// Get [9, 9]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(7, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = mgr.get_or_set(key, 9, 2, context); /// Get [9, 10]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 2);
            assert_range(8, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(9, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        {
            auto holder = mgr.get_or_set(key, 10, 1, context); /// Get [10, 10]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(10, blocks[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
        }
        complete(mgr.get_or_set(key, 17, 4, context)); /// Get [17, 20]
        complete(mgr.get_or_set(key, 24, 3, context)); /// Get [24, 26]

        /// Current mgr:    [__________][_____]   [____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 4);
        ASSERT_EQ(mgr.get_used_cache_size(cache_type), 22);
        {
            auto holder = mgr.get_or_set(key, 0, 31, context); /// Get [0, 25]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 7);
            assert_range(11, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(12, blocks[1], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            /// Missing [15, 16] should be added in mgr.
            assert_range(13, blocks[2], io::FileBlock::Range(15, 16), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[2]);

            assert_range(14, blocks[3], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(15, blocks[4], io::FileBlock::Range(21, 23), io::FileBlock::State::EMPTY);

            assert_range(16, blocks[5], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(16, blocks[6], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
            /// Current mgr:    [__________][_____][   ][____________]
            ///                   ^                       ^            ^
            ///                   0                        20          26
            ///

            /// Range [27, 30] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned blocks from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in mgr should fail.
            /// This will also check that [27, 27] was indeed evicted.

            auto holder1 = mgr.get_or_set(key, 27, 4, context);
            auto blocks_1 = fromHolder(holder1); /// Get [27, 30]
            ASSERT_EQ(blocks_1.size(), 1);
            assert_range(17, blocks_1[0], io::FileBlock::Range(27, 30),
                         io::FileBlock::State::SKIP_CACHE);
        }
        /// Current mgr:    [__________][_____][_][____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        {
            auto holder = mgr.get_or_set(key, 12, 10, context); /// Get [12, 21]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 4);

            assert_range(18, blocks[0], io::FileBlock::Range(10, 14),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(19, blocks[1], io::FileBlock::Range(15, 16),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(20, blocks[2], io::FileBlock::Range(17, 20),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(21, blocks[3], io::FileBlock::Range(21, 21), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[3]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[3]);
            ASSERT_TRUE(blocks[3]->state() == io::FileBlock::State::DOWNLOADED);
        }
        /// Current mgr:    [__________][_____][_][____][_]    [___]
        ///                   ^          ^^     ^   ^    ^       ^   ^
        ///                   0          910    14  17   20      24  26

        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 6);
        {
            auto holder = mgr.get_or_set(key, 23, 5, context); /// Get [23, 28]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 3);

            assert_range(22, blocks[0], io::FileBlock::Range(23, 23), io::FileBlock::State::EMPTY);
            assert_range(23, blocks[1], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(24, blocks[2], io::FileBlock::Range(27, 27), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[0]);
            download(blocks[2]);
        }
        /// Current mgr:    [__________][_____][_][____][_]  [_][___][_]
        ///                   ^          ^^     ^   ^    ^        ^   ^
        ///                   0          910    14  17   20       24  26

        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 8);
        {
            auto holder5 = mgr.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assert_range(25, s5[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);

            auto holder1 = mgr.get_or_set(key, 30, 2, context); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assert_range(26, s1[0], io::FileBlock::Range(30, 31), io::FileBlock::State::EMPTY);

            ASSERT_TRUE(s1[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(s1[0]);

            /// Current mgr:    [__________][_____][_][____][_]  [_][___][_]    [__]
            ///                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
            ///                   0          910    14  17   20       24  26 27   30 31

            auto holder2 = mgr.get_or_set(key, 23, 1, context); /// Get [23, 23]
            auto s2 = fromHolder(holder2);
            ASSERT_EQ(s2.size(), 1);

            auto holder3 = mgr.get_or_set(key, 24, 3, context); /// Get [24, 26]
            auto s3 = fromHolder(holder3);
            ASSERT_EQ(s3.size(), 1);

            auto holder4 = mgr.get_or_set(key, 27, 1, context); /// Get [27, 27]
            auto s4 = fromHolder(holder4);
            ASSERT_EQ(s4.size(), 1);

            /// All mgr is now unreleasable because pointers are still hold
            auto holder6 = mgr.get_or_set(key, 0, 40, context);
            auto f = fromHolder(holder6);
            ASSERT_EQ(f.size(), 12);

            assert_range(29, f[9], io::FileBlock::Range(28, 29), io::FileBlock::State::SKIP_CACHE);
            assert_range(30, f[11], io::FileBlock::Range(32, 39), io::FileBlock::State::SKIP_CACHE);
        }
        {
            auto holder = mgr.get_or_set(key, 2, 3, context); /// Get [2, 4]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(31, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);
        }
        // Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
        //                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
        //                   0          910    14  17   20       24  26 27   30 31

        {
            auto holder = mgr.get_or_set(key, 25, 5, context); /// Get [25, 29]
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 3);

            assert_range(32, blocks[0], io::FileBlock::Range(24, 26),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(33, blocks[1], io::FileBlock::Range(27, 27),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(34, blocks[2], io::FileBlock::Range(28, 29), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[2]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        mgr.get_or_set(key, 25, 5, other_context); /// Get [25, 29] once again.
                auto blocks_2 = fromHolder(holder_2);
                ASSERT_EQ(blocks.size(), 3);

                assert_range(35, blocks_2[0], io::FileBlock::Range(24, 26),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(36, blocks_2[1], io::FileBlock::Range(27, 27),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(37, blocks_2[2], io::FileBlock::Range(28, 29),
                             io::FileBlock::State::DOWNLOADING);

                ASSERT_TRUE(blocks[2]->get_or_set_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (blocks_2[2]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(blocks_2[2]->state() == io::FileBlock::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            download(blocks[2]);
            ASSERT_TRUE(blocks[2]->state() == io::FileBlock::State::DOWNLOADED);

            other_1.join();
        }
        ASSERT_EQ(mgr.get_file_blocks_num(cache_type), 9);
        // Current cache:    [__________][_____][_][____][_]  [_][___][_]    [__]
        //                   ^          ^^     ^   ^    ^        ^   ^  ^    ^  ^
        //                   0          910    14  17   20       24  26 27   30 31

        {
            /// Now let's check the similar case but getting ERROR state after block->wait(), when
            /// state is changed not manually via block->complete(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<io::FileBlocksHolder> holder;
            holder.emplace(mgr.get_or_set(key, 3, 23, context)); /// Get [3, 25]

            auto blocks = fromHolder(*holder);
            ASSERT_EQ(blocks.size(), 8);

            assert_range(38, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);

            assert_range(39, blocks[5], io::FileBlock::Range(22, 22), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[5]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[5]->state() == io::FileBlock::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 =
                        mgr.get_or_set(key, 3, 23, other_context); /// Get [3, 25] once again
                auto blocks_2 = fromHolder(*holder);
                ASSERT_EQ(blocks_2.size(), 8);

                assert_range(41, blocks_2[0], io::FileBlock::Range(0, 9),
                             io::FileBlock::State::DOWNLOADED);
                assert_range(42, blocks_2[5], io::FileBlock::Range(22, 22),
                             io::FileBlock::State::DOWNLOADING);

                ASSERT_TRUE(blocks_2[5]->get_downloader() != io::FileBlock::get_caller_id());
                ASSERT_TRUE(blocks_2[5]->state() == io::FileBlock::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (blocks_2[5]->wait() == io::FileBlock::State::DOWNLOADING) {
                }
                ASSERT_TRUE(blocks_2[5]->state() == io::FileBlock::State::EMPTY);
                ASSERT_TRUE(blocks_2[5]->get_or_set_downloader() == io::FileBlock::get_caller_id());
                download(blocks_2[5]);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }
            holder.reset();
            other_1.join();
            ASSERT_TRUE(blocks[5]->state() == io::FileBlock::State::DOWNLOADED);
        }
    }
    // Current cache:    [__________][_][____][_]  [_][___][_]    [__]
    //                   ^          ^   ^    ^        ^   ^  ^    ^  ^
    //                   0          9  17   20       24  26 27   30 31
    {
        /// Test LRUCache::restore().

        io::BlockFileCache cache2(cache_base_path, settings);
        ASSERT_TRUE(cache2.initialize().ok());
        for (int i = 0; i < 100; i++) {
            if (cache2.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 2, 28, context); /// Get [2, 29]

        auto blocks1 = fromHolder(holder1);
        ASSERT_EQ(blocks1.size(), 10);

        assert_range(44, blocks1[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        assert_range(45, blocks1[1], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        assert_range(45, blocks1[2], io::FileBlock::Range(15, 16),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(46, blocks1[3], io::FileBlock::Range(17, 20),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(47, blocks1[4], io::FileBlock::Range(21, 21),
                     io::FileBlock::State::DOWNLOADED);
    }

    {
        /// Test max file block size
        std::string cache_path2 = caches_dir / "cache2";
        if (fs::exists(cache_path2)) {
            fs::remove_all(cache_path2);
        }
        fs::create_directories(cache_path2);
        auto settings2 = settings;
        settings2.index_queue_elements = 5;
        settings2.index_queue_size = 30;
        settings2.disposable_queue_size = 0;
        settings2.disposable_queue_elements = 0;
        settings2.query_queue_size = 0;
        settings2.query_queue_elements = 0;
        settings2.max_file_block_size = 10;
        io::BlockFileCache cache2(cache_path2, settings2);
        ASSERT_TRUE(cache2.initialize().ok());
        for (int i = 0; i < 100; i++) {
            if (cache2.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto holder1 = cache2.get_or_set(key, 0, 25, context); /// Get [0, 24]
        auto blocks1 = fromHolder(holder1);

        ASSERT_EQ(blocks1.size(), 3);
        assert_range(48, blocks1[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
        assert_range(49, blocks1[1], io::FileBlock::Range(10, 19), io::FileBlock::State::EMPTY);
        assert_range(50, blocks1[2], io::FileBlock::Range(20, 24), io::FileBlock::State::EMPTY);
    }
}

TEST_F(BlockFileCacheTest, normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, resize) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    /// Current cache:    [__________][___][___][_][__]
    ///                   ^          ^      ^    ^  ^ ^
    ///                   0          9      24  26 27  29
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 10;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 0;
    settings.query_queue_elements = 0;
    settings.max_file_block_size = 100;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, max_ttl_size) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 100000000;
    settings.query_queue_elements = 100000;
    settings.capacity = 100000000;
    settings.max_file_block_size = 100000;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 120;
    auto key1 = io::BlockFileCache::hash("key5");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    int i = 0;
    for (; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(cache.get_lazy_open_success());
    int64_t offset = 0;
    for (; offset < 100000000; offset += 100000) {
        auto holder = cache.get_or_set(key1, offset, 100000, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        if (offset < 90000000) {
            assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[0]);
            assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                         io::FileBlock::State::DOWNLOADED);
        } else {
            assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                         io::FileBlock::State::SKIP_CACHE);
        }
        blocks.clear();
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, query_limit_heap_use_after_free) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 16, 9, context); /// Add range [16, 24]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(8, blocks[0], io::FileBlock::Range(16, 24), io::FileBlock::State::SKIP_CACHE);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, query_limit_dcheck) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    context.query_id = query_id;
    auto query_context_holder = cache.get_query_context_holder(query_id);
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(5, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key, 15, 1, context); /// Add range [15, 15]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(6, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(7, blocks[0], io::FileBlock::Range(15, 15), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    // double add
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 30, 5, context); /// Add range [30, 34]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(30, 34), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 40, 5, context); /// Add range [40, 44]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(40, 44), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key, 50, 5, context); /// Add range [50, 54]
        auto blocks = fromHolder(holder);
        ASSERT_GE(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(50, 54), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, reset_range) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    EXPECT_EQ(cache.capacity(), 15);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        ASSERT_TRUE(blocks[0]->is_downloader());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0], 6);
    }
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 2);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 5), io::FileBlock::State::DOWNLOADED);
        assert_range(2, blocks[1], io::FileBlock::Range(6, 8), io::FileBlock::State::EMPTY);
    }
    std::cout << cache.dump_structure(key) << std::endl;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, change_cache_type) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 5;
    settings.index_queue_size = 15;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        size_t size = blocks[0]->range().size();
        std::string data(size, '0');
        Slice result(data.data(), size);
        ASSERT_TRUE(blocks[0]->append(result).ok());
        ASSERT_TRUE(blocks[0]->change_cache_type_self(io::FileCacheType::INDEX));
        ASSERT_TRUE(blocks[0]->finalize().ok());
        auto key_str = key.to_string();
        auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                      (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
        ASSERT_TRUE(fs::exists(subdir / "0_idx"));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, fd_cache_remove) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 9), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 1), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 5), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 10)));
    }
    {
        auto holder = cache.get_or_set(key, 15, 10, context); /// Add range [15, 24]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(15, 24), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(10);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 10), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 15)));
    }
    EXPECT_FALSE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::FDCache::instance()->file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, fd_cache_evict) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    doris::config::enable_file_cache_query_limit = true;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.query_queue_size = 15;
    settings.query_queue_elements = 5;
    settings.max_file_block_size = 10;
    settings.max_query_cache_size = 15;
    settings.capacity = 15;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    auto key = io::BlockFileCache::hash("key1");
    config::file_cache_max_file_reader_cache_size = 2;
    {
        auto holder = cache.get_or_set(key, 0, 9, context); /// Add range [0, 8]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(0, 8), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(9);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 9), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 0)));
    }
    {
        auto holder = cache.get_or_set(key, 9, 1, context); /// Add range [9, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(2, blocks[0], io::FileBlock::Range(9, 9), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(1);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 1), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 9)));
    }
    {
        auto holder = cache.get_or_set(key, 10, 5, context); /// Add range [10, 14]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(3, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        assert_range(4, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADING);
        download(blocks[0]);
        std::unique_ptr<char[]> buffer = std::make_unique<char[]>(5);
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.get(), 5), 0).ok());
        EXPECT_TRUE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 10)));
    }
    EXPECT_FALSE(io::FDCache::instance()->contains_file_reader(std::make_pair(key, 0)));
    EXPECT_EQ(io::FDCache::instance()->file_reader_cache_size(), 2);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

template <std::size_t N>
int get_disk_info(const char* const (&argv)[N], int* percent) {
    assert((N > 0) && (argv[N - 1] == nullptr));
    std::vector<std::string> rets;
    int pipefds[2];
    if (::pipe(pipefds) == -1) {
        std::cerr << "Error creating pipe" << std::endl;
        return -1;
    }
    pid_t pid = ::fork();
    if (pid == -1) {
        std::cerr << "Error forking process" << std::endl;
        return -1;
    } else if (pid == 0) {
        ::close(pipefds[0]);
        ::dup2(pipefds[1], STDOUT_FILENO);
        ::execvp("df", const_cast<char* const*>(argv));
        std::cerr << "Error executing command" << std::endl;
        _Exit(-1);
    } else {
        waitpid(pid, nullptr, 0);
        close(pipefds[1]);
        char buffer[PATH_MAX];
        ssize_t nbytes;
        while ((nbytes = read(pipefds[0], buffer, PATH_MAX)) > 0) {
            buffer[nbytes - 1] = '\0';
            rets.push_back(buffer);
        }
        ::close(pipefds[0]);
    }
    // df return
    // 已用%
    // 73%
    // str save 73
    std::string str = rets[0].substr(rets[0].rfind('\n'), rets[0].rfind('%') - rets[0].rfind('\n'));
    *percent = std::stoi(str);
    return 0;
}

// TEST_F(BlockFileCacheTest, disk_used_percentage_test) {
//     std::string dir = "/dev";
//     std::pair<int, int> percent;
//     disk_used_percentage(dir, &percent);
//     int disk_used, inode_used;
//     auto ret = get_disk_info({(char*)"df", (char*)"--output=pcent", (char*)"/dev", (char*)nullptr},
//                              &disk_used);
//     ASSERT_TRUE(!ret);
//     ret = get_disk_info({(char*)"df", (char*)"--output=ipcent", (char*)"/dev", (char*)nullptr},
//                         &inode_used);
//     ASSERT_TRUE(!ret);
//     ASSERT_EQ(percent.first, disk_used);
//     ASSERT_EQ(percent.second, inode_used);
// }

void test_file_cache_run_in_resource_limit(io::FileCacheType cache_type) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    switch (cache_type) {
    case io::FileCacheType::INDEX:
        settings.index_queue_elements = 5;
        settings.index_queue_size = 60;
        break;
    case io::FileCacheType::NORMAL:
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        break;
    case io::FileCacheType::DISPOSABLE:
        settings.disposable_queue_size = 30;
        settings.disposable_queue_elements = 5;
        break;
    default:
        break;
    }
    settings.capacity = 100;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context, index_context;
    context.cache_type = other_context.cache_type = cache_type;
    context.query_id = query_id;
    other_context.query_id = other_query_id;
    auto key_1 = io::BlockFileCache::hash("key1");
    index_context.cache_type = io::FileCacheType::INDEX;
    {
        io::BlockFileCache cache(cache_base_path, settings);
        cache._index_queue.hot_data_interval = 0;
        ASSERT_TRUE(cache.initialize());
        for (int i = 0; i < 100; i++) {
            if (cache.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            auto key_index = io::BlockFileCache::hash("key_index");
            auto holder_index =
                    cache.get_or_set(key_index, 0, 50, index_context); // Add index range [0, 49]
            auto blocks_index = fromHolder(holder_index);
            blocks_index[0]->get_or_set_downloader();
            download(blocks_index[0]);
            assert_range(0, blocks_index[0], io::FileBlock::Range(0, 29),
                         io::FileBlock::State::DOWNLOADED);
            assert_range(1, blocks_index[1], io::FileBlock::Range(30, 49),
                         io::FileBlock::State::EMPTY);
            blocks_index[1]->get_or_set_downloader();
            download(blocks_index[1]);
            assert_range(2, blocks_index[1], io::FileBlock::Range(30, 49),
                         io::FileBlock::State::DOWNLOADED);

            auto holder_index_1 =
                    cache.get_or_set(key_index, 50, 10, index_context); // Add index range [50, 59]
            auto blocks_index_1 = fromHolder(holder_index_1);
            blocks_index_1[0]->get_or_set_downloader();
            download(blocks_index_1[0]);
            assert_range(0, blocks_index_1[0], io::FileBlock::Range(50, 59),
                         io::FileBlock::State::DOWNLOADED);
        }
        ASSERT_EQ(cache.get_file_blocks_num(io::FileCacheType::INDEX), 3);
        ASSERT_EQ(cache.get_used_cache_size(io::FileCacheType::INDEX), 60);
        {
            cache._disk_resource_limit_mode = true;
            auto holder = cache.get_or_set(key_1, 0, 10, context); /// Add range [0, 9]
            auto blocks = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file block.
            ASSERT_EQ(blocks.size(), 1);
            assert_range(3, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            assert_range(4, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADING);
            download(blocks[0]);
            assert_range(5, blocks[0], io::FileBlock::Range(0, 9),
                         io::FileBlock::State::DOWNLOADED);

            // ATTN: in disk limit mode, will remove 5*size
            /// Current index cache:    [__________][_______][_____]
            ///                         ^          ^^       ^^     ^
            ///                         0         29 30    4950    59
            // get size 10, in disk limit mode, will remove size 5 * 10 by other lru queue
            // so will remove index queue range 0~49
            ASSERT_EQ(cache.get_file_blocks_num(io::FileCacheType::INDEX), 1);
            ASSERT_EQ(cache.get_used_cache_size(io::FileCacheType::INDEX), 10);
        }
    }
}

// run in disk space or disk inode not enough mode
TEST_F(BlockFileCacheTest, run_in_resource_limit_mode) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    {
        test_file_cache_run_in_resource_limit(io::FileCacheType::NORMAL);

        if (fs::exists(cache_base_path)) {
            fs::remove_all(cache_base_path);
        }
        fs::create_directories(cache_base_path);
        test_file_cache(io::FileCacheType::NORMAL);
    }
}

TEST_F(BlockFileCacheTest, fix_tmp_file) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false}, flag2 {false};
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::TmpFile1",
            [&](auto&&) {
                while (!flag1) {
                }
            },
            &guard1);
    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "BlockFileCache::TmpFile2", [&](auto&&) { flag2 = true; }, &guard2);
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                  (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = blocks[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(blocks[0]->append(result));
    flag1 = true;
    while (!flag2) {
    }
    ASSERT_TRUE(blocks[0]->finalize());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_lazy_load) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false};
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::TmpFile2",
            [&](auto&&) {
                while (!flag1) {
                }
            },
            &guard1);
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait to load disk
    auto holder = cache.get_or_set(key, 100, 1, context);       /// Add range [9, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                  (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = blocks[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(blocks[0]->append(result));
    ASSERT_TRUE(blocks[0]->finalize());
    flag1 = true;
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache.get_file_blocks_num(io::FileCacheType::NORMAL), 10);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_lazy_load_with_limit) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    std::atomic_bool flag1 {false};
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::TmpFile2",
            [&](auto&&) {
                while (!flag1) {
                }
            },
            &guard1);
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait to load disk
    cache._disk_resource_limit_mode = true;
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                  (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = blocks[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(blocks[0]->append(result));
    ASSERT_TRUE(blocks[0]->finalize());
    flag1 = true;
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache.get_file_blocks_num(io::FileCacheType::NORMAL), 9);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::set_sleep_time",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; }, &guard1);
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 50;
    settings.query_queue_elements = 5;
    settings.capacity = 50;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 120;
    int64_t modify_time = cur_time + 5;
    auto key1 = io::BlockFileCache::hash("key5");
    auto key2 = io::BlockFileCache::hash("key6");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key1, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        context.cache_type = io::FileCacheType::INDEX;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 60, 10, context); /// Add range [60, 69]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(60, 69), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(60, 69), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        cache.modify_expiration_time(key2, modify_time);
        context.expiration_time = modify_time;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        EXPECT_EQ(blocks[0]->expiration_time(), modify_time);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    {
        context.cache_type = io::FileCacheType::INDEX;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::NORMAL);
        EXPECT_EQ(blocks[0]->expiration_time(), 0);
        std::string buffer(10, '1');
        ASSERT_TRUE(blocks[0]->read(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_modify) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::set_sleep_time",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; }, &guard1);
    sp->enable_processing();
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 120;
    int64_t modify_time = cur_time + 5;
    auto key1 = io::BlockFileCache::hash("key5");
    auto key2 = io::BlockFileCache::hash("key6");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key1, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    cache.modify_expiration_time(key2, 0);
    {
        context.cache_type = io::FileCacheType::INDEX;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::NORMAL);
        EXPECT_EQ(blocks[0]->expiration_time(), 0);
        std::string buffer(10, '1');
        EXPECT_TRUE(blocks[0]->read(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    {
        cache.modify_expiration_time(key2, modify_time);
        context.expiration_time = modify_time;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        EXPECT_EQ(blocks[0]->expiration_time(), modify_time);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_change_to_normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        context.cache_type = io::FileCacheType::NORMAL;
        context.expiration_time = 0;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        if (auto storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
            storage != nullptr) {
            std::string dir = storage->get_path_in_local_cache(key2, 0);
            EXPECT_TRUE(fs::exists(
                    storage->get_path_in_local_cache(dir, 50, io::FileCacheType::NORMAL)));
        }
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::NORMAL);
        EXPECT_EQ(blocks[0]->expiration_time(), 0);
        std::string buffer(10, '1');
        EXPECT_TRUE(blocks[0]->read(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_change_expiration_time) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    int64_t change_time = cur_time + 120;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
    }
    {
        context.cache_type = io::FileCacheType::TTL;
        context.expiration_time = change_time;
        auto holder = cache.get_or_set(key2, 50, 10, context); /// Add range [50, 59]
        if (auto storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
            storage != nullptr) {
            std::string dir = storage->get_path_in_local_cache(key2, change_time);
            EXPECT_TRUE(
                    fs::exists(storage->get_path_in_local_cache(dir, 50, io::FileCacheType::TTL)));
        }
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->cache_type(), io::FileCacheType::TTL);
        EXPECT_EQ(blocks[0]->expiration_time(), change_time);
        std::string buffer(10, '1');
        EXPECT_TRUE(blocks[0]->read(Slice(buffer.data(), 10), 0).ok());
        EXPECT_EQ(buffer, std::string(10, '0'));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_reverse) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 36;
    settings.query_queue_elements = 5;
    settings.capacity = 36;
    settings.max_file_block_size = 7;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::TTL;
    context.query_id = query_id;
    int64_t cur_time = UnixSeconds();
    context.expiration_time = cur_time + 180;
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(cache.get_lazy_open_success());
    for (size_t offset = 0; offset < 30; offset += 6) {
        auto holder = cache.get_or_set(key2, offset, 6, context);
        auto blocks = fromHolder(holder);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
    }
    {
        auto holder = cache.get_or_set(key2, 50, 7, context); /// Add range [50, 57]
        auto blocks = fromHolder(holder);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 56), io::FileBlock::State::SKIP_CACHE);
    }
    {
        context.cache_type = io::FileCacheType::NORMAL;
        auto holder = cache.get_or_set(key2, 50, 7, context); /// Add range [50, 57]
        auto blocks = fromHolder(holder);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 56), io::FileBlock::State::SKIP_CACHE);
    }

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, io_error) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    config::file_cache_max_file_reader_cache_size = 0;
    test_file_cache(io::FileCacheType::NORMAL);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context, other_context;
    context.cache_type = other_context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileSystem::open_file_impl",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        auto buffer = std::make_unique<char[]>(10);
        EXPECT_FALSE(blocks[0]->read(Slice(buffer.get(), 10), 0).ok());
    }
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileReader::read_at_impl",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADED);
        auto buffer = std::make_unique<char[]>(10);
        EXPECT_FALSE(blocks[0]->read(Slice(buffer.get(), 10), 0).ok());
    }
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileSystem::create_file_impl",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_FALSE(blocks[0]->append(result).ok());
    }
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileWriter::appendv",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_FALSE(blocks[0]->append(result).ok());
    }
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileSystem::rename",
                [](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_TRUE(blocks[0]->append(result).ok());
        EXPECT_FALSE(blocks[0]->finalize().ok());
    }
    {
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileWriter::close",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        std::string data(10, '0');
        Slice result(data.data(), 10);
        EXPECT_TRUE(blocks[0]->append(result).ok());
        EXPECT_FALSE(blocks[0]->finalize().ok());
    }
    {
        auto holder = cache.get_or_set(key, 50, 10, context); /// Add range [50, 59]
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(50, 59), io::FileBlock::State::DOWNLOADED);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, remove_directly_when_normal_change_to_ttl) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    context.expiration_time = 0;
    context.cache_type = io::FileCacheType::NORMAL;
    {
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }

    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    {
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    cache.remove_if_cached(key1);

    EXPECT_EQ(cache._cur_cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, recyle_cache_async) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::set_sleep_time",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; }, &guard1);
    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "BlockFileCache::set_remove_batch",
            [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; }, &guard2);
    SyncPoint::CallbackGuard guard3;
    sp->set_call_back(
            "BlockFileCache::recycle_deleted_blocks",
            [&](auto&&) {
                context.cache_type = io::FileCacheType::NORMAL;
                cache.get_or_set(key, 0, 5, context);
            },
            &guard3);
    sp->enable_processing();
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        context.cache_type = static_cast<io::FileCacheType>((offset / 5) % 3);
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    cache.clear_file_cache_async();
    while (cache._async_clear_file_cache)
        ;
    EXPECT_EQ(cache._cur_cache_size, 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, recyle_cache_async_ttl) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 120;
    settings.query_queue_elements = 20;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 1800;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::set_sleep_time",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; }, &guard1);
    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "BlockFileCache::set_remove_batch",
            [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; }, &guard2);
    SyncPoint::CallbackGuard guard3;
    sp->set_call_back(
            "BlockFileCache::recycle_deleted_blocks",
            [&](auto&&) {
                context.cache_type = io::FileCacheType::NORMAL;
                cache.get_or_set(key, 0, 5, context);
            },
            &guard3);
    sp->enable_processing();
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key2, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    cache.clear_file_cache_async();
    while (cache._async_clear_file_cache)
        ;
    EXPECT_EQ(cache._cur_cache_size, 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, remove_directly) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");
    auto key2 = io::BlockFileCache::hash("key2");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }

    context.expiration_time = 0;
    context.cache_type = io::FileCacheType::NORMAL;
    {
        auto holder = cache.get_or_set(key2, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    cache.remove_if_cached(key1);
    cache.remove_if_cached(key2);

    EXPECT_EQ(cache._cur_cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_factory_1) {
    std::string cache_path2 = caches_dir / "cache2" / "";
    std::string cache_path3 = caches_dir / "cache3" / "";
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    if (fs::exists(cache_path2)) {
        fs::remove_all(cache_path2);
    }
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "BlockFileCache::set_sleep_time",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; }, &guard1);
    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "BlockFileCache::set_remove_batch",
            [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; }, &guard2);
    sp->enable_processing();
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_path2, settings).ok());
    EXPECT_EQ(FileCacheFactory::instance()->get_cache_instance_size(), 2);
    EXPECT_EQ(FileCacheFactory::instance()->get_capacity(), 180);
    EXPECT_EQ(FileCacheFactory::instance()->get_by_path(cache_path2)->get_base_path(), cache_path2);
    auto key1 = io::BlockFileCache::hash("key1");
    EXPECT_EQ(FileCacheFactory::instance()->get_by_path(key1)->get_base_path(), cache_base_path);
    EXPECT_EQ(FileCacheFactory::instance()->get_by_path(cache_path3), nullptr);

    io::CacheContext context;
    auto cache = FileCacheFactory::instance()->get_by_path(key1);
    int i = 0;
    while (i++ < 1000) {
        if (cache->get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_LT(i, 1000);
    context.cache_type = io::FileCacheType::NORMAL;
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache->get_or_set(key1, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    FileCacheFactory::instance()->clear_file_caches(false);
    while (cache->_async_clear_file_cache)
        ;
    EXPECT_EQ(cache->_cur_cache_size, 0);

    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache->get_or_set(key1, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    FileCacheFactory::instance()->clear_file_caches(true);
    EXPECT_EQ(cache->_cur_cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, test_factory_2) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    config::clear_file_cache = true;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    auto key = io::BlockFileCache::hash("key1");
    auto cache = FileCacheFactory::instance()->get_by_path(key);
    int i = 0;
    while (i++ < 1000) {
        if (cache->get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_LT(i, 1000);
    EXPECT_EQ(cache->_cur_cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    config::clear_file_cache = false;
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, test_factory_3) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    io::FileCacheSettings settings;
    settings.query_queue_size = INT64_MAX;
    settings.query_queue_elements = 100000000;
    settings.index_queue_size = 0;
    settings.index_queue_elements = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = INT64_MAX;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    auto key = io::BlockFileCache::hash("key1");
    auto cache = FileCacheFactory::instance()->get_by_path(key);
    int i = 0;
    while (i++ < 1000) {
        if (cache->get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_LT(i, 1000);
    EXPECT_LT(cache->capacity(), INT64_MAX);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, test_hash_key) {
    auto key1 = io::BlockFileCache::hash("key1");
    EXPECT_EQ(key1.to_string(), "f36131fb4ba563c17e727cd0cdd63689");
}

TEST_F(BlockFileCacheTest, test_cache_context) {
    {
        io::IOContext io_ctx;
        io_ctx.is_index_data = true;
        CacheContext cache_context;
        cache_context.cache_type = FileCacheType::INDEX;
        EXPECT_EQ(CacheContext(&io_ctx), cache_context);
    }
    {
        TUniqueId query_id;
        query_id.hi = 1;
        query_id.lo = 2;
        io::IOContext io_ctx;
        io_ctx.query_id = &query_id;
        CacheContext cache_context;
        cache_context.query_id = query_id;
        cache_context.cache_type = FileCacheType::NORMAL;
        EXPECT_EQ(CacheContext(&io_ctx), cache_context);
    }
    {
        io::IOContext io_ctx;
        io_ctx.is_disposable = true;
        CacheContext cache_context;
        cache_context.cache_type = FileCacheType::DISPOSABLE;
        EXPECT_EQ(CacheContext(&io_ctx), cache_context);
    }
    {
        io::IOContext io_ctx;
        int64_t expiration_time = UnixSeconds() + 120;
        io_ctx.expiration_time = expiration_time;
        CacheContext cache_context;
        cache_context.cache_type = FileCacheType::TTL;
        cache_context.expiration_time = expiration_time;
        EXPECT_EQ(CacheContext(&io_ctx), cache_context);
    }
}

TEST_F(BlockFileCacheTest, test_disposable) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }

    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::NORMAL);
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = FileCacheType::DISPOSABLE;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::string queue_str;
    {
        std::lock_guard lock(cache._mutex);
        queue_str = cache._normal_queue.to_string(lock);
    }
    cache.get_or_set(key, 28, 1, context);
    {
        std::lock_guard lock(cache._mutex);
        EXPECT_EQ(queue_str, cache._normal_queue.to_string(lock));
    }
    EXPECT_EQ(cache.get_used_cache_size(FileCacheType::DISPOSABLE), 0);
}

TEST_F(BlockFileCacheTest, test_query_limit) {
    {
        config::enable_file_cache_query_limit = true;
        if (fs::exists(cache_base_path)) {
            fs::remove_all(cache_base_path);
        }
        fs::create_directories(cache_base_path);
        TUniqueId query_id;
        query_id.hi = 1;
        query_id.lo = 1;
        io::FileCacheSettings settings;
        settings.query_queue_size = 30;
        settings.query_queue_elements = 5;
        settings.index_queue_size = 0;
        settings.index_queue_elements = 0;
        settings.disposable_queue_size = 0;
        settings.disposable_queue_elements = 0;
        settings.capacity = 30;
        settings.max_file_block_size = 30;
        settings.max_query_cache_size = 15;
        io::CacheContext context;
        context.cache_type = FileCacheType::NORMAL;
        context.query_id = query_id;
        auto key = io::BlockFileCache::hash("key1");

        ASSERT_TRUE(
                FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
        auto cache = FileCacheFactory::instance()->get_by_path(key);
        int i = 0;
        while (i++ < 1000) {
            if (cache->get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_LT(i, 1000);
        auto query_context_holder =
                FileCacheFactory::instance()->get_query_context_holders(query_id);
        for (int64_t offset = 0; offset < 60; offset += 5) {
            auto holder = cache->get_or_set(key, offset, 5, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                         io::FileBlock::State::EMPTY);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            download(blocks[0]);
            assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                         io::FileBlock::State::DOWNLOADED);
        }
        EXPECT_EQ(cache->_cur_cache_size, 15);
        if (fs::exists(cache_base_path)) {
            fs::remove_all(cache_base_path);
        }
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, state_to_string) {
    EXPECT_EQ(FileBlock::state_to_string(FileBlock::State::EMPTY), "EMPTY");
    EXPECT_EQ(FileBlock::state_to_string(FileBlock::State::SKIP_CACHE), "SKIP_CACHE");
    EXPECT_EQ(FileBlock::state_to_string(FileBlock::State::DOWNLOADING), "DOWNLOADING");
    EXPECT_EQ(FileBlock::state_to_string(FileBlock::State::DOWNLOADED), "DOWNLOADED");
}

TEST_F(BlockFileCacheTest, string_to_cache_type) {
    EXPECT_EQ(BlockFileCache::string_to_cache_type("idx"), FileCacheType::INDEX);
    EXPECT_EQ(BlockFileCache::string_to_cache_type("disposable"), FileCacheType::DISPOSABLE);
    EXPECT_EQ(BlockFileCache::string_to_cache_type("ttl"), FileCacheType::TTL);
}

TEST_F(BlockFileCacheTest, append_many_time) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 0;
    settings.index_queue_elements = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 15;
    auto key = io::BlockFileCache::hash("key1");
    io::CacheContext context;
    context.cache_type = FileCacheType::NORMAL;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    {
        auto holder = cache.get_or_set(key, 0, 5, context);
        for (int64_t i = 0; i < 5; i++) {
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
            ASSERT_TRUE(blocks[0]->append(Slice("0", 1)).ok());
            std::lock_guard lock(blocks[0]->_mutex);
            EXPECT_EQ(blocks[0]->_downloaded_size, i + 1);
        }
    }
    {
        auto holder = cache.get_or_set(key, 0, 5, context);
        auto blocks = fromHolder(holder);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
        ASSERT_TRUE(blocks[0]->change_cache_type_by_mgr(FileCacheType::NORMAL).ok());
        ASSERT_TRUE(blocks[0]->change_cache_type_self(FileCacheType::INDEX).ok());
        if (auto storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
            storage != nullptr) {
            auto dir = storage->get_path_in_local_cache(blocks[0]->get_hash_value(),
                                                        blocks[0]->expiration_time());
            EXPECT_TRUE(fs::exists(storage->get_path_in_local_cache(dir, blocks[0]->offset(),
                                                                    blocks[0]->cache_type())));
        }
        ASSERT_TRUE(blocks[0]->change_cache_type_self(FileCacheType::INDEX).ok());
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileSystem::rename",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        {
            ASSERT_FALSE(blocks[0]->change_cache_type_self(FileCacheType::NORMAL).ok());
            EXPECT_EQ(blocks[0]->cache_type(), FileCacheType::INDEX);
            std::string buffer;
            buffer.resize(5);
            EXPECT_TRUE(blocks[0]->read(Slice(buffer.data(), 5), 0).ok());
            EXPECT_EQ(buffer, std::string(5, '0'));
        }
        {
            EXPECT_FALSE(blocks[0]->change_cache_type_by_mgr(FileCacheType::NORMAL).ok());
            EXPECT_EQ(blocks[0]->cache_type(), FileCacheType::INDEX);
            std::string buffer;
            buffer.resize(5);
            EXPECT_TRUE(blocks[0]->read(Slice(buffer.data(), 5), 0).ok());
            EXPECT_EQ(buffer, std::string(5, '0'));
        }
    }
    {
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        SyncPoint::CallbackGuard guard1;
        sp->set_call_back(
                "LocalFileWriter::close",
                [&](auto&& values) {
                    std::pair<Status, bool>* pairs =
                            try_any_cast<std::pair<Status, bool>*>(values.back());
                    pairs->second = true;
                },
                &guard1);
        auto holder = cache.get_or_set(key, 5, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        ASSERT_TRUE(blocks[0]->append(Slice("00000", 5)).ok());
    }
    {
        auto holder = cache.get_or_set(key, 5, 5, context);
        auto blocks = fromHolder(holder);
        assert_range(1, blocks[0], io::FileBlock::Range(5, 9), io::FileBlock::State::EMPTY);
    }
    {
        auto holder = cache.get_or_set(key, 5, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        ASSERT_TRUE(blocks[0]->append(Slice("00000", 5)).ok());
        ASSERT_TRUE(blocks[0]->finalize().ok());
        assert_range(1, blocks[0], io::FileBlock::Range(5, 9), io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(blocks[0]->wait(), FileBlock::State::DOWNLOADED);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, query_file_cache) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    TUniqueId id;
    id.hi = id.lo = 1;
    config::enable_file_cache_query_limit = false;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 0;
    settings.index_queue_elements = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 15;
    auto key = io::BlockFileCache::hash("key1");
    io::CacheContext context;
    context.cache_type = FileCacheType::NORMAL;
    {
        io::BlockFileCache cache(cache_base_path, settings);
        ASSERT_TRUE(cache.initialize());
        for (int i = 0; i < 100; i++) {
            if (cache.get_lazy_open_success()) {
                break;
            };
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_EQ(cache.get_query_context_holder(id), nullptr);
    }
    config::enable_file_cache_query_limit = true;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    id.hi = id.lo = 0;
    EXPECT_EQ(cache.get_query_context_holder(id)->context, nullptr);
    id.hi = id.lo = 1;
    auto query_ctx_1 = cache.get_query_context_holder(id);
    ASSERT_NE(query_ctx_1, nullptr);
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    auto query_ctx_2 = cache.get_query_context_holder(id);
    EXPECT_EQ(query_ctx_1->query_id, query_ctx_2->query_id);
    std::lock_guard lock(cache._mutex);
    EXPECT_EQ(query_ctx_1->context->get_cache_size(lock),
              query_ctx_2->context->get_cache_size(lock));
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, query_file_cache_reserve) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    TUniqueId id;
    id.hi = id.lo = 1;
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 0;
    settings.index_queue_elements = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 15;
    auto key = io::BlockFileCache::hash("key1");
    auto key2 = io::BlockFileCache::hash("key2");
    io::CacheContext context;
    context.cache_type = FileCacheType::NORMAL;
    context.query_id = id;
    config::enable_file_cache_query_limit = true;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto query_ctx_1 = cache.get_query_context_holder(id);
    ASSERT_NE(query_ctx_1, nullptr);
    {
        auto holder = cache.get_or_set(key, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    {
        auto holder = cache.get_or_set(key2, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    context.query_id.hi = context.query_id.lo = 0;
    for (int64_t offset = 5; offset < 30; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.query_id.hi = context.query_id.lo = 1;
    for (int64_t offset = 35; offset < 65; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    CachedRemoteFileReader reader(local_reader, opts);
    auto key = io::BlockFileCache::hash("tmp_file");
    EXPECT_EQ(reader._cache_hash, key);
    EXPECT_EQ(local_reader->path().native(), reader.path().native());
    EXPECT_EQ(local_reader->size(), reader.size());
    EXPECT_FALSE(reader.closed());
    EXPECT_EQ(local_reader->path().native(), reader.get_remote_reader()->path().native());
    {
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        RuntimeProfile profile("file_cache_test");
        FileCacheProfileReporter reporter(&profile);
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(reader.read_at(32222, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
        reporter.update(&stats);
    }
    {
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        EXPECT_FALSE(
                reader.read_at(10_mb + 2, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
    }
    {
        std::string buffer;
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        EXPECT_TRUE(
                reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        EXPECT_EQ(bytes_read, 0);
    }
    {
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(reader.read_at(32222, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    }
    {
        std::string buffer;
        buffer.resize(10_mb + 1);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(
                reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        for (int i = 0; i < 10; i++) {
            std::string data(1_mb, '0' + i);
            EXPECT_EQ(data, buffer.substr(i * 1024 * 1024, 1_mb));
        }
        std::string data(1, '0');
        EXPECT_EQ(data, buffer.substr(10_mb, 1));
    }
    EXPECT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader.closed());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_tail) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.cache_type = io::FileCacheType::NORMAL;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    CachedRemoteFileReader reader(local_reader, opts);
    {
        std::string buffer;
        buffer.resize(1);
        IOContext io_ctx;
        RuntimeProfile profile("file_cache_test");
        FileCacheProfileReporter reporter(&profile);
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(reader.read_at(10_mb, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(std::string(1, '0'), buffer);
        reporter.update(&stats);
    }
    EXPECT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader.closed());
    {
        auto key = io::BlockFileCache::hash("tmp_file");
        auto cache = FileCacheFactory::instance()->get_by_path(key);
        auto holder = cache->get_or_set(key, 9_mb, 1024_kb + 1, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 2);
        assert_range(1, blocks[0], io::FileBlock::Range(9_mb, 10_mb - 1),
                     io::FileBlock::State::DOWNLOADED);
        assert_range(2, blocks[1], io::FileBlock::Range(10_mb, 10_mb),
                     io::FileBlock::State::DOWNLOADED);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_error_handle) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    auto cache = FileCacheFactory::instance()->_caches[0].get();
    for (int i = 0; i < 100; i++) {
        if (cache->get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    CachedRemoteFileReader reader(local_reader, opts);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    {
        Defer defer {[sp] { sp->clear_call_back("LocalFileWriter::appendv"); }};
        sp->set_call_back("LocalFileWriter::appendv", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(
                reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    }
    {
        Defer defer {[sp] { sp->clear_call_back("LocalFileWriter::close"); }};
        sp->set_call_back("LocalFileWriter::close", [&](auto&& values) {
            std::pair<Status, bool>* pairs = try_any_cast<std::pair<Status, bool>*>(values.back());
            pairs->second = true;
        });
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(
                reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    }
    EXPECT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader.closed());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_init) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 83886080;
    settings.query_queue_elements = 80;
    settings.index_queue_size = 10485760;
    settings.index_queue_elements = 10;
    settings.disposable_queue_size = 10485760;
    settings.disposable_queue_elements = 10;
    settings.capacity = 104857600;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;
    io::CacheContext context;
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = false;
    {
        opts.mtime = UnixSeconds() - 1000;
        CachedRemoteFileReader reader(local_reader, opts);
        auto key = io::BlockFileCache::hash(
                fmt::format("{}:{}", local_reader->path().native(), opts.mtime));
        EXPECT_EQ(reader._cache_hash, key);
    }
    {
        opts.cache_base_path = cache_base_path;
        CachedRemoteFileReader reader(local_reader, opts);
        EXPECT_EQ(reader._cache->get_base_path(), cache_base_path);
    }
    {
        opts.cache_base_path = caches_dir / "cache2" / "";
        CachedRemoteFileReader reader(local_reader, opts);
        EXPECT_EQ(reader._cache->get_base_path(), cache_base_path);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_concurrent) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    bool flag1 = false;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer defer {[sp] {
        sp->clear_call_back("CachedRemoteFileReader::DOWNLOADING");
        sp->clear_call_back("CachedRemoteFileReader::EMPTY");
    }};
    sp->set_call_back("CachedRemoteFileReader::DOWNLOADING", [&](auto&&) { flag1 = true; });
    sp->set_call_back("CachedRemoteFileReader::EMPTY", [&](auto&&) {
        while (!flag1) {
        }
    });
    std::thread thread([&]() {
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    });
    std::string buffer;
    buffer.resize(64_kb);
    IOContext io_ctx;
    FileCacheStatistics stats;
    io_ctx.file_cache_stats = &stats;
    size_t bytes_read {0};
    ASSERT_TRUE(
            reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(std::string(64_kb, '0'), buffer);
    if (thread.joinable()) {
        thread.join();
    }
    EXPECT_TRUE(reader->close().ok());
    EXPECT_TRUE(reader->closed());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_concurrent_2) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer defer {[sp] {
        sp->clear_call_back("CachedRemoteFileReader::DOWNLOADING");
        sp->clear_call_back("CachedRemoteFileReader::EMPTY");
        sp->clear_call_back("CachedRemoteFileReader::max_wait_time");
    }};
    sp->set_call_back("CachedRemoteFileReader::EMPTY",
                      [&](auto&&) { std::this_thread::sleep_for(std::chrono::seconds(3)); });
    sp->set_call_back("CachedRemoteFileReader::max_wait_time",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 2; });
    std::thread thread([&]() {
        std::string buffer;
        buffer.resize(64_kb);
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    });
    std::string buffer;
    buffer.resize(64_kb);
    IOContext io_ctx;
    FileCacheStatistics stats;
    io_ctx.file_cache_stats = &stats;
    size_t bytes_read {0};
    ASSERT_TRUE(
            reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(std::string(64_kb, '0'), buffer);
    if (thread.joinable()) {
        thread.join();
    }
    EXPECT_TRUE(reader->close().ok());
    EXPECT_TRUE(reader->closed());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

TEST_F(BlockFileCacheTest, test_hot_data) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    auto key1 = io::BlockFileCache::hash("key1");
    auto key2 = io::BlockFileCache::hash("key2");
    int64_t expiration_time = UnixSeconds() + 300;
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    cache._normal_queue.hot_data_interval = 1;
    {
        context.cache_type = FileCacheType::INDEX;
        auto holder = cache.get_or_set(key1, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    {
        context.cache_type = FileCacheType::INDEX;
        auto holder = cache.get_or_set(key1, 15, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(15, 19), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(15, 19), io::FileBlock::State::DOWNLOADED);
    }
    {
        context.cache_type = FileCacheType::DISPOSABLE;
        auto holder = cache.get_or_set(key1, 5, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(5, 9), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(5, 9), io::FileBlock::State::DOWNLOADED);
    }
    {
        context.cache_type = FileCacheType::NORMAL;
        auto holder = cache.get_or_set(key1, 10, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(10, 14), io::FileBlock::State::DOWNLOADED);
    }
    {
        context.cache_type = FileCacheType::TTL;
        context.expiration_time = expiration_time;
        auto holder = cache.get_or_set(key2, 0, 5, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 4), io::FileBlock::State::DOWNLOADED);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(cache.get_hot_blocks_meta(key1).size(), 2);
    EXPECT_EQ(cache.get_hot_blocks_meta(key2).size(), 1);
}

TEST_F(BlockFileCacheTest, test_lazy_load_with_error_file_1) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->enable_processing();
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::INDEX;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::string dir;
    if (auto storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
        storage != nullptr) {
        dir = storage->get_path_in_local_cache(key, 0);
    }
    sp->set_call_back("BlockFileCache::TmpFile1", [&](auto&&) {
        FileWriterPtr writer;
        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "error", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "10086_tmp", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "20086_idx", &writer).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "30086_idx", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());
    });
    sp->set_call_back("BlockFileCache::REMOVE_FILE_2", [&](auto&& args) {
        if (*try_any_cast<std::string*>(args[0]) == "30086_idx") {
            static_cast<void>(global_local_filesystem()->delete_file(dir / "30086_idx"));
        }
    });
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                  (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = blocks[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(blocks[0]->append(result));
    ASSERT_TRUE(blocks[0]->finalize());
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_lazy_load_with_error_file_2) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(io::FileCacheType::INDEX);
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->enable_processing();
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::INDEX;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    std::string dir;
    if (auto storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
        storage != nullptr) {
        dir = storage->get_path_in_local_cache(key, 0);
    }
    std::atomic_bool flag1 = false;
    std::atomic_bool flag2 = false;
    sp->set_call_back("BlockFileCache::TmpFile1", [&](auto&&) {
        FileWriterPtr writer;
        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "error", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "10086_tmp", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "20086_idx", &writer).ok());
        ASSERT_TRUE(writer->close().ok());

        ASSERT_TRUE(global_local_filesystem()->create_file(dir / "30086_idx", &writer).ok());
        ASSERT_TRUE(writer->append(Slice("111", 3)).ok());
        ASSERT_TRUE(writer->close().ok());
        flag2 = true;
        while (!flag1) {
        }
    });
    sp->set_call_back("BlockFileCache::REMOVE_FILE_1", [&](auto&& args) {
        if (*try_any_cast<std::string*>(args[0]) == "30086_idx") {
            static_cast<void>(global_local_filesystem()->delete_file(dir / "30086_idx"));
        }
    });
    while (!flag2) {
    }
    auto holder = cache.get_or_set(key, 100, 1, context); /// Add range [9, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(2, blocks[0], io::FileBlock::Range(100, 100), io::FileBlock::State::DOWNLOADING);
    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str.substr(0, 3) /
                  (key_str + "_" + std::to_string(blocks[0]->expiration_time()));
    ASSERT_TRUE(fs::exists(subdir));
    size_t size = blocks[0]->range().size();
    std::string data(size, '0');
    Slice result(data.data(), size);
    ASSERT_TRUE(blocks[0]->append(result));
    ASSERT_TRUE(blocks[0]->finalize());
    flag1 = true;
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_check_disk_reource_limit_1) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    config::file_cache_enter_disk_resource_limit_mode_percent =
            config::file_cache_exit_disk_resource_limit_mode_percent = 50;
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(config::file_cache_enter_disk_resource_limit_mode_percent, 90);
    EXPECT_EQ(config::file_cache_exit_disk_resource_limit_mode_percent, 80);
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_check_disk_reource_limit_2) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    config::file_cache_enter_disk_resource_limit_mode_percent = 2;
    config::file_cache_exit_disk_resource_limit_mode_percent = 1;
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(config::file_cache_enter_disk_resource_limit_mode_percent, 2);
    EXPECT_EQ(config::file_cache_exit_disk_resource_limit_mode_percent, 1);
    EXPECT_TRUE(cache._disk_resource_limit_mode);
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_check_disk_reource_limit_3) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::BlockFileCache cache(cache_base_path, settings);
    cache._disk_resource_limit_mode = true;
    config::file_cache_exit_disk_resource_limit_mode_percent = 98;
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(cache._disk_resource_limit_mode);
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_align_size) {
    const size_t total_size = 10_mb + 10086;
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(0, 100, total_size);
        EXPECT_EQ(offset, 0);
        EXPECT_EQ(size, 1_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(1_mb - 1, 2, total_size);
        EXPECT_EQ(offset, 0);
        EXPECT_EQ(size, 2_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(0, 1_mb + 10086, total_size);
        EXPECT_EQ(offset, 0);
        EXPECT_EQ(size, 2_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(10_mb + 1, 1086, total_size);
        EXPECT_EQ(offset, 9_mb);
        EXPECT_EQ(size, 1_mb + 10086);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(10_mb + 1, 108600, total_size);
        EXPECT_EQ(offset, 9_mb);
        EXPECT_EQ(size, 1_mb + 10086);
    }
    {
        auto [offset, size] =
                CachedRemoteFileReader::s_align_size(4_mb + 108600, 108600, total_size);
        EXPECT_EQ(offset, 4_mb);
        EXPECT_EQ(size, 1_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(4_mb, 1_mb, total_size);
        EXPECT_EQ(offset, 4_mb);
        EXPECT_EQ(size, 1_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(4_mb, 1, total_size);
        EXPECT_EQ(offset, 4_mb);
        EXPECT_EQ(size, 1_mb);
    }
    {
        auto [offset, size] = CachedRemoteFileReader::s_align_size(4_mb + 108600, 1_mb, total_size);
        EXPECT_EQ(offset, 4_mb);
        EXPECT_EQ(size, 2_mb);
    }
    std::random_device rd;  // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<> distrib(0, 10_mb + 10086);
    std::ranges::for_each(std::ranges::iota_view {0, 1000}, [&](int) {
        size_t read_size = distrib(gen) % 1_mb;
        size_t read_offset = distrib(gen);
        auto [offset, size] =
                CachedRemoteFileReader::s_align_size(read_offset, read_size, total_size);
        EXPECT_EQ(offset % 1_mb, 0);
        EXPECT_GE(size, 1_mb);
        EXPECT_LE(size, 2_mb);
    });
}

TEST_F(BlockFileCacheTest, remove_if_cached_when_isnt_releasable) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto holder = cache.get_or_set(key, 0, 10, context); /// Add range [0, 9]
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    assert_range(1, blocks[0], io::FileBlock::Range(0, 9), io::FileBlock::State::DOWNLOADING);
    cache.remove_if_cached(key);
    ASSERT_TRUE(blocks[0]->append(Slice("aaaa", 4)).ok());
    ASSERT_TRUE(blocks[0]->finalize().ok());
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_opt_lock) {
    config::enable_read_cache_file_directly = true;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
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
    context.query_id = query_id;
    ASSERT_TRUE(FileCacheFactory::instance()->create_file_cache(cache_base_path, settings).ok());
    io::FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
        auto reader = CachedRemoteFileReader(local_reader, opts);
        EXPECT_EQ(reader._cache_file_readers.size(), 0);
        std::string buffer;
        buffer.resize(6_mb);
        IOContext io_ctx;
        size_t bytes_read {0};
        ASSERT_TRUE(reader.read_at(1_mb, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(reader._cache_file_readers.size(), 6);
    }
    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
        auto reader = CachedRemoteFileReader(local_reader, opts);
        EXPECT_EQ(reader._cache_file_readers.size(), 6);
        std::random_device rd;  // a seed source for the random number engine
        std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
        std::uniform_int_distribution<> distrib(1_mb, 7_mb);
        std::ranges::for_each(std::ranges::iota_view {0, 1000}, [&](int) {
            size_t read_offset = distrib(gen);
            size_t read_size = distrib(gen) % 1_mb;
            if (read_offset + read_size > 7_mb || read_size == 0) {
                read_size = 1;
            }
            std::string buffer;
            buffer.resize(read_size);
            IOContext io_ctx;
            size_t bytes_read {0};
            ASSERT_TRUE(reader.read_at(read_offset, Slice(buffer.data(), buffer.size()),
                                       &bytes_read, &io_ctx)
                                .ok());
            EXPECT_EQ(bytes_read, read_size);
            int num = read_offset / 1_mb;
            size_t upper_offset = (num + 1) * 1_mb;
            if (upper_offset < read_offset + read_size) {
                size_t limit_size = upper_offset - read_offset;
                EXPECT_EQ(std::string(limit_size, '0' + num), buffer.substr(0, limit_size));
                EXPECT_EQ(std::string(read_size - limit_size, '0' + (num + 1)),
                          buffer.substr(limit_size));
            } else {
                EXPECT_EQ(std::string(read_size, '0' + num), buffer);
            }
        });
    }
    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
        auto reader = CachedRemoteFileReader(local_reader, opts);
        std::string buffer;
        buffer.resize(10086);
        IOContext io_ctx;
        size_t bytes_read {0};
        ASSERT_TRUE(reader.read_at(9_mb, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                            .ok());
        EXPECT_EQ(buffer, std::string(10086, '9'));
        EXPECT_EQ(reader._cache_file_readers.size(), 7);
    }
    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
        auto reader = CachedRemoteFileReader(local_reader, opts);
        std::string buffer;
        buffer.resize(10086);
        IOContext io_ctx;
        size_t bytes_read {0};
        ASSERT_TRUE(
                reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
        EXPECT_EQ(buffer, std::string(10086, '0'));
        EXPECT_EQ(reader._cache_file_readers.size(), 8);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
    config::enable_read_cache_file_directly = false;
}

TEST_F(BlockFileCacheTest, remove_from_other_queue_1) {
    config::file_cache_enable_evict_from_other_queue_by_size = false;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 60;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::INDEX;

    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.cache_type = io::FileCacheType::NORMAL;
    for (int64_t offset = 60; offset < 70; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::SKIP_CACHE);
    }
    config::file_cache_enable_evict_from_other_queue_by_size = true;
    for (int64_t offset = 60; offset < 70; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    EXPECT_EQ(cache._cur_cache_size, 60);
    EXPECT_EQ(cache._index_queue.cache_size, 50);
    EXPECT_EQ(cache._normal_queue.cache_size, 10);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, remove_from_other_queue_2) {
    config::file_cache_enable_evict_from_other_queue_by_size = true;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 60;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::INDEX;

    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 40; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.cache_type = io::FileCacheType::NORMAL;
    for (int64_t offset = 40; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.cache_type = io::FileCacheType::INDEX;
    {
        int64_t offset = 60;
        auto holder = cache.get_or_set(key, offset, 1, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(cache._cur_cache_size, 56);
        EXPECT_EQ(cache._index_queue.cache_size, 36);
        EXPECT_EQ(cache._normal_queue.cache_size, 20);
    }
    {
        int64_t offset = 61;
        auto holder = cache.get_or_set(key, offset, 9, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 8),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 8),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(cache._cur_cache_size, 60);
        EXPECT_EQ(cache._index_queue.cache_size, 40);
        EXPECT_EQ(cache._normal_queue.cache_size, 20);
    }
    {
        int64_t offset = 70;
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
        EXPECT_EQ(cache._cur_cache_size, 60);
        EXPECT_EQ(cache._index_queue.cache_size, 40);
        EXPECT_EQ(cache._normal_queue.cache_size, 20);
    }
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, recyle_unvalid_ttl_async) {
    config::file_cache_ttl_valid_check_interval_second = 4;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 30;
    settings.disposable_queue_elements = 5;
    settings.capacity = 90;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("BlockFileCache::set_remove_batch");
        sp->clear_call_back("BlockFileCache::recycle_deleted_blocks");
        sp->clear_call_back("BlockFileCache::set_sleep_time");
    }};
    sp->set_call_back("BlockFileCache::set_sleep_time",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 1; });
    sp->set_call_back("BlockFileCache::set_remove_batch",
                      [](auto&& args) { *try_any_cast<int*>(args[0]) = 2; });
    sp->set_call_back("BlockFileCache::recycle_deleted_blocks",
                      [&](auto&&) { cache.get_or_set(key, 0, 5, context); });
    sp->enable_processing();
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < 60; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    std::this_thread::sleep_for(
            std::chrono::seconds(config::file_cache_ttl_valid_check_interval_second + 2));
    config::file_cache_ttl_valid_check_interval_second = 0;
    EXPECT_EQ(cache._cur_cache_size, 5);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_reserve_wo_evict_using_lru) {
    config::file_cache_ttl_valid_check_interval_second = 4;
    config::enable_ttl_cache_evict_using_lru = false;

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 60;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;

    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < (60 * config::max_ttl_cache_ratio / 100 - 5); offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    for (int64_t offset = 60; offset < 70; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::SKIP_CACHE);
    }

    EXPECT_EQ(cache._cur_cache_size, 50);
    EXPECT_EQ(cache._ttl_queue.cache_size, 0);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, ttl_reserve_with_evict_using_lru) {
    config::file_cache_ttl_valid_check_interval_second = 4;
    config::enable_ttl_cache_evict_using_lru = true;

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;
    settings.query_queue_size = 30;
    settings.query_queue_elements = 5;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 60;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    io::CacheContext context;
    context.query_id = query_id;
    auto key = io::BlockFileCache::hash("key1");
    io::BlockFileCache cache(cache_base_path, settings);
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;

    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_lazy_open_success()) {
            break;
        };
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (int64_t offset = 0; offset < (60 * config::max_ttl_cache_ratio / 100); offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }
    context.cache_type = io::FileCacheType::TTL;
    context.expiration_time = UnixSeconds() + 3600;
    for (int64_t offset = 60; offset < 70; offset += 5) {
        auto holder = cache.get_or_set(key, offset, 5, context);
        auto segments = fromHolder(holder);
        ASSERT_EQ(segments.size(), 1);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(segments[0]);
        assert_range(1, segments[0], io::FileBlock::Range(offset, offset + 4),
                     io::FileBlock::State::DOWNLOADED);
    }

    EXPECT_EQ(cache._cur_cache_size, 60);
    EXPECT_EQ(cache._ttl_queue.cache_size, 60);
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

} // namespace doris::io
