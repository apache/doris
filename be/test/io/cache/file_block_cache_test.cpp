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

#include <filesystem>
#include <thread>

#include "common/config.h"
#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_file_cache_settings.h"
#include "io/cloud/cloud_file_segment.h"
#include "io/cloud/cloud_lru_file_cache.h"
#include "olap/options.h"
#include "util/slice.h"

namespace doris::io {

namespace fs = std::filesystem;

fs::path caches_dir = fs::current_path() / "lru_cache_test";
std::string cache_base_path = caches_dir / "cache1" / "";

void assert_range([[maybe_unused]] size_t assert_n, io::FileSegmentSPtr file_segment,
                  const io::FileSegment::Range& expected_range,
                  io::FileSegment::State expected_state) {
    auto range = file_segment->range();

    ASSERT_EQ(range.left, expected_range.left);
    ASSERT_EQ(range.right, expected_range.right);
    ASSERT_EQ(file_segment->state(), expected_state);
}

std::vector<io::FileSegmentSPtr> fromHolder(const io::FileSegmentsHolder& holder) {
    return std::vector<io::FileSegmentSPtr>(holder.file_segments.begin(),
                                            holder.file_segments.end());
}

std::string getFileSegmentPath(const std::string& base_path, const io::IFileCache::Key& key,
                               size_t offset) {
    auto key_str = key.to_string();
    return fs::path(base_path) / key_str / std::to_string(offset);
}

void download(io::FileSegmentSPtr file_segment) {
    const auto& key = file_segment->key();
    size_t size = file_segment->range().size();

    auto key_str = key.to_string();
    auto subdir = fs::path(cache_base_path) / key_str;
    ASSERT_TRUE(fs::exists(subdir));

    std::string data(size, '0');
    Slice result(data.data(), size);
    file_segment->append(result);
    file_segment->finalize_write();
}

void complete(const io::FileSegmentsHolder& holder) {
    for (const auto& file_segment : holder.file_segments) {
        ASSERT_TRUE(file_segment->get_or_set_downloader() == io::FileSegment::get_caller_id());
        download(file_segment);
    }
}

TEST(LRUFileCache, init) {
    std::string string = std::string(R"(
        [
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "normal" : 193273528320,
            "persistent" : 193273528320,
            "query_limit" : 38654705664
        },
        {
            "path" : "/mnt/ssd01/clickbench/hot/be/file_cache",
            "normal" : 193273528320,
            "persistent" : 193273528320,
            "query_limit" : 38654705664
        }
        ]
        )");
    config::enable_file_cache_query_limit = true;
    std::vector<CachePath> cache_paths;
    parse_conf_cache_paths(string, cache_paths);
    EXPECT_EQ(cache_paths.size(), 2);
    for (const auto& cache_path : cache_paths) {
        io::FileCacheSettings settings = cache_path.init_settings();
        EXPECT_EQ(settings.max_size, 193273528320);
        EXPECT_EQ(settings.persistent_max_size, 193273528320);
        EXPECT_EQ(settings.max_query_cache_size, 38654705664);
    }
}

void test_file_cache(bool is_persistent) {
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    TUniqueId other_query_id;
    other_query_id.hi = 2;
    other_query_id.lo = 2;

    io::FileCacheSettings settings;
    settings.max_size = 30;
    settings.max_elements = 5;
    settings.persistent_max_size = 30;
    settings.persistent_max_elements = 5;
    settings.max_file_segment_size = 100;
    auto key = io::IFileCache::hash("key1");
    {
        io::LRUFileCache cache(cache_base_path, settings);
        cache.initialize();
        {
            auto holder =
                    cache.get_or_set(key, 0, 10, is_persistent, query_id); /// Add range [0, 9]
            auto segments = fromHolder(holder);
            /// Range was not present in cache. It should be added in cache as one while file segment.
            ASSERT_GE(segments.size(), 1);

            assert_range(1, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::EMPTY);

            /// Exception because space not reserved.
            /// EXPECT_THROW(download(segments[0]), DB::Exception);
            /// Exception because space can be reserved only by downloader
            /// EXPECT_THROW(segments[0]->reserve(segments[0]->range().size()), DB::Exception);
            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            assert_range(2, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADING);

            download(segments[0]);
            assert_range(3, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [__________]
        ///                   ^          ^
        ///                   0          9
        ASSERT_EQ(cache.get_file_segments_num(is_persistent), 1);
        ASSERT_EQ(cache.get_used_cache_size(is_persistent), 10);

        {
            /// Want range [5, 14], but [0, 9] already in cache, so only [10, 14] will be put in cache.
            auto holder = cache.get_or_set(key, 5, 10, is_persistent, query_id);
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);

            assert_range(4, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(5, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[1]);
            assert_range(6, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [__________][_____]
        ///                   ^          ^^     ^
        ///                   0          910    14
        ASSERT_EQ(cache.get_file_segments_num(is_persistent), 2);
        ASSERT_EQ(cache.get_used_cache_size(is_persistent), 15);

        {
            auto holder = cache.get_or_set(key, 9, 1, is_persistent, query_id); /// Get [9, 9]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(7, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
        }

        {
            auto holder = cache.get_or_set(key, 9, 2, is_persistent, query_id); /// Get [9, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 2);
            assert_range(8, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(9, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }

        {
            auto holder = cache.get_or_set(key, 10, 1, is_persistent, query_id); /// Get [10, 10]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(10, segments[0], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
        }

        complete(cache.get_or_set(key, 17, 4, is_persistent, query_id)); /// Get [17, 20]
        complete(cache.get_or_set(key, 24, 3, is_persistent, query_id)); /// Get [24, 26]

        /// Current cache:    [__________][_____]   [____]    [___]
        ///                   ^          ^^     ^   ^    ^    ^   ^
        ///                   0          910    14  17   20   24  26
        ///
        ASSERT_EQ(cache.get_file_segments_num(is_persistent), 4);
        ASSERT_EQ(cache.get_used_cache_size(is_persistent), 22);

        {
            auto holder = cache.get_or_set(key, 0, 26, is_persistent, query_id); /// Get [0, 25]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 6);

            assert_range(11, segments[0], io::FileSegment::Range(0, 9),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(12, segments[1], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);

            /// Missing [15, 16] should be added in cache.
            assert_range(13, segments[2], io::FileSegment::Range(15, 16),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[2]);

            assert_range(14, segments[3], io::FileSegment::Range(17, 20),
                         io::FileSegment::State::DOWNLOADED);

            /// New [21, 23], but will not be added in cache because of elements limit (5)
            assert_range(15, segments[4], io::FileSegment::Range(21, 23),
                         io::FileSegment::State::SKIP_CACHE);

            assert_range(16, segments[5], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);

            /// Current cache:    [__________][_____][   ][____]    [___]
            ///                   ^                            ^    ^
            ///                   0                            20   24
            ///

            /// Range [27, 27] must be evicted in previous getOrSet [0, 25].
            /// Let's not invalidate pointers to returned segments from range [0, 25] and
            /// as max elements size is reached, next attempt to put something in cache should fail.
            /// This will also check that [27, 27] was indeed evicted.

            auto holder1 = cache.get_or_set(key, 27, 1, is_persistent, query_id);
            auto segments_1 = fromHolder(holder1); /// Get [27, 27]
            ASSERT_EQ(segments_1.size(), 1);
            assert_range(17, segments_1[0], io::FileSegment::Range(27, 27),
                         io::FileSegment::State::SKIP_CACHE);
        }

        {
            auto holder = cache.get_or_set(key, 12, 10, is_persistent, query_id); /// Get [12, 21]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 4);

            assert_range(18, segments[0], io::FileSegment::Range(10, 14),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(19, segments[1], io::FileSegment::Range(15, 16),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(20, segments[2], io::FileSegment::Range(17, 20),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(21, segments[3], io::FileSegment::Range(21, 21),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[3]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[3]);
            ASSERT_TRUE(segments[3]->state() == io::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [_____][__][____][_]   [___]
        ///                   ^          ^       ^   ^   ^
        ///                   10         17      21  24  26

        ASSERT_EQ(cache.get_file_segments_num(is_persistent), 5);

        {
            auto holder = cache.get_or_set(key, 23, 5, is_persistent, query_id); /// Get [23, 28]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(22, segments[0], io::FileSegment::Range(23, 23),
                         io::FileSegment::State::EMPTY);
            assert_range(23, segments[1], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(24, segments[2], io::FileSegment::Range(27, 27),
                         io::FileSegment::State::EMPTY);

            ASSERT_TRUE(segments[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(segments[0]);
            download(segments[2]);
        }

        /// Current cache:    [____][_]  [][___][__]
        ///                   ^       ^  ^^^   ^^  ^
        ///                   17      21 2324  26  27

        {
            auto holder5 = cache.get_or_set(key, 2, 3, is_persistent, query_id); /// Get [2, 4]
            auto s5 = fromHolder(holder5);
            ASSERT_EQ(s5.size(), 1);
            assert_range(25, s5[0], io::FileSegment::Range(2, 4), io::FileSegment::State::EMPTY);

            auto holder1 = cache.get_or_set(key, 30, 2, is_persistent, query_id); /// Get [30, 31]
            auto s1 = fromHolder(holder1);
            ASSERT_EQ(s1.size(), 1);
            assert_range(26, s1[0], io::FileSegment::Range(30, 31), io::FileSegment::State::EMPTY);

            ASSERT_TRUE(s5[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(s1[0]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            download(s5[0]);
            download(s1[0]);

            /// Current cache:    [___]       [_][___][_]   [__]
            ///                   ^   ^       ^  ^   ^  ^   ^  ^
            ///                   2   4       23 24  26 27  30 31

            auto holder2 = cache.get_or_set(key, 23, 1, is_persistent, query_id); /// Get [23, 23]
            auto s2 = fromHolder(holder2);
            ASSERT_EQ(s2.size(), 1);

            auto holder3 = cache.get_or_set(key, 24, 3, is_persistent, query_id); /// Get [24, 26]
            auto s3 = fromHolder(holder3);
            ASSERT_EQ(s3.size(), 1);

            auto holder4 = cache.get_or_set(key, 27, 1, is_persistent, query_id); /// Get [27, 27]
            auto s4 = fromHolder(holder4);
            ASSERT_EQ(s4.size(), 1);

            /// All cache is now unreleasable because pointers are still hold
            auto holder6 = cache.get_or_set(key, 0, 40, is_persistent, query_id);
            auto f = fromHolder(holder6);
            ASSERT_EQ(f.size(), 9);

            assert_range(27, f[0], io::FileSegment::Range(0, 1),
                         io::FileSegment::State::SKIP_CACHE);
            assert_range(28, f[2], io::FileSegment::Range(5, 22),
                         io::FileSegment::State::SKIP_CACHE);
            assert_range(29, f[6], io::FileSegment::Range(28, 29),
                         io::FileSegment::State::SKIP_CACHE);
            assert_range(30, f[8], io::FileSegment::Range(32, 39),
                         io::FileSegment::State::SKIP_CACHE);
        }

        {
            auto holder = cache.get_or_set(key, 2, 3, is_persistent, query_id); /// Get [2, 4]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 1);
            assert_range(31, segments[0], io::FileSegment::Range(2, 4),
                         io::FileSegment::State::DOWNLOADED);
        }

        /// Current cache:    [___]       [_][___][_]   [__]
        ///                   ^   ^       ^  ^   ^  ^   ^  ^
        ///                   2   4       23 24  26 27  30 31

        {
            auto holder = cache.get_or_set(key, 25, 5, is_persistent, query_id); /// Get [25, 29]
            auto segments = fromHolder(holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(32, segments[0], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);
            assert_range(33, segments[1], io::FileSegment::Range(27, 27),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(34, segments[2], io::FileSegment::Range(28, 29),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[2]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADING);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 = cache.get_or_set(key, 25, 5, is_persistent,
                                                 other_query_id); /// Get [25, 29] once again.
                auto segments_2 = fromHolder(holder_2);
                ASSERT_EQ(segments.size(), 3);

                assert_range(35, segments_2[0], io::FileSegment::Range(24, 26),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(36, segments_2[1], io::FileSegment::Range(27, 27),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(37, segments_2[2], io::FileSegment::Range(28, 29),
                             io::FileSegment::State::DOWNLOADING);

                ASSERT_TRUE(segments[2]->get_or_set_downloader() !=
                            io::FileSegment::get_caller_id());
                ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[2]->wait() == io::FileSegment::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[2]->state() == io::FileSegment::State::DOWNLOADED);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            download(segments[2]);
            ASSERT_TRUE(segments[2]->state() == io::FileSegment::State::DOWNLOADED);

            other_1.join();
        }

        /// Current cache:    [___]       [___][_][__][__]
        ///                   ^   ^       ^   ^  ^^  ^^  ^
        ///                   2   4       24  26 27  2930 31

        {
            /// Now let's check the similar case but getting ERROR state after segment->wait(), when
            /// state is changed not manually via segment->complete(state) but from destructor of holder
            /// and notify_all() is also called from destructor of holder.

            std::optional<io::FileSegmentsHolder> holder;
            holder.emplace(cache.get_or_set(key, 3, 23, is_persistent, query_id)); /// Get [3, 25]

            auto segments = fromHolder(*holder);
            ASSERT_EQ(segments.size(), 3);

            assert_range(38, segments[0], io::FileSegment::Range(2, 4),
                         io::FileSegment::State::DOWNLOADED);

            assert_range(39, segments[1], io::FileSegment::Range(5, 23),
                         io::FileSegment::State::EMPTY);
            ASSERT_TRUE(segments[1]->get_or_set_downloader() == io::FileSegment::get_caller_id());
            ASSERT_TRUE(segments[1]->state() == io::FileSegment::State::DOWNLOADING);

            assert_range(40, segments[2], io::FileSegment::Range(24, 26),
                         io::FileSegment::State::DOWNLOADED);

            bool lets_start_download = false;
            std::mutex mutex;
            std::condition_variable cv;

            std::thread other_1([&] {
                auto holder_2 = cache.get_or_set(key, 3, 23, is_persistent,
                                                 other_query_id); /// Get [3, 25] once again
                auto segments_2 = fromHolder(*holder);
                ASSERT_EQ(segments_2.size(), 3);

                assert_range(41, segments_2[0], io::FileSegment::Range(2, 4),
                             io::FileSegment::State::DOWNLOADED);
                assert_range(42, segments_2[1], io::FileSegment::Range(5, 23),
                             io::FileSegment::State::DOWNLOADING);
                assert_range(43, segments_2[2], io::FileSegment::Range(24, 26),
                             io::FileSegment::State::DOWNLOADED);

                ASSERT_TRUE(segments_2[1]->get_downloader() != io::FileSegment::get_caller_id());
                ASSERT_TRUE(segments_2[1]->state() == io::FileSegment::State::DOWNLOADING);

                {
                    std::lock_guard lock(mutex);
                    lets_start_download = true;
                }
                cv.notify_one();

                while (segments_2[1]->wait() == io::FileSegment::State::DOWNLOADING) {
                }
                ASSERT_TRUE(segments_2[1]->state() == io::FileSegment::State::EMPTY);
                ASSERT_TRUE(segments_2[1]->get_or_set_downloader() ==
                            io::FileSegment::get_caller_id());
                download(segments_2[1]);
            });

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return lets_start_download; });
            }

            holder.reset();
            other_1.join();
            ASSERT_TRUE(segments[1]->state() == io::FileSegment::State::DOWNLOADED);
        }
    }
    /// Current cache:    [___][        ][___][_][__]
    ///                   ^   ^^         ^   ^^  ^  ^
    ///                   2   45       24  2627 28 29

    {
        /// Test LRUCache::restore().

        io::LRUFileCache cache2(cache_base_path, settings);
        cache2.initialize();
        auto holder1 = cache2.get_or_set(key, 2, 28, is_persistent, query_id); /// Get [2, 29]

        auto segments1 = fromHolder(holder1);
        ASSERT_EQ(segments1.size(), 5);

        assert_range(44, segments1[0], io::FileSegment::Range(2, 4),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(45, segments1[1], io::FileSegment::Range(5, 23),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(45, segments1[2], io::FileSegment::Range(24, 26),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(46, segments1[3], io::FileSegment::Range(27, 27),
                     io::FileSegment::State::DOWNLOADED);
        assert_range(47, segments1[4], io::FileSegment::Range(28, 29),
                     io::FileSegment::State::DOWNLOADED);
    }

    {
        /// Test max file segment size

        auto settings2 = settings;
        settings2.max_size = 30;
        settings2.max_elements = 5;
        settings2.persistent_max_size = 30;
        settings2.persistent_max_elements = 5;
        settings2.max_file_segment_size = 10;
        io::LRUFileCache cache2(caches_dir / "cache2", settings2);

        auto holder1 = cache2.get_or_set(key, 0, 25, is_persistent, query_id); /// Get [0, 24]
        auto segments1 = fromHolder(holder1);

        ASSERT_EQ(segments1.size(), 3);
        assert_range(48, segments1[0], io::FileSegment::Range(0, 9), io::FileSegment::State::EMPTY);
        assert_range(49, segments1[1], io::FileSegment::Range(10, 19),
                     io::FileSegment::State::EMPTY);
        assert_range(50, segments1[2], io::FileSegment::Range(20, 24),
                     io::FileSegment::State::EMPTY);
    }
}

TEST(LRUFileCache, normal) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    test_file_cache(false);
    test_file_cache(true);
}

} // namespace doris::io
