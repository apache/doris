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

#include "block_file_cache_test_common.h"

namespace doris::io {

TEST_F(BlockFileCacheTest, test_lru_log_record_replay_dump_restore) {
    config::enable_evict_file_cache_in_advance = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    config::file_cache_background_lru_dump_interval_ms = 3000;
    config::file_cache_background_lru_dump_update_cnt_threshold = 0;
    config::enable_normal_queue_cold_hot_separation = false;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;

    settings.ttl_queue_size = 5000000;
    settings.ttl_queue_elements = 50000;
    settings.query_queue_size = 5000000;
    settings.query_queue_elements = 50000;
    settings.index_queue_size = 5000000;
    settings.index_queue_elements = 50000;
    settings.disposable_queue_size = 5000000;
    settings.disposable_queue_elements = 50000;
    settings.capacity = 20000000;
    settings.max_file_block_size = 100000;
    settings.max_query_cache_size = 30;

    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    int i = 0;
    for (; i < 100; i++) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    io::CacheContext context1;
    ReadStatistics rstats;
    context1.stats = &rstats;
    context1.cache_type = io::FileCacheType::NORMAL;
    context1.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");

    int64_t offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key1, offset, 100000, context1);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }
    io::CacheContext context2;
    context2.stats = &rstats;
    context2.cache_type = io::FileCacheType::INDEX;
    context2.query_id = query_id;
    auto key2 = io::BlockFileCache::hash("key2");

    offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key2, offset, 100000, context2);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }
    io::CacheContext context3;
    context3.stats = &rstats;
    context3.cache_type = io::FileCacheType::TTL;
    context3.query_id = query_id;
    context3.expiration_time = UnixSeconds() + 120;
    auto key3 = io::BlockFileCache::hash("key3");

    offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key3, offset, 100000, context3);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }

    io::CacheContext context4;
    context4.stats = &rstats;
    context4.cache_type = io::FileCacheType::DISPOSABLE;
    context4.query_id = query_id;
    auto key4 = io::BlockFileCache::hash("key4");

    offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key4, offset, 100000, context4);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }
    ASSERT_EQ(cache.get_stats_unsafe()["disposable_queue_curr_size"], 500000);
    ASSERT_EQ(cache.get_stats_unsafe()["ttl_queue_curr_size"], 500000);
    ASSERT_EQ(cache.get_stats_unsafe()["index_queue_curr_size"], 500000);
    ASSERT_EQ(cache.get_stats_unsafe()["normal_queue_curr_size"], 500000);

    // all queue are filled, let's check the lru log records
    ASSERT_EQ(cache._lru_recorder->_ttl_lru_log_queue.size_approx(), 5);
    ASSERT_EQ(cache._lru_recorder->_index_lru_log_queue.size_approx(), 5);
    ASSERT_EQ(cache._lru_recorder->_normal_lru_log_queue.size_approx(), 5);
    ASSERT_EQ(cache._lru_recorder->_disposable_lru_log_queue.size_approx(), 5);

    // then check the log replay
    std::this_thread::sleep_for(std::chrono::milliseconds(
            2 * config::file_cache_background_lru_log_replay_interval_ms));
    ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 5);

    // ok, let do some MOVETOBACK & REMOVE
    {
        auto holder = cache.get_or_set(key2, 200000, 100000,
                                       context2); // move index queue 3rd element to the end
        cache.remove_if_cached(key3);             // remove all element from ttl queue
    }
    ASSERT_EQ(cache._lru_recorder->_ttl_lru_log_queue.size_approx(), 5);
    ASSERT_EQ(cache._lru_recorder->_index_lru_log_queue.size_approx(), 1);
    ASSERT_EQ(cache._lru_recorder->_normal_lru_log_queue.size_approx(), 0);
    ASSERT_EQ(cache._lru_recorder->_disposable_lru_log_queue.size_approx(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(
            2 * config::file_cache_background_lru_log_replay_interval_ms));
    ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 5);

    // check the order
    std::vector<size_t> offsets;
    for (auto it = cache._lru_recorder->_shadow_index_queue.begin();
         it != cache._lru_recorder->_shadow_index_queue.end(); ++it) {
        offsets.push_back(it->offset);
    }
    ASSERT_EQ(offsets.size(), 5);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 100000);
    ASSERT_EQ(offsets[2], 300000);
    ASSERT_EQ(offsets[3], 400000);
    ASSERT_EQ(offsets[4], 200000);

    std::this_thread::sleep_for(
            std::chrono::milliseconds(2 * config::file_cache_background_lru_dump_interval_ms));

#if 1
    // Verify all 4 dump files
    // TODO(zhengyu): abstract those read/write into a function
    {
        std::string filename = fmt::format("{}/lru_dump_{}.tail", cache_base_path, "ttl");

        struct stat file_stat;
        EXPECT_EQ(stat(filename.c_str(), &file_stat), 0) << "File " << filename << " not found";

        EXPECT_EQ(file_stat.st_size, 12) << "File " << filename << " has more data than footer";
        std::ifstream in(filename, std::ios::binary);
        ASSERT_TRUE(in) << "Failed to open " << filename;
        size_t entry_num = 0;
        int8_t version = 0;
        char magic_str[3];
        char target_str[3] = {'D', 'O', 'R'};
        in.read(reinterpret_cast<char*>(&entry_num), sizeof(entry_num));
        in.read(reinterpret_cast<char*>(&version), sizeof(version));
        in.read(magic_str, sizeof(magic_str));
        EXPECT_EQ(entry_num, 0);
        EXPECT_EQ(version, 1);
        EXPECT_TRUE(memcmp(magic_str, target_str, sizeof(magic_str)) == 0);
    }

    {
        std::string filename = fmt::format("{}/lru_dump_{}.tail", cache_base_path, "normal");

        struct stat file_stat;
        EXPECT_EQ(stat(filename.c_str(), &file_stat), 0) << "File " << filename << " not found";

        EXPECT_GT(file_stat.st_size, 12) << "File " << filename << " is empty";

        std::ifstream in(filename, std::ios::binary);
        ASSERT_TRUE(in) << "Failed to open " << filename;
        UInt128Wrapper hash;
        size_t offset, size;
        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key1")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 0) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key1")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 100000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key1")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 200000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key1")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 300000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key1")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 400000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        EXPECT_TRUE(in.fail()) << "still read from " << filename << " which should be EOF";
    }

    {
        std::string filename = fmt::format("{}/lru_dump_{}.tail", cache_base_path, "index");

        struct stat file_stat;
        EXPECT_EQ(stat(filename.c_str(), &file_stat), 0) << "File " << filename << " not found";

        EXPECT_GT(file_stat.st_size, 12) << "File " << filename << " is empty";

        std::ifstream in(filename, std::ios::binary);
        ASSERT_TRUE(in) << "Failed to open " << filename;
        UInt128Wrapper hash;
        size_t offset, size;
        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key2")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 0) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key2")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 100000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key2")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 300000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key2")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 400000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        in.read(reinterpret_cast<char*>(&size), sizeof(size));

        EXPECT_FALSE(in.fail()) << "Failed to read from " << filename;
        EXPECT_EQ(hash, io::BlockFileCache::hash("key2")) << "wrong hash value in " << filename;
        EXPECT_EQ(offset, 200000) << "wrong offset value in " << filename;
        EXPECT_EQ(size, 100000) << "wrong size value in " << filename;

        in.read(reinterpret_cast<char*>(&hash), sizeof(hash));
        EXPECT_TRUE(in.fail()) << "still read from " << filename << " which should be EOF";
    }
#endif

    // dump looks good, let's try restore
    io::BlockFileCache cache2(cache_base_path, settings);
    ASSERT_TRUE(cache2.initialize());
    for (i = 0; i < 100; i++) {
        if (cache2.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(cache2.get_async_open_success());

    // check the size of cache2
    ASSERT_EQ(cache2._ttl_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._index_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._disposable_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._cur_cache_size, 1500000);

    // then check the order of restored cache2
    std::vector<size_t> offsets2;
    for (auto it = cache2._index_queue.begin(); it != cache2._index_queue.end(); ++it) {
        offsets2.push_back(it->offset);
    }
    ASSERT_EQ(offsets2.size(), 5);
    ASSERT_EQ(offsets2[0], 0);
    ASSERT_EQ(offsets2[1], 100000);
    ASSERT_EQ(offsets2[2], 300000);
    ASSERT_EQ(offsets2[3], 400000);
    ASSERT_EQ(offsets2[4], 200000);

    io::CacheContext context22;
    context22.stats = &rstats;
    context22.cache_type = io::FileCacheType::INDEX;
    context22.query_id = query_id;

    offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache2.get_or_set(key2, offset, 100000, context22);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);
        blocks.clear();
    }

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_lru_duplicate_queue_entry_restore) {
    config::enable_evict_file_cache_in_advance = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    config::file_cache_background_lru_dump_interval_ms = 3000;
    config::file_cache_background_lru_dump_update_cnt_threshold = 0;
    config::enable_normal_queue_cold_hot_separation = false;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;

    settings.ttl_queue_size = 5000000;
    settings.ttl_queue_elements = 50000;
    settings.query_queue_size = 5000000;
    settings.query_queue_elements = 50000;
    settings.index_queue_size = 5000000;
    settings.index_queue_elements = 50000;
    settings.disposable_queue_size = 5000000;
    settings.disposable_queue_elements = 50000;
    settings.capacity = 20000000;
    settings.max_file_block_size = 100000;
    settings.max_query_cache_size = 30;

    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    int i = 0;
    for (; i < 100; i++) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    io::CacheContext context1;
    ReadStatistics rstats;
    context1.stats = &rstats;
    context1.cache_type = io::FileCacheType::NORMAL;
    context1.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");

    int64_t offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key1, offset, 100000, context1);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }

    std::this_thread::sleep_for(
            std::chrono::milliseconds(2 * config::file_cache_background_lru_dump_interval_ms));

    // now we have NORMAL queue dump, let's copy the dump and name it as TTL to create dup
    std::filesystem::path src = cache_base_path / "lru_dump_normal.tail";
    std::filesystem::path dst = cache_base_path / "lru_dump_ttl.tail";
    std::filesystem::copy(src, dst);

    // let's try restore
    io::BlockFileCache cache2(cache_base_path, settings);
    ASSERT_TRUE(cache2.initialize());
    for (i = 0; i < 100; i++) {
        if (cache2.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(cache2.get_async_open_success());

    // the dup part should be ttl because ttl has higner priority
    ASSERT_EQ(cache2._ttl_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._index_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._disposable_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._cur_cache_size, 500000);

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, test_lru_log_record_replay_dump_restore_cold_hot_seperation) {
    config::enable_evict_file_cache_in_advance = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    config::file_cache_background_lru_dump_interval_ms = 3000;
    config::file_cache_background_lru_dump_update_cnt_threshold = 0;
    config::enable_normal_queue_cold_hot_separation = true;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::FileCacheSettings settings;

    settings.ttl_queue_size = 0;
    settings.ttl_queue_elements = 0;
    settings.query_queue_size = 5000000;
    settings.query_queue_elements = 50000;
    settings.cold_query_queue_size = 5000000;
    settings.cold_query_queue_elements = 50000;
    settings.index_queue_size = 0;
    settings.index_queue_elements = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.capacity = 10000000;
    settings.max_file_block_size = 100000;
    settings.max_query_cache_size = 30;

    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());
    int i = 0;
    for (; i < 100; i++) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    io::CacheContext context1;
    ReadStatistics rstats;
    context1.stats = &rstats;
    context1.cache_type = io::FileCacheType::NORMAL;
    context1.query_id = query_id;
    auto key1 = io::BlockFileCache::hash("key1");

    int64_t offset = 0;

    for (; offset < 1000000; offset += 100000) {
        auto holder = cache.get_or_set(key1, offset, 100000, context1);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);

        blocks.clear();
    }

    offset = 0;

    for (; offset < 500000; offset += 100000) {
        auto holder = cache.get_or_set(key1, offset, 100000, context1);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);

        assert_range(1, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);
        blocks.clear();
    }

    ASSERT_EQ(cache.get_stats_unsafe()["ttl_queue_curr_size"], 0);
    ASSERT_EQ(cache.get_stats_unsafe()["index_queue_curr_size"], 0);
    ASSERT_EQ(cache.get_stats_unsafe()["normal_queue_curr_size"], 500000);
    ASSERT_EQ(cache.get_stats_unsafe()["cold_normal_queue_curr_size"], 500000);
    ASSERT_EQ(cache.get_stats_unsafe()["disposable_queue_curr_size"], 0);

    // all queue are filled, let's check the lru log records
    ASSERT_EQ(cache._lru_recorder->_ttl_lru_log_queue.size_approx(), 0);
    ASSERT_EQ(cache._lru_recorder->_index_lru_log_queue.size_approx(), 0);
    ASSERT_EQ(cache._lru_recorder->_normal_lru_log_queue.size_approx(), 5);
    ASSERT_EQ(cache._lru_recorder->_cold_normal_lru_log_queue.size_approx(), 15);
    ASSERT_EQ(cache._lru_recorder->_disposable_lru_log_queue.size_approx(), 0);

    // then check the log replay
    std::this_thread::sleep_for(std::chrono::milliseconds(
            2 * config::file_cache_background_lru_log_replay_interval_ms));
    ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_cold_normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 0);

    // check the order
    std::vector<size_t> offsets;
    for (auto it = cache._lru_recorder->_shadow_normal_queue.begin();
         it != cache._lru_recorder->_shadow_normal_queue.end(); ++it) {
        offsets.push_back(it->offset);
    }
    ASSERT_EQ(offsets.size(), 5);
    ASSERT_EQ(offsets[0], 0);
    ASSERT_EQ(offsets[1], 100000);
    ASSERT_EQ(offsets[2], 200000);
    ASSERT_EQ(offsets[3], 300000);
    ASSERT_EQ(offsets[4], 400000);

    offsets.clear();
    for (auto it = cache._lru_recorder->_shadow_cold_normal_queue.begin();
         it != cache._lru_recorder->_shadow_cold_normal_queue.end(); ++it) {
        offsets.push_back(it->offset);
    }
    ASSERT_EQ(offsets.size(), 5);
    ASSERT_EQ(offsets[0], 500000);
    ASSERT_EQ(offsets[1], 600000);
    ASSERT_EQ(offsets[2], 700000);
    ASSERT_EQ(offsets[3], 800000);
    ASSERT_EQ(offsets[4], 900000);

    std::this_thread::sleep_for(
            std::chrono::milliseconds(2 * config::file_cache_background_lru_dump_interval_ms));

    // dump looks good, let's try restore
    io::BlockFileCache cache2(cache_base_path, settings);
    ASSERT_TRUE(cache2.initialize());
    for (i = 0; i < 100; i++) {
        if (cache2.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(cache2.get_async_open_success());

    // check the size of cache2
    ASSERT_EQ(cache2._ttl_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._index_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._cold_normal_queue.get_elements_num_unsafe(), 5);
    ASSERT_EQ(cache2._disposable_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._cur_cache_size, 1000000);

    // then check the order of restored cache2
    std::vector<size_t> offsets2;
    for (auto it = cache2._normal_queue.begin(); it != cache2._normal_queue.end(); ++it) {
        offsets2.push_back(it->offset);
    }
    ASSERT_EQ(offsets2.size(), 5);
    ASSERT_EQ(offsets2[0], 0);
    ASSERT_EQ(offsets2[1], 100000);
    ASSERT_EQ(offsets2[2], 200000);
    ASSERT_EQ(offsets2[3], 300000);
    ASSERT_EQ(offsets2[4], 400000);

    offsets2.clear();
    for (auto it = cache2._cold_normal_queue.begin(); it != cache2._cold_normal_queue.end(); ++it) {
        offsets2.push_back(it->offset);
    }
    ASSERT_EQ(offsets2.size(), 5);
    ASSERT_EQ(offsets2[0], 500000);
    ASSERT_EQ(offsets2[1], 600000);
    ASSERT_EQ(offsets2[2], 700000);
    ASSERT_EQ(offsets2[3], 800000);
    ASSERT_EQ(offsets2[4], 900000);

    offset = 0;

    for (; offset < 1000000; offset += 100000) {
        auto holder = cache2.get_or_set(key1, offset, 100000, context1);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(2, blocks[0], io::FileBlock::Range(offset, offset + 99999),
                     io::FileBlock::State::DOWNLOADED);
        blocks.clear();
    }

    ASSERT_EQ(cache2.get_stats_unsafe()["ttl_queue_curr_size"], 0);
    ASSERT_EQ(cache2.get_stats_unsafe()["index_queue_curr_size"], 0);
    ASSERT_EQ(cache2.get_stats_unsafe()["normal_queue_curr_size"], 1000000);
    ASSERT_EQ(cache2.get_stats_unsafe()["cold_normal_queue_curr_size"], 0);
    ASSERT_EQ(cache2.get_stats_unsafe()["disposable_queue_curr_size"], 0);

    // all queue are filled, let's check the lru log records
    ASSERT_EQ(cache2._lru_recorder->_ttl_lru_log_queue.size_approx(), 0);
    ASSERT_EQ(cache2._lru_recorder->_index_lru_log_queue.size_approx(), 0);
    ASSERT_EQ(cache2._lru_recorder->_normal_lru_log_queue.size_approx(), 15);
    ASSERT_EQ(cache2._lru_recorder->_cold_normal_lru_log_queue.size_approx(), 10);
    ASSERT_EQ(cache2._lru_recorder->_disposable_lru_log_queue.size_approx(), 0);

    // then check the log replay
    std::this_thread::sleep_for(std::chrono::milliseconds(
            2 * config::file_cache_background_lru_log_replay_interval_ms));
    ASSERT_EQ(cache2._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 10);
    ASSERT_EQ(cache2._lru_recorder->_shadow_cold_normal_queue.get_elements_num_unsafe(), 0);
    ASSERT_EQ(cache2._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 0);

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

} // namespace doris::io
