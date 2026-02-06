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

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif

#define private public
#define protected public
#include "block_file_cache_test_common.h"
#undef private
#undef protected

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::io {

namespace {

static size_t verify_meta_key_cnt = 0;

void verify_meta_key(CacheBlockMetaStore& meta_store, int64_t tablet_id,
                     const std::string& key_name, size_t offset, FileCacheType expected_type,
                     uint64_t ttl, size_t size) {
    verify_meta_key_cnt++;
    std::cout << "verify_meta_key called " << verify_meta_key_cnt << " times" << std::endl;
    BlockMetaKey mkey(tablet_id, io::BlockFileCache::hash(key_name), offset);
    auto meta = meta_store.get(mkey);
    ASSERT_TRUE(meta.has_value());
    ASSERT_EQ(meta->type, expected_type);
    ASSERT_EQ(meta->ttl, ttl);
    ASSERT_EQ(meta->size, size);
}

} // namespace

TEST_F(BlockFileCacheTest, version3_add_remove_restart) {
    config::enable_evict_file_cache_in_advance = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    config::file_cache_background_lru_dump_interval_ms = 3000;
    config::file_cache_background_lru_dump_update_cnt_threshold = 0;
    config::file_cache_background_lru_dump_tail_record_num =
            2; // only dump last 2, to check dump works with meta store
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

    uint64_t expiration_time = 120;

    int i = 0;
    { // cache1
        io::BlockFileCache cache(cache_base_path, settings);
        ASSERT_TRUE(cache.initialize());
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
        context1.tablet_id = 47;
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
        context2.tablet_id = 48;
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
        context3.expiration_time = expiration_time;
        context3.tablet_id = 49;
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
        context4.tablet_id = 50;
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

        // check the meta store to see the content
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            // Check if storage is FSFileCacheStorage before accessing _meta_store
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(*meta_store, 47, "key1", 0, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 100000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 200000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 300000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 400000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 48, "key2", 0, FileCacheType::INDEX, 0, 100000);
            verify_meta_key(*meta_store, 48, "key2", 100000, FileCacheType::INDEX, 0, 100000);
            verify_meta_key(*meta_store, 48, "key2", 200000, FileCacheType::INDEX, 0, 100000);
            verify_meta_key(*meta_store, 48, "key2", 300000, FileCacheType::INDEX, 0, 100000);
            verify_meta_key(*meta_store, 48, "key2", 400000, FileCacheType::INDEX, 0, 100000);
            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 50, "key4", 0, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 100000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 200000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 300000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 400000, FileCacheType::DISPOSABLE, 0, 100000);
        }

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

        // do some REMOVE
        {
            cache.remove_if_cached(key2); // remove all element from index queue
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(
                2 * config::file_cache_background_lru_log_replay_interval_ms));
        ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 5);
        ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 0);
        ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 5);
        ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 5);

        // check the meta store to see the content
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(*meta_store, 47, "key1", 0, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 100000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 200000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 300000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 400000, FileCacheType::NORMAL, 0, 100000);

            BlockMetaKey mkey(48, io::BlockFileCache::hash("key2"), 0);
            auto meta = meta_store->get(mkey);
            ASSERT_FALSE(meta.has_value());

            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 50, "key4", 0, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 100000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 200000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 300000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 400000, FileCacheType::DISPOSABLE, 0, 100000);
        }
        std::this_thread::sleep_for(
                std::chrono::milliseconds(2 * config::file_cache_background_lru_dump_interval_ms));
    }

    { // cache2
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

        // check the size of cache2
        ASSERT_EQ(cache2._ttl_queue.get_elements_num_unsafe(), 5);
        ASSERT_EQ(cache2._index_queue.get_elements_num_unsafe(), 0);
        ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 5);
        ASSERT_EQ(cache2._disposable_queue.get_elements_num_unsafe(), 5);
        ASSERT_EQ(cache2._cur_cache_size, 1500000);

        // check meta store
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache2._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(*meta_store, 47, "key1", 0, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 100000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 200000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 300000, FileCacheType::NORMAL, 0, 100000);
            verify_meta_key(*meta_store, 47, "key1", 400000, FileCacheType::NORMAL, 0, 100000);

            BlockMetaKey mkey(48, io::BlockFileCache::hash("key2"), 0);
            auto meta = meta_store->get(mkey);
            ASSERT_FALSE(meta.has_value());

            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::TTL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 50, "key4", 0, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 100000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 200000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 300000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 400000, FileCacheType::DISPOSABLE, 0, 100000);
        }

        // check blocks restored from lru dump get updated ttl and tablet_id
        {
            io::CacheContext context;
            ReadStatistics rstats;
            context.stats = &rstats;
            context.cache_type = io::FileCacheType::TTL;
            context.tablet_id = 49;
            context.expiration_time = expiration_time;
            auto key = io::BlockFileCache::hash("key3");

            auto holder =
                    cache2.get_or_set(key, 0, 100000, context); // offset = 0 is restore from dump
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            auto block = blocks[0];
            ASSERT_EQ(block->tablet_id(), 49);
        }

        // do some meta change - type
        {
            io::CacheContext context;
            ReadStatistics rstats;
            context.stats = &rstats;
            context.cache_type = io::FileCacheType::NORMAL;
            context.tablet_id = 47;
            auto key = io::BlockFileCache::hash("key1");

            auto holder = cache2.get_or_set(key, 300000, 100000, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            auto block = blocks[0];
            ASSERT_EQ(block->tablet_id(), 47);

            ASSERT_TRUE(blocks[0]->change_cache_type(io::FileCacheType::INDEX));
        }
        // check the meta
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache2._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(*meta_store, 47, "key1", 300000, FileCacheType::INDEX, 0, 100000);
        }
        // change ttl
        {
            io::CacheContext context;
            ReadStatistics rstats;
            context.stats = &rstats;
            context.cache_type = io::FileCacheType::TTL;
            context.tablet_id = 49;
            context.expiration_time = expiration_time + 3600;
            auto key = io::BlockFileCache::hash("key3");

            auto holder =
                    cache2.get_or_set(key, 0, 100000, context); // offset = 0 is restore from dump
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            auto block = blocks[0];
        }
        // check the meta
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache2._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(
                    *meta_store, 49, "key3", 0, FileCacheType::TTL, expiration_time,
                    100000); // won't change ttl when get_or_set now as we introduce ttl mgr to manage ttl
        }
    }

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, version3_write_version_when_cache_dir_empty) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);

    io::FileCacheSettings settings;
    settings.storage = "disk";
    settings.capacity = 10_mb;
    settings.max_file_block_size = 1_mb;
    settings.max_query_cache_size = settings.capacity;
    settings.disposable_queue_size = settings.capacity;
    settings.disposable_queue_elements = 8;
    settings.index_queue_size = settings.capacity;
    settings.index_queue_elements = 8;
    settings.query_queue_size = settings.capacity;
    settings.query_queue_elements = 8;
    settings.ttl_queue_size = settings.capacity;
    settings.ttl_queue_elements = 8;

    io::BlockFileCache cache(cache_base_path, settings);
    ASSERT_TRUE(cache.initialize());

    for (int i = 0; i < 100; ++i) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    std::ifstream ifs(cache_base_path + "/version", std::ios::binary);
    ASSERT_TRUE(ifs.good());
    char buf[3] = {0};
    ifs.read(buf, 3);
    ASSERT_EQ(std::string(buf, static_cast<size_t>(ifs.gcount())), "3.0");
}

TEST_F(BlockFileCacheTest, clear_retains_meta_directory_and_clears_meta_entries) {
    config::enable_evict_file_cache_in_advance = false;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);

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
    for (int i = 0; i < 100; i++) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    io::CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = io::FileCacheType::NORMAL;
    context.query_id.hi = 1;
    context.query_id.lo = 2;
    context.tablet_id = 314;
    auto key = io::BlockFileCache::hash("meta_clear_key");

    auto holder = cache.get_or_set(key, 0, 100000, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    assert_range(1, blocks[0], io::FileBlock::Range(0, 99999), io::FileBlock::State::EMPTY);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    download(blocks[0]);
    assert_range(2, blocks[0], io::FileBlock::Range(0, 99999), io::FileBlock::State::DOWNLOADED);
    blocks.clear();

    auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
    ASSERT_NE(fs_storage, nullptr) << "Expected FSFileCacheStorage but got different storage type";
    auto& meta_store = fs_storage->_meta_store;

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    verify_meta_key(*meta_store, context.tablet_id, "meta_clear_key", 0, FileCacheType::NORMAL, 0,
                    100000);

    cache.clear_file_cache_directly();

    std::string meta_dir = cache.get_base_path() + "/meta";
    ASSERT_TRUE(fs::exists(meta_dir));
    ASSERT_TRUE(fs::is_directory(meta_dir));

    BlockMetaKey mkey(context.tablet_id, key, 0);
    auto meta = meta_store->get(mkey);
    ASSERT_FALSE(meta.has_value());

    auto iterator = meta_store->get_all();
    if (iterator != nullptr) {
        bool has_entry = false;
        for (; iterator->valid(); iterator->next()) {
            has_entry = true;
            break;
        }
        ASSERT_FALSE(has_entry) << "Meta store still contains entries after clearing cache";
    }

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, handle_already_loaded_block_updates_size_and_tablet) {
    config::enable_evict_file_cache_in_advance = false;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);

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
    for (int i = 0; i < 100; ++i) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    io::CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = io::FileCacheType::NORMAL;
    context.query_id.hi = 11;
    context.query_id.lo = 12;
    context.tablet_id = 0;
    auto key = io::BlockFileCache::hash("sync_cached_block_meta_key");

    constexpr size_t kOriginalSize = 100000;
    auto holder = cache.get_or_set(key, 0, kOriginalSize, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
    download(blocks[0], kOriginalSize);
    blocks.clear();

    auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
    ASSERT_NE(fs_storage, nullptr) << "Expected FSFileCacheStorage but got different storage type";

    constexpr size_t kNewSize = 2 * kOriginalSize;
    constexpr int64_t kTabletId = 4242;
    bool handled = false;
    {
        SCOPED_CACHE_LOCK(cache._mutex, (&cache));
        handled = fs_storage->handle_already_loaded_block(&cache, key, 0, kNewSize, kTabletId,
                                                          cache_lock);
    }

    ASSERT_TRUE(handled);
    auto& cell = cache._files[key][0];
    EXPECT_EQ(cell.file_block->tablet_id(), kTabletId);
    EXPECT_EQ(cache._cur_cache_size, kNewSize);
    EXPECT_EQ(cache._normal_queue.get_capacity_unsafe(), kNewSize);

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

//TODO(zhengyu): check lazy load
//TODO(zhengyu): check version2 start
//TODO(zhengyu): check version2 version3 mixed start

} // namespace doris::io
