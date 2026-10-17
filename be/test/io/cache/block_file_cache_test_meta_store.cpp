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

#include <tuple>

#include "io/cache/block_file_cache_test_common.h"
#include "util/defer_op.h"

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
    const auto old_enable_evict = config::enable_evict_file_cache_in_advance;
    const auto old_disk_limit_percent = config::file_cache_enter_disk_resource_limit_mode_percent;
    const auto old_dump_interval_ms = config::file_cache_background_lru_dump_interval_ms;
    const auto old_dump_update_cnt_threshold =
            config::file_cache_background_lru_dump_update_cnt_threshold;
    const auto old_dump_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_replay_interval_ms = config::file_cache_background_lru_log_replay_interval_ms;
    Defer defer {[old_enable_evict, old_disk_limit_percent, old_dump_interval_ms,
                  old_dump_update_cnt_threshold, old_dump_tail_record_num, old_replay_interval_ms] {
        config::enable_evict_file_cache_in_advance = old_enable_evict;
        config::file_cache_enter_disk_resource_limit_mode_percent = old_disk_limit_percent;
        config::file_cache_background_lru_dump_interval_ms = old_dump_interval_ms;
        config::file_cache_background_lru_dump_update_cnt_threshold = old_dump_update_cnt_threshold;
        config::file_cache_background_lru_dump_tail_record_num = old_dump_tail_record_num;
        config::file_cache_background_lru_log_replay_interval_ms = old_replay_interval_ms;
    }};

    config::enable_evict_file_cache_in_advance = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 99;
    config::file_cache_background_lru_dump_interval_ms = 3000;
    config::file_cache_background_lru_dump_update_cnt_threshold = 0;
    config::file_cache_background_lru_dump_tail_record_num =
            2; // only dump last 2, to check dump works with meta store
    config::file_cache_background_lru_log_replay_interval_ms = 60 * 60 * 1000;
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

    // key3 is written with a TTL that has not run out. What is persisted for it is its class
    // (NORMAL) and this deadline; the TTL queue is where it goes because the deadline is ahead.
    const uint64_t expiration_time = UnixSeconds() + 120;

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
            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::NORMAL, expiration_time,
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
        ASSERT_EQ(cache.replay_lru_logs_once(), 20);
        ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 2);
        ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 2);
        ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 2);
        ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 2);

        // do some REMOVE
        {
            cache.remove_if_cached(key2); // remove all element from index queue
        }

        ASSERT_EQ(cache.replay_lru_logs_once(), 5);
        ASSERT_EQ(cache._lru_recorder->_shadow_ttl_queue.get_elements_num_unsafe(), 2);
        ASSERT_EQ(cache._lru_recorder->_shadow_index_queue.get_elements_num_unsafe(), 0);
        ASSERT_EQ(cache._lru_recorder->_shadow_normal_queue.get_elements_num_unsafe(), 2);
        ASSERT_EQ(cache._lru_recorder->_shadow_disposable_queue.get_elements_num_unsafe(), 2);
        EXPECT_EQ(cache.replay_lru_logs_once(), 0);
        EXPECT_EQ(cache._lru_recorder_log_replay_idle_metrics->get_value(), 1);
        cache.dump_lru_queues(true);

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

            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 50, "key4", 0, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 100000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 200000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 300000, FileCacheType::DISPOSABLE, 0, 100000);
            verify_meta_key(*meta_store, 50, "key4", 400000, FileCacheType::DISPOSABLE, 0, 100000);
        }
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

            verify_meta_key(*meta_store, 49, "key3", 0, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 100000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 200000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 300000, FileCacheType::NORMAL, expiration_time,
                            100000);
            verify_meta_key(*meta_store, 49, "key3", 400000, FileCacheType::NORMAL, expiration_time,
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
        }
        // check the meta
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache2._storage.get());
            ASSERT_NE(fs_storage, nullptr)
                    << "Expected FSFileCacheStorage but got different storage type";

            auto& meta_store = fs_storage->_meta_store;
            verify_meta_key(*meta_store, 47, "key1", 300000, FileCacheType::NORMAL, 0, 100000);
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
                    *meta_store, 49, "key3", 0, FileCacheType::NORMAL, expiration_time,
                    100000); // won't change ttl when get_or_set now as we introduce ttl mgr to manage ttl
        }
    }

    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
}

TEST_F(BlockFileCacheTest, sharded_writer_map_tracks_multiple_inflight_writers) {
    std::string cache_path =
            caches_dir / "sharded_writer_map_tracks_multiple_inflight_writers" / "";
    if (fs::exists(cache_path)) {
        fs::remove_all(cache_path);
    }
    fs::create_directories(cache_path);

    io::FileCacheSettings settings;
    settings.query_queue_size = 1024 * 1024;
    settings.query_queue_elements = 1024;
    settings.index_queue_size = 1024;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1024;
    settings.disposable_queue_elements = 1;
    settings.capacity =
            settings.query_queue_size + settings.index_queue_size + settings.disposable_queue_size;
    settings.max_file_block_size = 4096;
    settings.max_query_cache_size = 4096;

    io::BlockFileCache cache(cache_path, settings);
    ASSERT_TRUE(cache.initialize());
    for (int i = 0; i < 100; i++) {
        if (cache.get_async_open_success()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(cache.get_async_open_success());

    auto* storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
    ASSERT_NE(storage, nullptr);

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;
    io::CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = io::FileCacheType::NORMAL;
    context.query_id = query_id;
    context.tablet_id = 701;

    std::vector<io::FileBlocksHolder> holders;
    std::vector<io::FileBlockSPtr> blocks;
    std::vector<std::string> payloads;
    std::unordered_set<size_t> expected_shards;
    constexpr size_t block_count = 32;
    holders.reserve(block_count);
    blocks.reserve(block_count);
    payloads.reserve(block_count);

    for (size_t i = 0; i < block_count; ++i) {
        auto hash = io::BlockFileCache::hash("sharded_writer_map_key_" + std::to_string(i));
        size_t offset = i * settings.max_file_block_size;
        payloads.emplace_back(128, static_cast<char>('a' + i % 26));
        auto holder = cache.get_or_set(hash, offset, payloads.back().size(), context);
        auto holder_blocks = fromHolder(holder);
        ASSERT_EQ(holder_blocks.size(), 1);
        ASSERT_EQ(holder_blocks[0]->get_or_set_downloader(), io::FileBlock::get_caller_id());
        ASSERT_TRUE(holder_blocks[0]
                            ->append(Slice(payloads.back().data(), payloads.back().size()))
                            .ok());

        expected_shards.insert(FileWriterMapKeyHash {}({hash, offset}) &
                               FSFileCacheStorage::kWriterShardMask);
        blocks.push_back(holder_blocks[0]);
        holders.emplace_back(std::move(holder));
    }
    ASSERT_GT(expected_shards.size(), 1);

    size_t pending_writers = 0;
    size_t nonempty_shards = 0;
    for (auto& shard : storage->_writer_shards) {
        std::lock_guard lock(shard->mtx);
        pending_writers += shard->map.size();
        if (!shard->map.empty()) {
            ++nonempty_shards;
        }
    }
    ASSERT_EQ(pending_writers, block_count);
    ASSERT_EQ(nonempty_shards, expected_shards.size());

    for (auto& block : blocks) {
        ASSERT_TRUE(block->finalize().ok());
    }

    pending_writers = 0;
    for (auto& shard : storage->_writer_shards) {
        std::lock_guard lock(shard->mtx);
        pending_writers += shard->map.size();
    }
    ASSERT_EQ(pending_writers, 0);

    for (size_t i = 0; i < blocks.size(); ++i) {
        std::string data(payloads[i].size(), '\0');
        ASSERT_TRUE(blocks[i]->read(Slice(data.data(), data.size()), 0).ok());
        ASSERT_EQ(data, payloads[i]);
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

    {
        auto holder = cache.get_or_set(key, 0, 100000, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        assert_range(1, blocks[0], io::FileBlock::Range(0, 99999), io::FileBlock::State::EMPTY);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == io::FileBlock::get_caller_id());
        download(blocks[0]);
        assert_range(2, blocks[0], io::FileBlock::Range(0, 99999),
                     io::FileBlock::State::DOWNLOADED);
    }

    auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
    ASSERT_NE(fs_storage, nullptr) << "Expected FSFileCacheStorage but got different storage type";
    auto& meta_store = fs_storage->_meta_store;

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    verify_meta_key(*meta_store, context.tablet_id, "meta_clear_key", 0, FileCacheType::NORMAL, 0,
                    100000);

    cache.clear_file_cache_sync();

    std::string meta_dir = cache.get_base_path() + "/meta";
    ASSERT_TRUE(fs::exists(meta_dir));
    ASSERT_TRUE(fs::is_directory(meta_dir));

    BlockMetaKey mkey(context.tablet_id, key, 0);
    for (int i = 0; i < 100 && meta_store->get(mkey).has_value(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(meta_store->get(mkey).has_value());

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
    io::CacheContext loaded;
    loaded.cache_type = io::FileCacheType::NORMAL;
    loaded.tablet_id = kTabletId;
    bool handled = false;
    {
        SCOPED_CACHE_LOCK(cache._mutex, (&cache));
        handled = fs_storage->handle_already_loaded_block(&cache, key, 0, kNewSize, loaded,
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

namespace {

class CountingFSFileCacheStorage : public FSFileCacheStorage {
public:
    void load_file_blocks_directly_unlocked(BlockFileCache* mgr, const UInt128Wrapper& hash,
                                            int64_t tablet_id,
                                            std::lock_guard<std::mutex>& cache_lock) override {
        ++direct_loads;
        FSFileCacheStorage::load_file_blocks_directly_unlocked(mgr, hash, tablet_id, cache_lock);
    }

    size_t direct_loads = 0;
};

TEST_F(BlockFileCacheTest, CompletedAsyncOpenSkipsDirectLoad) {
    const auto path = caches_dir / "completed_async_open_skips_direct_load";
    fs::remove_all(path);
    fs::create_directories(path);
    Defer remove_cache {[&] { fs::remove_all(path); }};
    auto settings = get_file_cache_settings(8_mb, 0);
    settings.max_file_block_size = 4096;
    BlockFileCache cache(path.string(), settings);
    auto storage = std::make_unique<CountingFSFileCacheStorage>();
    auto* storage_ptr = storage.get();
    cache._storage = std::move(storage);
    ASSERT_TRUE(cache.initialize().ok());
    storage_ptr->_cache_background_load_thread.join();
    ASSERT_TRUE(cache.get_async_open_success());

    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    const auto hash = BlockFileCache::hash("completed_async_open_skips_direct_load");
    FileBlocks blocks;
    bool fully_covered = true;
    ASSERT_TRUE(cache.get_downloaded_blocks_if_fully_covered(hash, 4096, 8192, context, &blocks,
                                                             &fully_covered)
                        .ok());
    EXPECT_FALSE(fully_covered);
    auto result = cache.probe(hash, 4096, 8192, context);
    EXPECT_EQ(result.file_blocks, (std::vector<FileBlockSPtr> {nullptr, nullptr}));
    auto holder = cache.get_or_set(hash, 4096, 8192, context);
    EXPECT_EQ(storage_ptr->direct_loads, 0);
}

enum class LazyRestoreLookup { GET_OR_SET, DOWNLOADED, PROBE };

class FileCacheLazyRestoreTest
        : public BlockFileCacheTest,
          public testing::WithParamInterface<std::tuple<LazyRestoreLookup, bool>> {
protected:
    void SetUp() override {
        fs::remove_all(path);
        fs::create_directories(path);
        {
            // Destruction drains pending writes before the restarted cache opens the database.
            CacheBlockMetaStore meta_store((path / "meta").string());
            meta_store.put(BlockMetaKey(tablet_id, hash, block_size),
                           BlockMeta(FileCacheType::INDEX, block_size));
            meta_store.put(BlockMetaKey(tablet_id, hash, 2 * block_size),
                           BlockMeta(FileCacheType::NORMAL, block_size));
        }
        sync_point->set_call_back(
                "BlockFileCache::TmpFile1",
                [&](auto&&) {
                    std::unique_lock lock(mutex);
                    background_paused = true;
                    cv.notify_all();
                    cv.wait(lock, [&] { return resume_background; });
                },
                &guard);
        sync_point->enable_processing();
        auto settings = get_file_cache_settings(8_mb, 0);
        settings.max_file_block_size = block_size;
        cache = std::make_unique<BlockFileCache>(path.string(), settings);
        ASSERT_TRUE(cache->initialize().ok());
        {
            std::unique_lock lock(mutex);
            ASSERT_TRUE(
                    cv.wait_for(lock, std::chrono::seconds(10), [&] { return background_paused; }));
        }
        ASSERT_FALSE(cache->get_async_open_success());
        ASSERT_FALSE(cache->_files.contains(hash));
    }

    void TearDown() override {
        {
            std::lock_guard lock(mutex);
            resume_background = true;
        }
        cv.notify_all();
        cache.reset();
        sync_point->disable_processing();
        fs::remove_all(path);
    }

    static constexpr int64_t tablet_id = 47;
    static constexpr size_t block_size = 4096;
    const fs::path path = caches_dir / "lazy_restore_file_blocks";
    const UInt128Wrapper hash = BlockFileCache::hash("lazy_restore_file_blocks");
    std::mutex mutex;
    std::condition_variable cv;
    bool background_paused = false;
    bool resume_background = false;
    SyncPoint* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    std::unique_ptr<BlockFileCache> cache;
};

TEST_P(FileCacheLazyRestoreTest, RestoreAllFileBlocksBeforeBackgroundLoad) {
    const auto [lookup, unaligned] = GetParam();
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    context.tablet_id = tablet_id;
    const size_t offset = block_size + (unaligned ? 100 : 0);
    const size_t size = 3 * block_size - offset;
    FileBlocks blocks;
    switch (lookup) {
    case LazyRestoreLookup::GET_OR_SET: {
        auto holder = cache->get_or_set(hash, offset, size, context);
        blocks = holder.file_blocks;
        break;
    }
    case LazyRestoreLookup::DOWNLOADED: {
        bool fully_covered = false;
        ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context,
                                                                  &blocks, &fully_covered)
                            .ok());
        EXPECT_TRUE(fully_covered);
        break;
    }
    case LazyRestoreLookup::PROBE: {
        auto result = cache->probe(hash, offset, size, context);
        blocks.assign(result.file_blocks.begin(), result.file_blocks.end());
        break;
    }
    }

    ASSERT_EQ(blocks.size(), 2);
    size_t expected_offset = block_size;
    for (const auto& block : blocks) {
        ASSERT_NE(block, nullptr);
        EXPECT_EQ(block->state(), FileBlock::State::DOWNLOADED);
        EXPECT_EQ(block->offset(), expected_offset);
        EXPECT_EQ(block->range().size(), block_size);
        EXPECT_EQ(block->tablet_id(), tablet_id);
        EXPECT_EQ(block->cache_type(),
                  expected_offset == block_size ? FileCacheType::INDEX : FileCacheType::NORMAL);
        expected_offset += block_size;
    }
    EXPECT_EQ(cache->_cur_cache_size, 2 * block_size);

    {
        std::lock_guard lock(mutex);
        resume_background = true;
    }
    cv.notify_all();
    auto* storage = static_cast<FSFileCacheStorage*>(cache->_storage.get());
    storage->_cache_background_load_thread.join();
    ASSERT_TRUE(cache->get_async_open_success());
    std::lock_guard cache_lock(cache->_mutex);
    EXPECT_EQ(cache->_cur_cache_size, 2 * block_size);
    for (const auto& block : blocks) {
        EXPECT_EQ(cache->_files.at(hash).at(block->offset()).file_block, block);
    }
}

INSTANTIATE_TEST_SUITE_P(Startup, FileCacheLazyRestoreTest,
                         testing::Values(std::make_tuple(LazyRestoreLookup::GET_OR_SET, false),
                                         std::make_tuple(LazyRestoreLookup::GET_OR_SET, true),
                                         std::make_tuple(LazyRestoreLookup::DOWNLOADED, false),
                                         std::make_tuple(LazyRestoreLookup::DOWNLOADED, true),
                                         std::make_tuple(LazyRestoreLookup::PROBE, false)));

} // namespace

//TODO(zhengyu): check version2 start
//TODO(zhengyu): check version2 version3 mixed start

} // namespace doris::io
