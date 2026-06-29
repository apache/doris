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

#include "io/cache/cache_lru_dumper.h"

#include <filesystem>

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "util/defer_op.h"

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

namespace doris::io {
std::mutex _mutex;

static const std::string test_dir = "./cache_lru_dumper_test_dir/";

class MockBlockFileCache : public BlockFileCache {
public:
    LRUQueue* dst_queue; // Pointer to the destination queue

    MockBlockFileCache(LRUQueue* queue) : BlockFileCache("", {}), dst_queue(queue) {
        _cache_base_path = test_dir;
    }

    FileBlockCell* add_cell(const UInt128Wrapper& hash, const CacheContext& ctx, size_t offset,
                            size_t size, FileBlock::State state,
                            std::lock_guard<std::mutex>& lock) {
        static std::unordered_set<std::string> added_entries;
        std::string key = hash.to_string() + ":" + std::to_string(offset);

        if (added_entries.find(key) != added_entries.end()) {
            std::cerr << "Error: Duplicate entry detected for hash: " << key << std::endl;
            EXPECT_TRUE(false);
            return nullptr;
        }

        added_entries.insert(key);
        dst_queue->add(hash, offset, size, lock);
        return nullptr;
    }

    std::mutex& mutex() { return _mutex; }

private:
    std::mutex _mutex;
    struct {
        std::string _cache_base_path;
    } _mgr;
};

class CacheLRUDumperTest : public ::testing::Test {
protected:
    LRUQueue dst_queue; // Member variable for destination queue

    void SetUp() override {
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directory(test_dir);

        mock_cache = std::make_unique<NiceMock<MockBlockFileCache>>(&dst_queue);
        recorder = std::make_unique<LRUQueueRecorder>(mock_cache.get());

        dumper = std::make_unique<CacheLRUDumper>(mock_cache.get(), recorder.get());
    }

    void TearDown() override {
        dumper.reset();
        mock_cache.reset();
        std::filesystem::remove_all(test_dir);
    }

    std::unique_ptr<NiceMock<MockBlockFileCache>> mock_cache;
    std::unique_ptr<CacheLRUDumper> dumper;
    std::unique_ptr<LRUQueueRecorder> recorder;
};

TEST_F(CacheLRUDumperTest, test_finalize_dump_and_parse_dump_footer) {
    std::string tmp_filename = test_dir + "test_finalize.bin.tmp";
    std::string final_filename = test_dir + "test_finalize.bin";
    std::ofstream out(tmp_filename, std::ios::binary);
    size_t file_size = 0;
    size_t entry_num = 10;

    // Test finalize dump
    EXPECT_TRUE(
            dumper->finalize_dump(out, entry_num, tmp_filename, final_filename, file_size).ok());

    // Test parse footer
    std::ifstream in(final_filename, std::ios::binary);
    size_t parsed_entry_num = 0;
    EXPECT_TRUE(dumper->parse_dump_footer(in, final_filename, parsed_entry_num).ok());
    EXPECT_EQ(entry_num, parsed_entry_num);
    in.close();
}

TEST_F(CacheLRUDumperTest, test_remove_lru_dump_files) {
    // Create test files
    std::vector<std::string> queue_names = {"disposable", "index", "normal", "ttl"};
    for (const auto& name : queue_names) {
        std::ofstream(fmt::format("{}lru_dump_{}.tail", test_dir, name));
    }

    // Test remove
    dumper->remove_lru_dump_files();

    // Verify files are removed
    for (const auto& name : queue_names) {
        EXPECT_FALSE(std::filesystem::exists(fmt::format("{}lru_dump_{}.tail", test_dir, name)));
    }
}

TEST_F(CacheLRUDumperTest, test_dump_and_restore_queue) {
    LRUQueue src_queue;
    std::string queue_name = "normal";

    // Add test data
    UInt128Wrapper hash(123456789ULL);
    size_t offset = 1024;
    size_t size = 4096;
    std::lock_guard<std::mutex> lock(_mutex);
    src_queue.add(hash, offset, size, lock);

    // Test dump
    dumper->do_dump_queue(src_queue, queue_name);

    // Test restore
    std::lock_guard<std::mutex> cache_lock(mock_cache->mutex());
    dumper->restore_queue(dst_queue, queue_name, cache_lock);

    // Verify queue content and order
    auto src_it = src_queue.begin();
    auto dst_it = dst_queue.begin();
    while (src_it != src_queue.end() && dst_it != dst_queue.end()) {
        EXPECT_EQ(src_it->hash, dst_it->hash);
        EXPECT_EQ(src_it->offset, dst_it->offset);
        EXPECT_EQ(src_it->size, dst_it->size);
        ++src_it;
        ++dst_it;
    }
}

TEST_F(CacheLRUDumperTest, test_lru_log_record_disabled_keeps_existing_backlog) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_queue_limit = config::file_cache_background_lru_log_queue_max_size;
    Defer defer {[old_tail_record_num, old_queue_limit] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
        config::file_cache_background_lru_log_queue_max_size = old_queue_limit;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 2;
    config::file_cache_background_lru_log_queue_max_size = 10;

    UInt128Wrapper hash(123456789ULL);
    recorder->record_queue_event(FileCacheType::NORMAL, CacheLRULogType::ADD, hash, 0, 4096);
    ASSERT_EQ(recorder->lru_log_queue_size(FileCacheType::NORMAL), 1);

    config::file_cache_background_lru_dump_tail_record_num = 0;
    recorder->record_queue_event(FileCacheType::NORMAL, CacheLRULogType::ADD, hash, 4096, 4096);

    EXPECT_EQ(recorder->lru_log_queue_size(FileCacheType::NORMAL), 1);
    EXPECT_EQ(recorder->get_lru_log_queue(FileCacheType::NORMAL).size_approx(), 1);
    EXPECT_EQ(recorder->replay_queue_event(FileCacheType::NORMAL), 1);
    EXPECT_EQ(recorder->get_shadow_queue(FileCacheType::NORMAL).get_elements_num_unsafe(), 1);
}

TEST_F(CacheLRUDumperTest, test_lru_log_record_queue_hard_cap) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_queue_limit = config::file_cache_background_lru_log_queue_max_size;
    Defer defer {[old_tail_record_num, old_queue_limit] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
        config::file_cache_background_lru_log_queue_max_size = old_queue_limit;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 100;
    config::file_cache_background_lru_log_queue_max_size = 2;

    UInt128Wrapper hash(987654321ULL);
    recorder->record_queue_event(FileCacheType::INDEX, CacheLRULogType::ADD, hash, 0, 4096);
    recorder->record_queue_event(FileCacheType::INDEX, CacheLRULogType::ADD, hash, 4096, 4096);
    recorder->record_queue_event(FileCacheType::INDEX, CacheLRULogType::ADD, hash, 8192, 4096);

    EXPECT_EQ(recorder->lru_log_queue_size(FileCacheType::INDEX), 2);
    EXPECT_EQ(recorder->get_lru_log_queue(FileCacheType::INDEX).size_approx(), 2);
    EXPECT_EQ(recorder->replay_queue_event(FileCacheType::INDEX), 2);
    EXPECT_EQ(recorder->lru_log_queue_size(FileCacheType::INDEX), 0);
    EXPECT_EQ(recorder->get_shadow_queue(FileCacheType::INDEX).get_elements_num_unsafe(), 2);
}

TEST_F(CacheLRUDumperTest, test_shadow_queue_keeps_tail_when_replay_exceeds_limit) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_queue_limit = config::file_cache_background_lru_log_queue_max_size;
    Defer defer {[old_tail_record_num, old_queue_limit] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
        config::file_cache_background_lru_log_queue_max_size = old_queue_limit;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 3;
    config::file_cache_background_lru_log_queue_max_size = 10;

    UInt128Wrapper hash(112233ULL);
    for (size_t offset = 0; offset < 5; ++offset) {
        recorder->record_queue_event(FileCacheType::NORMAL, CacheLRULogType::ADD, hash, offset,
                                     4096);
    }

    EXPECT_EQ(recorder->replay_queue_event(FileCacheType::NORMAL), 5);
    auto& shadow_queue = recorder->get_shadow_queue(FileCacheType::NORMAL);
    ASSERT_EQ(shadow_queue.get_elements_num_unsafe(), 3);
    auto stats = mock_cache->get_stats_unsafe();
    EXPECT_EQ(stats["lru_recorder_normal_shadow_queue_curr_elements"], 3);

    std::vector<size_t> offsets;
    for (auto it = shadow_queue.begin(); it != shadow_queue.end(); ++it) {
        offsets.push_back(it->offset);
    }
    EXPECT_EQ(offsets, std::vector<size_t>({2, 3, 4}));
}

TEST_F(CacheLRUDumperTest, test_remove_event_trims_existing_oversized_shadow_queue) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_queue_limit = config::file_cache_background_lru_log_queue_max_size;
    Defer defer {[old_tail_record_num, old_queue_limit] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
        config::file_cache_background_lru_log_queue_max_size = old_queue_limit;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 2;
    config::file_cache_background_lru_log_queue_max_size = 10;

    UInt128Wrapper hash(556677ULL);
    {
        std::lock_guard lru_log_lock(recorder->_mutex_lru_log);
        auto& shadow_queue = recorder->get_shadow_queue(FileCacheType::NORMAL);
        for (size_t offset = 0; offset < 4; ++offset) {
            shadow_queue.add(hash, offset, 4096, lru_log_lock);
        }
    }
    recorder->record_queue_event(FileCacheType::NORMAL, CacheLRULogType::REMOVE, hash, 3, 4096);

    EXPECT_EQ(recorder->replay_queue_event(FileCacheType::NORMAL), 1);
    auto& shadow_queue = recorder->get_shadow_queue(FileCacheType::NORMAL);
    ASSERT_EQ(shadow_queue.get_elements_num_unsafe(), 2);

    std::vector<size_t> offsets;
    for (auto it = shadow_queue.begin(); it != shadow_queue.end(); ++it) {
        offsets.push_back(it->offset);
    }
    EXPECT_EQ(offsets, std::vector<size_t>({1, 2}));
}

TEST_F(CacheLRUDumperTest, test_update_shadow_queue_metric_does_not_trim_queue) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    Defer defer {[old_tail_record_num] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 1;

    UInt128Wrapper hash(778899ULL);
    {
        std::lock_guard lru_log_lock(recorder->_mutex_lru_log);
        auto& shadow_queue = recorder->get_shadow_queue(FileCacheType::INDEX);
        for (size_t offset = 0; offset < 3; ++offset) {
            shadow_queue.add(hash, offset, 4096, lru_log_lock);
        }
    }

    recorder->update_shadow_queue_element_count_metrics();

    EXPECT_EQ(recorder->get_shadow_queue(FileCacheType::INDEX).get_elements_num_unsafe(), 3);
    auto stats = mock_cache->get_stats_unsafe();
    EXPECT_EQ(stats["lru_recorder_index_shadow_queue_curr_elements"], 3);
}

TEST_F(CacheLRUDumperTest, test_remove_event_still_obeys_replay_queue_cap) {
    const auto old_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    const auto old_queue_limit = config::file_cache_background_lru_log_queue_max_size;
    Defer defer {[old_tail_record_num, old_queue_limit] {
        config::file_cache_background_lru_dump_tail_record_num = old_tail_record_num;
        config::file_cache_background_lru_log_queue_max_size = old_queue_limit;
    }};

    config::file_cache_background_lru_dump_tail_record_num = 100;
    config::file_cache_background_lru_log_queue_max_size = 1;

    UInt128Wrapper hash(445566ULL);
    recorder->record_queue_event(FileCacheType::INDEX, CacheLRULogType::ADD, hash, 0, 4096);
    recorder->record_queue_event(FileCacheType::INDEX, CacheLRULogType::REMOVE, hash, 0, 4096);

    EXPECT_EQ(recorder->lru_log_queue_size(FileCacheType::INDEX), 1);
    EXPECT_EQ(recorder->replay_queue_event(FileCacheType::INDEX), 1);
    EXPECT_EQ(recorder->get_shadow_queue(FileCacheType::INDEX).get_elements_num_unsafe(), 1);
}

} // namespace doris::io
