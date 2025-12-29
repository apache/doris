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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#include "io/cache/block_file_cache.h"
#include "io/cache/fs_file_cache_storage.h"
#undef private
#undef protected

#include "block_file_cache_test_common.h"

namespace doris::io {

namespace fs = std::filesystem;

class ScopedLeakCleanerConfig {
public:
    ScopedLeakCleanerConfig()
            : ratio(config::file_cache_leak_fs_to_meta_ratio_threshold),
              interval(config::file_cache_leak_scan_interval_seconds),
              batch(config::file_cache_leak_scan_batch_files),
              pause(config::file_cache_leak_scan_pause_ms),
              grace(config::file_cache_leak_grace_seconds) {}

    ~ScopedLeakCleanerConfig() {
        config::file_cache_leak_fs_to_meta_ratio_threshold = ratio;
        config::file_cache_leak_scan_interval_seconds = interval;
        config::file_cache_leak_scan_batch_files = batch;
        config::file_cache_leak_scan_pause_ms = pause;
        config::file_cache_leak_grace_seconds = grace;
    }

private:
    double ratio;
    int64_t interval;
    int32_t batch;
    int32_t pause;
    int64_t grace;
};

class FSFileCacheLeakCleanerTest : public BlockFileCacheTest {
protected:
    static FileCacheSettings default_settings() {
        FileCacheSettings settings;
        settings.capacity = 10 * 1024 * 1024;
        settings.max_file_block_size = 1 * 1024 * 1024;
        settings.max_query_cache_size = settings.capacity;
        settings.disposable_queue_size = settings.capacity;
        settings.disposable_queue_elements = 8;
        settings.index_queue_size = settings.capacity;
        settings.index_queue_elements = 8;
        settings.query_queue_size = settings.capacity;
        settings.query_queue_elements = 8;
        settings.ttl_queue_size = settings.capacity;
        settings.ttl_queue_elements = 8;
        settings.storage = "disk";
        return settings;
    }

    fs::path prepare_test_dir(const std::string& name) const {
        fs::path dir = caches_dir / "leak_cleaner" / name;
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        return dir;
    }

    static std::string current_test_name() {
        if (auto* info = ::testing::UnitTest::GetInstance()->current_test_info()) {
            return std::string(info->name());
        }
        return "unknown";
    }

    fs::path prepare_test_dir() const { return prepare_test_dir(current_test_name()); }

    static void add_metadata_entry(BlockFileCache& mgr, const UInt128Wrapper& hash, size_t offset) {
        std::lock_guard<std::mutex> l(mgr._mutex);
        mgr._files[hash].try_emplace(offset);
    }

    static void create_regular_file(const std::string& path, char fill = 'x') {
        fs::create_directories(fs::path(path).parent_path());
        std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(ofs.good());
        std::string payload(16, fill);
        ofs.write(payload.data(), payload.size());
        ofs.close();
        ASSERT_TRUE(std::filesystem::exists(path));
    }
};

TEST_F(FSFileCacheLeakCleanerTest, disable_when_interval_non_positive) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_interval_seconds = 0;
    auto dir = prepare_test_dir();

    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    storage.start_leak_cleaner(&mgr);
    EXPECT_FALSE(storage._cache_leak_cleaner_thread.joinable());
    EXPECT_FALSE(storage._stop_leak_cleaner.load(std::memory_order_relaxed));

    storage.stop_leak_cleaner();
    EXPECT_TRUE(storage._stop_leak_cleaner.load(std::memory_order_relaxed));
}

TEST_F(FSFileCacheLeakCleanerTest, start_and_stop_thread) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    config::file_cache_leak_fs_to_meta_ratio_threshold = 1e12;
    config::file_cache_leak_scan_batch_files = 4;
    config::file_cache_leak_scan_pause_ms = 0;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    add_metadata_entry(mgr, BlockFileCache::hash("thread_guard"), 0);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    storage.start_leak_cleaner(&mgr);
    ASSERT_TRUE(storage._cache_leak_cleaner_thread.joinable());

    storage.stop_leak_cleaner();
    EXPECT_TRUE(storage._stop_leak_cleaner.load(std::memory_order_relaxed));
    EXPECT_FALSE(storage._cache_leak_cleaner_thread.joinable());
}

TEST_F(FSFileCacheLeakCleanerTest, skip_cleanup_when_ratio_below_threshold) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_fs_to_meta_ratio_threshold = 1e12;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    const auto metadata_hash = BlockFileCache::hash("metadata_key");
    for (size_t i = 0; i < 64; ++i) {
        add_metadata_entry(mgr, metadata_hash, i);
    }

    const auto orphan_hash = BlockFileCache::hash("ratio_skip_orphan");
    const auto orphan_dir = storage.get_path_in_local_cache_v3(orphan_hash);
    create_regular_file(FSFileCacheStorage::get_path_in_local_cache_v3(orphan_dir, 0, false));

    storage.run_leak_cleanup(&mgr);
    EXPECT_TRUE(std::filesystem::exists(
            FSFileCacheStorage::get_path_in_local_cache_v3(orphan_dir, 0, false)));
}

TEST_F(FSFileCacheLeakCleanerTest, remove_orphan_and_tmp_files) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_fs_to_meta_ratio_threshold = 0.5;
    config::file_cache_leak_scan_batch_files = 1;
    config::file_cache_leak_scan_pause_ms = 0;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    auto kept_hash = BlockFileCache::hash("kept_hash");
    add_metadata_entry(mgr, kept_hash, 0);

    auto kept_dir = storage.get_path_in_local_cache_v3(kept_hash);
    auto kept_file = FSFileCacheStorage::get_path_in_local_cache_v3(kept_dir, 0, false);
    auto tmp_file = FSFileCacheStorage::get_path_in_local_cache_v3(kept_dir, 8, true);
    create_regular_file(kept_file, 'k');
    create_regular_file(tmp_file, 't');

    auto orphan_hash = BlockFileCache::hash("orphan_hash");
    auto orphan_dir = storage.get_path_in_local_cache_v3(orphan_hash);
    auto orphan_file = FSFileCacheStorage::get_path_in_local_cache_v3(orphan_dir, 4, false);
    create_regular_file(orphan_file, 'o');

    storage.run_leak_cleanup(&mgr);

    EXPECT_TRUE(std::filesystem::exists(kept_file));
    EXPECT_FALSE(std::filesystem::exists(tmp_file));
    EXPECT_FALSE(std::filesystem::exists(orphan_file));
    EXPECT_FALSE(std::filesystem::exists(orphan_dir));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_F(FSFileCacheLeakCleanerTest, snapshot_metadata_for_hash_offsets_handles_missing_hash) {
    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;

    auto missing_hash = BlockFileCache::hash("missing_hash_case");
    auto offsets = storage.snapshot_metadata_for_hash_offsets(&mgr, missing_hash);
    EXPECT_TRUE(offsets.empty());

    add_metadata_entry(mgr, missing_hash, 7);
    add_metadata_entry(mgr, missing_hash, 3);

    offsets = storage.snapshot_metadata_for_hash_offsets(&mgr, missing_hash);
    std::sort(offsets.begin(), offsets.end());
    ASSERT_EQ(2, offsets.size());
    EXPECT_EQ(3u, offsets[0]);
    EXPECT_EQ(7u, offsets[1]);
}

TEST_F(FSFileCacheLeakCleanerTest, leak_cleaner_loop_catches_std_exception) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    std::atomic<int> callback_count {0};
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::before_run",
                      [&storage, &callback_count](auto&&) {
                          callback_count.fetch_add(1, std::memory_order_relaxed);
                          storage._stop_leak_cleaner.store(true, std::memory_order_relaxed);
                          storage._leak_cleaner_cv.notify_all();
                          throw std::runtime_error("injected std exception");
                      });
    sp->enable_processing();

    storage._stop_leak_cleaner.store(false, std::memory_order_relaxed);
    std::thread worker([&]() { storage.leak_cleaner_loop(); });

    for (int i = 0; i < 10 && callback_count.load(std::memory_order_relaxed) == 0; ++i) {
        storage._leak_cleaner_cv.notify_all();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    storage._stop_leak_cleaner.store(true, std::memory_order_relaxed);
    storage._leak_cleaner_cv.notify_all();
    worker.join();

    sp->disable_processing();
    sp->clear_all_call_backs();

    ASSERT_GE(callback_count.load(std::memory_order_relaxed), 1);
}

TEST_F(FSFileCacheLeakCleanerTest, leak_cleaner_loop_catches_unknown_exception) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    struct NonStdException {};

    std::atomic<int> callback_count {0};
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::before_run",
                      [&storage, &callback_count](auto&&) {
                          callback_count.fetch_add(1, std::memory_order_relaxed);
                          storage._stop_leak_cleaner.store(true, std::memory_order_relaxed);
                          storage._leak_cleaner_cv.notify_all();
                          throw NonStdException {};
                      });
    sp->enable_processing();

    storage._stop_leak_cleaner.store(false, std::memory_order_relaxed);
    std::thread worker([&]() { storage.leak_cleaner_loop(); });

    for (int i = 0; i < 10 && callback_count.load(std::memory_order_relaxed) == 0; ++i) {
        storage._leak_cleaner_cv.notify_all();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    storage._stop_leak_cleaner.store(true, std::memory_order_relaxed);
    storage._leak_cleaner_cv.notify_all();
    worker.join();

    sp->disable_processing();
    sp->clear_all_call_backs();

    ASSERT_GE(callback_count.load(std::memory_order_relaxed), 1);
}

TEST_F(FSFileCacheLeakCleanerTest, run_leak_cleanup_removes_orphan_when_metadata_missing) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_fs_to_meta_ratio_threshold = 0.5;
    config::file_cache_leak_grace_seconds = 0;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    auto hash = BlockFileCache::hash("zero_meta_orphan");
    auto key_dir = storage.get_path_in_local_cache_v3(hash);
    fs::create_directories(key_dir);
    auto orphan_path = FSFileCacheStorage::get_path_in_local_cache_v3(key_dir, 0, false);
    create_regular_file(orphan_path, 'z');

    storage.run_leak_cleanup(&mgr);

    auto prefix_dir = fs::path(key_dir).parent_path();
    EXPECT_FALSE(fs::exists(orphan_path));
    EXPECT_FALSE(fs::exists(key_dir));
    EXPECT_FALSE(fs::exists(prefix_dir));
}

TEST_F(FSFileCacheLeakCleanerTest, cleanup_handles_missing_base_directory) {
    ScopedLeakCleanerConfig guard;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = (dir / "missing_root").string();
    storage._mgr = &mgr;
    fs::path missing_path(storage._cache_base_path);
    if (fs::exists(missing_path)) {
        fs::remove_all(missing_path);
    }

    storage.cleanup_leaked_files(&mgr, 0);
    EXPECT_FALSE(fs::exists(missing_path));
}

TEST_F(FSFileCacheLeakCleanerTest, cleanup_skips_invalid_prefixes_and_keys) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_grace_seconds = 0;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    fs::create_directories(dir);
    create_regular_file((dir / "root_file").string());               // non-directory prefix entry
    fs::create_directories(dir / FSFileCacheStorage::META_DIR_NAME); // meta dir skip
    fs::create_directories(dir / "abcd");                            // invalid prefix length

    auto prefix_dir = dir / "abc";
    fs::create_directories(prefix_dir);
    create_regular_file((prefix_dir / "plain_file").string()); // !key_it->is_directory branch
    fs::create_directories(prefix_dir / "deadbeef" /* missing '_' */);
    fs::create_directories(prefix_dir / "zzzg000_0" /* invalid hex */);
    fs::create_directories(prefix_dir / "123abc_bad" /* invalid expiration */);

    storage.cleanup_leaked_files(&mgr, 0);

    EXPECT_TRUE(fs::exists(prefix_dir));
    EXPECT_TRUE(fs::exists(dir / "abcd"));
}

TEST_F(FSFileCacheLeakCleanerTest, cleanup_flush_candidates_when_empty) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_grace_seconds = 0;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    auto hash = BlockFileCache::hash("metadata_kept_hash");
    add_metadata_entry(mgr, hash, 0);

    auto key_dir = storage.get_path_in_local_cache_v3(hash);
    fs::create_directories(key_dir);
    auto file_path = FSFileCacheStorage::get_path_in_local_cache_v3(key_dir, 0, false);
    create_regular_file(file_path, 'm');

    storage.cleanup_leaked_files(&mgr, 1);

    EXPECT_TRUE(fs::exists(file_path));
}

TEST_F(FSFileCacheLeakCleanerTest, cleanup_flush_candidates_remove_directories) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_grace_seconds = 0;
    config::file_cache_leak_scan_batch_files = 2;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    storage._cache_base_path = dir.string();
    storage._mgr = &mgr;

    auto hash = BlockFileCache::hash("cleanup_orphan_batch");
    auto key_dir = storage.get_path_in_local_cache_v3(hash);
    fs::create_directories(key_dir);
    auto orphan_path = FSFileCacheStorage::get_path_in_local_cache_v3(key_dir, 4, false);
    create_regular_file(orphan_path, 'c');

    storage.cleanup_leaked_files(&mgr, 0);

    auto prefix_dir = fs::path(key_dir).parent_path();
    EXPECT_FALSE(fs::exists(orphan_path));
    EXPECT_FALSE(fs::exists(key_dir));
    EXPECT_FALSE(fs::exists(prefix_dir));
}

} // namespace doris::io
