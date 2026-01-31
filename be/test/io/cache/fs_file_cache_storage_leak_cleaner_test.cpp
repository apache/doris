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
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>

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
              grace(config::file_cache_leak_grace_seconds) {
        config::file_cache_leak_grace_seconds = 0;
    }

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

class ScopedInodeTestHooks {
public:
    ScopedInodeTestHooks() { FSFileCacheStorage::set_inode_estimation_test_hooks(&hooks); }
    ~ScopedInodeTestHooks() { FSFileCacheStorage::set_inode_estimation_test_hooks(nullptr); }

    FSFileCacheStorage::InodeEstimationTestHooks hooks;
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

    void setup_storage(FSFileCacheStorage& storage, BlockFileCache& mgr, const fs::path& dir) {
        storage._cache_base_path = dir.string();
        storage._mgr = &mgr;
        storage._meta_store = std::make_unique<CacheBlockMetaStore>(dir.string() + "/meta", 10000);
        EXPECT_TRUE(storage._meta_store->init().ok());
        EXPECT_TRUE(storage.write_file_cache_version().ok());
    }

    static void add_metadata_entry(BlockFileCache& mgr, FSFileCacheStorage& storage,
                                   const UInt128Wrapper& hash, size_t offset) {
        {
            std::lock_guard<std::mutex> l(mgr._mutex);
            mgr._files[hash].try_emplace(offset);
        }
        if (storage._meta_store) {
            BlockMetaKey mkey(0, hash, offset);
            BlockMeta meta(FileCacheType::NORMAL, 16, 0);
            storage._meta_store->put(mkey, meta);
            // Wait for async write to complete for test stability
            for (int i = 0; i < 100 && storage._meta_store->get_write_queue_size() > 0; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
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
    setup_storage(storage, mgr, dir);

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

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

    add_metadata_entry(mgr, storage, BlockFileCache::hash("thread_guard"), 0);

    storage.start_leak_cleaner(&mgr);
    ASSERT_TRUE(storage._cache_leak_cleaner_thread.joinable());

    storage.stop_leak_cleaner();
    EXPECT_TRUE(storage._stop_leak_cleaner.load(std::memory_order_relaxed));
    EXPECT_FALSE(storage._cache_leak_cleaner_thread.joinable());
}

TEST_F(FSFileCacheLeakCleanerTest, skip_cleanup_when_ratio_below_threshold) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_fs_to_meta_ratio_threshold = 1e12;
    config::file_cache_leak_scan_interval_seconds = 1;

    ScopedInodeTestHooks hooks_guard;
    std::atomic<int> statvfs_calls {0};
    hooks_guard.hooks.statvfs_override = [&statvfs_calls](const std::string&, struct statvfs* vfs) {
        statvfs_calls.fetch_add(1, std::memory_order_relaxed);
        *vfs = {};
        vfs->f_files = 100;
        vfs->f_ffree = 36;
        return 0;
    };
    hooks_guard.hooks.lstat_override = [](const std::string&, struct stat* st) {
        *st = {};
        st->st_dev = 1;
        return 0;
    };
    hooks_guard.hooks.non_cache_override = [](const FSFileCacheStorage&) { return 0; };
    hooks_guard.hooks.cache_dir_override = [](const FSFileCacheStorage&) { return 0; };

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

    const auto metadata_hash = BlockFileCache::hash("metadata_key");
    for (size_t i = 0; i < 64; ++i) {
        add_metadata_entry(mgr, storage, metadata_hash, i);
    }

    const auto orphan_hash = BlockFileCache::hash("ratio_skip_orphan");
    const auto orphan_dir = storage.get_path_in_local_cache_v3(orphan_hash);
    create_regular_file(FSFileCacheStorage::get_path_in_local_cache_v3(orphan_dir, 0, false));

    storage.run_leak_cleanup(&mgr);
    EXPECT_TRUE(std::filesystem::exists(
            FSFileCacheStorage::get_path_in_local_cache_v3(orphan_dir, 0, false)));
    EXPECT_EQ(1, statvfs_calls.load(std::memory_order_relaxed));
}

TEST_F(FSFileCacheLeakCleanerTest, remove_orphan_and_tmp_files) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_batch_files = 1;
    config::file_cache_leak_scan_pause_ms = 0;
    config::file_cache_leak_scan_interval_seconds = 1;

    ScopedInodeTestHooks hooks_guard;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 100;
        vfs->f_ffree = 10; // 90 used
        return 0;
    };
    hooks_guard.hooks.non_cache_override = [](const FSFileCacheStorage&) { return 0; };
    hooks_guard.hooks.cache_dir_override = [](const FSFileCacheStorage&) { return 0; };

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);
    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

    auto kept_hash = BlockFileCache::hash("kept_hash");
    add_metadata_entry(mgr, storage, kept_hash, 0);

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

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

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
    setup_storage(storage, mgr, dir);
    config::file_cache_leak_scan_interval_seconds = 1;

    auto missing_hash = BlockFileCache::hash("missing_hash_case");
    auto offsets = storage.snapshot_metadata_for_hash_offsets(&mgr, missing_hash);
    EXPECT_TRUE(offsets.empty());

    add_metadata_entry(mgr, storage, missing_hash, 7);
    add_metadata_entry(mgr, storage, missing_hash, 3);

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
    setup_storage(storage, mgr, dir);

    std::atomic<int> callback_count {0};
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::initial_delay",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 0; });
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::interval",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 0; });
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

    for (int i = 0; i < 100 && callback_count.load(std::memory_order_relaxed) == 0; ++i) {
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
    setup_storage(storage, mgr, dir);

    struct NonStdException {};

    std::atomic<int> callback_count {0};
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::initial_delay",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 0; });
    sp->set_call_back("FSFileCacheStorage::leak_cleaner_loop::interval",
                      [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 0; });
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

    for (int i = 0; i < 100 && callback_count.load(std::memory_order_relaxed) == 0; ++i) {
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
    config::file_cache_leak_scan_interval_seconds = 1;

    ScopedInodeTestHooks hooks_guard;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 1000;
        vfs->f_ffree = 900; // 100 used
        return 0;
    };
    hooks_guard.hooks.non_cache_override = [](const FSFileCacheStorage&) { return 0; };
    hooks_guard.hooks.cache_dir_override = [](const FSFileCacheStorage&) { return 0; };

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

    auto hash = BlockFileCache::hash("zero_meta_orphan");
    auto key_dir = storage.get_path_in_local_cache_v3(hash);
    fs::create_directories(key_dir);
    auto orphan_path = FSFileCacheStorage::get_path_in_local_cache_v3(key_dir, 0, false);
    create_regular_file(orphan_path, 'z');

    auto dummy_hash = BlockFileCache::hash("dummy");
    add_metadata_entry(mgr, storage, dummy_hash, 0);

    storage.run_leak_cleanup(&mgr);

    auto prefix_dir = fs::path(key_dir).parent_path();
    EXPECT_FALSE(fs::exists(orphan_path));
    EXPECT_FALSE(fs::exists(key_dir));
    EXPECT_FALSE(fs::exists(prefix_dir));
}

TEST_F(FSFileCacheLeakCleanerTest, cleanup_handles_missing_base_directory) {
    ScopedLeakCleanerConfig guard;
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir / "missing_root");
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
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

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
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

    auto hash = BlockFileCache::hash("metadata_kept_hash");
    add_metadata_entry(mgr, storage, hash, 0);

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
    config::file_cache_leak_scan_interval_seconds = 1;

    auto dir = prepare_test_dir();
    FileCacheSettings settings = default_settings();
    BlockFileCache mgr(dir.string(), settings);

    FSFileCacheStorage storage;
    setup_storage(storage, mgr, dir);

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

TEST_F(FSFileCacheLeakCleanerTest, estimate_file_count_handles_statvfs_failure) {
    ScopedInodeTestHooks hooks_guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        errno = EIO;
        return -1;
    };

    FSFileCacheStorage storage;
    storage._cache_base_path = "/tmp/nonexistent_statvfs";
    EXPECT_EQ(0u, storage.estimate_file_count_from_inode());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_file_count_handles_zero_total_inodes) {
    ScopedInodeTestHooks hooks_guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 0;
        return 0;
    };
    hooks_guard.hooks.lstat_override = [](const std::string&, struct stat* st) {
        *st = {};
        st->st_dev = 1;
        return 0;
    };

    FSFileCacheStorage storage;
    storage._cache_base_path = "/tmp/cache_zero_inodes";
    EXPECT_EQ(0u, storage.estimate_file_count_from_inode());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_file_count_handles_lstat_failure) {
    ScopedInodeTestHooks hooks_guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 100;
        vfs->f_ffree = 10;
        return 0;
    };
    hooks_guard.hooks.lstat_override = [](const std::string&, struct stat*) {
        errno = ENOENT;
        return -1;
    };

    FSFileCacheStorage storage;
    storage._cache_base_path = "/tmp/cache_lstat_failure";
    EXPECT_EQ(0u, storage.estimate_file_count_from_inode());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_file_count_handles_underflow) {
    ScopedInodeTestHooks hooks_guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 200;
        vfs->f_ffree = 150;
        return 0;
    };
    hooks_guard.hooks.lstat_override = [](const std::string&, struct stat* st) {
        *st = {};
        st->st_dev = 9;
        return 0;
    };
    hooks_guard.hooks.non_cache_override = [](const FSFileCacheStorage&) { return 80; };
    hooks_guard.hooks.cache_dir_override = [](const FSFileCacheStorage&) { return 90; };

    FSFileCacheStorage storage;
    storage._cache_base_path = "/tmp/cache_underflow";
    EXPECT_EQ(0u, storage.estimate_file_count_from_inode());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_file_count_combines_counts) {
    ScopedInodeTestHooks hooks_guard;
    config::file_cache_leak_scan_interval_seconds = 1;
    hooks_guard.hooks.statvfs_override = [](const std::string&, struct statvfs* vfs) {
        *vfs = {};
        vfs->f_files = 500;
        vfs->f_ffree = 200;
        return 0;
    };
    hooks_guard.hooks.lstat_override = [](const std::string&, struct stat* st) {
        *st = {};
        st->st_dev = 7;
        return 0;
    };
    hooks_guard.hooks.non_cache_override = [](const FSFileCacheStorage&) { return 50; };
    hooks_guard.hooks.cache_dir_override = [](const FSFileCacheStorage&) { return 30; };

    FSFileCacheStorage storage;
    storage._cache_base_path = "/tmp/cache_estimation";
    EXPECT_EQ(220u, storage.estimate_file_count_from_inode());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_non_cache_inode_usage_counts_other_paths) {
    auto root = prepare_test_dir("inode_non_cache_root");
    auto cache_dir = root / "cache";
    auto other_dir = root / "others";
    auto nested_dir = other_dir / "nested";
    fs::create_directories(cache_dir);
    fs::create_directories(nested_dir);
    create_regular_file((root / "root_file.bin").string());
    create_regular_file((other_dir / "leaf.txt").string());
    create_regular_file((nested_dir / "inner.bin").string());

    FSFileCacheStorage storage;
    storage._cache_base_path = cache_dir.string();

    ScopedInodeTestHooks hooks_guard;
    hooks_guard.hooks.find_mount_root_override = [root](const FSFileCacheStorage&, dev_t) {
        return root;
    };

    EXPECT_EQ(6u, storage.estimate_non_cache_inode_usage());
}

TEST_F(FSFileCacheLeakCleanerTest, estimate_cache_directory_inode_usage_samples_prefixes) {
    auto base = prepare_test_dir("inode_cache_directory");
    auto prefix_a = base / "abc";
    auto prefix_b = base / "def";
    fs::create_directories(prefix_a);
    fs::create_directories(prefix_b);
    fs::create_directories(base / FSFileCacheStorage::META_DIR_NAME);
    fs::create_directories(base / "abcd");
    fs::create_directories(prefix_a / "deadbeef_0");
    fs::create_directories(prefix_b / "feed000_0");
    fs::create_directories(prefix_b / "feed000_1");
    fs::create_directories(prefix_b / "feed000_2");

    FSFileCacheStorage storage;
    storage._cache_base_path = base.string();

    EXPECT_EQ(6u, storage.estimate_cache_directory_inode_usage());
}

TEST_F(FSFileCacheLeakCleanerTest, count_inodes_for_path_respects_exclusions) {
    auto base = prepare_test_dir("inode_counting");
    auto include_dir = base / "include";
    auto exclude_dir = base / "exclude";
    fs::create_directories(include_dir);
    fs::create_directories(exclude_dir);
    create_regular_file((base / "root.bin").string());
    create_regular_file((include_dir / "child.bin").string());
    create_regular_file((exclude_dir / "skip.bin").string());

    FSFileCacheStorage storage;
    struct stat st {};
    ASSERT_EQ(0, lstat(base.c_str(), &st));
    std::unordered_set<FSFileCacheStorage::InodeKey, FSFileCacheStorage::InodeKeyHash> visited;
    size_t count = storage.count_inodes_for_path(base, st.st_dev, exclude_dir, visited);
    EXPECT_EQ(4u, count);
}

TEST_F(FSFileCacheLeakCleanerTest, is_cache_prefix_directory_filters_entries) {
    auto base = prepare_test_dir("inode_prefix_filter");
    auto valid = base / "abc";
    auto invalid = base / "abcd";
    auto meta_dir = base / FSFileCacheStorage::META_DIR_NAME;
    fs::create_directories(valid);
    fs::create_directories(invalid);
    fs::create_directories(meta_dir);
    create_regular_file((base / "plain_file").string());

    FSFileCacheStorage storage;
    storage._cache_base_path = base.string();

    EXPECT_TRUE(storage.is_cache_prefix_directory(fs::directory_entry(valid)));
    EXPECT_FALSE(storage.is_cache_prefix_directory(fs::directory_entry(invalid)));
    EXPECT_FALSE(storage.is_cache_prefix_directory(fs::directory_entry(meta_dir)));
    EXPECT_FALSE(storage.is_cache_prefix_directory(fs::directory_entry(base / "plain_file")));
}

} // namespace doris::io
