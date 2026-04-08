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

#pragma once

#include <bvar/bvar.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris::io {

class FDCache {
public:
    static FDCache* instance();

    std::shared_ptr<FileReader> get_file_reader(const AccessKeyAndOffset& key);

    void insert_file_reader(const AccessKeyAndOffset& key, std::shared_ptr<FileReader> file_reader);

    void remove_file_reader(const AccessKeyAndOffset& key);

    // use for test
    bool contains_file_reader(const AccessKeyAndOffset& key);
    size_t file_reader_cache_size();

private:
    std::list<std::pair<AccessKeyAndOffset, std::shared_ptr<FileReader>>> _file_reader_list;
    std::unordered_map<AccessKeyAndOffset, decltype(_file_reader_list.begin()), KeyAndOffsetHash>
            _file_name_to_reader;
    mutable std::shared_mutex _mtx;
};

class FSFileCacheStorage : public FileCacheStorage {
public:
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    static constexpr int KEY_PREFIX_LENGTH = 3;
    static constexpr std::string META_DIR_NAME = "meta";

    FSFileCacheStorage() = default;
    ~FSFileCacheStorage() override;
    Status init(BlockFileCache* _mgr) override;
    Status append(const FileCacheKey& key, const Slice& value) override;
    Status finalize(const FileCacheKey& key, const size_t size) override;
    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    Status remove(const FileCacheKey& key) override;
    Status change_key_meta_type(const FileCacheKey& key, const FileCacheType type,
                                const size_t size) override;
    void load_blocks_directly_unlocked(BlockFileCache* _mgr, const FileCacheKey& key,
                                       std::lock_guard<std::mutex>& cache_lock) override;
    Status clear(std::string& msg) override;
    std::string get_local_file(const FileCacheKey& key) override;

    [[nodiscard]] static std::string get_path_in_local_cache_v3(const std::string& dir,
                                                                size_t offset, bool is_tmp = false);

    [[nodiscard]] std::string get_path_in_local_cache_v3(const UInt128Wrapper&) const;

    [[nodiscard]] static std::string get_path_in_local_cache_v2(const std::string& dir,
                                                                size_t offset, FileCacheType type,
                                                                bool is_tmp = false);

    [[nodiscard]] std::string get_path_in_local_cache_v2(const UInt128Wrapper&,
                                                         uint64_t expiration_time) const;

    FileCacheStorageType get_type() override { return DISK; }

    // Get the meta store instance (only available for DISK storage type)
    CacheBlockMetaStore* get_meta_store() { return _meta_store.get(); }

    struct InodeKey {
        dev_t device;
        ino_t inode;
        bool operator==(const InodeKey& other) const {
            return device == other.device && inode == other.inode;
        }
    };
    struct InodeKeyHash {
        size_t operator()(const InodeKey& key) const {
            return std::hash<uint64_t>()((static_cast<uint64_t>(key.device) << 32) ^
                                         static_cast<uint64_t>(key.inode));
        }
    };

#ifdef BE_TEST
    struct InodeEstimationTestHooks {
        std::function<int(const std::string&, struct statvfs*)> statvfs_override;
        std::function<int(const std::string&, struct stat*)> lstat_override;
        std::function<size_t(const FSFileCacheStorage&)> non_cache_override;
        std::function<size_t(const FSFileCacheStorage&)> cache_dir_override;
        std::function<std::filesystem::path(const FSFileCacheStorage&, dev_t)>
                find_mount_root_override;
        std::function<size_t(const FSFileCacheStorage&, const std::filesystem::path&, dev_t,
                             const std::filesystem::path&,
                             std::unordered_set<InodeKey, InodeKeyHash>&)>
                count_inodes_override;
    };
    static void set_inode_estimation_test_hooks(InodeEstimationTestHooks* hooks);
#endif

private:
    void remove_old_version_directories();

    Status collect_directory_entries(const std::filesystem::path& dir_path,
                                     std::vector<std::string>& file_list) const;

    Status upgrade_cache_dir_if_necessary() const;

    Status read_file_cache_version(std::string* buffer) const;

    Status parse_filename_suffix_to_cache_type(const std::shared_ptr<LocalFileSystem>& fs,
                                               const Path& file_path, long expiration_time,
                                               size_t size, size_t* offset, bool* is_tmp,
                                               FileCacheType* cache_type) const;

    Status write_file_cache_version() const;

    [[nodiscard]] std::string get_version_path() const;

    void load_cache_info_into_memory(BlockFileCache* _mgr) const;

    bool handle_already_loaded_block(BlockFileCache* mgr, const UInt128Wrapper& hash, size_t offset,
                                     size_t new_size, int64_t tablet_id,
                                     std::lock_guard<std::mutex>& cache_lock) const;

private:
    // Helper function to count files in cache directory using inode stats
    size_t estimate_file_count_from_inode() const;
    size_t estimate_non_cache_inode_usage() const;
    size_t estimate_cache_directory_inode_usage() const;
    size_t count_inodes_for_path(const std::filesystem::path& path, dev_t target_dev,
                                 const std::filesystem::path& excluded_root,
                                 std::unordered_set<InodeKey, InodeKeyHash>& visited) const;
    std::filesystem::path find_mount_root(dev_t cache_dev) const;
    bool is_cache_prefix_directory(const std::filesystem::directory_entry& entry) const;
    size_t snapshot_metadata_block_count(BlockFileCache* mgr) const;
    std::vector<size_t> snapshot_metadata_for_hash_offsets(BlockFileCache* mgr,
                                                           const UInt128Wrapper& hash) const;
    void start_leak_cleaner(BlockFileCache* mgr);
    void stop_leak_cleaner();
    void leak_cleaner_loop();
    void run_leak_cleanup(BlockFileCache* mgr);
    void cleanup_leaked_files(BlockFileCache* mgr, size_t metadata_block_count);
    void load_cache_info_into_memory_from_fs(BlockFileCache* _mgr) const;
    void load_cache_info_into_memory_from_db(BlockFileCache* _mgr) const;

    Status get_file_cache_infos(std::vector<FileCacheInfo>& infos,
                                std::lock_guard<std::mutex>& cache_lock) const override;

    std::string _cache_base_path;
    BlockFileCache* _mgr {nullptr};
    std::thread _cache_background_load_thread;
    std::thread _cache_leak_cleaner_thread;
    std::atomic<bool> _stop_leak_cleaner {false};
    std::condition_variable _leak_cleaner_cv;
    std::mutex _leak_cleaner_mutex;
    const std::shared_ptr<LocalFileSystem>& fs = global_local_filesystem();
    // TODO(Lchangliang): use a more efficient data structure
    std::mutex _mtx;
    std::unordered_map<FileWriterMapKey, FileWriterPtr, FileWriterMapKeyHash> _key_to_writer;
    std::shared_ptr<bvar::LatencyRecorder> _iterator_dir_retry_cnt;
    std::shared_ptr<bvar::Adder<size_t>> _leak_scan_removed_files;
    std::unique_ptr<CacheBlockMetaStore> _meta_store;
};

} // namespace doris::io
