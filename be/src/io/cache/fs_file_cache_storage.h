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

#include <memory>
#include <shared_mutex>
#include <thread>

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

    FSFileCacheStorage() = default;
    ~FSFileCacheStorage() override;
    Status init(BlockFileCache* _mgr) override;
    Status append(const FileCacheKey& key, const Slice& value) override;
    Status finalize(const FileCacheKey& key, const size_t size) override;
    Status read(const FileCacheKey& key, size_t value_offset, Slice buffer) override;
    Status remove(const FileCacheKey& key) override;
    Status change_key_meta_type(const FileCacheKey& key, const FileCacheType type,
                                const size_t size) override;
    Status change_key_meta_expiration(const FileCacheKey& key, const uint64_t expiration,
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

private:
    // Helper function to count files in cache directory using statfs
    size_t estimate_file_count_from_statfs() const;
    void load_cache_info_into_memory_from_fs(BlockFileCache* _mgr) const;
    void load_cache_info_into_memory_from_db(BlockFileCache* _mgr) const;

    Status get_file_cache_infos(std::vector<FileCacheInfo>& infos,
                                std::lock_guard<std::mutex>& cache_lock) const override;

    std::string _cache_base_path;
    std::thread _cache_background_load_thread;
    const std::shared_ptr<LocalFileSystem>& fs = global_local_filesystem();
    // TODO(Lchangliang): use a more efficient data structure
    std::mutex _mtx;
    std::unordered_map<FileWriterMapKey, FileWriterPtr, FileWriterMapKeyHash> _key_to_writer;
    std::shared_ptr<bvar::LatencyRecorder> _iterator_dir_retry_cnt;
    std::unique_ptr<CacheBlockMetaStore> _meta_store;
};

} // namespace doris::io
