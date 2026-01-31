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

#include "io/cache/fs_file_cache_storage.h"

#include <fmt/core.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <ctime>
#include <filesystem>
#include <limits>
#include <mutex>
#include <random>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/file_cache_storage.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "vec/common/hex.h"

namespace doris::io {

#ifdef BE_TEST
namespace {
FSFileCacheStorage::InodeEstimationTestHooks* g_inode_estimation_hooks = nullptr;

FSFileCacheStorage::InodeEstimationTestHooks* inode_test_hooks() {
    return g_inode_estimation_hooks;
}
} // namespace

void FSFileCacheStorage::set_inode_estimation_test_hooks(
        FSFileCacheStorage::InodeEstimationTestHooks* hooks) {
    g_inode_estimation_hooks = hooks;
}
#endif

struct BatchLoadArgs {
    UInt128Wrapper hash;
    CacheContext ctx;
    uint64_t offset;
    size_t size;
    std::string key_path;
    std::string offset_path;
    bool is_tmp;
};

FDCache* FDCache::instance() {
    return ExecEnv::GetInstance()->file_cache_open_fd_cache();
}

std::shared_ptr<FileReader> FDCache::get_file_reader(const AccessKeyAndOffset& key) {
    if (config::file_cache_max_file_reader_cache_size == 0) [[unlikely]] {
        return nullptr;
    }
    DCHECK(ExecEnv::GetInstance());
    std::shared_lock rlock(_mtx);
    if (auto iter = _file_name_to_reader.find(key); iter != _file_name_to_reader.end()) {
        return iter->second->second;
    }
    return nullptr;
}

void FDCache::insert_file_reader(const AccessKeyAndOffset& key,
                                 std::shared_ptr<FileReader> file_reader) {
    if (config::file_cache_max_file_reader_cache_size == 0) [[unlikely]] {
        return;
    }
    std::lock_guard wlock(_mtx);

    if (auto iter = _file_name_to_reader.find(key); iter == _file_name_to_reader.end()) {
        if (config::file_cache_max_file_reader_cache_size == _file_reader_list.size()) {
            _file_name_to_reader.erase(_file_reader_list.back().first);
            _file_reader_list.pop_back();
        }
        _file_reader_list.emplace_front(key, std::move(file_reader));
        _file_name_to_reader.insert(std::make_pair(key, _file_reader_list.begin()));
    }
}

void FDCache::remove_file_reader(const AccessKeyAndOffset& key) {
    if (config::file_cache_max_file_reader_cache_size == 0) [[unlikely]] {
        return;
    }
    DCHECK(ExecEnv::GetInstance());
    std::lock_guard wlock(_mtx);
    if (auto iter = _file_name_to_reader.find(key); iter != _file_name_to_reader.end()) {
        _file_reader_list.erase(iter->second);
        _file_name_to_reader.erase(key);
    }
}

bool FDCache::contains_file_reader(const AccessKeyAndOffset& key) {
    std::shared_lock rlock(_mtx);
    return _file_name_to_reader.contains(key);
}

size_t FDCache::file_reader_cache_size() {
    std::shared_lock rlock(_mtx);
    return _file_reader_list.size();
}

Status FSFileCacheStorage::init(BlockFileCache* mgr) {
    const char* metrics_prefix = mgr->_cache_base_path.c_str();
    _iterator_dir_retry_cnt = std::make_shared<bvar::LatencyRecorder>(
            metrics_prefix, "file_cache_fs_storage_iterator_dir_retry_cnt");
    _leak_scan_removed_files = std::make_shared<bvar::Adder<size_t>>(
            metrics_prefix, "file_cache_leak_removed_files_cnt");
    _cache_base_path = mgr->_cache_base_path;
    _mgr = mgr;
    _meta_store = std::make_unique<CacheBlockMetaStore>(_cache_base_path + "/meta", 10000);
    _cache_background_load_thread = std::thread([this, mgr]() {
        try {
            auto mem_tracker = MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::OTHER, fmt::format("FileCacheVersionReader"));
            SCOPED_ATTACH_TASK(mem_tracker);
            Status st = upgrade_cache_dir_if_necessary();
            if (!st.ok()) {
                std::string msg = fmt::format(
                        "file cache {} upgrade done with error. upgrade version failed. st={}",
                        _cache_base_path, st.to_string());
                if (doris::config::ignore_file_cache_dir_upgrade_failure) {
                    LOG(WARNING)
                            << msg << " be conf: `ignore_file_cache_dir_upgrade_failure = true`"
                            << " so we are ignoring the error (unsuccessful cache files will be "
                               "removed)";
                    remove_old_version_directories();
                } else {
                    LOG(WARNING) << msg << " please fix error and restart BE or"
                                 << " use be conf: `ignore_file_cache_dir_upgrade_failure = true`"
                                 << " to skip the error (unsuccessful cache files will be removed)";
                    throw doris::Exception(Status::InternalError(msg));
                }
            }
            load_cache_info_into_memory(mgr);
            mgr->_async_open_done = true;
            LOG_INFO("file cache {} lazy load done.", _cache_base_path);
            start_leak_cleaner(mgr);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Background cache loading thread failed with exception: " << e.what();
        } catch (...) {
            LOG(ERROR) << "Background cache loading thread failed with unknown exception";
        }
    });
    return Status::OK();
}

Status FSFileCacheStorage::append(const FileCacheKey& key, const Slice& value) {
    FileWriter* writer = nullptr;
    {
        std::lock_guard lock(_mtx);
        auto file_writer_map_key = std::make_pair(key.hash, key.offset);
        if (auto iter = _key_to_writer.find(file_writer_map_key); iter != _key_to_writer.end()) {
            writer = iter->second.get();
        } else {
            std::string dir = get_path_in_local_cache_v3(key.hash);
            auto st = fs->create_directory(dir, false);
            if (!st.ok() && !st.is<ErrorCode::ALREADY_EXIST>()) {
                return st;
            }
            std::string tmp_file = get_path_in_local_cache_v3(dir, key.offset, true);
            FileWriterPtr file_writer;
            FileWriterOptions opts {.sync_file_data = false};
            RETURN_IF_ERROR(fs->create_file(tmp_file, &file_writer, &opts));
            writer = file_writer.get();
            _key_to_writer.emplace(file_writer_map_key, std::move(file_writer));
        }
    }
    DCHECK_NE(writer, nullptr);
    return writer->append(value);
}

Status FSFileCacheStorage::finalize(const FileCacheKey& key, const size_t size) {
    FileWriterPtr file_writer;
    {
        std::lock_guard lock(_mtx);
        auto file_writer_map_key = std::make_pair(key.hash, key.offset);
        auto iter = _key_to_writer.find(file_writer_map_key);
        DCHECK(iter != _key_to_writer.end());
        file_writer = std::move(iter->second);
        _key_to_writer.erase(iter);
    }
    if (file_writer->state() != FileWriter::State::CLOSED) {
        RETURN_IF_ERROR(file_writer->close());
    }
    std::string dir = get_path_in_local_cache_v3(key.hash);
    std::string true_file = get_path_in_local_cache_v3(dir, key.offset);
    auto s = fs->rename(file_writer->path(), true_file);
    if (!s.ok()) {
        return s;
    }

    BlockMetaKey mkey(key.meta.tablet_id, UInt128Wrapper(key.hash), key.offset);
    BlockMeta meta(key.meta.type, size, key.meta.expiration_time);
    _meta_store->put(mkey, meta);

    return Status::OK();
}

Status FSFileCacheStorage::read(const FileCacheKey& key, size_t value_offset, Slice buffer) {
    AccessKeyAndOffset fd_key = std::make_pair(key.hash, key.offset);
    FileReaderSPtr file_reader = FDCache::instance()->get_file_reader(fd_key);
    if (!file_reader) {
        std::string file =
                get_path_in_local_cache_v3(get_path_in_local_cache_v3(key.hash), key.offset);
        Status s = fs->open_file(file, &file_reader);
        if (!s.ok()) {
            if (s.is<ErrorCode::NOT_FOUND>()) {
                // Try to open file with old v2 format
                std::string dir = get_path_in_local_cache_v2(key.hash, key.meta.expiration_time);
                std::string v2_file = get_path_in_local_cache_v2(dir, key.offset, key.meta.type);
                Status s2 = fs->open_file(v2_file, &file_reader);
                if (!s2.ok()) {
                    LOG(WARNING) << "open file failed with both v3 and v2 format, v3_file=" << file
                                 << ", v2_file=" << v2_file << ", error=" << s2.to_string();
                    return s2;
                }
            } else {
                LOG(WARNING) << "open file failed, file=" << file << ", error=" << s.to_string();
                return s;
            }
        }

        FDCache::instance()->insert_file_reader(fd_key, file_reader);
    }
    size_t bytes_read = 0;
    auto s = file_reader->read_at(value_offset, buffer, &bytes_read);
    if (!s.ok()) {
        LOG(WARNING) << "read file failed, file=" << file_reader->path()
                     << ", error=" << s.to_string();
        return s;
    }
    DCHECK(bytes_read == buffer.get_size());
    return Status::OK();
}

Status FSFileCacheStorage::remove(const FileCacheKey& key) {
    std::string dir = get_path_in_local_cache_v3(key.hash);
    std::string file = get_path_in_local_cache_v3(dir, key.offset);
    FDCache::instance()->remove_file_reader(std::make_pair(key.hash, key.offset));
    RETURN_IF_ERROR(fs->delete_file(file));
    // return OK not means the file is deleted, it may be not exist

    { // try to detect the file with old v2 format
        dir = get_path_in_local_cache_v2(key.hash, key.meta.expiration_time);
        file = get_path_in_local_cache_v2(dir, key.offset, key.meta.type);
        RETURN_IF_ERROR(fs->delete_file(file));
    }

    BlockMetaKey mkey(key.meta.tablet_id, UInt128Wrapper(key.hash), key.offset);
    _meta_store->delete_key(mkey);
    std::vector<FileInfo> files;
    bool exists {false};
    RETURN_IF_ERROR(fs->list(dir, true, &files, &exists));
    if (files.empty()) {
        RETURN_IF_ERROR(fs->delete_directory(dir));
    }
    return Status::OK();
}

Status FSFileCacheStorage::change_key_meta_type(const FileCacheKey& key, const FileCacheType type,
                                                const size_t size) {
    // file operation
    if (key.meta.type != type) {
        BlockMetaKey mkey(key.meta.tablet_id, UInt128Wrapper(key.hash), key.offset);
        BlockMeta meta(type, size, key.meta.expiration_time);
        _meta_store->put(mkey, meta);
    }
    return Status::OK();
}

std::string FSFileCacheStorage::get_path_in_local_cache_v3(const std::string& dir, size_t offset,
                                                           bool is_tmp) {
    if (is_tmp) {
        return Path(dir) / (std::to_string(offset) + "_tmp");
    } else {
        return Path(dir) / std::to_string(offset);
    }
}

std::string FSFileCacheStorage::get_path_in_local_cache_v2(const std::string& dir, size_t offset,
                                                           FileCacheType type, bool is_tmp) {
    if (is_tmp) {
        return Path(dir) / (std::to_string(offset) + "_tmp");
    } else if (type == FileCacheType::TTL) {
        return Path(dir) / std::to_string(offset);
    } else {
        return Path(dir) / (std::to_string(offset) + cache_type_to_surfix(type));
    }
}

std::string FSFileCacheStorage::get_path_in_local_cache_v3(const UInt128Wrapper& value) const {
    auto str = value.to_string();
    try {
        return Path(_cache_base_path) / str.substr(0, KEY_PREFIX_LENGTH) / (str + "_0");
    } catch (std::filesystem::filesystem_error& e) {
        LOG_WARNING("fail to get_path_in_local_cache")
                .tag("err", e.what())
                .tag("key", value.to_string());
        return "";
    }
}

std::string FSFileCacheStorage::get_path_in_local_cache_v2(const UInt128Wrapper& value,
                                                           uint64_t expiration_time) const {
    auto str = value.to_string();
    try {
        return Path(_cache_base_path) / str.substr(0, KEY_PREFIX_LENGTH) /
               (str + "_" + std::to_string(expiration_time));
    } catch (std::filesystem::filesystem_error& e) {
        LOG_WARNING("fail to get_path_in_local_cache")
                .tag("err", e.what())
                .tag("key", value.to_string())
                .tag("expiration_time", expiration_time);
        return "";
    }
}

void FSFileCacheStorage::remove_old_version_directories() {
    std::error_code ec;
    std::filesystem::directory_iterator key_it {_cache_base_path, ec};
    if (ec) {
        LOG(WARNING) << "Failed to list directory: " << _cache_base_path
                     << ", error: " << ec.message();
        return;
    }

    std::vector<std::filesystem::path> file_list;
    // the dir is concurrently accessed, so handle invalid iter with retry
    bool success = false;
    size_t retry_count = 0;
    const size_t max_retry = 5;
    while (!success && retry_count < max_retry) {
        try {
            ++retry_count;
            for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
                file_list.push_back(key_it->path());
            }
            success = true;
        } catch (const std::exception& e) {
            LOG(WARNING) << "Error occurred while iterating directory: " << e.what();
            file_list.clear();
        }
    }

    if (!success) {
        LOG_WARNING("iteration of cache dir still failed after retry {} times.", max_retry);
    }

    auto path_itr = file_list.begin();
    for (; path_itr != file_list.end(); ++path_itr) {
        if (std::filesystem::is_directory(*path_itr)) {
            std::string cache_key = path_itr->filename().native();
            if (cache_key.size() > KEY_PREFIX_LENGTH) {
                // try our best to delete, not care the return
                (void)fs->delete_directory(*path_itr);
            }
        }
    }
    auto s = fs->delete_file(get_version_path());
    if (!s.ok()) {
        LOG(WARNING) << "deleted old version file failed: " << s.to_string();
        return;
    }
    s = write_file_cache_version();
    if (!s.ok()) {
        LOG(WARNING) << "write new version file failed: " << s.to_string();
        return;
    }
}

Status FSFileCacheStorage::collect_directory_entries(const std::filesystem::path& dir_path,
                                                     std::vector<std::string>& file_list) const {
    std::error_code ec;
    bool success = false;
    size_t retry_count = 0;
    const size_t max_retry = 5;

    while (!success && retry_count < max_retry) {
        try {
            ++retry_count;
            std::filesystem::directory_iterator it {dir_path, ec};
            TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::collect_directory_entries");
            if (ec) {
                LOG(WARNING) << "Failed to list directory: " << dir_path
                             << ", error: " << ec.message();
                continue;
            }

            file_list.clear();
            for (; it != std::filesystem::directory_iterator(); ++it) {
                file_list.push_back(it->path().string());
            }
            success = true;
        } catch (const std::exception& e) {
            LOG(WARNING) << "Error occurred while iterating directory: " << dir_path
                         << " err: " << e.what();
            file_list.clear();
        }
    }

    *_iterator_dir_retry_cnt << retry_count;

    if (!success) {
        LOG_WARNING("iteration of cache dir still failed after retry {} times.", max_retry);
        return Status::InternalError("Failed to iterate directory after retries.");
    }

    return Status::OK();
}

Status FSFileCacheStorage::upgrade_cache_dir_if_necessary() const {
    std::string version;
    int rename_count = 0;
    int failure_count = 0;
    auto start_time = std::chrono::steady_clock::now();

    RETURN_IF_ERROR(read_file_cache_version(&version));

    if (version == "1.0") {
        LOG(ERROR) << "Cache version upgrade issue: Cannot upgrade directly from 1.0 to 3.0.Please "
                      "upgrade to 2.0 first (>= doris-3.0.0),or clear the file cache directory to "
                      "start anew "
                      "(LOSING ALL THE CACHE).";
        exit(-1);
    } else if (version == "2.0") {
        LOG(INFO) << "Cache will upgrade from 2.0 to 3.0 progressively during running. 2.0 data "
                     "format will evict eventually.";
        return Status::OK();
    } else if (version == "3.0") {
        LOG(INFO) << "Readly 3.0 format, no need to upgrade.";
        return Status::OK();
    } else {
        LOG(ERROR) << "Cache version upgrade issue: current version " << version
                   << " is not valid. Clear the file cache directory to start anew (LOSING ALL THE "
                      "CACHE).";
        exit(-1);
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG(INFO) << "Cache directory upgrade completed. Total files renamed: " << rename_count
              << ", Time taken: " << duration.count() << "ms"
              << ", Failure count: " << failure_count;
    return Status::OK();
}

Status FSFileCacheStorage::write_file_cache_version() const {
    std::string version_path = get_version_path();

    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    // Add version field to JSON
    rapidjson::Value version_value;
    version_value.SetString("3.0", allocator);
    doc.AddMember("version", version_value, allocator);

    // Serialize JSON to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    // Combine version string with JSON for backward compatibility
    std::string version_content = "3.0" + std::string(buffer.GetString(), buffer.GetSize());
    Slice version_slice(version_content);

    FileWriterPtr version_writer;
    RETURN_IF_ERROR(fs->create_file(version_path, &version_writer));
    RETURN_IF_ERROR(version_writer->append(version_slice));
    return version_writer->close();
}

Status FSFileCacheStorage::read_file_cache_version(std::string* buffer) const {
    std::string version_path = get_version_path();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(version_path, &exists));
    if (!exists) {
        *buffer = "2.0"; // return 2.0 if not exist to utilize filesystem
        return Status::OK();
    }
    FileReaderSPtr version_reader;
    int64_t file_size = -1;
    RETURN_IF_ERROR(fs->file_size(version_path, &file_size));
    buffer->resize(file_size);
    RETURN_IF_ERROR(fs->open_file(version_path, &version_reader));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(version_reader->read_at(0, Slice(buffer->data(), file_size), &bytes_read));
    RETURN_IF_ERROR(version_reader->close());

    // Extract only the version number part (before JSON starts) for backward compatibility
    // New format: "3.0{\"version\":\"3.0\"}", old format: "3.0"
    std::string content = *buffer;
    size_t json_start = content.find('{');
    if (json_start != std::string::npos) {
        // New format with JSON, extract version number only
        *buffer = content.substr(0, json_start);
    } else {
        // Old format, keep as is
        *buffer = content;
    }

    auto st = Status::OK();
    TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::read_file_cache_version", &st);
    return st;
}

std::string FSFileCacheStorage::get_version_path() const {
    return Path(_cache_base_path) / "version";
}

Status FSFileCacheStorage::parse_filename_suffix_to_cache_type(
        const std::shared_ptr<LocalFileSystem>& input_fs, const Path& file_path,
        long expiration_time, size_t size, size_t* offset, bool* is_tmp,
        FileCacheType* cache_type) const {
    std::error_code ec;
    std::string offset_with_suffix = file_path.native();
    auto delim_pos1 = offset_with_suffix.find('_');
    bool parsed = true;

    try {
        if (delim_pos1 == std::string::npos) {
            // same as type "normal"
            *offset = stoull(offset_with_suffix);
        } else {
            *offset = stoull(offset_with_suffix.substr(0, delim_pos1));
            std::string suffix = offset_with_suffix.substr(delim_pos1 + 1);
            // not need persistent anymore
            // if suffix is equals to "tmp", it should be removed too.
            if (suffix == "tmp") [[unlikely]] {
                *is_tmp = true;
            } else {
                *cache_type = surfix_to_cache_type(suffix);
            }
        }
    } catch (...) {
        parsed = false;
    }

    // File in dir with expiration time > 0 should all be TTL type
    // while expiration time == 0 should all be NORMAL type but
    // in old days, bug happens, thus break such consistency, e.g.
    // BEs shut down during cache type transition.
    // Nowadays, we only use expiration time to decide the type,
    // i.e. whenever expiration time > 0, it IS TTL, otherwise
    // it is NORMAL or INDEX depending on its suffix.
    // From now on, the ttl type encoding in file name is only for
    // compatibility. It won't be build into the filename, and existing
    // ones will be ignored.
    if (expiration_time > 0) {
        *cache_type = FileCacheType::TTL;
    } else if (*cache_type == FileCacheType::TTL && expiration_time == 0) {
        *cache_type = FileCacheType::NORMAL;
    }

    if (!parsed) {
        LOG(WARNING) << "parse offset err, path=" << file_path.native();
        return Status::InternalError("parse offset err, path={}", file_path.native());
    }
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::REMOVE_FILE", &offset_with_suffix);

    if (ec) {
        LOG(WARNING) << "failed to file_size: file_name=" << offset_with_suffix
                     << "error=" << ec.message();
        return Status::InternalError("failed to file_size: file_name={}, error={}",
                                     offset_with_suffix, ec.message());
    }

    if (size == 0 && !(*is_tmp)) {
        auto st = input_fs->delete_file(file_path);
        if (!st.ok()) {
            LOG_WARNING("delete file {} error", file_path.native()).error(st);
        }
        return Status::InternalError("file size is 0, file_name={}", offset_with_suffix);
    }
    return Status::OK();
}

bool FSFileCacheStorage::handle_already_loaded_block(
        BlockFileCache* mgr, const UInt128Wrapper& hash, size_t offset, size_t new_size,
        int64_t tablet_id, std::lock_guard<std::mutex>& cache_lock) const {
    auto file_it = mgr->_files.find(hash);
    if (file_it == mgr->_files.end()) {
        return false;
    }

    auto cell_it = file_it->second.find(offset);
    if (cell_it == file_it->second.end()) {
        return false;
    }

    auto block = cell_it->second.file_block;
    if (tablet_id != 0 && block->tablet_id() == 0) {
        block->set_tablet_id(tablet_id);
    }

    size_t old_size = block->range().size();
    if (old_size != new_size) {
        mgr->reset_range(hash, offset, old_size, new_size, cache_lock);
    }
    return true;
}

void FSFileCacheStorage::load_cache_info_into_memory_from_fs(BlockFileCache* mgr) const {
    int scan_length = 10000;
    std::vector<BatchLoadArgs> batch_load_buffer;
    batch_load_buffer.reserve(scan_length);
    auto add_cell_batch_func = [&]() {
        SCOPED_CACHE_LOCK(mgr->_mutex, mgr);

        auto f = [&](const BatchLoadArgs& args) {
            // in async load mode, a cell may be added twice.
            if (handle_already_loaded_block(_mgr, args.hash, args.offset, args.size,
                                            args.ctx.tablet_id, cache_lock)) {
                return;
            }
            // if the file is tmp, it means it is the old file and it should be removed
            if (!args.is_tmp) {
                mgr->add_cell(args.hash, args.ctx, args.offset, args.size,
                              FileBlock::State::DOWNLOADED, cache_lock);
                return;
            }
            std::error_code ec;
            std::filesystem::remove(args.offset_path, ec);
            if (ec) {
                LOG(WARNING) << fmt::format("cannot remove {}: {}", args.offset_path, ec.message());
            }
        };
        std::for_each(batch_load_buffer.begin(), batch_load_buffer.end(), f);
        batch_load_buffer.clear();
    };

    auto scan_file_cache = [&](std::filesystem::directory_iterator& key_it) {
        TEST_SYNC_POINT_CALLBACK("BlockFileCache::TmpFile1");
        for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
            auto key_with_suffix = key_it->path().filename().native();
            auto delim_pos = key_with_suffix.find('_');
            if (delim_pos == std::string::npos || delim_pos != sizeof(uint128_t) * 2) {
                continue;
            }
            std::string key_str = key_with_suffix.substr(0, delim_pos);
            std::string expiration_time_str = key_with_suffix.substr(delim_pos + 1);
            auto hash = UInt128Wrapper(vectorized::unhex_uint<uint128_t>(key_str.c_str()));
            std::error_code ec;
            std::filesystem::directory_iterator offset_it(key_it->path(), ec);
            if (ec) [[unlikely]] {
                LOG(WARNING) << "filesystem error, failed to list dir, dir=" << key_it->path()
                             << " error=" << ec.message();
                continue;
            }
            CacheContext context;
            context.query_id = TUniqueId();
            long expiration_time = std::stoul(expiration_time_str);
            context.expiration_time = expiration_time;
            for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
                size_t size = offset_it->file_size(ec);
                size_t offset = 0;
                bool is_tmp = false;
                FileCacheType cache_type = FileCacheType::NORMAL;
                if (!parse_filename_suffix_to_cache_type(fs, offset_it->path().filename().native(),
                                                         expiration_time, size, &offset, &is_tmp,
                                                         &cache_type)) {
                    continue;
                }
                context.cache_type = cache_type;
                BatchLoadArgs args;
                args.ctx = context;
                args.hash = hash;
                args.key_path = key_it->path();
                args.offset_path = offset_it->path();
                args.size = size;
                args.offset = offset;
                args.is_tmp = is_tmp;
                batch_load_buffer.push_back(std::move(args));

                // add lock
                if (batch_load_buffer.size() >= scan_length) {
                    add_cell_batch_func();
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }
        }
    };
    std::error_code ec;

    TEST_SYNC_POINT_CALLBACK("BlockFileCache::BeforeScan");
    std::filesystem::directory_iterator key_prefix_it {_cache_base_path, ec};
    if (ec) {
        LOG(WARNING) << ec.message();
        return;
    }
    for (; key_prefix_it != std::filesystem::directory_iterator(); ++key_prefix_it) {
        if (!key_prefix_it->is_directory()) {
            // skip version file
            continue;
        }
        if (key_prefix_it->path().filename().native() == META_DIR_NAME) {
            // skip rocksdb dir
            continue;
        }
        if (key_prefix_it->path().filename().native().size() != KEY_PREFIX_LENGTH) {
            LOG(WARNING) << "Unknown directory " << key_prefix_it->path().native()
                         << ", try to remove it";
            std::filesystem::remove(key_prefix_it->path(), ec);
            if (ec) {
                LOG(WARNING) << "failed to remove=" << key_prefix_it->path()
                             << " msg=" << ec.message();
            }
            continue;
        }
        std::filesystem::directory_iterator key_it {key_prefix_it->path(), ec};
        if (ec) {
            LOG(WARNING) << ec.message();
            continue;
        }
        scan_file_cache(key_it);
    }

    if (!batch_load_buffer.empty()) {
        add_cell_batch_func();
    }
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::TmpFile2");
}

Status FSFileCacheStorage::get_file_cache_infos(std::vector<FileCacheInfo>& infos,
                                                std::lock_guard<std::mutex>& cache_lock) const {
    std::error_code ec;
    std::filesystem::directory_iterator key_prefix_it {_cache_base_path, ec};
    if (ec) [[unlikely]] {
        LOG(ERROR) << fmt::format("Failed to list dir {}, err={}", _cache_base_path, ec.message());
        return Status::InternalError("Failed to list dir {}, err={}", _cache_base_path,
                                     ec.message());
    }
    // Only supports version 2. For more details, refer to 'USE_CACHE_VERSION2'.
    for (; key_prefix_it != std::filesystem::directory_iterator(); ++key_prefix_it) {
        if (!key_prefix_it->is_directory()) {
            // skip version file
            continue;
        }
        if (key_prefix_it->path().filename().native().size() != KEY_PREFIX_LENGTH) {
            LOG(WARNING) << "Unknown directory " << key_prefix_it->path().native();
            continue;
        }
        std::filesystem::directory_iterator key_it {key_prefix_it->path(), ec};
        if (ec) [[unlikely]] {
            LOG(ERROR) << fmt::format("Failed to list dir {}, err={}",
                                      key_prefix_it->path().filename().native(), ec.message());
            return Status::InternalError("Failed to list dir {}, err={}",
                                         key_prefix_it->path().filename().native(), ec.message());
        }
        for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
            auto key_with_suffix = key_it->path().filename().native();
            auto delim_pos = key_with_suffix.find('_');
            if (delim_pos == std::string::npos || delim_pos != sizeof(uint128_t) * 2) {
                continue;
            }
            std::string key_str = key_with_suffix.substr(0, delim_pos);
            std::string expiration_time_str = key_with_suffix.substr(delim_pos + 1);
            long expiration_time = std::stoul(expiration_time_str);
            auto hash = UInt128Wrapper(vectorized::unhex_uint<uint128_t>(key_str.c_str()));
            std::filesystem::directory_iterator offset_it(key_it->path(), ec);
            if (ec) [[unlikely]] {
                LOG(ERROR) << fmt::format("Failed to list dir {}, err={}",
                                          key_it->path().filename().native(), ec.message());
                return Status::InternalError("Failed to list dir {}, err={}",
                                             key_it->path().filename().native(), ec.message());
            }
            for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
                size_t size = offset_it->file_size(ec);
                if (ec) [[unlikely]] {
                    LOG(ERROR) << fmt::format("Failed to get file size, file name {}, err={}",
                                              key_it->path().filename().native(), ec.message());
                    return Status::InternalError("Failed to get file size, file name {}, err={}",
                                                 key_it->path().filename().native(), ec.message());
                }
                size_t offset = 0;
                bool is_tmp = false;
                FileCacheType cache_type = FileCacheType::NORMAL;
                RETURN_IF_ERROR(this->parse_filename_suffix_to_cache_type(
                        fs, offset_it->path().filename().native(), expiration_time, size, &offset,
                        &is_tmp, &cache_type));
                infos.emplace_back(hash, expiration_time, size, offset, is_tmp, cache_type);
            }
        }
    }
    return Status::OK();
}

void FSFileCacheStorage::load_cache_info_into_memory_from_db(BlockFileCache* mgr) const {
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::TmpFile1");
    int scan_length = 10000;
    std::vector<BatchLoadArgs> batch_load_buffer;
    batch_load_buffer.reserve(scan_length);
    auto add_cell_batch_func = [&]() {
        SCOPED_CACHE_LOCK(mgr->_mutex, mgr);

        auto f = [&](const BatchLoadArgs& args) {
            // in async load mode, a cell may be added twice.
            if (handle_already_loaded_block(_mgr, args.hash, args.offset, args.size,
                                            args.ctx.tablet_id, cache_lock)) {
                return;
            }
            mgr->add_cell(args.hash, args.ctx, args.offset, args.size, FileBlock::State::DOWNLOADED,
                          cache_lock);
            return;
        };
        std::for_each(batch_load_buffer.begin(), batch_load_buffer.end(), f);
        batch_load_buffer.clear();
    };

    auto iterator = _meta_store->get_all();
    if (!iterator) {
        LOG(WARNING) << "Failed to create iterator for meta store";
        return;
    }

    while (iterator->valid()) {
        BlockMetaKey meta_key = iterator->key();
        BlockMeta meta_value = iterator->value();

        // Check for deserialization errors
        if (!iterator->get_last_key_error().ok() || !iterator->get_last_value_error().ok()) {
            LOG(WARNING) << "Failed to deserialize cache block metadata: "
                         << "key_error=" << iterator->get_last_key_error().to_string()
                         << ", value_error=" << iterator->get_last_value_error().to_string();
            iterator->next();
            continue; // Skip invalid records
        }

        VLOG_DEBUG << "Processing cache block: tablet_id=" << meta_key.tablet_id
                   << ", hash=" << meta_key.hash.low() << "-" << meta_key.hash.high()
                   << ", offset=" << meta_key.offset << ", type=" << meta_value.type
                   << ", size=" << meta_value.size << ", ttl=" << meta_value.ttl;

        BatchLoadArgs args;
        args.hash = meta_key.hash;
        args.offset = meta_key.offset;
        args.size = meta_value.size;
        args.is_tmp = false;

        CacheContext ctx;
        ctx.cache_type = static_cast<FileCacheType>(meta_value.type);
        ctx.expiration_time = meta_value.ttl;
        ctx.tablet_id =
                meta_key.tablet_id; //TODO(zhengyu): zero if loaded from v2, we can use this to decide whether the block is loaded from v2 or v3
        args.ctx = ctx;

        args.key_path = "";
        args.offset_path = "";

        batch_load_buffer.push_back(std::move(args));

        if (batch_load_buffer.size() >= scan_length) {
            add_cell_batch_func();
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        iterator->next();
    }

    LOG(INFO) << "Finished loading cache info from meta store using RocksDB iterator";

    if (!batch_load_buffer.empty()) {
        add_cell_batch_func();
    }
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::TmpFile2");
}

void FSFileCacheStorage::load_cache_info_into_memory(BlockFileCache* mgr) const {
    // First load from database
    load_cache_info_into_memory_from_db(mgr);

    std::string version;
    auto st = read_file_cache_version(&version);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to read file cache version: " << st.to_string();
        return;
    }
    if (version == "3.0") {
        return;
    }

    // If cache directory is effectively empty (no cache data entries), write version hint and
    // return directly.
    auto is_cache_base_path_empty = [&]() -> bool {
        std::error_code ec;
        std::filesystem::directory_iterator it {_cache_base_path, ec};
        if (ec) {
            LOG(WARNING) << "Failed to list cache directory: " << _cache_base_path
                         << ", error: " << ec.message();
            return false;
        }

        for (; it != std::filesystem::directory_iterator(); ++it) {
            auto name = it->path().filename().native();
            if (name == META_DIR_NAME || name == "version") {
                continue;
            }
            return false;
        }
        return true;
    };

    if (is_cache_base_path_empty()) {
        if (st = write_file_cache_version(); !st.ok()) {
            LOG(WARNING) << "Failed to write version hints for file cache, err=" << st.to_string();
        }
        return;
    }

    // Count blocks loaded from database
    size_t db_block_count = 0;
    {
        std::lock_guard<std::mutex> lock(mgr->_mutex);
        for (const auto& hash_entry : mgr->_files) {
            db_block_count += hash_entry.second.size();
        }
    }

    // Estimate file count from filesystem using statfs
    size_t estimated_file_count = estimate_file_count_from_inode();

    LOG(INFO) << "Cache loading statistics - DB blocks: " << db_block_count
              << ", Estimated FS files: " << estimated_file_count;

    // If the difference is more than threshold, load from filesystem as well
    if (estimated_file_count > 100) {
        double difference_ratio =
                (static_cast<double>(estimated_file_count) - static_cast<double>(db_block_count)) /
                static_cast<double>(estimated_file_count);

        if (difference_ratio > config::file_cache_meta_store_vs_file_system_diff_num_threshold) {
            LOG(WARNING) << "Significant difference between DB blocks (" << db_block_count
                         << ") and estimated FS files (" << estimated_file_count
                         << "), difference ratio: " << difference_ratio * 100 << "%"
                         << ". Loading from filesystem as well.";
            load_cache_info_into_memory_from_fs(_mgr);
        } else {
            LOG(INFO) << "DB and FS counts are consistent, difference ratio: "
                      << difference_ratio * 100 << "%, skipping FS load.";
            if (st = write_file_cache_version(); !st.ok()) {
                LOG(WARNING) << "Failed to write version hints for file cache, err="
                             << st.to_string();
            }
            // TODO(zhengyu): use anti-leak machinism to remove v2 format directory
        }
    } else {
        LOG(INFO) << "FS contains low number of files, num = " << estimated_file_count
                  << ", skipping FS load.";
        if (st = write_file_cache_version(); !st.ok()) {
            LOG(WARNING) << "Failed to write version hints for file cache, err=" << st.to_string();
        }
    }
}

void FSFileCacheStorage::load_blocks_directly_unlocked(BlockFileCache* mgr, const FileCacheKey& key,
                                                       std::lock_guard<std::mutex>& cache_lock) {
    BlockMetaKey mkey(key.meta.tablet_id, UInt128Wrapper(key.hash), key.offset);
    auto block_meta = _meta_store->get(mkey);
    if (!block_meta.has_value()) {
        // cache miss
        return;
    }

    CacheContext context_original;
    context_original.query_id = TUniqueId();
    context_original.expiration_time = block_meta->ttl;
    context_original.cache_type = static_cast<FileCacheType>(block_meta->type);
    context_original.tablet_id = key.meta.tablet_id;

    if (handle_already_loaded_block(mgr, key.hash, key.offset, block_meta->size, key.meta.tablet_id,
                                    cache_lock)) {
        return;
    } else {
        mgr->add_cell(key.hash, context_original, key.offset, block_meta->size,
                      FileBlock::State::DOWNLOADED, cache_lock);
    }
}

Status FSFileCacheStorage::clear(std::string& msg) {
    LOG(INFO) << "clear file storage, path=" << _cache_base_path;
    std::error_code ec;
    std::filesystem::directory_iterator key_it {_cache_base_path, ec};
    if (ec) {
        LOG(WARNING) << "Failed to list directory: " << _cache_base_path
                     << ", error: " << ec.message();
        return Status::InternalError("Failed to list dir {}: {}", _cache_base_path, ec.message());
    }
    int failed = 0;
    int total = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
        if (!key_it->is_directory()) continue; // all file cache data is in sub-directories
        if (key_it->path().filename().native() == META_DIR_NAME) continue;
        ++total;
        std::string cache_key = key_it->path().string();
        auto st = global_local_filesystem()->delete_directory(cache_key);
        if (st.ok()) continue;
        failed++;
        LOG(WARNING) << "failed to clear base_path=" << _cache_base_path
                     << " path_to_delete=" << cache_key << " error=" << st;
    }
    _meta_store->clear();
    auto t1 = std::chrono::steady_clock::now();
    std::stringstream ss;
    ss << "finished clear file storage, path=" << _cache_base_path
       << " deleted=" << (total - failed) << " failed=" << failed
       << " elapsed_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    LOG(INFO) << ss.str();
    if (failed > 0) {
        msg = ss.str();
        return Status::InternalError(msg);
    }
    return Status::OK();
}

std::string FSFileCacheStorage::get_local_file(const FileCacheKey& key) {
    return get_path_in_local_cache_v3(get_path_in_local_cache_v3(key.hash), key.offset, false);
}

FSFileCacheStorage::~FSFileCacheStorage() {
    if (_cache_background_load_thread.joinable()) {
        _cache_background_load_thread.join();
    }
    stop_leak_cleaner();
}

size_t FSFileCacheStorage::estimate_file_count_from_inode() const {
    int64_t duration_ns = 0;
    size_t cache_files = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        do {
            struct statvfs vfs {};
            int statvfs_res = 0;
#ifdef BE_TEST
            if (auto* hooks = inode_test_hooks(); hooks && hooks->statvfs_override) {
                statvfs_res = hooks->statvfs_override(_cache_base_path, &vfs);
            } else
#endif
            {
                statvfs_res = statvfs(_cache_base_path.c_str(), &vfs);
            }
            if (statvfs_res != 0) {
                LOG(WARNING) << "Failed to get filesystem statistics for path: " << _cache_base_path
                             << ", error: " << strerror(errno);
                break;
            }

            if (vfs.f_files == 0) {
                LOG(WARNING) << "Filesystem returned zero total inodes for path "
                             << _cache_base_path;
                break;
            }

            struct stat cache_stat {};
            int lstat_res = 0;
#ifdef BE_TEST
            if (auto* hooks = inode_test_hooks(); hooks && hooks->lstat_override) {
                lstat_res = hooks->lstat_override(_cache_base_path, &cache_stat);
            } else
#endif
            {
                lstat_res = lstat(_cache_base_path.c_str(), &cache_stat);
            }
            if (lstat_res != 0) {
                LOG(WARNING) << "Failed to stat cache base path " << _cache_base_path << ": "
                             << strerror(errno);
                break;
            }

            size_t total_inodes_used = vfs.f_files - vfs.f_ffree;
            size_t non_cache_inodes = estimate_non_cache_inode_usage();
            size_t directory_inodes = estimate_cache_directory_inode_usage();

            if (total_inodes_used > non_cache_inodes + directory_inodes) {
                cache_files = total_inodes_used - non_cache_inodes - directory_inodes;
            } else {
                LOG(WARNING) << fmt::format(
                        "Inode subtraction underflow: total={} non_cache={} directory={}",
                        total_inodes_used, non_cache_inodes, directory_inodes);
            }

            LOG(INFO) << fmt::format(
                    "Cache inode estimation: total_used={}, non_cache={}, directories≈{}, files≈{}",
                    total_inodes_used, non_cache_inodes, directory_inodes, cache_files);
        } while (false);
    }
    const double duration_ms = static_cast<double>(duration_ns) / 1'000'000.0;
    LOG(INFO) << fmt::format("estimate_file_count_from_inode duration_ms={:.3f}, files={}",
                             duration_ms, cache_files);
    return cache_files;
}

size_t FSFileCacheStorage::count_inodes_for_path(
        const std::filesystem::path& path, dev_t target_dev,
        const std::filesystem::path& excluded_root,
        std::unordered_set<InodeKey, InodeKeyHash>& visited) const {
#ifdef BE_TEST
    if (auto* hooks = inode_test_hooks(); hooks && hooks->count_inodes_override) {
        return hooks->count_inodes_override(*this, path, target_dev, excluded_root, visited);
    }
#endif
    if (!excluded_root.empty()) {
        std::error_code eq_ec;
        bool is_excluded = std::filesystem::equivalent(path, excluded_root, eq_ec);
        if (eq_ec) {
            LOG(WARNING) << "Failed to compare " << path << " with " << excluded_root << ": "
                         << eq_ec.message();
        } else if (is_excluded) {
            return 0;
        }
    }

    struct stat st {};
    if (lstat(path.c_str(), &st) != 0) {
        LOG(WARNING) << "Failed to stat path " << path << ": " << strerror(errno);
        return 0;
    }
    if (st.st_dev != target_dev) {
        return 0;
    }
    InodeKey key {st.st_dev, st.st_ino};
    if (!visited.insert(key).second) {
        return 0;
    }

    size_t count = 1;
    if (S_ISDIR(st.st_mode)) {
        std::error_code ec;
        for (std::filesystem::directory_iterator it {path, ec};
             !ec && it != std::filesystem::directory_iterator(); ++it) {
            count += count_inodes_for_path(it->path(), target_dev, excluded_root, visited);
        }
        if (ec) {
            LOG(WARNING) << "Failed to iterate directory " << path << ": " << ec.message();
        }
    }
    return count;
}

bool FSFileCacheStorage::is_cache_prefix_directory(
        const std::filesystem::directory_entry& entry) const {
    if (!entry.is_directory()) {
        return false;
    }
    auto name = entry.path().filename().native();
    if (name == META_DIR_NAME || name.empty()) {
        return false;
    }
    if (name.size() != KEY_PREFIX_LENGTH) {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](unsigned char c) { return std::isxdigit(c); });
}

std::filesystem::path FSFileCacheStorage::find_mount_root(dev_t cache_dev) const {
#ifdef BE_TEST
    if (auto* hooks = inode_test_hooks(); hooks && hooks->find_mount_root_override) {
        return hooks->find_mount_root_override(*this, cache_dev);
    }
#endif
    std::error_code ec;
    std::filesystem::path current = std::filesystem::absolute(_cache_base_path, ec);
    if (ec) {
        LOG(WARNING) << "Failed to resolve absolute cache base path " << _cache_base_path << ": "
                     << ec.message();
        current = _cache_base_path;
    }

    std::filesystem::path result = current;
    while (result.has_parent_path()) {
        auto parent = result.parent_path();
        if (parent.empty() || parent == result) {
            break;
        }
        struct stat st {};
        if (lstat(parent.c_str(), &st) != 0) {
            LOG(WARNING) << "Failed to stat parent path " << parent << ": " << strerror(errno);
            break;
        }
        if (st.st_dev != cache_dev) {
            break;
        }
        result = parent;
    }
    return result;
}

size_t FSFileCacheStorage::estimate_non_cache_inode_usage() const {
#ifdef BE_TEST
    if (auto* hooks = inode_test_hooks(); hooks && hooks->non_cache_override) {
        return hooks->non_cache_override(*this);
    }
#endif
    struct stat cache_stat {};
    if (lstat(_cache_base_path.c_str(), &cache_stat) != 0) {
        LOG(WARNING) << "Failed to stat cache base path " << _cache_base_path << ": "
                     << strerror(errno);
        return 0;
    }

    auto mount_root = find_mount_root(cache_stat.st_dev);
    if (mount_root.empty()) {
        LOG(WARNING) << "Failed to determine mount root for cache path " << _cache_base_path;
        return 0;
    }

    std::unordered_set<InodeKey, InodeKeyHash> visited;
    std::error_code abs_ec;
    std::filesystem::path excluded = std::filesystem::absolute(_cache_base_path, abs_ec);
    if (abs_ec) {
        LOG(WARNING) << "Failed to get absolute cache base path " << _cache_base_path << ": "
                     << abs_ec.message();
        excluded = _cache_base_path;
    }

    return count_inodes_for_path(mount_root, cache_stat.st_dev, excluded, visited);
}

size_t FSFileCacheStorage::estimate_cache_directory_inode_usage() const {
#ifdef BE_TEST
    if (auto* hooks = inode_test_hooks(); hooks && hooks->cache_dir_override) {
        return hooks->cache_dir_override(*this);
    }
#endif
    constexpr size_t kSampleLimit = 3;
    size_t prefix_dirs = 0;
    std::vector<std::filesystem::path> samples;

    std::error_code ec;
    std::filesystem::directory_iterator it {_cache_base_path, ec};
    if (ec) {
        LOG(WARNING) << "Failed to list cache base path for directory estimation: " << ec.message();
        return 0;
    }

    for (; it != std::filesystem::directory_iterator(); ++it) {
        if (!is_cache_prefix_directory(*it)) {
            continue;
        }
        ++prefix_dirs;
        if (samples.size() < kSampleLimit) {
            samples.emplace_back(it->path());
        }
    }

    if (prefix_dirs == 0 || samples.empty()) {
        return 0;
    }

    size_t sampled_second_level = 0;
    for (const auto& prefix_path : samples) {
        size_t local_count = 0;
        std::error_code sample_ec;
        for (std::filesystem::directory_iterator prefix_it {prefix_path, sample_ec};
             !sample_ec && prefix_it != std::filesystem::directory_iterator(); ++prefix_it) {
            if (prefix_it->is_directory()) {
                ++local_count;
            }
        }
        if (sample_ec) {
            LOG(WARNING) << "Failed to enumerate prefix directory " << prefix_path << ": "
                         << sample_ec.message();
            sample_ec.clear();
        }
        sampled_second_level += local_count;
    }

    double average_second_level = static_cast<double>(sampled_second_level) / samples.size();
    size_t estimated_second_level =
            static_cast<size_t>(std::llround(average_second_level * prefix_dirs));
    return prefix_dirs + estimated_second_level;
}

size_t FSFileCacheStorage::snapshot_metadata_block_count(BlockFileCache* /*mgr*/) const {
    // TODO(zhengyu): if the cache_lock problem is solved, we can then use _mgr
    int64_t duration_ns = 0;
    size_t block_count = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        if (_meta_store) {
            block_count = _meta_store->approximate_entry_count();
        } else {
            LOG(INFO) << "snapshot_metadata_block_count skipped because meta store is null";
            block_count = 0;
        }
    }
    const double duration_ms = static_cast<double>(duration_ns) / 1'000'000.0;
    LOG(INFO) << fmt::format("snapshot_metadata_block_count duration_ms={:.3f}, blocks={}",
                             duration_ms, block_count);
    return block_count;
}

std::vector<size_t> FSFileCacheStorage::snapshot_metadata_for_hash_offsets(
        BlockFileCache* mgr, const UInt128Wrapper& hash) const {
    std::vector<size_t> offsets;
    std::lock_guard<std::mutex> lock(mgr->_mutex);
    auto it = mgr->_files.find(hash);
    if (it == mgr->_files.end()) {
        return offsets;
    }
    offsets.reserve(it->second.size());
    for (const auto& [offset, _] : it->second) {
        offsets.push_back(offset);
    }
    return offsets;
}

void FSFileCacheStorage::start_leak_cleaner(BlockFileCache* mgr) {
    if (config::file_cache_leak_scan_interval_seconds <= 0) {
        LOG(WARNING) << "File cache leak cleaner disabled because interval <= 0";
        return;
    }

    // if version file not 3.0 then just return, clean nothing
    std::string version;
    if (auto st = read_file_cache_version(&version); !st.ok()) {
        LOG(WARNING) << "Failed to read file cache version: " << st.to_string();
        return;
    }
    if (version != "3.0") {
        LOG(WARNING) << "File cache leak cleaner skipped because version is not 3.0";
        return;
    }

    _stop_leak_cleaner.store(false, std::memory_order_relaxed);
    _cache_leak_cleaner_thread = std::thread([this]() { leak_cleaner_loop(); });
}

void FSFileCacheStorage::stop_leak_cleaner() {
    _stop_leak_cleaner.store(true, std::memory_order_relaxed);
    _leak_cleaner_cv.notify_all();
    if (_cache_leak_cleaner_thread.joinable()) {
        _cache_leak_cleaner_thread.join();
    }
}

void FSFileCacheStorage::leak_cleaner_loop() {
    Thread::set_self_name("leak_cleaner_loop");

    // randomly waiting before start the loop helps avoid thundering herd problem
    // for all strorages.
    const int64_t interval_seconds =
            std::max<int64_t>(1, config::file_cache_leak_scan_interval_seconds);
    std::mt19937_64 rng(std::random_device {}());
    std::uniform_int_distribution<int64_t> dist(0, interval_seconds);
    int64_t initial_delay = dist(rng);
    TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::leak_cleaner_loop::initial_delay",
                             &initial_delay);
    if (initial_delay > 0) {
        std::unique_lock<std::mutex> lock(_leak_cleaner_mutex);
        _leak_cleaner_cv.wait_for(lock, std::chrono::seconds(initial_delay), [this]() {
            return _stop_leak_cleaner.load(std::memory_order_relaxed);
        });
        lock.unlock();
        if (_stop_leak_cleaner.load(std::memory_order_relaxed)) {
            return;
        }
    }

    while (!_stop_leak_cleaner.load(std::memory_order_relaxed)) {
        int64_t interval_s = interval_seconds;
        TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::leak_cleaner_loop::interval", &interval_s);
        auto interval = std::chrono::seconds(interval_s);
        std::unique_lock<std::mutex> lock(_leak_cleaner_mutex);
        _leak_cleaner_cv.wait_for(lock, interval, [this]() {
            return _stop_leak_cleaner.load(std::memory_order_relaxed);
        });
        lock.unlock();
        if (_stop_leak_cleaner.load(std::memory_order_relaxed)) {
            break;
        }
        try {
            TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::leak_cleaner_loop::before_run");
            run_leak_cleanup(_mgr);
        } catch (const std::exception& e) {
            LOG(WARNING) << "File cache leak cleaner encountered exception: " << e.what();
        } catch (...) {
            LOG(WARNING) << "File cache leak cleaner encountered unknown exception";
        }
    }
}

void FSFileCacheStorage::run_leak_cleanup(BlockFileCache* mgr) {
    size_t metadata_blocks = snapshot_metadata_block_count(mgr);
    if (metadata_blocks == 0) {
        LOG(INFO) << "file cache leak scan found zero metadata blocks, skip cleanup";
        return;
    }

    size_t fs_files = estimate_file_count_from_inode();
    double ratio = static_cast<double>(fs_files) / static_cast<double>(metadata_blocks);

    LOG(INFO) << fmt::format(
            "file cache leak scan stats: fs_files={}, metadata_blocks={}, ratio={:.4f}", fs_files,
            metadata_blocks, ratio);

    double threshold = config::file_cache_leak_fs_to_meta_ratio_threshold;
    if (ratio <= threshold) {
        LOG_INFO("file cache leak ratio {0:.4f} within threshold {1:.4f}, no cleanup needed", ratio,
                 threshold);
        return;
    }

    LOG(WARNING) << fmt::format(
            "file cache leak ratio {0:.4f} exceeds threshold {1:.4f}, start cleanup", ratio,
            threshold);

    cleanup_leaked_files(mgr, metadata_blocks);
}

void FSFileCacheStorage::cleanup_leaked_files(BlockFileCache* mgr, size_t metadata_block_count) {
    const size_t batch_size = std::max<int32_t>(1, config::file_cache_leak_scan_batch_files);
    const size_t pause_ms = std::max<int32_t>(0, config::file_cache_leak_scan_pause_ms);

    int64_t cleanup_wall_time_ns = 0;
    int64_t metadata_hash_time_ns = 0;
    int64_t metadata_index_time_ns = 0;
    int64_t remove_candidates_time_ns = 0;
    int64_t directory_loop_time_ns = 0;
    size_t removed_files = 0;
    size_t examined_files = 0;

    std::vector<UInt128Wrapper> hash_keys;

    {
        SCOPED_RAW_TIMER(&cleanup_wall_time_ns);
        {
            SCOPED_RAW_TIMER(&metadata_hash_time_ns);
            std::lock_guard<std::mutex> lock(mgr->_mutex);
            hash_keys.reserve(mgr->_files.size());
            for (const auto& [hash, _] : mgr->_files) {
                hash_keys.push_back(hash);
            }
        }

        std::unordered_set<AccessKeyAndOffset, KeyAndOffsetHash> metadata_index;
        if (metadata_block_count > 0) {
            metadata_index.reserve(metadata_block_count * 2);
        }

        {
            SCOPED_RAW_TIMER(&metadata_index_time_ns);
            for (const auto& hash : hash_keys) {
                auto offsets = snapshot_metadata_for_hash_offsets(mgr, hash);
                for (const auto& offset : offsets) {
                    metadata_index.emplace(hash, offset);
                }
            }
        }

        struct OrphanCandidate {
            std::string path;
            UInt128Wrapper hash;
            size_t offset;
            std::string key_dir;
        };

        auto try_remove_empty_directory = [&](const std::string& dir) {
            std::error_code ec;
            std::filesystem::directory_iterator it(dir, ec);
            if (ec || it != std::filesystem::directory_iterator()) {
                return;
            }
            auto st = fs->delete_directory(dir);
            if (!st.ok() && !st.is<ErrorCode::NOT_FOUND>()) {
                LOG_WARNING("delete_directory {} failed", dir).error(st);
            }
        };

        std::vector<OrphanCandidate> candidates;
        candidates.reserve(batch_size);

        auto remove_candidates = [&]() {
            if (candidates.empty()) {
                return;
            }
            int64_t remove_once_ns = 0;
            {
                SCOPED_RAW_TIMER(&remove_once_ns);
                for (auto& candidate : candidates) {
                    auto st = fs->delete_file(candidate.path);
                    if (!st.ok() && !st.is<ErrorCode::NOT_FOUND>()) {
                        LOG_WARNING("delete orphan cache file {} failed", candidate.path).error(st);
                        continue;
                    }
                    removed_files++;
                    try_remove_empty_directory(candidate.key_dir);
                    auto prefix_dir =
                            std::filesystem::path(candidate.key_dir).parent_path().string();
                    try_remove_empty_directory(prefix_dir);
                }
                candidates.clear();
            }
            remove_candidates_time_ns += remove_once_ns;
            if (pause_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
            }
        };

        std::error_code ec;
        std::filesystem::directory_iterator prefix_it {_cache_base_path, ec};
        if (ec) {
            LOG(WARNING) << "Leak scan failed to list cache directory: " << _cache_base_path
                         << ", error: " << ec.message();
            return;
        }

        for (; prefix_it != std::filesystem::directory_iterator(); ++prefix_it) {
            int64_t loop_once_ns = 0;
            {
                SCOPED_RAW_TIMER(&loop_once_ns);
                std::string prefix_name = prefix_it->path().filename().native();
                if (!prefix_it->is_directory() || prefix_name == META_DIR_NAME ||
                    prefix_name.size() != KEY_PREFIX_LENGTH) {
                    continue;
                }

                std::filesystem::directory_iterator key_it {prefix_it->path(), ec};
                if (ec) {
                    LOG(WARNING) << "Leak scan failed to list prefix " << prefix_it->path().native()
                                 << ", error: " << ec.message();
                    continue;
                }

                for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
                    if (!key_it->is_directory()) {
                        continue;
                    }
                    auto key_with_suffix = key_it->path().filename().native();
                    auto delim_pos = key_with_suffix.find('_');
                    if (delim_pos == std::string::npos || delim_pos != sizeof(uint128_t) * 2) {
                        continue;
                    }

                    UInt128Wrapper hash;
                    try {
                        hash = UInt128Wrapper(vectorized::unhex_uint<uint128_t>(
                                key_with_suffix.substr(0, delim_pos).c_str()));
                    } catch (...) {
                        LOG(WARNING) << "Leak scan failed to parse hash from " << key_with_suffix;
                        continue;
                    }

                    long expiration = 0;
                    try {
                        expiration = std::stol(key_with_suffix.substr(delim_pos + 1));
                    } catch (...) {
                        LOG(WARNING)
                                << "Leak scan failed to parse expiration from " << key_with_suffix;
                        continue;
                    }

                    std::filesystem::directory_iterator offset_it {key_it->path(), ec};
                    if (ec) {
                        LOG(WARNING) << "Leak scan failed to list key directory "
                                     << key_it->path().native() << ", error: " << ec.message();
                        continue;
                    }

                    for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
                        if (!offset_it->is_regular_file()) {
                            continue;
                        }
                        const auto file_path = offset_it->path();
                        const std::string file_path_str = file_path.string();
                        size_t file_size = offset_it->file_size(ec);
                        if (ec) {
                            LOG(WARNING) << "Leak scan failed to fetch file size of "
                                         << file_path.native() << ": " << ec.message();
                            continue;
                        }

                        size_t offset = 0;
                        bool is_tmp = false;
                        FileCacheType cache_type = FileCacheType::NORMAL;
                        Status st = parse_filename_suffix_to_cache_type(
                                fs, offset_it->path().filename().native(), expiration, file_size,
                                &offset, &is_tmp, &cache_type);
                        if (!st.ok()) {
                            continue;
                        }

                        AccessKeyAndOffset meta_key {hash, offset};

                        // If the file is present in metadata and not a tmp file, skip it.
                        if (!is_tmp && metadata_index.find(meta_key) != metadata_index.end()) {
                            continue;
                        }

                        // For any file that is not referenced by metadata (or tmp files),
                        // protect recently-created files from immediate deletion. This avoids
                        // racing with writers. The grace window is configured by
                        // file_cache_leak_grace_seconds and applies to all orphan files.
                        const int64_t grace_seconds =
                                std::max<int64_t>(0, config::file_cache_leak_grace_seconds);
                        if (grace_seconds > 0) {
                            struct stat st_buf {};
                            if (::stat(file_path.c_str(), &st_buf) != 0) {
                                LOG(WARNING) << "Leak scan failed to stat file " << file_path_str
                                             << ": " << strerror(errno);
                            } else {
                                const std::time_t now = std::time(nullptr);
                                if (now == static_cast<std::time_t>(-1)) {
                                    LOG(WARNING)
                                            << "Leak scan failed to get current time when checking "
                                            << file_path_str;
                                } else {
                                    const int64_t age_seconds =
                                            static_cast<int64_t>(now) -
                                            static_cast<int64_t>(st_buf.st_mtime);
                                    if (age_seconds < grace_seconds) {
                                        VLOG_DEBUG << fmt::format(
                                                "Leak scan skipping young orphan file {} because "
                                                "age={}s < grace={}s",
                                                file_path_str, age_seconds, grace_seconds);
                                        continue;
                                    }
                                }
                            }
                        }

                        candidates.emplace_back(file_path_str, hash, offset,
                                                key_it->path().string());
                        examined_files++;
                        if (candidates.size() >= batch_size) {
                            remove_candidates();
                        }
                    }
                }
            }
            directory_loop_time_ns += loop_once_ns;
        }

        remove_candidates();
    }

    auto ns_to_ms = [](int64_t ns) { return static_cast<double>(ns) / 1'000'000.0; };

    LOG(INFO) << fmt::format(
            "file cache leak cleanup finished: examined_files={}, removed_orphans={}, "
            "wall_time_ms={:.3f}, metadata_hash_time_ms={:.3f}, metadata_index_ms={:.3f}, "
            "remove_candidates_ms={:.3f}, prefix_loop_ms={:.3f}",
            examined_files, removed_files, ns_to_ms(cleanup_wall_time_ns),
            ns_to_ms(metadata_hash_time_ns), ns_to_ms(metadata_index_time_ns),
            ns_to_ms(remove_candidates_time_ns), ns_to_ms(directory_loop_time_ns));
    if (_leak_scan_removed_files) {
        *_leak_scan_removed_files << removed_files;
    }
}

} // namespace doris::io
