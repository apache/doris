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

#include <filesystem>
#include <mutex>
#include <system_error>

#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "vec/common/hex.h"

namespace doris::io {

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

Status FSFileCacheStorage::init(BlockFileCache* _mgr) {
    _iterator_dir_retry_cnt = std::make_shared<bvar::LatencyRecorder>(
            _cache_base_path.c_str(), "file_cache_fs_storage_iterator_dir_retry_cnt");
    _cache_base_path = _mgr->_cache_base_path;
    _cache_background_load_thread = std::thread([this, mgr = _mgr]() {
        auto mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                            fmt::format("FileCacheVersionReader"));
        SCOPED_ATTACH_TASK(mem_tracker);
        Status st = upgrade_cache_dir_if_necessary();
        if (!st.ok()) {
            std::string msg = fmt::format(
                    "file cache {} upgrade done with error. upgrade version failed. st={}",
                    _cache_base_path, st.to_string());
            if (doris::config::ignore_file_cache_dir_upgrade_failure) {
                LOG(WARNING) << msg << " be conf: `ignore_file_cache_dir_upgrade_failure = true`"
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
            std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
            auto st = fs->create_directory(dir, false);
            if (!st.ok() && !st.is<ErrorCode::ALREADY_EXIST>()) {
                return st;
            }
            std::string tmp_file = get_path_in_local_cache(dir, key.offset, key.meta.type, true);
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

Status FSFileCacheStorage::finalize(const FileCacheKey& key) {
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
    std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
    std::string true_file = get_path_in_local_cache(dir, key.offset, key.meta.type);
    return fs->rename(file_writer->path(), true_file);
}

Status FSFileCacheStorage::read(const FileCacheKey& key, size_t value_offset, Slice buffer) {
    AccessKeyAndOffset fd_key = std::make_pair(key.hash, key.offset);
    FileReaderSPtr file_reader = FDCache::instance()->get_file_reader(fd_key);
    if (!file_reader) {
        std::string file =
                get_path_in_local_cache(get_path_in_local_cache(key.hash, key.meta.expiration_time),
                                        key.offset, key.meta.type);
        Status s = fs->open_file(file, &file_reader);

        // handle the case that the file is not found but actually exists in other type format
        // TODO(zhengyu): nasty! better eliminate the type encoding in file name in the future
        if (!s.ok() && !s.is<ErrorCode::NOT_FOUND>()) {
            LOG(WARNING) << "open file failed, file=" << file << ", error=" << s.to_string();
            return s;                                         // return other error directly
        } else if (!s.ok() && s.is<ErrorCode::NOT_FOUND>()) { // but handle NOT_FOUND error
            auto candidates = get_path_in_local_cache_all_candidates(
                    get_path_in_local_cache(key.hash, key.meta.expiration_time), key.offset);
            for (auto& candidate : candidates) {
                s = fs->open_file(candidate, &file_reader);
                if (s.ok()) {
                    break; // success with one of there candidates
                }
            }
            if (!s.ok()) { // still not found, return error
                LOG(WARNING) << "open file failed, file=" << file << ", error=" << s.to_string();
                return s;
            }
        } // else, s.ok() means open file success

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
    std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
    std::string file = get_path_in_local_cache(dir, key.offset, key.meta.type);
    FDCache::instance()->remove_file_reader(std::make_pair(key.hash, key.offset));
    RETURN_IF_ERROR(fs->delete_file(file));
    // return OK not means the file is deleted, it may be not exist
    // So for TTL, we make sure the old format will be removed well
    if (key.meta.type == FileCacheType::TTL) {
        bool exists {false};
        // try to detect the file with old ttl format
        file = get_path_in_local_cache_old_ttl_format(dir, key.offset, key.meta.type);
        RETURN_IF_ERROR(fs->exists(file, &exists));
        if (exists) {
            VLOG(7) << "try to remove the file with old ttl format"
                    << " file=" << file;
            RETURN_IF_ERROR(fs->delete_file(file));
        }
    }
    std::vector<FileInfo> files;
    bool exists {false};
    RETURN_IF_ERROR(fs->list(dir, true, &files, &exists));
    if (files.empty()) {
        RETURN_IF_ERROR(fs->delete_directory(dir));
    }
    return Status::OK();
}

Status FSFileCacheStorage::change_key_meta_type(const FileCacheKey& key, const FileCacheType type) {
    // file operation
    if (key.meta.type != type) {
        // TTL type file dose not need to change the suffix
        bool expr = (key.meta.type != FileCacheType::TTL && type != FileCacheType::TTL);
        if (!expr) {
            LOG(WARNING) << "TTL type file dose not need to change the suffix"
                         << " key=" << key.hash.to_string() << " offset=" << key.offset
                         << " old_type=" << cache_type_to_string(key.meta.type)
                         << " new_type=" << cache_type_to_string(type);
        }
        DCHECK(expr);
        std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
        std::string original_file = get_path_in_local_cache(dir, key.offset, key.meta.type);
        std::string new_file = get_path_in_local_cache(dir, key.offset, type);
        RETURN_IF_ERROR(fs->rename(original_file, new_file));
    }
    return Status::OK();
}

Status FSFileCacheStorage::change_key_meta_expiration(const FileCacheKey& key,
                                                      const uint64_t expiration) {
    // directory operation
    if (key.meta.expiration_time != expiration) {
        std::string original_dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
        std::string new_dir = get_path_in_local_cache(key.hash, expiration);
        // It will be concurrent, but we don't care who rename
        Status st = fs->rename(original_dir, new_dir);
        if (!st.ok() && !st.is<ErrorCode::NOT_FOUND>()) {
            return st;
        }
    }
    return Status::OK();
}

std::string FSFileCacheStorage::get_path_in_local_cache(const std::string& dir, size_t offset,
                                                        FileCacheType type, bool is_tmp) {
    if (is_tmp) {
        return Path(dir) / (std::to_string(offset) + "_tmp");
    } else if (type == FileCacheType::TTL) {
        return Path(dir) / std::to_string(offset);
    } else {
        return Path(dir) / (std::to_string(offset) + cache_type_to_surfix(type));
    }
}

std::string FSFileCacheStorage::get_path_in_local_cache_old_ttl_format(const std::string& dir,
                                                                       size_t offset,
                                                                       FileCacheType type,
                                                                       bool is_tmp) {
    DCHECK(type == FileCacheType::TTL);
    return Path(dir) / (std::to_string(offset) + cache_type_to_surfix(type));
}

std::vector<std::string> FSFileCacheStorage::get_path_in_local_cache_all_candidates(
        const std::string& dir, size_t offset) {
    std::vector<std::string> candidates;
    std::string base = get_path_in_local_cache(dir, offset, FileCacheType::NORMAL);
    candidates.push_back(base);
    candidates.push_back(base + "_idx");
    candidates.push_back(base + "_ttl");
    candidates.push_back(base + "_disposable");
    return candidates;
}

std::string FSFileCacheStorage::get_path_in_local_cache(const UInt128Wrapper& value,
                                                        uint64_t expiration_time) const {
    auto str = value.to_string();
    try {
        if constexpr (USE_CACHE_VERSION2) {
            return Path(_cache_base_path) / str.substr(0, KEY_PREFIX_LENGTH) /
                   (str + "_" + std::to_string(expiration_time));
        } else {
            return Path(_cache_base_path) / (str + "_" + std::to_string(expiration_time));
        }
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
    /*
     * If use version2 but was version 1, do upgrade:
     *
     * Action I:
     *     version 1.0: cache_base_path / key / offset
     *     version 2.0: cache_base_path / key_prefix / key / offset
     *
     * Action II:
     *     add '_0' to hash dir
     *
     * Note: This is a sync operation with tons of IOs, so it may affect BE
     * boot time heavily. Fortunately, Action I & II will only happen when
     * upgrading (once in the cluster life time).
     */

    std::string version;
    std::error_code ec;
    int rename_count = 0;
    int failure_count = 0;
    auto start_time = std::chrono::steady_clock::now();

    RETURN_IF_ERROR(read_file_cache_version(&version));

    LOG(INFO) << "Checking cache version upgrade. Current version: " << version
              << ", target version: 2.0, need upgrade: "
              << (USE_CACHE_VERSION2 && version != "2.0");
    if (USE_CACHE_VERSION2 && version != "2.0") {
        // move directories format as version 2.0
        std::vector<std::string> file_list;
        file_list.reserve(10000);
        RETURN_IF_ERROR(collect_directory_entries(_cache_base_path, file_list));

        // this directory_iterator should be a problem in concurrent access
        for (const auto& file_path : file_list) {
            try {
                if (std::filesystem::is_directory(file_path)) {
                    std::string cache_key = std::filesystem::path(file_path).filename().native();
                    if (cache_key.size() > KEY_PREFIX_LENGTH) {
                        if (cache_key.find('_') == std::string::npos) {
                            cache_key += "_0";
                        }
                        std::string key_prefix =
                                Path(_cache_base_path) / cache_key.substr(0, KEY_PREFIX_LENGTH);
                        bool exists = false;
                        auto exists_status = fs->exists(key_prefix, &exists);
                        if (!exists_status.ok()) {
                            LOG(WARNING) << "Failed to check directory existence: " << key_prefix
                                         << ", error: " << exists_status.to_string();
                            ++failure_count;
                            continue;
                        }
                        if (!exists) {
                            auto create_status = fs->create_directory(key_prefix);
                            if (!create_status.ok() &&
                                create_status.code() != TStatusCode::type::ALREADY_EXIST) {
                                LOG(WARNING) << "Failed to create directory: " << key_prefix
                                             << ", error: " << create_status.to_string();
                                ++failure_count;
                                continue;
                            }
                        }
                        auto rename_status = Status::OK();
                        const std::string new_file_path = key_prefix + "/" + cache_key;
                        TEST_SYNC_POINT_CALLBACK(
                                "FSFileCacheStorage::upgrade_cache_dir_if_necessary_rename",
                                &file_path, &new_file_path);
                        rename_status = fs->rename(file_path, new_file_path);
                        if (rename_status.ok() ||
                            rename_status.code() == TStatusCode::type::DIRECTORY_NOT_EMPTY) {
                            ++rename_count;
                        } else {
                            LOG(WARNING)
                                    << "Failed to rename directory from " << file_path << " to "
                                    << new_file_path << ", error: " << rename_status.to_string();
                            ++failure_count;
                            continue;
                        }
                    }
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "Error occurred while upgrading file cache directory: " << file_path
                             << " err: " << e.what();
                ++failure_count;
            }
        }

        std::vector<std::string> rebuilt_file_list;
        rebuilt_file_list.reserve(10000);
        RETURN_IF_ERROR(collect_directory_entries(_cache_base_path, rebuilt_file_list));

        for (const auto& key_it : rebuilt_file_list) {
            if (!std::filesystem::is_directory(key_it)) {
                // maybe version hits file
                continue;
            }
            try {
                if (Path(key_it).filename().native().size() != KEY_PREFIX_LENGTH) {
                    LOG(WARNING) << "Unknown directory " << key_it << ", try to remove it";
                    auto delete_status = fs->delete_directory(key_it);
                    if (!delete_status.ok()) {
                        LOG(WARNING) << "Failed to delete unknown directory: " << key_it
                                     << ", error: " << delete_status.to_string();
                        ++failure_count;
                        continue;
                    }
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "Error occurred while upgrading file cache directory: " << key_it
                             << " err: " << e.what();
                ++failure_count;
            }
        }
        if (auto st = write_file_cache_version(); !st.ok()) {
            return Status::InternalError("Failed to write version hints for file cache, err={}",
                                         st.to_string());
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG(INFO) << "Cache directory upgrade completed. Total files renamed: " << rename_count
              << ", Time taken: " << duration.count() << "ms"
              << ", Failure count: " << failure_count;
    return Status::OK();
}

Status FSFileCacheStorage::write_file_cache_version() const {
    if constexpr (USE_CACHE_VERSION2) {
        std::string version_path = get_version_path();
        Slice version("2.0");
        FileWriterPtr version_writer;
        RETURN_IF_ERROR(fs->create_file(version_path, &version_writer));
        RETURN_IF_ERROR(version_writer->append(version));
        return version_writer->close();
    }
    return Status::OK();
}

Status FSFileCacheStorage::read_file_cache_version(std::string* buffer) const {
    std::string version_path = get_version_path();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(version_path, &exists));
    if (!exists) {
        *buffer = "1.0";
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
    auto st = Status::OK();
    TEST_SYNC_POINT_CALLBACK("FSFileCacheStorage::read_file_cache_version", &st);
    return st;
}

std::string FSFileCacheStorage::get_version_path() const {
    return Path(_cache_base_path) / "version";
}

Status FSFileCacheStorage::parse_filename_suffix_to_cache_type(
        const std::shared_ptr<LocalFileSystem>& fs, const Path& file_path, long expiration_time,
        size_t size, size_t* offset, bool* is_tmp, FileCacheType* cache_type) const {
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
        auto st = fs->delete_file(file_path);
        if (!st.ok()) {
            LOG_WARNING("delete file {} error", file_path.native()).error(st);
        }
        return Status::InternalError("file size is 0, file_name={}", offset_with_suffix);
    }
    return Status::OK();
}

void FSFileCacheStorage::load_cache_info_into_memory(BlockFileCache* _mgr) const {
    int scan_length = 10000;
    std::vector<BatchLoadArgs> batch_load_buffer;
    batch_load_buffer.reserve(scan_length);
    auto add_cell_batch_func = [&]() {
        SCOPED_CACHE_LOCK(_mgr->_mutex, _mgr);

        auto f = [&](const BatchLoadArgs& args) {
            // in async load mode, a cell may be added twice.
            if (_mgr->_files.contains(args.hash) && _mgr->_files[args.hash].contains(args.offset)) {
                // TODO(zhengyu): update type&expiration if need
                return;
            }
            // if the file is tmp, it means it is the old file and it should be removed
            if (!args.is_tmp) {
                _mgr->add_cell(args.hash, args.ctx, args.offset, args.size,
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
            DCHECK(delim_pos != std::string::npos);
            std::string key_str = key_with_suffix.substr(0, delim_pos);
            std::string expiration_time_str = key_with_suffix.substr(delim_pos + 1);
            auto hash = UInt128Wrapper(vectorized::unhex_uint<uint128_t>(key_str.c_str()));
            std::error_code ec;
            std::filesystem::directory_iterator offset_it(key_it->path(), ec);
            if (ec) [[unlikely]] {
                LOG(WARNING) << "filesystem error, failed to remove file, file=" << key_it->path()
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
    if constexpr (USE_CACHE_VERSION2) {
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
            if (key_prefix_it->path().filename().native().size() != KEY_PREFIX_LENGTH) {
                LOG(WARNING) << "Unknown directory " << key_prefix_it->path().native()
                             << ", try to remove it";
                std::error_code ec;
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
    } else {
        std::filesystem::directory_iterator key_it {_cache_base_path, ec};
        if (ec) {
            LOG(WARNING) << ec.message();
            return;
        }
        scan_file_cache(key_it);
    }
    if (!batch_load_buffer.empty()) {
        add_cell_batch_func();
    }
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::TmpFile2");
}

void FSFileCacheStorage::load_blocks_directly_unlocked(BlockFileCache* mgr, const FileCacheKey& key,
                                                       std::lock_guard<std::mutex>& cache_lock) {
    // async load, can't find key, need to check exist.
    auto key_path = get_path_in_local_cache(key.hash, key.meta.expiration_time);
    bool exists = false;
    auto st = fs->exists(key_path, &exists);
    if (auto st = fs->exists(key_path, &exists); !exists && st.ok()) {
        // cache miss
        return;
    } else if (!st.ok()) [[unlikely]] {
        LOG_WARNING("failed to exists file {}", key_path).error(st);
        return;
    }

    CacheContext context_original;
    context_original.query_id = TUniqueId();
    context_original.expiration_time = key.meta.expiration_time;
    std::error_code ec;
    std::filesystem::directory_iterator check_it(key_path, ec);
    if (ec) [[unlikely]] {
        LOG(WARNING) << "fail to directory_iterator " << ec.message();
        return;
    }
    for (; check_it != std::filesystem::directory_iterator(); ++check_it) {
        size_t size = check_it->file_size(ec);
        size_t offset = 0;
        bool is_tmp = false;
        FileCacheType cache_type = FileCacheType::NORMAL;
        if (!parse_filename_suffix_to_cache_type(fs, check_it->path().filename().native(),
                                                 context_original.expiration_time, size, &offset,
                                                 &is_tmp, &cache_type)) {
            continue;
        }
        if (!mgr->_files.contains(key.hash) || !mgr->_files[key.hash].contains(offset)) {
            // if the file is tmp, it means it is the old file and it should be removed
            if (is_tmp) {
                std::error_code ec;
                std::filesystem::remove(check_it->path(), ec);
                if (ec) {
                    LOG(WARNING) << fmt::format("cannot remove {}: {}", check_it->path().native(),
                                                ec.message());
                }
            } else {
                context_original.cache_type = cache_type;
                mgr->add_cell(key.hash, context_original, offset, size,
                              FileBlock::State::DOWNLOADED, cache_lock);
            }
        }
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
        ++total;
        std::string cache_key = key_it->path().string();
        auto st = global_local_filesystem()->delete_directory(cache_key);
        if (st.ok()) continue;
        failed++;
        LOG(WARNING) << "failed to clear base_path=" << _cache_base_path
                     << " path_to_delete=" << cache_key << " error=" << st;
    }
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
    return get_path_in_local_cache(get_path_in_local_cache(key.hash, key.meta.expiration_time),
                                   key.offset, key.meta.type, false);
}

FSFileCacheStorage::~FSFileCacheStorage() {
    if (_cache_background_load_thread.joinable()) {
        _cache_background_load_thread.join();
    }
}

} // namespace doris::io
