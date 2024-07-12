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
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
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
    _cache_base_path = _mgr->_cache_base_path;
    RETURN_IF_ERROR(rebuild_data_structure());
    _cache_background_load_thread = std::thread([this, mgr = _mgr]() {
        load_cache_info_into_memory(mgr);
        mgr->_lazy_open_done = true;
        LOG_INFO("FileCache {} lazy load done.", _cache_base_path);
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
        RETURN_IF_ERROR(fs->open_file(file, &file_reader));
        FDCache::instance()->insert_file_reader(fd_key, file_reader);
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(value_offset, buffer, &bytes_read));
    DCHECK(bytes_read == buffer.get_size());
    return Status::OK();
}

Status FSFileCacheStorage::remove(const FileCacheKey& key) {
    std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
    std::string file = get_path_in_local_cache(dir, key.offset, key.meta.type);
    RETURN_IF_ERROR(fs->delete_file(file));
    std::vector<FileInfo> files;
    bool exists {false};
    RETURN_IF_ERROR(fs->list(dir, true, &files, &exists));
    DCHECK(exists);
    if (files.empty()) {
        RETURN_IF_ERROR(fs->delete_directory(dir));
    }
    FDCache::instance()->remove_file_reader(std::make_pair(key.hash, key.offset));
    return Status::OK();
}

Status FSFileCacheStorage::change_key_meta(const FileCacheKey& key, const KeyMeta& new_meta) {
    // TTL change
    if (key.meta.expiration_time != new_meta.expiration_time) {
        std::string original_dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
        std::string new_dir = get_path_in_local_cache(key.hash, new_meta.expiration_time);
        // It will be concurrent, but we don't care who rename
        Status st = fs->rename(original_dir, new_dir);
        if (!st.ok() && !st.is<ErrorCode::NOT_FOUND>()) {
            return st;
        }
    } else if (key.meta.type != new_meta.type) {
        std::string dir = get_path_in_local_cache(key.hash, key.meta.expiration_time);
        std::string original_file = get_path_in_local_cache(dir, key.offset, key.meta.type);
        std::string new_file = get_path_in_local_cache(dir, key.offset, new_meta.type);
        RETURN_IF_ERROR(fs->rename(original_file, new_file));
    }
    return Status::OK();
}

std::string FSFileCacheStorage::get_path_in_local_cache(const std::string& dir, size_t offset,
                                                        FileCacheType type, bool is_tmp) {
    return Path(dir) / (std::to_string(offset) +
                        (is_tmp ? "_tmp" : BlockFileCache::cache_type_to_string(type)));
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

Status FSFileCacheStorage::rebuild_data_structure() const {
    /// version 1.0: cache_base_path / key / offset
    /// version 2.0: cache_base_path / key_prefix / key / offset
    std::string version;
    RETURN_IF_ERROR(read_file_cache_version(&version));
    if (USE_CACHE_VERSION2 && version != "2.0") {
        // move directories format as version 2.0
        std::error_code ec;
        std::filesystem::directory_iterator key_it {_cache_base_path, ec};
        if (ec) {
            return Status::InternalError("Failed to list dir {}: {}", _cache_base_path,
                                         ec.message());
        }
        for (; key_it != std::filesystem::directory_iterator(); ++key_it) {
            if (key_it->is_directory()) {
                std::string cache_key = key_it->path().filename().native();
                if (cache_key.size() > KEY_PREFIX_LENGTH) {
                    std::string key_prefix =
                            Path(_cache_base_path) / cache_key.substr(0, KEY_PREFIX_LENGTH);
                    bool exists = false;
                    RETURN_IF_ERROR(fs->exists(key_prefix, &exists));
                    if (!exists) {
                        RETURN_IF_ERROR(fs->create_directory(key_prefix));
                    }
                    RETURN_IF_ERROR(fs->rename(key_it->path(), key_prefix / cache_key));
                }
            }
        }
        if (!write_file_cache_version().ok()) {
            return Status::InternalError("Failed to write version hints for file cache");
        }
    }

    auto rebuild_dir = [&](std::filesystem::directory_iterator& upgrade_key_it) -> Status {
        for (; upgrade_key_it != std::filesystem::directory_iterator(); ++upgrade_key_it) {
            if (upgrade_key_it->path().filename().native().find('_') == std::string::npos) {
                RETURN_IF_ERROR(fs->delete_directory(upgrade_key_it->path().native() + "_0"));
                RETURN_IF_ERROR(
                        fs->rename(upgrade_key_it->path(), upgrade_key_it->path().native() + "_0"));
            }
        }
        return Status::OK();
    };
    std::error_code ec;
    if constexpr (USE_CACHE_VERSION2) {
        std::filesystem::directory_iterator key_prefix_it {_cache_base_path, ec};
        if (ec) [[unlikely]] {
            LOG(WARNING) << ec.message();
            return Status::IOError(ec.message());
        }
        for (; key_prefix_it != std::filesystem::directory_iterator(); ++key_prefix_it) {
            if (!key_prefix_it->is_directory()) {
                // maybe version hits file
                continue;
            }
            if (key_prefix_it->path().filename().native().size() != KEY_PREFIX_LENGTH) {
                LOG(WARNING) << "Unknown directory " << key_prefix_it->path().native()
                             << ", try to remove it";
                RETURN_IF_ERROR(fs->delete_directory(key_prefix_it->path()));
            }
            std::filesystem::directory_iterator key_it {key_prefix_it->path(), ec};
            if (ec) [[unlikely]] {
                return Status::IOError(ec.message());
            }
            RETURN_IF_ERROR(rebuild_dir(key_it));
        }
    } else {
        std::filesystem::directory_iterator key_it {_cache_base_path, ec};
        if (ec) [[unlikely]] {
            return Status::IOError(ec.message());
        }
        RETURN_IF_ERROR(rebuild_dir(key_it));
    }
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
    return Status::OK();
}

std::string FSFileCacheStorage::get_version_path() const {
    return Path(_cache_base_path) / "version";
}

void FSFileCacheStorage::load_cache_info_into_memory(BlockFileCache* _mgr) const {
    int scan_length = 10000;
    std::vector<BatchLoadArgs> batch_load_buffer;
    batch_load_buffer.reserve(scan_length);
    auto add_cell_batch_func = [&]() {
        std::lock_guard cache_lock(_mgr->_mutex);
        auto f = [&](const BatchLoadArgs& args) {
            // in async load mode, a cell may be added twice.
            if (_mgr->_files.contains(args.hash) && _mgr->_files[args.hash].contains(args.offset)) {
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
            context.expiration_time = std::stoul(expiration_time_str);
            for (; offset_it != std::filesystem::directory_iterator(); ++offset_it) {
                std::string offset_with_suffix = offset_it->path().filename().native();
                auto delim_pos1 = offset_with_suffix.find('_');
                FileCacheType cache_type = FileCacheType::NORMAL;
                bool parsed = true;
                bool is_tmp = false;
                size_t offset = 0;
                try {
                    if (delim_pos1 == std::string::npos) {
                        // same as type "normal"
                        offset = stoull(offset_with_suffix);
                    } else {
                        offset = stoull(offset_with_suffix.substr(0, delim_pos1));
                        std::string suffix = offset_with_suffix.substr(delim_pos1 + 1);
                        // not need persistent anymore
                        // if suffix is equals to "tmp", it should be removed too.
                        if (suffix == "tmp") [[unlikely]] {
                            is_tmp = true;
                        } else {
                            cache_type = BlockFileCache::string_to_cache_type(suffix);
                        }
                    }
                } catch (...) {
                    parsed = false;
                }

                if (!parsed) {
                    LOG(WARNING) << "parse offset err, path=" << offset_it->path().native();
                    continue;
                }
                TEST_SYNC_POINT_CALLBACK("BlockFileCache::REMOVE_FILE_2", &offset_with_suffix);
                size_t size = offset_it->file_size(ec);
                if (ec) {
                    LOG(WARNING) << "failed to file_size: file_name=" << offset_with_suffix
                                 << "error=" << ec.message();
                    continue;
                }

                if (size == 0 && !is_tmp) {
                    auto st = fs->delete_file(offset_it->path());
                    if (!st.ok()) {
                        LOG_WARNING("delete file {} error", offset_it->path().native()).error(st);
                    }
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
        std::filesystem::directory_iterator key_prefix_it {_cache_base_path, ec};
        if (ec) {
            LOG(WARNING) << ec.message();
            return;
        }
        for (; key_prefix_it != std::filesystem::directory_iterator(); ++key_prefix_it) {
            if (!key_prefix_it->is_directory()) {
                // maybe version hits file
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
        uint64_t offset = 0;
        std::string offset_with_suffix = check_it->path().filename().native();
        auto delim_pos1 = offset_with_suffix.find('_');
        FileCacheType cache_type = FileCacheType::NORMAL;
        bool parsed = true;
        bool is_tmp = false;
        try {
            if (delim_pos1 == std::string::npos) {
                // same as type "normal"
                offset = stoull(offset_with_suffix);
            } else {
                offset = stoull(offset_with_suffix.substr(0, delim_pos1));
                std::string suffix = offset_with_suffix.substr(delim_pos1 + 1);
                if (suffix == "tmp") [[unlikely]] {
                    is_tmp = true;
                } else {
                    cache_type = BlockFileCache::string_to_cache_type(suffix);
                }
            }
        } catch (...) {
            parsed = false;
        }

        if (!parsed) [[unlikely]] {
            LOG(WARNING) << "parse offset err, path=" << offset_with_suffix;
            continue;
        }

        TEST_SYNC_POINT_CALLBACK("BlockFileCache::REMOVE_FILE_1", &offset_with_suffix);
        std::error_code ec;
        size_t size = check_it->file_size(ec);
        if (ec) {
            LOG(WARNING) << "failed to file_size: error=" << ec.message();
            continue;
        }
        if (size == 0 && !is_tmp) [[unlikely]] {
            auto st = fs->delete_file(check_it->path());
            if (!st.ok()) {
                LOG_WARNING("Failed to delete file {}", check_it->path().native()).error(st);
            }
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

FSFileCacheStorage::~FSFileCacheStorage() {
    if (_cache_background_load_thread.joinable()) {
        _cache_background_load_thread.join();
    }
}

} // namespace doris::io
