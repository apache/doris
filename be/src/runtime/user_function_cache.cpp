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

#include "runtime/user_function_cache.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <regex>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "http/http_client.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/dynamic_util.h"
#include "util/md5.h"
#include "util/spinlock.h"
#include "util/string_util.h"

namespace doris {

static const int kLibShardNum = 128;

// function cache entry, store information for
struct UserFunctionCacheEntry {
    ENABLE_FACTORY_CREATOR(UserFunctionCacheEntry);
    UserFunctionCacheEntry(int64_t fid_, const std::string& checksum_, const std::string& lib_file_,
                           LibType type)
            : function_id(fid_), checksum(checksum_), lib_file(lib_file_), type(type) {}
    ~UserFunctionCacheEntry();

    std::string debug_string() {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg,
                       " the info of UserFunctionCacheEntry save in BE, function_id:{}, "
                       "checksum:{}, lib_file:{}, is_downloaded:{}. ",
                       function_id, checksum, lib_file, is_downloaded);
        return fmt::to_string(error_msg);
    }

    int64_t function_id = 0;
    // used to check if this library is valid.
    std::string checksum;

    // library file
    std::string lib_file;

    // make it atomic variable instead of holding a lock
    std::atomic<bool> is_loaded {false};

    // Set to true when this library is not needed.
    // e.g. deleting some unused library to re
    std::atomic<bool> should_delete_library {false};

    // lock to make sure only one can load this cache
    std::mutex load_lock;

    // To reduce cache lock held time, cache entry is
    // added to cache map before library is downloaded.
    // And this is used to indicate whether library is downloaded.
    bool is_downloaded = false;

    // used to lookup a symbol
    void* lib_handle = nullptr;

    SpinLock map_lock;
    // from symbol_name to function pointer
    std::unordered_map<std::string, void*> fptr_map;

    LibType type;
};

UserFunctionCacheEntry::~UserFunctionCacheEntry() {
    // close lib_handle if it was opened
    if (lib_handle != nullptr) {
        dynamic_close(lib_handle);
        lib_handle = nullptr;
    }

    // delete library file if should_delete_library is set
    if (should_delete_library.load()) {
        unlink(lib_file.c_str());
    }
}

UserFunctionCache::UserFunctionCache() = default;

UserFunctionCache::~UserFunctionCache() {
    std::lock_guard<std::mutex> l(_cache_lock);
    auto it = _entry_map.begin();
    while (it != _entry_map.end()) {
        auto entry = it->second;
        it = _entry_map.erase(it);
    }
}

UserFunctionCache* UserFunctionCache::instance() {
    return ExecEnv::GetInstance()->user_function_cache();
}

Status UserFunctionCache::init(const std::string& lib_dir) {
#ifndef BE_TEST
    // _lib_dir may be reused between unit tests
    DCHECK(_lib_dir.empty()) << _lib_dir;
#endif
    _lib_dir = lib_dir;
    // 1. dynamic open current process
    RETURN_IF_ERROR(dynamic_open(nullptr, &_current_process_handle));
    // 2. load all cached
    RETURN_IF_ERROR(_load_cached_lib());
    return Status::OK();
}

Status UserFunctionCache::_load_entry_from_lib(const std::string& dir, const std::string& file) {
    LibType lib_type;
    if (ends_with(file, ".so")) {
        lib_type = LibType::SO;
    } else if (ends_with(file, ".jar")) {
        lib_type = LibType::JAR;
    } else {
        return Status::InternalError(
                "unknown library file format. the file type is not end with xxx.jar or xxx.so : " +
                file);
    }

    std::vector<std::string> split_parts = _split_string_by_checksum(file);
    if (split_parts.size() != 3 && split_parts.size() != 4) {
        return Status::InternalError(
                "user function's name should be function_id.checksum[.file_name].file_type, now "
                "the all split parts are by delimiter(.): " +
                file);
    }
    int64_t function_id = std::stol(split_parts[0]);
    std::string checksum = split_parts[1];
    auto it = _entry_map.find(function_id);
    if (it != _entry_map.end()) {
        LOG(WARNING) << "meet a same function id user function library, function_id=" << function_id
                     << ", one_checksum=" << checksum
                     << ", other_checksum info: = " << it->second->debug_string();
        return Status::InternalError("duplicate function id");
    }
    // create a cache entry and put it into entry map
    std::shared_ptr<UserFunctionCacheEntry> entry = UserFunctionCacheEntry::create_shared(
            function_id, checksum, dir + "/" + file, lib_type);
    entry->is_downloaded = true;
    _entry_map[function_id] = entry;

    return Status::OK();
}

Status UserFunctionCache::_load_cached_lib() {
    // create library directory if not exist
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_lib_dir));

    for (int i = 0; i < kLibShardNum; ++i) {
        std::string sub_dir = _lib_dir + "/" + std::to_string(i);
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(sub_dir));

        auto scan_cb = [this, &sub_dir](const io::FileInfo& file) {
            if (!file.is_file) {
                return true;
            }
            auto st = _load_entry_from_lib(sub_dir, file.file_name);
            if (!st.ok()) {
                LOG(WARNING) << "load a library failed, dir=" << sub_dir
                             << ", file=" << file.file_name << ": " << st.to_string();
            }
            return true;
        };
        RETURN_IF_ERROR(io::global_local_filesystem()->iterate_directory(sub_dir, scan_cb));
    }
    return Status::OK();
}

Status UserFunctionCache::_get_cache_entry(int64_t fid, const std::string& url,
                                           const std::string& checksum,
                                           std::shared_ptr<UserFunctionCacheEntry>& output_entry,
                                           LibType type) {
    std::shared_ptr<UserFunctionCacheEntry> entry = nullptr;
    std::string file_name = _get_file_name_from_url(url);
    {
        std::lock_guard<std::mutex> l(_cache_lock);
        auto it = _entry_map.find(fid);
        if (it != _entry_map.end()) {
            entry = it->second;
        } else {
            entry = UserFunctionCacheEntry::create_shared(
                    fid, checksum, _make_lib_file(fid, checksum, type, file_name), type);
            _entry_map.emplace(fid, entry);
        }
    }
    auto st = _load_cache_entry(url, entry);
    if (!st.ok()) {
        LOG(WARNING) << "fail to load cache entry, fid=" << fid << " " << file_name << " " << url;
        // if we load a cache entry failed, I think we should delete this entry cache
        // even if this cache was valid before.
        _destroy_cache_entry(entry);
        return st;
    }

    output_entry = entry;
    return Status::OK();
}

void UserFunctionCache::_destroy_cache_entry(std::shared_ptr<UserFunctionCacheEntry> entry) {
    // 1. we remove cache entry from entry map
    std::lock_guard<std::mutex> l(_cache_lock);
    // set should delete flag to true, so that the jar file will be removed when
    // the entry is removed from map, and deconstruct method is called.
    entry->should_delete_library.store(true);
    _entry_map.erase(entry->function_id);
}

Status UserFunctionCache::_load_cache_entry(const std::string& url,
                                            std::shared_ptr<UserFunctionCacheEntry> entry) {
    if (entry->is_loaded.load()) {
        return Status::OK();
    }

    std::unique_lock<std::mutex> l(entry->load_lock);
    if (!entry->is_downloaded) {
        RETURN_IF_ERROR(_download_lib(url, entry));
    }

    if (entry->type == LibType::SO) {
        RETURN_IF_ERROR(_load_cache_entry_internal(entry));
    } else if (entry->type != LibType::JAR) {
        return Status::InvalidArgument(
                "Unsupported lib type! Make sure your lib type is one of 'so' and 'jar'!");
    }
    return Status::OK();
}

// entry's lock must be held
Status UserFunctionCache::_download_lib(const std::string& url,
                                        std::shared_ptr<UserFunctionCacheEntry> entry) {
    DCHECK(!entry->is_downloaded);

    // get local path to save library
    std::string tmp_file = entry->lib_file + ".tmp";
    auto fp_closer = [](FILE* fp) { fclose(fp); };
    std::unique_ptr<FILE, decltype(fp_closer)> fp(fopen(tmp_file.c_str(), "w"), fp_closer);
    if (fp == nullptr) {
        LOG(WARNING) << "fail to open file, file=" << tmp_file;
        return Status::InternalError("fail to open file");
    }

    std::string real_url = _get_real_url(url);

    Md5Digest digest;
    HttpClient client;
    int64_t file_size = 0;
    RETURN_IF_ERROR(client.init(real_url));
    Status status;
    auto download_cb = [&status, &tmp_file, &fp, &digest, &file_size](const void* data,
                                                                      size_t length) {
        digest.update(data, length);
        file_size = file_size + length;
        auto res = fwrite(data, length, 1, fp.get());
        if (res != 1) {
            LOG(WARNING) << "fail to write data to file, file=" << tmp_file
                         << ", error=" << ferror(fp.get());
            status = Status::InternalError("fail to write data when download");
            return false;
        }
        return true;
    };
    RETURN_IF_ERROR(client.execute(download_cb));
    RETURN_IF_ERROR(status);
    digest.digest();
    if (!iequal(digest.hex(), entry->checksum)) {
        fmt::memory_buffer error_msg;
        fmt::format_to(
                error_msg,
                " The checksum is not equal of {} ({}). The init info of first create entry is:"
                "{} But download file check_sum is: {}, file_size is: {}.",
                url, real_url, entry->debug_string(), digest.hex(), file_size);
        std::string error(fmt::to_string(error_msg));
        LOG(WARNING) << error;
        return Status::InternalError(error);
    }
    // close this file
    fp.reset();

    // rename temporary file to library file
    auto ret = rename(tmp_file.c_str(), entry->lib_file.c_str());
    if (ret != 0) {
        char buf[64];
        LOG(WARNING) << "fail to rename file from=" << tmp_file << ", to=" << entry->lib_file
                     << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        return Status::InternalError("fail to rename file");
    }

    // check download
    entry->is_downloaded = true;
    return Status::OK();
}

std::string UserFunctionCache::_get_real_url(const std::string& url) {
    if (url.find(":/") == std::string::npos) {
        return "file://" + config::jdbc_drivers_dir + "/" + url;
    }
    return url;
}

std::string UserFunctionCache::_get_file_name_from_url(const std::string& url) const {
    std::string file_name;
    size_t last_slash_pos = url.find_last_of('/');
    if (last_slash_pos != std::string::npos) {
        file_name = url.substr(last_slash_pos + 1, url.size());
    } else {
        file_name = url;
    }
    return file_name;
}

// entry's lock must be held
Status UserFunctionCache::_load_cache_entry_internal(
        std::shared_ptr<UserFunctionCacheEntry> entry) {
    RETURN_IF_ERROR(dynamic_open(entry->lib_file.c_str(), &entry->lib_handle));
    entry->is_loaded.store(true);
    return Status::OK();
}

std::string UserFunctionCache::_make_lib_file(int64_t function_id, const std::string& checksum,
                                              LibType type, const std::string& file_name) {
    int shard = function_id % kLibShardNum;
    std::stringstream ss;
    ss << _lib_dir << '/' << shard << '/' << function_id << '.' << checksum;
    if (type == LibType::JAR) {
        ss << '.' << file_name;
    } else {
        ss << ".so";
    }
    return ss.str();
}

Status UserFunctionCache::get_jarpath(int64_t fid, const std::string& url,
                                      const std::string& checksum, std::string* libpath) {
    std::shared_ptr<UserFunctionCacheEntry> entry = nullptr;
    RETURN_IF_ERROR(_get_cache_entry(fid, url, checksum, entry, LibType::JAR));
    *libpath = entry->lib_file;
    return Status::OK();
}

std::vector<std::string> UserFunctionCache::_split_string_by_checksum(const std::string& file) {
    std::vector<std::string> result;

    // Find the first dot from the start
    size_t firstDot = file.find('.');
    if (firstDot == std::string::npos) return {};

    // Find the second dot starting from the first dot's position
    size_t secondDot = file.find('.', firstDot + 1);
    if (secondDot == std::string::npos) return {};

    // Find the last dot from the end
    size_t lastDot = file.rfind('.');
    if (lastDot == std::string::npos || lastDot <= secondDot) return {};

    // Split based on these dots
    result.push_back(file.substr(0, firstDot));
    result.push_back(file.substr(firstDot + 1, secondDot - firstDot - 1));
    result.push_back(file.substr(secondDot + 1, lastDot - secondDot - 1));
    result.push_back(file.substr(lastDot + 1));

    return result;
}
} // namespace doris
