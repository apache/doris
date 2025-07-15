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

#include <stdint.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "common/factory_creator.h"
#include "common/status.h"

namespace doris {

struct UserFunctionCacheEntry;

// Used to cache a user function. Theses functions include
// UDF(User Defined Function) and UDAF(User Defined Aggregate
// Function), and maybe include UDTF(User Defined Table
// Function) in future. A user defined function may be splitted
// into several functions, for example, UDAF is splitted into
// InitFn, MergeFn, FinalizeFn...
// In Doris, we call UDF/UDAF/UDTF UserFunction, and we call
// implement function Function.
// An UserFunction have a function id, we can find library with
// this id. When we add user function into cache, we need to
// download from URL and check its checksum. So if we find a function
// with id, this function library is valid. And when user wants to
// change its implementation(URL), Doris will generate a new function
// id.
enum class LibType { JAR, SO };

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

    // from symbol_name to function pointer
    std::unordered_map<std::string, void*> fptr_map;

    LibType type;
};

class UserFunctionCache {
public:
    // local_dir is the directory which contain cached library.
    UserFunctionCache();
    ~UserFunctionCache();

    // initialize this cache, call this function before others
    Status init(const std::string& local_path);

    static UserFunctionCache* instance();

    Status get_jarpath(int64_t fid, const std::string& url, const std::string& checksum,
                       std::string* libpath);

private:
    Status _load_cached_lib();
    Status _load_entry_from_lib(const std::string& dir, const std::string& file_name);

    Status _get_cache_entry(int64_t fid, const std::string& url, const std::string& checksum,
                            std::string* libpath, LibType type);
    Status _load_cache_entry(const std::string& url, std::shared_ptr<UserFunctionCacheEntry> entry);
    Status _download_lib(const std::string& url, std::shared_ptr<UserFunctionCacheEntry> entry);

    std::string _make_lib_file(int64_t function_id, const std::string& checksum, LibType type,
                               const std::string& file_name);
    void _destroy_cache_entry(std::shared_ptr<UserFunctionCacheEntry> entry);

    std::string _get_file_name_from_url(const std::string& url, LibType type) const;
    std::vector<std::string> _split_string_by_checksum(const std::string& file);

private:
    std::string _lib_dir;
    void* _current_process_handle = nullptr;

    std::mutex _cache_lock;
    std::unordered_map<int64_t, std::shared_ptr<UserFunctionCacheEntry>> _entry_map;
};

} // namespace doris
