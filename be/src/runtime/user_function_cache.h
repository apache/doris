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

#include <mutex>
#include <string>
#include <unordered_map>

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
enum class LibType {
    JAR,
    SO
};

class UserFunctionCache {
public:
    // local_dir is the directory which contain cached library.
    UserFunctionCache();
    ~UserFunctionCache();

    // initialize this cache, call this function before others
    Status init(const std::string& local_path);

    static UserFunctionCache* instance();

    // Return function pointer for given fid and symbol.
    // If fid is 0, lookup symbol from this doris-be process.
    // Otherwise find symbol in UserFunction's library.
    // Found function pointer is returned in fn_ptr, and cache entry
    // is returned by entry. Client must call release_entry to release
    // cache entry if didn't need it.
    // If *entry is not true means that we should find symbol in this
    // entry.
    Status get_function_ptr(int64_t fid, const std::string& symbol, const std::string& url,
                            const std::string& checksum, void** fn_ptr,
                            UserFunctionCacheEntry** entry);
    void release_entry(UserFunctionCacheEntry* entry);

    Status get_jarpath(int64_t fid, const std::string& url, const std::string& checksum, std::string* libpath);

private:
    Status _load_cached_lib();
    Status _load_entry_from_lib(const std::string& dir, const std::string& file);
    Status _get_cache_entry(int64_t fid, const std::string& url, const std::string& checksum,
                            UserFunctionCacheEntry** output_entry, LibType type);
    Status _load_cache_entry(const std::string& url, UserFunctionCacheEntry* entry);
    Status _download_lib(const std::string& url, UserFunctionCacheEntry* entry);
    Status _load_cache_entry_internal(UserFunctionCacheEntry* entry);

    Status _add_to_classpath(UserFunctionCacheEntry* entry);

    std::string _make_lib_file(int64_t function_id, const std::string& checksum, LibType type);
    void _destroy_cache_entry(UserFunctionCacheEntry* entry);

private:
    std::string _lib_dir;
    void* _current_process_handle = nullptr;

    std::mutex _cache_lock;
    std::unordered_map<int64_t, UserFunctionCacheEntry*> _entry_map;
};

} // namespace doris
