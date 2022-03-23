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

#include <boost/algorithm/string/classification.hpp> // boost::is_any_of
#include <boost/algorithm/string/predicate.hpp>      // boost::algorithm::ends_with
#include <atomic>
#include <regex>
#include <vector>

#include "env/env.h"
#include "gutil/strings/split.h"
#include "http/http_client.h"
#include "util/dynamic_util.h"
#include "util/file_utils.h"
#include "util/jni-util.h"
#include "util/md5.h"
#include "util/spinlock.h"

namespace doris {

static const int kLibShardNum = 128;

// function cache entry, store information for
struct UserFunctionCacheEntry {
    UserFunctionCacheEntry(int64_t fid_, const std::string& checksum_, const std::string& lib_file_,
                           LibType type)
            : function_id(fid_), checksum(checksum_), lib_file(lib_file_), type(type) {}
    ~UserFunctionCacheEntry();

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    int64_t function_id = 0;
    // used to check if this library is valid.
    std::string checksum;

    // library file
    std::string lib_file;

    // make it atomic variable instead of holding a lock
    std::atomic<bool> is_loaded{false};

    // Set to true when this library is not needed.
    // e.g. deleting some unused library to re
    std::atomic<bool> should_delete_library{false};

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

private:
    std::atomic<int> _refs{0};
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

UserFunctionCache::UserFunctionCache() {}

UserFunctionCache::~UserFunctionCache() {
    std::lock_guard<std::mutex> l(_cache_lock);
    auto it = _entry_map.begin();
    while (it != _entry_map.end()) {
        auto entry = it->second;
        it = _entry_map.erase(it);
        if (entry->unref()) {
            delete entry;
        }
    }
}

UserFunctionCache* UserFunctionCache::instance() {
    static UserFunctionCache s_cache;
    return &s_cache;
}

Status UserFunctionCache::init(const std::string& lib_dir) {
    DCHECK(_lib_dir.empty());
    _lib_dir = lib_dir;
    // 1. dynamic open current process
    RETURN_IF_ERROR(dynamic_open(nullptr, &_current_process_handle));
    // 2. load all cached
    RETURN_IF_ERROR(_load_cached_lib());
    return Status::OK();
}

Status UserFunctionCache::_load_entry_from_lib(const std::string& dir, const std::string& file) {
    if (!boost::algorithm::ends_with(file, ".so")) {
        return Status::InternalError("unknown library file format");
    }

    std::vector<std::string> split_parts = strings::Split(file, ".");
    if (split_parts.size() != 3) {
        return Status::InternalError("user function's name should be function_id.checksum.so");
    }
    int64_t function_id = std::stol(split_parts[0]);
    std::string checksum = split_parts[1];
    auto it = _entry_map.find(function_id);
    if (it != _entry_map.end()) {
        LOG(WARNING) << "meet a same function id user function library, function_id=" << function_id
                     << ", one_checksum=" << checksum
                     << ", other_checksum=" << it->second->checksum;
        return Status::InternalError("duplicate function id");
    }
    // create a cache entry and put it into entry map
    UserFunctionCacheEntry* entry =
            new UserFunctionCacheEntry(function_id, checksum, dir + "/" + file, LibType::SO);
    entry->is_downloaded = true;

    entry->ref();
    _entry_map[function_id] = entry;

    return Status::OK();
}

Status UserFunctionCache::_load_cached_lib() {
    // create library directory if not exist
    RETURN_IF_ERROR(FileUtils::create_dir(_lib_dir));

    for (int i = 0; i < kLibShardNum; ++i) {
        std::string sub_dir = _lib_dir + "/" + std::to_string(i);
        RETURN_IF_ERROR(FileUtils::create_dir(sub_dir));

        auto scan_cb = [this, &sub_dir](const char* file) {
            if (is_dot_or_dotdot(file)) {
                return true;
            }
            auto st = _load_entry_from_lib(sub_dir, file);
            if (!st.ok()) {
                LOG(WARNING) << "load a library failed, dir=" << sub_dir << ", file=" << file;
            }
            return true;
        };
        RETURN_IF_ERROR(Env::Default()->iterate_dir(sub_dir, scan_cb));
    }
    return Status::OK();
}

std::string get_real_symbol(const std::string& symbol) {
    static std::regex rx1("8palo_udf");
    std::string str1 = std::regex_replace(symbol, rx1, "9doris_udf");
    static std::regex rx2("4palo");
    std::string str2 = std::regex_replace(str1, rx2, "5doris");
    return str2;
}

Status UserFunctionCache::get_function_ptr(int64_t fid, const std::string& orig_symbol,
                                           const std::string& url, const std::string& checksum,
                                           void** fn_ptr, UserFunctionCacheEntry** output_entry) {
    auto symbol = get_real_symbol(orig_symbol);
    if (fid == 0) {
        // Just loading a function ptr in the current process. No need to take any locks.
        RETURN_IF_ERROR(dynamic_lookup(_current_process_handle, symbol.c_str(), fn_ptr));
        return Status::OK();
    }

    // if we need to unref entry
    bool need_unref_entry = false;
    UserFunctionCacheEntry* entry = nullptr;
    // find the library entry for this function. If *output_entry is not null
    // find symbol in it without to get other entry
    if (output_entry != nullptr && *output_entry != nullptr) {
        entry = *output_entry;
    } else {
        RETURN_IF_ERROR(_get_cache_entry(fid, url, checksum, &entry, LibType::SO));
        need_unref_entry = true;
    }

    Status status;
    {
        std::lock_guard<SpinLock> l(entry->map_lock);
        // now, we have the library entry, we need to lock it to find symbol
        auto it = entry->fptr_map.find(symbol);
        if (it != entry->fptr_map.end()) {
            *fn_ptr = it->second;
        } else {
            status = dynamic_lookup(entry->lib_handle, symbol.c_str(), fn_ptr);
            if (status.ok()) {
                entry->fptr_map.emplace(symbol, *fn_ptr);
            } else {
                LOG(WARNING) << "fail to lookup symbol in library, symbol=" << symbol
                             << ", file=" << entry->lib_file;
            }
        }
    }

    if (status.ok() && output_entry != nullptr && *output_entry == nullptr) {
        *output_entry = entry;
        need_unref_entry = false;
    }

    if (need_unref_entry) {
        if (entry->unref()) {
            delete entry;
        }
    }

    return status;
}

Status UserFunctionCache::_get_cache_entry(int64_t fid, const std::string& url,
                                           const std::string& checksum,
                                           UserFunctionCacheEntry** output_entry,
                                           LibType type) {
    UserFunctionCacheEntry* entry = nullptr;
    {
        std::lock_guard<std::mutex> l(_cache_lock);
        auto it = _entry_map.find(fid);
        if (it != _entry_map.end()) {
            entry = it->second;
        } else {
            entry = new UserFunctionCacheEntry(fid, checksum, _make_lib_file(fid, checksum, type), type);

            entry->ref();
            _entry_map.emplace(fid, entry);
        }
        entry->ref();
    }
    auto st = _load_cache_entry(url, entry);
    if (!st.ok()) {
        LOG(WARNING) << "fail to load cache entry, fid=" << fid;
        // if we load a cache entry failed, I think we should delete this entry cache
        // evenif this cache was valid before.
        _destroy_cache_entry(entry);
        return st;
    }

    *output_entry = entry;
    return Status::OK();
}

void UserFunctionCache::_destroy_cache_entry(UserFunctionCacheEntry* entry) {
    // 1. we remove cache entry from entry map
    size_t num_removed = 0;
    {
        std::lock_guard<std::mutex> l(_cache_lock);
        num_removed = _entry_map.erase(entry->function_id);
    }
    if (num_removed > 0) {
        entry->unref();
    }
    entry->should_delete_library.store(true);
    // now we need to drop
    if (entry->unref()) {
        delete entry;
    }
}

Status UserFunctionCache::_load_cache_entry(const std::string& url, UserFunctionCacheEntry* entry) {
    if (entry->is_loaded.load()) {
        return Status::OK();
    }

    std::unique_lock<std::mutex> l(entry->load_lock);
    if (!entry->is_downloaded) {
        RETURN_IF_ERROR(_download_lib(url, entry));
    }

    if (entry->type == LibType::SO) {
        RETURN_IF_ERROR(_load_cache_entry_internal(entry));
    } else if (entry->type == LibType::JAR) {
        RETURN_IF_ERROR(_add_to_classpath(entry));
    } else {
        return Status::InvalidArgument(
                "Unsupported lib type! Make sure your lib type is one of 'so' and 'jar'!");
    }
    return Status::OK();
}

// entry's lock must be held
Status UserFunctionCache::_download_lib(const std::string& url, UserFunctionCacheEntry* entry) {
    DCHECK(!entry->is_downloaded);

    // get local path to save library
    std::string tmp_file = entry->lib_file + ".tmp";
    auto fp_closer = [](FILE* fp) { fclose(fp); };
    std::unique_ptr<FILE, decltype(fp_closer)> fp(fopen(tmp_file.c_str(), "w"), fp_closer);
    if (fp == nullptr) {
        LOG(WARNING) << "fail to open file, file=" << tmp_file;
        return Status::InternalError("fail to open file");
    }

    Md5Digest digest;
    HttpClient client;
    RETURN_IF_ERROR(client.init(url));
    Status status;
    auto download_cb = [&status, &tmp_file, &fp, &digest](const void* data, size_t length) {
        digest.update(data, length);
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
    if (!boost::iequals(digest.hex(), entry->checksum)) {
        LOG(WARNING) << "UDF's checksum is not equal, one=" << digest.hex()
                     << ", other=" << entry->checksum;
        return Status::InternalError("UDF's library checksum is not match");
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

// entry's lock must be held
Status UserFunctionCache::_load_cache_entry_internal(UserFunctionCacheEntry* entry) {
    RETURN_IF_ERROR(dynamic_open(entry->lib_file.c_str(), &entry->lib_handle));
    entry->is_loaded.store(true);
    return Status::OK();
}

Status UserFunctionCache::_add_to_classpath(UserFunctionCacheEntry* entry) {
#ifdef LIBJVM
    const std::string path = "file://" + entry->lib_file;
    LOG(INFO) << "Add jar " << path << " to classpath";
    JNIEnv* env;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    jclass class_class_loader = env->FindClass("java/lang/ClassLoader");
    jmethodID method_get_system_class_loader =
            env->GetStaticMethodID(class_class_loader, "getSystemClassLoader", "()Ljava/lang/ClassLoader;");
    jobject class_loader = env->CallStaticObjectMethod(class_class_loader, method_get_system_class_loader);
    jclass class_url_class_loader = env->FindClass("java/net/URLClassLoader");
    jmethodID method_add_url = env->GetMethodID(class_url_class_loader, "addURL", "(Ljava/net/URL;)V");
    jclass class_url = env->FindClass("java/net/URL");
    jmethodID url_ctor = env->GetMethodID(class_url, "<init>", "(Ljava/lang/String;)V");
    jobject urlInstance = env->NewObject(class_url, url_ctor, env->NewStringUTF(path.c_str()));
    env->CallVoidMethod(class_loader, method_add_url, urlInstance);
    return Status::OK();
#else
    return Status::InternalError("No libjvm is found!");
#endif
}

std::string UserFunctionCache::_make_lib_file(int64_t function_id, const std::string& checksum,
                                              LibType type) {
    int shard = function_id % kLibShardNum;
    std::stringstream ss;
    ss << _lib_dir << '/' << shard << '/' << function_id << '.' << checksum;
    if (type == LibType::JAR) {
        ss << ".jar";
    } else {
        ss << ".so";
    }
    return ss.str();
}

void UserFunctionCache::release_entry(UserFunctionCacheEntry* entry) {
    if (entry == nullptr) {
        return;
    }
    if (entry->unref()) {
        delete entry;
    }
}

Status UserFunctionCache::get_jarpath(int64_t fid, const std::string& url, const std::string& checksum,
                                      std::string* libpath) {
    UserFunctionCacheEntry* entry = nullptr;
    RETURN_IF_ERROR(_get_cache_entry(fid, url, checksum, &entry, LibType::JAR));
    *libpath = entry->lib_file;
    return Status::OK();
}

} // namespace doris
