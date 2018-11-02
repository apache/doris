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

#ifndef DORIS_BE_RUNTIME_LIB_CACHE_H
#define DORIS_BE_RUNTIME_LIB_CACHE_H

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>
#include "common/object_pool.h"
#include "common/status.h"

namespace doris {

class RuntimeState;

/// Process-wide cache of dynamically-linked libraries loaded from HDFS.
/// These libraries can either be shared objects, llvm modules or jars. For
/// shared objects, when we load the shared object, we dlopen() it and keep
/// it in our process. For modules, we store the symbols in the module to
/// service symbol lookups. We can't cache the module since it (i.e. the external
/// module) is consumed when it is linked with the query codegen module.
//
/// Locking strategy: We don't want to grab a big lock across all operations since
/// one of the operations is copying a file from HDFS. With one lock that would
/// prevent any UDFs from running on the system. Instead, we have a global lock
/// that is taken when doing the cache lookup, but is not taking during any blocking calls.
/// During the block calls, we take the per-lib lock.
//
/// Entry lifetime management: We cannot delete the entry while a query is
/// using the library. When the caller requests a ptr into the library, they
/// are given the entry handle and must decrement the ref count when they
/// are done.
//
/// TODO:
/// - refresh libraries
/// - better cached module management.
class LibCache {
public:
    struct LibCacheEntry;

    enum LibType {
        TYPE_SO,      // Shared object
        TYPE_IR,      // IR intermediate
        TYPE_JAR,     // Java jar file. We don't care about the contents in the BE.
    };

    static LibCache* instance() { 
        return LibCache::_s_instance.get(); 
    }

    /// Calls dlclose on all cached handles.
    ~LibCache();

    /// Initializes the libcache. Must be called before any other APIs.
    static Status init();

    /// Gets the local file system path for the library at 'hdfs_lib_file'. If
    /// this file is not already on the local fs, it copies it and caches the
    /// result. Returns an error if 'hdfs_lib_file' cannot be copied to the local fs.
    Status get_local_lib_path(const std::string& hdfs_lib_file, LibType type,
                           std::string* local_path);

    /// Returns status.ok() if the symbol exists in 'hdfs_lib_file', non-ok otherwise.
    /// If 'quiet' is true, the error status for non-Java unfound symbols will not be logged.
    Status check_symbol_exists(
        const std::string& hdfs_lib_file, LibType type,
        const std::string& symbol, bool quiet);

    /// Returns a pointer to the function for the given library and symbol.
    /// If 'hdfs_lib_file' is empty, the symbol is looked up in the impalad process.
    /// Otherwise, 'hdfs_lib_file' should be the HDFS path to a shared library (.so) file.
    /// dlopen handles and symbols are cached.
    /// Only usable if 'hdfs_lib_file' refers to a shared object.
    //
    /// If entry is non-null and *entry is null, *entry will be set to the cached entry. If
    /// entry is non-null and *entry is non-null, *entry will be reused (i.e., the use count
    /// is not increased). The caller must call decrement_use_count(*entry) when it is done
    /// using fn_ptr and it is no longer valid to use fn_ptr.
    //
    /// If 'quiet' is true, returned error statuses will not be logged.
    Status get_so_function_ptr(
        const std::string& hdfs_lib_file, const std::string& symbol,
        void** fn_ptr, LibCacheEntry** entry, bool quiet);

    Status get_so_function_ptr(
            const std::string& hdfs_lib_file, const std::string& symbol,
            void** fn_ptr, LibCacheEntry** entry) {
        return get_so_function_ptr(hdfs_lib_file, symbol, fn_ptr, entry, true);
    }

    /// Marks the entry for 'hdfs_lib_file' as needing to be refreshed if the file in HDFS is
    /// newer than the local cached copied. The refresh will occur the next time the entry is
    /// accessed.
    void set_needs_refresh(const std::string& hdfs_lib_file);

    /// See comment in get_so_function_ptr().
    void decrement_use_count(LibCacheEntry* entry);

    /// Removes the cache entry for 'hdfs_lib_file'
    void remove_entry(const std::string& hdfs_lib_file);

    /// Removes all cached entries.
    void drop_cache();

private:
    /// Singleton instance. Instantiated in Init().
    static boost::scoped_ptr<LibCache> _s_instance;

    /// dlopen() handle for the current process (i.e. impalad).
    void* _current_process_handle;

    /// The number of libs that have been copied from HDFS to the local FS.
    /// This is appended to the local fs path to remove collisions.
    int64_t _num_libs_copied;

    /// Protects _lib_cache. For lock ordering, this lock must always be taken before
    /// the per entry lock.
    boost::mutex _lock;

    /// Maps HDFS library path => cache entry.
    /// Entries in the cache need to be explicitly deleted.
    typedef boost::unordered_map<std::string, LibCacheEntry*> LibMap;
    LibMap _lib_cache;

    LibCache();
    LibCache(LibCache const& l); // disable copy ctor
    LibCache& operator=(LibCache const& l); // disable assignment

    Status init_internal();

    /// Returns the cache entry for 'hdfs_lib_file'. If this library has not been
    /// copied locally, it will copy it and add a new LibCacheEntry to '_lib_cache'.
    /// Result is returned in *entry.
    /// No locks should be take before calling this. On return the entry's lock is
    /// taken and returned in *entry_lock.
    /// If an error is returned, there will be no entry in _lib_cache and *entry is NULL.
    Status get_chache_entry(
        const std::string& hdfs_lib_file, LibType type,
        boost::unique_lock<boost::mutex>* entry_lock, LibCacheEntry** entry);

    /// Implementation to get the cache entry for 'hdfs_lib_file'. Errors are returned
    /// without evicting the cache entry if the status is not OK and *entry is not NULL.
    Status get_chache_entry_internal(
        const std::string& hdfs_lib_file, LibType type,
        boost::unique_lock<boost::mutex>* entry_lock, LibCacheEntry** entry);

    /// Utility function for generating a filename unique to this process and
    /// 'hdfs_path'. This is to prevent multiple impalad processes or different library files
    /// with the same name from clobbering each other. 'hdfs_path' should be the full path
    /// (including the filename) of the file we're going to copy to the local FS, and
    /// 'local_dir' is the local directory prefix of the returned path.
    std::string make_local_path(const std::string& hdfs_path, const std::string& local_dir);

    /// Implementation to remove an entry from the cache.
    /// _lock must be held. The entry's lock should not be held.
    void remove_entry_internal(const std::string& hdfs_lib_file,
                             const LibMap::iterator& entry_iterator);
};

}

#endif
