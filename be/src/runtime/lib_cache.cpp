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

#include "runtime/lib_cache.h"

#include <boost/filesystem.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>

#include "codegen/llvm_codegen.h"
#include "runtime/runtime_state.h"
#include "util/dynamic_util.h"
#include "util/hash_util.hpp"
#include "util/path_builder.h"

namespace doris {

boost::scoped_ptr<LibCache> LibCache::_s_instance;

struct LibCache::LibCacheEntry {
    // Lock protecting all fields in this entry
    boost::mutex lock;

    // The number of users that are using this cache entry. If this is
    // a .so, we can't dlclose unless the use_count goes to 0.
    int use_count;

    // If true, this cache entry should be removed from _lib_cache when
    // the use_count goes to 0.
    bool should_remove;

    // If true, we need to check if there is a newer version of the cached library in HDFS
    // on next access. Should hold _lock to read/write.
    bool check_needs_refresh;

    // The type of this file.
    LibType type;

    // The path on the local file system for this library.
    std::string local_path;

    // Status returned from copying this file from HDFS.
    Status copy_file_status;

    // The last modification time of the HDFS file in seconds.
    time_t last_mod_time;

    // Handle from dlopen.
    void* shared_object_handle;

    // mapping from symbol => address of loaded symbol.
    // Only used if the type is TYPE_SO.
    typedef boost::unordered_map<std::string, void*> SymbolMap;
    SymbolMap symbol_cache;

    // Set of symbols in this entry. This is populated once on load and read
    // only. This is only used if it is a llvm module.
    // TODO: it would be nice to be able to do this for .so's as well but it's
    // not trivial to walk an .so for the symbol table.
    boost::unordered_set<std::string> symbols;

    // Set if an error occurs loading the cache entry before the cache entry
    // can be evicted. This allows other threads that attempt to use the entry
    // before it is removed to return the same error.
    Status loading_status;

    LibCacheEntry() : use_count(0), should_remove(false), check_needs_refresh(false),
    shared_object_handle(NULL) {}
    ~LibCacheEntry();
};

LibCache::LibCache() : 
        _current_process_handle(NULL) {
}

LibCache::~LibCache() {
    drop_cache();
    if (_current_process_handle != NULL) {
        dynamic_close(_current_process_handle);
    }
}

Status LibCache::init() {
    DCHECK(LibCache::_s_instance.get() == NULL);
    LibCache::_s_instance.reset(new LibCache());
    return LibCache::_s_instance->init_internal();
}

Status LibCache::init_internal() {
    // if (TestInfo::is_fe_test()) {
    //     // In the FE tests, NULL gives the handle to the java process.
    //     // Explicitly load the fe-support shared object.
    //     std::string fe_support_path;
    //     PathBuilder::GetFullBuildPath("service/libfesupport.so", &fe_support_path);
    //     RETURN_IF_ERROR(dynamic_open(fe_support_path.c_str(), &_current_process_handle));
    // } else {
    RETURN_IF_ERROR(dynamic_open(NULL, &_current_process_handle));
    DCHECK(_current_process_handle != NULL)
        << "We should always be able to get current process handle.";
    return Status::OK;
}

LibCache::LibCacheEntry::~LibCacheEntry() {
    if (shared_object_handle != NULL) {
        DCHECK_EQ(use_count, 0);
        DCHECK(should_remove);
        dynamic_close(shared_object_handle);
    }
    unlink(local_path.c_str());
}

Status LibCache::get_so_function_ptr(
        const std::string& hdfs_lib_file, const std::string& symbol,
        void** fn_ptr, LibCacheEntry** ent, bool quiet) {
    const std::string& real_symbol = get_real_symbol(symbol);
    if (hdfs_lib_file.empty()) {
        // Just loading a function ptr in the current process. No need to take any locks.
        DCHECK(_current_process_handle != NULL);
        RETURN_IF_ERROR(dynamic_lookup(_current_process_handle, real_symbol.c_str(), fn_ptr));
        return Status::OK;
    }
    LibCacheEntry* entry = NULL;
    boost::unique_lock<boost::mutex> lock;
    if (ent != NULL && *ent != NULL) {
        // Reuse already-cached entry provided by user
        entry = *ent;
        boost::unique_lock<boost::mutex> l(entry->lock);
        lock.swap(l);
    } else {
        RETURN_IF_ERROR(get_chache_entry(hdfs_lib_file, TYPE_SO, &lock, &entry));
    }
    DCHECK(entry != NULL);
    DCHECK_EQ(entry->type, TYPE_SO);

    LibCacheEntry::SymbolMap::iterator it = entry->symbol_cache.find(real_symbol);
    if (it != entry->symbol_cache.end()) {
        *fn_ptr = it->second;
    } else {
        RETURN_IF_ERROR(
            dynamic_lookup(entry->shared_object_handle, real_symbol.c_str(), fn_ptr));
        entry->symbol_cache[real_symbol] = *fn_ptr;
    }

    DCHECK(*fn_ptr != NULL);
    if (ent != NULL && *ent == NULL) {
        // Only set and increment user's entry if it wasn't already cached
        *ent = entry;
        ++(*ent)->use_count;
    }
    return Status::OK;
}

void LibCache::decrement_use_count(LibCacheEntry* entry) {
    if (entry == NULL) {
        return;
    }
    bool can_delete = false;
    {
        boost::unique_lock<boost::mutex> lock(entry->lock);
        --entry->use_count;
        can_delete = (entry->use_count == 0 && entry->should_remove);
    }
    if (can_delete) {
        delete entry;
    }
}

Status LibCache::get_local_lib_path(
        const std::string& hdfs_lib_file, LibType type, std::string* local_path) {
    boost::unique_lock<boost::mutex> lock;
    LibCacheEntry* entry = NULL;
    RETURN_IF_ERROR(get_chache_entry(hdfs_lib_file, type, &lock, &entry));
    DCHECK(entry != NULL);
    DCHECK_EQ(entry->type, type);
    *local_path = entry->local_path;
    return Status::OK;
}

Status LibCache::check_symbol_exists(
        const std::string& hdfs_lib_file, LibType type,
        const std::string& symbol, bool quiet) {
    const std::string& real_symbol = get_real_symbol(symbol);
    if (type == TYPE_SO) {
        void* dummy_ptr = NULL;
        return get_so_function_ptr(hdfs_lib_file, real_symbol, &dummy_ptr, NULL, quiet);
    } else if (type == TYPE_IR) {
        boost::unique_lock<boost::mutex> lock;
        LibCacheEntry* entry = NULL;
        RETURN_IF_ERROR(get_chache_entry(hdfs_lib_file, type, &lock, &entry));
        DCHECK(entry != NULL);
        DCHECK_EQ(entry->type, TYPE_IR);
        if (entry->symbols.find(real_symbol) == entry->symbols.end()) {
            std::stringstream ss;
            ss << "Symbol '" << real_symbol << "' does not exist in module: " << hdfs_lib_file
                << " (local path: " << entry->local_path << ")";
            // return quiet ? Status::Expected(ss.str()) : Status(ss.str());
            return Status(ss.str());
        }
        return Status::OK;
    } else if (type == TYPE_JAR) {
        // TODO: figure out how to inspect contents of jars
        boost::unique_lock<boost::mutex> lock;
        LibCacheEntry* dummy_entry = NULL;
        return get_chache_entry(hdfs_lib_file, type, &lock, &dummy_entry);
    } else {
        DCHECK(false);
        return Status("Shouldn't get here.");
    }
}

void LibCache::set_needs_refresh(const std::string& hdfs_lib_file) {
    boost::unique_lock<boost::mutex> lib_cache_lock(_lock);
    LibMap::iterator it = _lib_cache.find(hdfs_lib_file);
    if (it == _lib_cache.end()) {
        return;
    }
    LibCacheEntry* entry = it->second;

    boost::unique_lock<boost::mutex> entry_lock(entry->lock);
    // Need to hold _lock before setting check_needs_refresh.
    entry->check_needs_refresh = true;
}

void LibCache::remove_entry(const std::string& hdfs_lib_file) {
    boost::unique_lock<boost::mutex> lib_cache_lock(_lock);
    LibMap::iterator it = _lib_cache.find(hdfs_lib_file);
    if (it == _lib_cache.end()) {
        return;
    }
    remove_entry_internal(hdfs_lib_file, it);
}

void LibCache::remove_entry_internal(const std::string& hdfs_lib_file,
                                   const LibMap::iterator& entry_iter) {
    LibCacheEntry* entry = entry_iter->second;
    VLOG(1) << "Removing lib cache entry: " << hdfs_lib_file
        << ", local path: " << entry->local_path;
    boost::unique_lock<boost::mutex> entry_lock(entry->lock);

    // We have both locks so no other thread can be updating _lib_cache or trying to get
    // the entry.
    _lib_cache.erase(entry_iter);

    entry->should_remove = true;
    DCHECK_GE(entry->use_count, 0);
    bool can_delete = entry->use_count == 0;

    // Now that the entry is removed from the map, it means no future threads
    // can find it->second (the entry), so it is safe to unlock.
    entry_lock.unlock();

    // Now that we've unlocked, we can delete this entry if no one is using it.
    if (can_delete) {
        delete entry;
    }
}

void LibCache::drop_cache() {
    boost::unique_lock<boost::mutex> lib_cache_lock(_lock);
    BOOST_FOREACH(LibMap::value_type& v, _lib_cache) {
        bool can_delete = false;
        {
            // Lock to wait for any threads currently processing the entry.
            boost::unique_lock<boost::mutex> entry_lock(v.second->lock);
            v.second->should_remove = true;
            DCHECK_GE(v.second->use_count, 0);
            can_delete = v.second->use_count == 0;
        }
        VLOG(1) << "Removed lib cache entry: " << v.first;
        if (can_delete) delete v.second;
    }
    _lib_cache.clear();
}

Status LibCache::get_chache_entry(
        const std::string& hdfs_lib_file, LibType type,
        boost::unique_lock<boost::mutex>* entry_lock, LibCacheEntry** entry) {
    Status status;
    {
        // If an error occurs, local_entry_lock is released before calling remove_entry()
        // below because it takes the global _lock which must be acquired before taking entry
        // locks.
        boost::unique_lock<boost::mutex> local_entry_lock;
        status = get_chache_entry_internal(hdfs_lib_file, type, &local_entry_lock, entry);
        if (status.ok()) {
            entry_lock->swap(local_entry_lock);
            return status;
        }
        if (*entry == NULL) return status;

        // Set loading_status on the entry so that if another thread calls
        // get_chache_entry() for this lib before this thread is able to acquire _lock in
        // remove_entry(), it is able to return the same error.
        (*entry)->loading_status = status;
    }
    // Takes _lock
    remove_entry(hdfs_lib_file);
    return status;
}

Status LibCache::get_chache_entry_internal(
        const std::string& hdfs_lib_file, LibType type,
        boost::unique_lock<boost::mutex>* entry_lock, LibCacheEntry** entry) {
    DCHECK(!hdfs_lib_file.empty());
    *entry = NULL;
#if 0
    // Check if this file is already cached or an error occured on another thread while
    // loading the library.
    boost::unique_lock<boost::mutex> lib_cache_lock(_lock);
    LibMap::iterator it = _lib_cache.find(hdfs_lib_file);
    if (it != _lib_cache.end()) {
        {
            boost::unique_lock<boost::mutex> local_entry_lock((it->second)->lock);
            if (!(it->second)->loading_status.ok()) {
                // If loading_status is already set, the returned *entry should be NULL.
                DCHECK(*entry == NULL);
                return (it->second)->loading_status;
            }
        }

        *entry = it->second;
        if ((*entry)->check_needs_refresh) {
            // Check if file has been modified since loading the cached copy. If so, remove the
            // cached entry and create a new one.
            (*entry)->check_needs_refresh = false;
            time_t last_mod_time;
            hdfsFS hdfs_conn;
            Status status = HdfsFsCache::instance()->GetConnection(hdfs_lib_file, &hdfs_conn);
            if (!status.ok()) {
                remove_entry_internal(hdfs_lib_file, it);
                *entry = NULL;
                return status;
            }
            status = GetLastModificationTime(hdfs_conn, hdfs_lib_file.c_str(), &last_mod_time);
            if (!status.ok() || (*entry)->last_mod_time < last_mod_time) {
                remove_entry_internal(hdfs_lib_file, it);
                *entry = NULL;
            }
            RETURN_IF_ERROR(status);
        }
    }

    if (*entry != NULL) {
        // Release the _lib_cache lock. This guarantees other threads looking at other
        // libs can continue.
        lib_cache_lock.unlock();
        boost::unique_lock<boost::mutex> local_entry_lock((*entry)->lock);
        entry_lock->swap(local_entry_lock);

        RETURN_IF_ERROR((*entry)->copy_file_status);
        DCHECK_EQ((*entry)->type, type);
        DCHECK(!(*entry)->local_path.empty());
        return Status::OK;
    }

    // Entry didn't exist. Add the entry then release _lock (so other libraries
    // can be accessed).
    *entry = new LibCacheEntry();

    // Grab the entry lock before adding it to _lib_cache. We still need to do more
    // work to initialize *entry and we don't want another thread to pick up
    // the uninitialized entry.
    boost::unique_lock<boost::mutex> local_entry_lock((*entry)->lock);
    entry_lock->swap(local_entry_lock);
    _lib_cache[hdfs_lib_file] = *entry;
    lib_cache_lock.unlock();

    // At this point we have the entry lock but not the lib cache lock.
    DCHECK(*entry != NULL);
    (*entry)->type = type;

    // Copy the file
    (*entry)->local_path = make_local_path(hdfs_lib_file, FLAGS_local_library_dir);
    VLOG(1) << "Adding lib cache entry: " << hdfs_lib_file
        << ", local path: " << (*entry)->local_path;

    hdfsFS hdfs_conn, local_conn;
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(hdfs_lib_file, &hdfs_conn));
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetLocalConnection(&local_conn));

    // Note: the file can be updated between getting last_mod_time and copying the file to
    // local_path. This can only result in the file unnecessarily being refreshed, and does
    // not affect correctness.
    (*entry)->copy_file_status = GetLastModificationTime(
        hdfs_conn, hdfs_lib_file.c_str(), &(*entry)->last_mod_time);
    RETURN_IF_ERROR((*entry)->copy_file_status);

    (*entry)->copy_file_status = CopyHdfsFile(
        hdfs_conn, hdfs_lib_file, local_conn, (*entry)->local_path);
    RETURN_IF_ERROR((*entry)->copy_file_status);

    if (type == TYPE_SO) {
        // dlopen the local library
        RETURN_IF_ERROR(
            DynamicOpen((*entry)->local_path.c_str(), &(*entry)->shared_object_handle));
    } else if (type == TYPE_IR) {
        // Load the module and populate all symbols.
        ObjectPool pool;
        scoped_ptr<LlvmCodeGen> codegen;
        std::string module_id = boost::filesystem::path((*entry)->local_path).stem().string();
        RETURN_IF_ERROR(LlvmCodeGen::LoadFromFile(
                &pool, (*entry)->local_path, module_id, &codegen));
        codegen->GetSymbols(&(*entry)->symbols);
    } else {
        DCHECK_EQ(type, TYPE_JAR);
        // Nothing to do.
    }
#endif
    return Status::OK;
}

std::string LibCache::make_local_path(
        const std::string& hdfs_path, const std::string& local_dir) {
    // Append the pid and library number to the local directory.
    boost::filesystem::path src(hdfs_path);
    std::stringstream dst;
    dst << local_dir << "/" << src.stem().native() << "." << getpid() << "."
        << (__sync_fetch_and_add(&_num_libs_copied, 1)) << src.extension().native();
    return dst.str();
}

}

