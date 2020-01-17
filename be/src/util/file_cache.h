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

#include <memory>
#include <string>
#include <unordered_map>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "common/status.h"
#include "env/env.h"
#include "util/spinlock.h"
#include "util/once.h"
#include "olap/lru_cache.h"

namespace doris {

class Env;

namespace internal {

template <class FileType>
class BaseDescriptor;

template <class FileType>
class Descriptor;

} // namespace internal

// Cache of open files.
//
// The purpose of this cache is to enforce an upper bound on the maximum number
// of files open at a time. Files opened through the cache may be closed at any
// time, only to be reopened upon next use.
//
// The file cache can be viewed as having two logical parts: the client-facing
// API and the LRU cache.
//
// Client-facing API
// -----------------
// The core of the client-facing API is the cache descriptor. A descriptor
// uniquely identifies an opened file. To a client, a descriptor is just an
// open file interface of the variety defined in util/env.h. Clients open
// descriptors via the OpenExistingFile() cache method.
//
// Descriptors are shared objects; an existing descriptor is handed back to a
// client if a file with the same name is already opened. To facilitate
// descriptor sharing, the file cache maintains a by-file-name descriptor map.
// The values are weak references to the descriptors so that map entries don't
// affect the descriptor lifecycle.
//
// LRU cache
// ---------
// The lower half of the file cache is a standard LRU cache whose keys are file
// names and whose values are pointers to opened file objects allocated on the
// heap. Unlike the descriptor map, this cache has an upper bound on capacity,
// and handles are evicted (and closed) according to an LRU algorithm.
//
// Whenever a descriptor is used by a client in file I/O, its file name is used
// in an LRU cache lookup. If found, the underlying file is still open and the
// file access is performed. Otherwise, the file must have been evicted and
// closed, so it is reopened and reinserted (possibly evicting a different open
// file) before the file access is performed.
//
// Other notes
// -----------
// In a world where files are opened and closed transparently, file deletion
// demands special care if UNIX semantics are to be preserved. When a call to
// DeleteFile() is made to a file with an opened descriptor, the descriptor is
// simply "marked" as to-be-deleted-later. Only when all references to the
// descriptor are dropped is the file actually deleted. If there is no open
// descriptor, the file is deleted immediately.
//
// Every public method in the file cache is thread safe.
template <class FileType>
class FileCache {
public:
    // Creates a new file cache.
    //
    // The 'cache_name' is used to disambiguate amongst other file cache
    // instances. The cache will use 'max_open_files' as a soft upper bound on
    // the number of files open at any given time.
    FileCache(const std::string& cache_name, Env* env, int max_open_files);

    // Destroys the file cache.
    ~FileCache();

    // Initializes the file cache. Initialization done here may fail.
    Status init();

    // Opens an file by name through the cache.
    //
    // The returned 'file' is actually an object called a descriptor. It adheres
    // to a file-like interface but interfaces with the cache under the hood to
    // reopen a file as needed during file operations.
    //
    // The descriptor is opened immediately to verify that the on-disk file can
    // be opened, but may be closed later if the cache reaches its upper bound
    // on the number of open files.
    Status open_file(const std::string& file_name,
                     std::shared_ptr<FileType>* file);

    // Deletes a file by name through the cache.
    //
    // If there is an outstanding descriptor for the file, the deletion will be
    // deferred until the last referent is dropped. Otherwise, the file is
    // deleted immediately.
    Status delete_file(const std::string& file_name);

    // Invalidate the given path in the cache if present. This removes the
    // path from the cache, and invalidates any previously-opened descriptors
    // associated with this file.
    //
    // If a file with the same path is opened again, the actual path will be
    // opened from disk.
    //
    // NOTE: if any reader of 'p' holds an open descriptor from the cache
    // prior to this operation, that descriptor is invalidated and any
    // further operations on that descriptor will result in a CHECK failure.
    // Hence this is not safe to use without some external synchronization
    // which prevents concurrent access to the same file.
    //
    // NOTE: this function must not be called concurrently on the same file name
    // from multiple threads.
    void invalidate(const std::string& file_name);

    // periodically removes expired descriptors from _descriptors
    void run_descriptor_expiry();

private:
    friend class internal::BaseDescriptor<FileType>;

    // Looks up a descriptor by file name.
    //
    // Must be called with 'lock_' held.
    Status find_descriptor_unlocked(
        const std::string& file_name,
        std::shared_ptr<internal::Descriptor<FileType>>* file);

    Status _init_once();

private:
    // Interface to the underlying filesystem.
    Env* _env;

    // Name of the cache.
    std::string _cache_name;

    // Underlying cache instance. Caches opened files.
    std::unique_ptr<Cache> _cache;

    // Protects the descriptor map.
    SpinLock _lock;

    DorisCallOnce<Status> _once;

    // to collect expired descriptors
    std::mutex _expire_lock;
    std::condition_variable _expire_cond;
    // thread to collect expired descriptors
    std::unique_ptr<std::thread> _expire_thread;


    // Maps filenames to descriptors.
    std::unordered_map<std::string, std::weak_ptr<internal::Descriptor<FileType>>> _descriptors;

    DISALLOW_COPY_AND_ASSIGN(FileCache);
};

} // namespace doris
