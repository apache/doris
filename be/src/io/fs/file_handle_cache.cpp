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
//
// This file is copied from
// https://github.com/apache/impala/blob/master/be/src/runtime/io/handle-cache.inline.h
// and modified by Doris

#include "io/fs/file_handle_cache.h"

#include <thread>
#include <tuple>

#include "io/fs/err_utils.h"
#include "util/hash_util.hpp"
#include "util/time.h"

namespace doris::io {

HdfsFileHandle::~HdfsFileHandle() {
    if (_hdfs_file != nullptr && _fs != nullptr) {
        VLOG_FILE << "hdfsCloseFile() fid=" << _hdfs_file;
        hdfsCloseFile(_fs, _hdfs_file); // TODO: check return code
    }
    _fs = nullptr;
    _hdfs_file = nullptr;
}

Status HdfsFileHandle::init(int64_t file_size) {
    _hdfs_file = hdfsOpenFile(_fs, _fname.c_str(), O_RDONLY, 0, 0, 0);
    if (_hdfs_file == nullptr) {
        std::string _err_msg = hdfs_error();
        // invoker maybe just skip Status.NotFound and continue
        // so we need distinguish between it and other kinds of errors
        if (_err_msg.find("No such file or directory") != std::string::npos) {
            return Status::NotFound(_err_msg);
        }
        return Status::IOError("failed to open {}: {}", _fname, _err_msg);
    }

    _file_size = file_size;
    if (_file_size <= 0) {
        hdfsFileInfo* file_info = hdfsGetPathInfo(_fs, _fname.c_str());
        if (file_info == nullptr) {
            return Status::IOError("failed to get file size of {}: {}", _fname, hdfs_error());
        }
        _file_size = file_info->mSize;
        hdfsFreeFileInfo(file_info, 1);
    }
    return Status::OK();
}

CachedHdfsFileHandle::CachedHdfsFileHandle(const hdfsFS& fs, const std::string& fname,
                                           int64_t mtime)
        : HdfsFileHandle(fs, fname, mtime) {}

CachedHdfsFileHandle::~CachedHdfsFileHandle() {}

FileHandleCache::Accessor::Accessor() : _cache_accessor() {}

FileHandleCache::Accessor::Accessor(FileHandleCachePartition::CacheType::Accessor&& cache_accessor)
        : _cache_accessor(std::move(cache_accessor)) {}

void FileHandleCache::Accessor::set(
        FileHandleCachePartition::CacheType::Accessor&& cache_accessor) {
    _cache_accessor = std::move(cache_accessor);
}

CachedHdfsFileHandle* FileHandleCache::Accessor::get() {
    return _cache_accessor.get();
}

void FileHandleCache::Accessor::release() {
    if (_cache_accessor.get()) {
        _cache_accessor.release();
    }
}

void FileHandleCache::Accessor::destroy() {
    if (_cache_accessor.get()) {
        _cache_accessor.destroy();
    }
}

FileHandleCache::Accessor::~Accessor() {
    if (_cache_accessor.get()) {
#ifdef USE_HADOOP_HDFS
        if (hdfsUnbufferFile(get()->file()) != 0) {
            VLOG_FILE << "FS does not support file handle unbuffering, closing file="
                      << _cache_accessor.get_key()->first;
            destroy();
        } else {
            // Calling explicit release to handle metrics
            release();
        }
#else
        destroy();
#endif
    }
}

FileHandleCache::FileHandleCache(size_t capacity, size_t num_partitions,
                                 uint64_t unused_handle_timeout_secs)
        : _cache_partitions(num_partitions),
          _unused_handle_timeout_secs(unused_handle_timeout_secs) {
    DCHECK_GT(num_partitions, 0);

    size_t remainder = capacity % num_partitions;
    size_t base_capacity = capacity / num_partitions;
    size_t partition_capacity = (remainder > 0 ? base_capacity + 1 : base_capacity);

    for (FileHandleCachePartition& p : _cache_partitions) {
        p.cache.set_capacity(partition_capacity);
    }
}

FileHandleCache::~FileHandleCache() {
    _is_shut_down.store(true);
    if (_eviction_thread != nullptr) {
        _eviction_thread->join();
    }
}

Status FileHandleCache::init() {
    return Thread::create("file-handle-cache", "File Handle Timeout",
                          &FileHandleCache::_evict_handles_loop, this, &_eviction_thread);
}

Status FileHandleCache::get_file_handle(const hdfsFS& fs, const std::string& fname, int64_t mtime,
                                        int64_t file_size, bool require_new_handle,
                                        FileHandleCache::Accessor* accessor, bool* cache_hit) {
    DCHECK_GE(mtime, 0);
    // Hash the key and get appropriate partition
    int index = HashUtil::hash(fname.data(), fname.size(), 0) % _cache_partitions.size();
    FileHandleCachePartition& p = _cache_partitions[index];

    auto cache_key = std::make_pair(fname, mtime);

    // If this requires a new handle, skip to the creation codepath. Otherwise,
    // find an unused entry with the same mtime
    if (!require_new_handle) {
        auto cache_accessor = p.cache.get(cache_key);

        if (cache_accessor.get()) {
            // Found a handler in cache and reserved it
            *cache_hit = true;
            accessor->set(std::move(cache_accessor));
            return Status::OK();
        }
    }

    // There was no entry that was free or caller asked for a new handle
    *cache_hit = false;

    // Emplace a new file handle and get access
    auto accessor_tmp = p.cache.emplace_and_get(cache_key, fs, fname, mtime);

    // Opening a file handle requires talking to the NameNode so it can take some time.
    Status status = accessor_tmp.get()->init(file_size);
    if (UNLIKELY(!status.ok())) {
        // Removing the handler from the cache after failed initialization.
        accessor_tmp.destroy();
        return status;
    }

    // Moving the cache accessor to the in/out parameter
    accessor->set(std::move(accessor_tmp));

    return Status::OK();
}

void FileHandleCache::_evict_handles_loop() {
    while (!_is_shut_down.load()) {
        if (_unused_handle_timeout_secs) {
            for (FileHandleCachePartition& p : _cache_partitions) {
                uint64_t now = MonotonicSeconds();
                uint64_t oldest_allowed_timestamp =
                        now > _unused_handle_timeout_secs ? now - _unused_handle_timeout_secs : 0;
                p.cache.evict_older_than(oldest_allowed_timestamp);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

} // namespace doris::io
