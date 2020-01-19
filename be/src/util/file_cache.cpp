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

#include "util/file_cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <chrono>
#include <functional>

#include "env/env.h"
#include "gutil/strings/substitute.h"

namespace doris {

namespace internal {

// Reference to an on-disk file that may or may not be opened (and thus
// cached) in the file cache.
//
// This empty template is just a specification; actual descriptor classes must
// be fully specialized.
template <class FileType>
class Descriptor : public FileType {
};

// A descriptor adhering to the RandomAccessFile interface (i.e. when opened,
// provides a read-only interface to the underlying file).
template <>
class Descriptor<RandomAccessFile> : public RandomAccessFile {
public:
    Descriptor(FileCache<RandomAccessFile>* file_cache, const std::string& filename)
        : _base(file_cache, filename) {}

    ~Descriptor() = default;

    // return a file handle point to the RandomAccessFile to iterator for performance
    Status file_handle(std::unique_ptr<OpenedFileHandle<RandomAccessFile>>* file) {
        file->reset(new OpenedFileHandle<RandomAccessFile>(&_base));
        RETURN_IF_ERROR(reopen_if_necessary(file->get()));
        return Status::OK();
    }

    Status read_at(uint64_t offset, const Slice& result) const override {
        OpenedFileHandle<RandomAccessFile> opened(&_base);
        RETURN_IF_ERROR(reopen_if_necessary(&opened));
        return opened.file()->read_at(offset, result);
    }

    Status readv_at(uint64_t offset, const Slice* results, size_t res_cnt) const override {
        OpenedFileHandle<RandomAccessFile> opened(&_base);
        RETURN_IF_ERROR(reopen_if_necessary(&opened));
        return opened.file()->readv_at(offset, results, res_cnt);
    }

    Status size(uint64_t* size) const override {
        OpenedFileHandle<RandomAccessFile> opened(&_base);
        RETURN_IF_ERROR(reopen_if_necessary(&opened));
        return opened.file()->size(size);
    }

    const std::string& file_name() const override { return _base.filename(); }

private:
    friend class FileCache<RandomAccessFile>;

    Status init() {
        return _once.call([this] { return init_once(); });
    }

    Status init_once() { return reopen_if_necessary(nullptr); }

    Status reopen_if_necessary(OpenedFileHandle<RandomAccessFile>* out) const {
        OpenedFileHandle<RandomAccessFile> found(_base.lookup_from_cache());
        CHECK(!_base.invalidated());
        if (found.opened()) {
            // The file is already open in the cache, return it.
            if (out) {
                *out = std::move(found);
            }
            return Status::OK();
        }

        // The file was evicted, reopen it.
        std::unique_ptr<RandomAccessFile> f;
        RETURN_IF_ERROR(_base.env()->new_random_access_file(_base.filename(), &f));

        // The cache will take ownership of the newly opened file.
        OpenedFileHandle<RandomAccessFile> opened(_base.insert_into_cache(f.release()));
        if (out) {
            *out = std::move(opened);
        }
        return Status::OK();
    }

private:    
    BaseDescriptor<RandomAccessFile> _base;
    DorisCallOnce<Status> _once;

    DISALLOW_COPY_AND_ASSIGN(Descriptor);
};

} // namespace internal

template <class FileType>
FileCache<FileType>::FileCache(const std::string& cache_name, Env* env,
                               int max_open_files)
    : _env(env),
      _cache_name(cache_name),
      _cache(new_lru_cache(max_open_files)) {
    LOG(INFO) << strings::Substitute("Constructed file cache $0 with capacity $1", cache_name, max_open_files);
}

template <class FileType>
FileCache<FileType>::~FileCache() {
    _expire_cond.notify_all();
    if (_expire_thread != nullptr) {
        _expire_thread->join();
    }
}

template <class FileType>
Status FileCache<FileType>::init() {
    return _once.call([this]{ return _init_once(); });
}

template <class FileType>
Status FileCache<FileType>::_init_once() {
    _expire_thread.reset(new std::thread(std::bind<void>(
        std::mem_fn(&FileCache<FileType>::run_descriptor_expiry), this)));
    return Status::OK();
}

template <class FileType>
Status FileCache<FileType>::open_file(const std::string& file_name, std::shared_ptr<FileType>* file) {
    std::shared_ptr<internal::Descriptor<FileType>> desc;
    {
        // Find an existing descriptor, or create one if none exists.
        std::lock_guard<SpinLock> l(_lock);
        RETURN_IF_ERROR(find_descriptor_unlocked(file_name, &desc));
        if (desc) {
            VLOG(2) << "Found existing descriptor: " << desc->file_name();
        } else {
            desc = std::make_shared<internal::Descriptor<FileType>>(this,
                                                                    file_name);
            _descriptors.insert({file_name, desc});
            VLOG(2) << "Created new descriptor: " << desc->file_name();
        }
    }

    // Check that the underlying file can be opened (no-op for found
    // descriptors). Done outside the lock.
    RETURN_IF_ERROR(desc->init());
    *file = std::move(desc);
    return Status::OK();
}

template <class FileType>
Status FileCache<FileType>::delete_file(const std::string& file_name) {
    {
        std::lock_guard<SpinLock> l(_lock);
        std::shared_ptr<internal::Descriptor<FileType>> desc;
        RETURN_IF_ERROR(find_descriptor_unlocked(file_name, &desc));

        if (desc) {
            VLOG(2) << "Marking file for deletion: " << file_name;
            desc->_base.mark_deleted();
            return Status::OK();
        }
    }

    // There is no outstanding descriptor. Delete the file now.
    //
    // Make sure it's been fully evicted from the cache (perhaps it was opened
    // previously?) so that the filesystem can reclaim the file data instantly.
    _cache->erase(file_name);
    return _env->delete_file(file_name);
}

template <class FileType>
void FileCache<FileType>::invalidate(const std::string& file_name) {
    // Ensure that there is an invalidated descriptor in the map for this
    // filename.
    //
    // This ensures that any concurrent OpenExistingFile() during this method
    // wil see the invalidation and issue a CHECK failure.
    std::shared_ptr<internal::Descriptor<FileType>> desc;
    {
        // Find an existing descriptor, or create one if none exists.
        std::lock_guard<SpinLock> l(_lock);
        auto it = _descriptors.find(file_name);
        if (it != _descriptors.end()) {
            desc = it->second.lock();
        }
        if (!desc) {
            desc = std::make_shared<internal::Descriptor<FileType>>(this,
                                                                    file_name);
            _descriptors.emplace(file_name, desc);
        }

        desc->_base.mark_invalidated();
    }
    // Remove it from the cache so that if the same path is opened again, we
    // will re-open a new FD rather than retrieving one that might have been
    // cached prior to invalidation.
    _cache->erase(file_name);

    // Remove the invalidated descriptor from the map. We are guaranteed it
    // is still there because we've held a strong reference to it for
    // the duration of this method, and no other methods erase strong
    // references from the map.
    {
        std::lock_guard<SpinLock> l(_lock);
        CHECK_EQ(1, _descriptors.erase(file_name));
    }
}

template <class FileType>
void FileCache<FileType>::run_descriptor_expiry() {
    std::unique_lock<std::mutex> lock(_expire_lock);
    while (_expire_cond.wait_for(lock,
            std::chrono::milliseconds(config::fd_cache_expiry_period_ms)) == std::cv_status::timeout) {
        std::lock_guard<SpinLock> l(_lock);
        for (auto it = _descriptors.begin(); it != _descriptors.end();) {
            if (it->second.expired()) {
                it = _descriptors.erase(it);
            } else {
                ++it;
            }
        }
    }
}

template <class FileType>
Status FileCache<FileType>::find_descriptor_unlocked(
    const std::string& file_name, std::shared_ptr<internal::Descriptor<FileType>>* file) {

    auto it = _descriptors.find(file_name);
    if (it != _descriptors.end()) {
        // Found the descriptor. Has it expired?
        std::shared_ptr<internal::Descriptor<FileType>> desc = it->second.lock();
        if (desc) {
            CHECK(!desc->_base.invalidated());
            if (desc->_base.deleted()) {
                return Status::NotFound(strings::Substitute("File already marked for deletion:$0", file_name));
            }

            // Descriptor is still valid, return it.
            if (file) {
                *file = desc;
            }
            return Status::OK();
        }
        // Descriptor has expired; erase it and pretend we found nothing.
        _descriptors.erase(it);
    }
    return Status::OK();
}

// Explicit specialization for callers outside this compilation unit.
template class FileCache<RandomAccessFile>;

} // namespace doris
