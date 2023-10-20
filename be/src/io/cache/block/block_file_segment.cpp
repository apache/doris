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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileSegment.cpp
// and modified by Doris

#include "io/cache/block/block_file_segment.h"

#include <glog/logging.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <string>
#include <thread>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace io {

FileBlock::FileBlock(size_t offset_, size_t size_, const Key& key_, IFileCache* cache_,
                     State download_state_, CacheType cache_type)
        : _segment_range(offset_, offset_ + size_ - 1),
          _download_state(download_state_),
          _file_key(key_),
          _cache(cache_),
          _cache_type(cache_type) {
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (_download_state) {
    /// EMPTY is used when file segment is not in cache and
    /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
    case State::EMPTY:
    case State::SKIP_CACHE: {
        break;
    }
    /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
    /// or on reduceSizeToDownloaded() -- when file segment object is updated.
    case State::DOWNLOADED: {
        _downloaded_size = size_;
        break;
    }
    /// DOWNLOADING is used only for write-through caching (e.g. getOrSetDownloader() is not
    /// needed, downloader is set on file segment creation).
    case State::DOWNLOADING: {
        _downloader_id = get_caller_id();
        break;
    }
    default: {
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING, SKIP_CACHE ";
    }
    }
}

FileBlock::~FileBlock() {
    std::shared_ptr<FileReader> reader;
    if ((reader = _cache_reader.lock())) {
        IFileCache::remove_file_reader(std::make_pair(_file_key, offset()));
    }
}

FileBlock::State FileBlock::state() const {
    std::lock_guard segment_lock(_mutex);
    return _download_state;
}

size_t FileBlock::get_download_offset() const {
    std::lock_guard segment_lock(_mutex);
    return range().left + get_downloaded_size(segment_lock);
}

size_t FileBlock::get_downloaded_size() const {
    std::lock_guard segment_lock(_mutex);
    return get_downloaded_size(segment_lock);
}

size_t FileBlock::get_downloaded_size(std::lock_guard<std::mutex>& /* segment_lock */) const {
    if (_download_state == State::DOWNLOADED) {
        return _downloaded_size;
    }

    std::lock_guard download_lock(_download_mutex);
    return _downloaded_size;
}

std::string FileBlock::get_caller_id() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

std::string FileBlock::get_or_set_downloader() {
    std::lock_guard segment_lock(_mutex);

    if (_downloader_id.empty()) {
        DCHECK(_download_state != State::DOWNLOADING);

        _downloader_id = get_caller_id();
        _download_state = State::DOWNLOADING;
    } else if (_downloader_id == get_caller_id()) {
        LOG(INFO) << "Attempt to set the same downloader for segment " << range().to_string()
                  << " for the second time";
    }

    return _downloader_id;
}

void FileBlock::reset_downloader(std::lock_guard<std::mutex>& segment_lock) {
    DCHECK(!_downloader_id.empty()) << "There is no downloader";

    DCHECK(get_caller_id() == _downloader_id) << "Downloader can be reset only by downloader";

    reset_downloader_impl(segment_lock);
}

void FileBlock::reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock) {
    if (_downloaded_size == range().size()) {
        static_cast<void>(set_downloaded(segment_lock));
    } else {
        _downloaded_size = 0;
        _download_state = State::EMPTY;
        _downloader_id.clear();
        _cache_writer.reset();
    }
}

std::string FileBlock::get_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return _downloader_id;
}

bool FileBlock::is_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return get_caller_id() == _downloader_id;
}

bool FileBlock::is_downloader_impl(std::lock_guard<std::mutex>& /* segment_lock */) const {
    return get_caller_id() == _downloader_id;
}

Status FileBlock::append(Slice data) {
    DCHECK(data.size != 0) << "Writing zero size is not allowed";
    Status st = Status::OK();
    if (!_cache_writer) {
        auto download_path = get_path_in_local_cache();
        st = global_local_filesystem()->create_file(download_path, &_cache_writer);
        if (!st) {
            _cache_writer.reset();
            return st;
        }
    }

    RETURN_IF_ERROR(_cache_writer->append(data));

    std::lock_guard download_lock(_download_mutex);

    _downloaded_size += data.size;
    return st;
}

std::string FileBlock::get_path_in_local_cache() const {
    return _cache->get_path_in_local_cache(key(), offset(), _cache_type);
}

Status FileBlock::read_at(Slice buffer, size_t read_offset) {
    Status st = Status::OK();
    std::shared_ptr<FileReader> reader;
    if (!(reader = _cache_reader.lock())) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!(reader = _cache_reader.lock())) {
            auto download_path = get_path_in_local_cache();
            RETURN_IF_ERROR(global_local_filesystem()->open_file(download_path, &reader));
            _cache_reader =
                    IFileCache::cache_file_reader(std::make_pair(_file_key, offset()), reader);
        }
    }
    size_t bytes_reads = buffer.size;
    RETURN_IF_ERROR(reader->read_at(read_offset, buffer, &bytes_reads));
    DCHECK(bytes_reads == buffer.size);
    return st;
}

bool FileBlock::change_cache_type(CacheType new_type) {
    std::unique_lock segment_lock(_mutex);
    if (new_type == _cache_type) {
        return true;
    }
    if (_download_state == State::DOWNLOADED) {
        std::error_code ec;
        std::filesystem::rename(get_path_in_local_cache(),
                                _cache->get_path_in_local_cache(key(), offset(), new_type), ec);
        if (ec) {
            LOG(ERROR) << "change cache type failed due to rename error " << ec.message();
            return false;
        }
    }
    _cache_type = new_type;
    return true;
}

Status FileBlock::change_cache_type_self(CacheType new_type) {
    std::lock_guard cache_lock(_cache->_mutex);
    std::unique_lock segment_lock(_mutex);
    Status st = Status::OK();
    if (_cache_type == CacheType::TTL || new_type == _cache_type) {
        return st;
    }
    _cache_type = new_type;
    _cache->change_cache_type(_file_key, _segment_range.left, new_type, cache_lock);
    return st;
}

Status FileBlock::finalize_write() {
    std::lock_guard segment_lock(_mutex);

    RETURN_IF_ERROR(set_downloaded(segment_lock));
    _cv.notify_all();
    return Status::OK();
}

FileBlock::State FileBlock::wait() {
    std::unique_lock segment_lock(_mutex);

    if (_downloader_id.empty()) {
        return _download_state;
    }

    if (_download_state == State::DOWNLOADING) {
        DCHECK(!_downloader_id.empty());
        DCHECK(_downloader_id != get_caller_id());

        _cv.wait_for(segment_lock, std::chrono::seconds(1));
    }

    return _download_state;
}

Status FileBlock::set_downloaded(std::lock_guard<std::mutex>& /* segment_lock */) {
    if (_is_downloaded) {
        return Status::OK();
    }

    if (_cache_writer) {
        RETURN_IF_ERROR(_cache_writer->close());
        _cache_writer.reset();
    }

    _download_state = State::DOWNLOADED;
    _is_downloaded = true;
    _downloader_id.clear();
    return Status::OK();
}

void FileBlock::complete_unlocked(std::lock_guard<std::mutex>& segment_lock) {
    if (is_downloader_impl(segment_lock)) {
        reset_downloader(segment_lock);
        _cv.notify_all();
    }
}

std::string FileBlock::get_info_for_log() const {
    std::lock_guard segment_lock(_mutex);
    return get_info_for_log_impl(segment_lock);
}

std::string FileBlock::get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const {
    std::stringstream info;
    info << "File segment: " << range().to_string() << ", ";
    info << "state: " << state_to_string(_download_state) << ", ";
    info << "downloaded size: " << get_downloaded_size(segment_lock) << ", ";
    info << "downloader id: " << _downloader_id << ", ";
    info << "caller id: " << get_caller_id();

    return info.str();
}

FileBlock::State FileBlock::state_unlock(std::lock_guard<std::mutex>&) const {
    return _download_state;
}

std::string FileBlock::state_to_string(FileBlock::State state) {
    switch (state) {
    case FileBlock::State::DOWNLOADED:
        return "DOWNLOADED";
    case FileBlock::State::EMPTY:
        return "EMPTY";
    case FileBlock::State::DOWNLOADING:
        return "DOWNLOADING";
    case FileBlock::State::SKIP_CACHE:
        return "SKIP_CACHE";
    default:
        DCHECK(false);
        return "";
    }
}

bool FileBlock::has_finalized_state() const {
    return _download_state == State::DOWNLOADED;
}

FileBlocksHolder::~FileBlocksHolder() {
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileBlocksHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    IFileCache* cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();) {
        auto current_file_segment_it = file_segment_it;
        auto& file_segment = *current_file_segment_it;

        if (!cache) {
            cache = file_segment->_cache;
        }

        {
            std::lock_guard cache_lock(cache->_mutex);
            std::lock_guard segment_lock(file_segment->_mutex);
            file_segment->complete_unlocked(segment_lock);
            if (file_segment->state_unlock(segment_lock) == FileBlock::State::EMPTY) {
                // one in cache, one in here
                if (file_segment.use_count() == 2) {
                    cache->remove(file_segment, cache_lock, segment_lock);
                }
            }
        }

        file_segment_it = file_segments.erase(current_file_segment_it);
    }
}

std::string FileBlocksHolder::to_string() {
    std::string ranges;
    for (const auto& file_segment : file_segments) {
        if (!ranges.empty()) {
            ranges += ", ";
        }
        ranges += file_segment->range().to_string();
    }
    return ranges;
}

} // namespace io
} // namespace doris
