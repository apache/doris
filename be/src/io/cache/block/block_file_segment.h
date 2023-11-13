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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileSegment.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <stddef.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "common/status.h"
#include "io/cache/block/block_file_cache.h"
#include "io/fs/file_writer.h"
#include "util/slice.h"

namespace doris {
namespace io {

class FileBlock;
class FileReader;

using FileBlockSPtr = std::shared_ptr<FileBlock>;
using FileBlocks = std::list<FileBlockSPtr>;

class FileBlock {
    friend class LRUFileCache;
    friend struct FileBlocksHolder;

public:
    using Key = IFileCache::Key;
    using LocalWriterPtr = std::unique_ptr<FileWriter>;
    using LocalReaderPtr = std::weak_ptr<FileReader>;

    enum class State {
        DOWNLOADED,
        /**
         * When file segment is first created and returned to user, it has state EMPTY.
         * EMPTY state can become DOWNLOADING when getOrSetDownaloder is called successfully
         * by any owner of EMPTY state file segment.
         */
        EMPTY,
        /**
         * A newly created file segment never has DOWNLOADING state until call to getOrSetDownloader
         * because each cache user might acquire multiple file segments and reads them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        SKIP_CACHE,
    };

    FileBlock(size_t offset, size_t size, const Key& key, IFileCache* cache, State download_state,
              CacheType cache_type);

    ~FileBlock();

    State state() const;

    static std::string state_to_string(FileBlock::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range {
        size_t left;
        size_t right;

        Range(size_t left, size_t right) : left(left), right(right) {}

        bool operator==(const Range& other) const {
            return left == other.left && right == other.right;
        }

        size_t size() const { return right - left + 1; }

        std::string to_string() const {
            return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right));
        }
    };

    const Range& range() const { return _segment_range; }

    const Key& key() const { return _file_key; }

    size_t offset() const { return range().left; }

    State wait();

    // append data to cache file
    Status append(Slice data);

    // read data from cache file
    Status read_at(Slice buffer, size_t read_offset);

    // finish write, release the file writer
    Status finalize_write();

    // set downloader if state == EMPTY
    std::string get_or_set_downloader();

    std::string get_downloader() const;

    void reset_downloader(std::lock_guard<std::mutex>& segment_lock);

    bool is_downloader() const;

    bool is_downloaded() const { return _is_downloaded.load(); }

    CacheType cache_type() const { return _cache_type; }

    static std::string get_caller_id();

    size_t get_download_offset() const;

    size_t get_downloaded_size() const;

    std::string get_info_for_log() const;

    std::string get_path_in_local_cache() const;

    bool change_cache_type(CacheType new_type);

    Status change_cache_type_self(CacheType new_type);

    State state_unlock(std::lock_guard<std::mutex>&) const;

    FileBlock& operator=(const FileBlock&) = delete;
    FileBlock(const FileBlock&) = delete;

private:
    size_t get_downloaded_size(std::lock_guard<std::mutex>& segment_lock) const;
    std::string get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const;
    bool has_finalized_state() const;

    Status set_downloaded(std::lock_guard<std::mutex>& segment_lock);
    bool is_downloader_impl(std::lock_guard<std::mutex>& segment_lock) const;

    void complete_unlocked(std::lock_guard<std::mutex>& segment_lock);

    void reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock);

    const Range _segment_range;

    State _download_state;

    std::string _downloader_id;

    LocalWriterPtr _cache_writer;
    LocalReaderPtr _cache_reader;

    size_t _downloaded_size = 0;

    /// global locking order rule:
    /// 1. cache lock
    /// 2. segment lock

    mutable std::mutex _mutex;
    std::condition_variable _cv;

    /// Protects downloaded_size access with actual write into fs.
    /// downloaded_size is not protected by download_mutex in methods which
    /// can never be run in parallel to FileBlock::write() method
    /// as downloaded_size is updated only in FileBlock::write() method.
    /// Such methods are identified by isDownloader() check at their start,
    /// e.g. they are executed strictly by the same thread, sequentially.
    mutable std::mutex _download_mutex;

    Key _file_key;
    IFileCache* _cache;

    std::atomic<bool> _is_downloaded {false};
    CacheType _cache_type;
};

struct FileBlocksHolder {
    explicit FileBlocksHolder(FileBlocks&& file_segments_)
            : file_segments(std::move(file_segments_)) {}

    FileBlocksHolder(FileBlocksHolder&& other) noexcept
            : file_segments(std::move(other.file_segments)) {}

    FileBlocksHolder& operator=(const FileBlocksHolder&) = delete;
    FileBlocksHolder(const FileBlocksHolder&) = delete;

    ~FileBlocksHolder();

    FileBlocks file_segments {};

    std::string to_string();
};

} // namespace io
} // namespace doris
