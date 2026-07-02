// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/parquet_file_context.h"

#include <arrow/buffer.h>
#include <arrow/result.h>
#include <fmt/format.h>
#include <gen_cpp/segment_v2.pb.h>
#include <parquet/exception.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/check.h"
#include "common/config.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "storage/cache/page_cache.h"
#include "util/slice.h"

namespace doris::format::parquet {

namespace detail {

std::vector<ParquetPageCacheReadPlanEntry> plan_page_cache_range_read(
        int64_t position, int64_t nbytes, const std::vector<ParquetPageCacheRange>& cached_ranges) {
    if (position < 0 || nbytes <= 0) {
        return {};
    }

    std::vector<ParquetPageCacheRange> ranges;
    ranges.reserve(cached_ranges.size());
    const int64_t request_end = position + nbytes;
    for (const auto& range : cached_ranges) {
        if (range.size > 0 && range.offset < request_end && position < range.end_offset()) {
            ranges.push_back(range);
        }
    }
    std::sort(ranges.begin(), ranges.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.offset != rhs.offset) {
            return lhs.offset < rhs.offset;
        }
        return lhs.size > rhs.size;
    });

    std::vector<ParquetPageCacheReadPlanEntry> plan;
    int64_t cursor = position;
    while (cursor < request_end) {
        // At each cursor position, choose the cached range that already covers the cursor and
        // extends farthest to the right. This handles both adjacent ranges and overlapping
        // ranges. If no range covers the current cursor, there is a gap and the request must
        // miss as a whole.
        auto best = ranges.end();
        int64_t best_end = cursor;
        for (auto it = ranges.begin(); it != ranges.end(); ++it) {
            const int64_t cached_end = it->end_offset();
            if (it->offset <= cursor && cursor < cached_end && cached_end > best_end) {
                best = it;
                best_end = cached_end;
            }
        }
        if (best == ranges.end()) {
            return {};
        }
        const int64_t copy_size = std::min(best_end, request_end) - cursor;
        ParquetPageCacheReadPlanEntry entry;
        entry.cached_range = *best;
        entry.copy_offset_in_cache = cursor - best->offset;
        entry.output_offset = cursor - position;
        entry.copy_size = copy_size;
        plan.push_back(entry);
        cursor += copy_size;
    }
    return plan;
}

} // namespace detail

namespace {

// StoragePageCache only supports exact-key lookup. Keep lightweight range metadata here so later
// Arrow ReadAt requests can reuse cached bytes when their requested ranges are subsets of, or are
// fully covered by, previously cached ranges. Stale metadata is pruned on lookup.
std::mutex cached_page_range_index_mutex;
std::unordered_map<std::string, std::vector<ParquetPageCacheRange>> cached_page_range_index;
constexpr size_t MAX_CACHED_PAGE_RANGE_FILES = 4096;
constexpr size_t MAX_CACHED_PAGE_RANGES_PER_FILE = 65536;

void register_cached_page_range(const std::string& file_key, int64_t position, int64_t nbytes) {
    DORIS_CHECK(nbytes > 0);
    std::lock_guard lock(cached_page_range_index_mutex);
    if (cached_page_range_index.find(file_key) == cached_page_range_index.end() &&
        cached_page_range_index.size() >= MAX_CACHED_PAGE_RANGE_FILES) {
        cached_page_range_index.erase(cached_page_range_index.begin());
    }
    auto& ranges = cached_page_range_index[file_key];
    auto it = std::find_if(ranges.begin(), ranges.end(), [&](const ParquetPageCacheRange& range) {
        return range.offset == position && range.size == nbytes;
    });
    if (it == ranges.end()) {
        if (ranges.size() >= MAX_CACHED_PAGE_RANGES_PER_FILE) {
            ranges.erase(ranges.begin());
        }
        ranges.push_back(ParquetPageCacheRange {position, nbytes});
    }
}

void unregister_cached_page_range(const std::string& file_key,
                                  const ParquetPageCacheRange& stale_range) {
    std::lock_guard lock(cached_page_range_index_mutex);
    auto it = cached_page_range_index.find(file_key);
    if (it == cached_page_range_index.end()) {
        return;
    }
    auto& ranges = it->second;
    ranges.erase(std::remove_if(ranges.begin(), ranges.end(),
                                [&](const ParquetPageCacheRange& range) {
                                    return range.offset == stale_range.offset &&
                                           range.size == stale_range.size;
                                }),
                 ranges.end());
    if (ranges.empty()) {
        cached_page_range_index.erase(it);
    }
}

std::vector<ParquetPageCacheRange> cached_page_ranges_for_file(const std::string& file_key) {
    std::lock_guard lock(cached_page_range_index_mutex);
    auto it = cached_page_range_index.find(file_key);
    if (it == cached_page_range_index.end()) {
        return {};
    }
    return it->second;
}

std::string build_page_cache_file_key(const io::FileReader& file_reader,
                                      const io::FileDescription& file_description) {
    const int64_t mtime =
            file_description.mtime != 0 ? file_description.mtime : file_reader.mtime();
    if (mtime == 0) {
        // StoragePageCache is process-global. A key with only path + unknown mtime can outlive a
        // rewritten local test file, or any external file whose version was not propagated. Disable
        // v2 parquet page cache until the scan descriptor carries a stable object version.
        return {};
    }
    const int64_t file_size = file_description.file_size >= 0
                                      ? file_description.file_size
                                      : static_cast<int64_t>(file_reader.size());
    return fmt::format("{}::{}::mtime={}::size={}", file_description.fs_name,
                       file_reader.path().native(), mtime, file_size);
}

class DorisRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    DorisRandomAccessFile(io::FileReaderSPtr file_reader, io::IOContext* io_ctx,
                          bool enable_page_cache, std::string page_cache_file_key)
            : _file_reader(std::move(file_reader)),
              _io_ctx(io_ctx),
              _enable_page_cache(enable_page_cache),
              _page_cache_file_key(std::move(page_cache_file_key)) {
        DORIS_CHECK(_file_reader != nullptr);
        set_mode(arrow::io::FileMode::READ);
    }

    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    bool closed() const override { return _closed; }

    arrow::Result<int64_t> Tell() const override { return _pos; }

    arrow::Status Seek(int64_t position) override {
        if (position < 0) {
            return arrow::Status::Invalid("negative seek position");
        }
        _pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> GetSize() override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        return static_cast<int64_t>(_file_reader->size());
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(_pos, nbytes, out));
        _pos += bytes_read;
        return bytes_read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (!_file_reader) {
            return arrow::Status::IOError("Doris file reader is not open");
        }
        if (position < 0 || nbytes < 0) {
            return arrow::Status::Invalid("negative read position or length");
        }
        if (try_read_from_page_cache(position, nbytes, out)) {
            return nbytes;
        }
        size_t bytes_read = 0;
        Status st = _file_reader->read_at(
                static_cast<size_t>(position),
                Slice(static_cast<uint8_t*>(out), static_cast<size_t>(nbytes)), &bytes_read,
                _io_ctx);
        if (!st.ok()) {
            return arrow::Status::IOError(st.to_string_no_stack());
        }
        insert_page_cache(position, nbytes, out, bytes_read);
        return static_cast<int64_t>(bytes_read);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                         int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(auto bytes_read, ReadAt(position, nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
        buffer->ZeroPadding();
        return buffer;
    }

    void register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges) {
        std::lock_guard lock(_page_cache_mutex);
        _page_cache_ranges = std::move(ranges);
    }

    void prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                         const io::IOContext* io_ctx) {
        auto cached_reader = cached_remote_file_reader();
        if (cached_reader == nullptr) {
            return;
        }
        const auto* prefetch_io_ctx = io_ctx != nullptr ? io_ctx : _io_ctx;
        for (const auto& range : ranges) {
            if (range.offset < 0 || range.size <= 0) {
                continue;
            }
            cached_reader->prefetch_range(static_cast<size_t>(range.offset),
                                          static_cast<size_t>(range.size), prefetch_io_ctx);
        }
    }

    ParquetPageCacheStats page_cache_stats() const {
        std::lock_guard lock(_page_cache_mutex);
        return _page_cache_stats;
    }

private:
    bool page_cache_enabled() const {
        return _enable_page_cache && !config::disable_storage_page_cache &&
               StoragePageCache::instance() != nullptr && !_page_cache_file_key.empty();
    }

    bool range_in_page_cache_scope(int64_t position, int64_t nbytes) const {
        if (nbytes <= 0) {
            return false;
        }
        const int64_t end = position + nbytes;
        for (const auto& range : _page_cache_ranges) {
            const int64_t range_end = range.offset + range.size;
            if (position >= range.offset && end <= range_end) {
                return true;
            }
        }
        return false;
    }

    StoragePageCache::CacheKey page_cache_key(int64_t position, int64_t nbytes) const {
        return StoragePageCache::CacheKey(_page_cache_file_key,
                                          static_cast<size_t>(position + nbytes), position);
    }

    bool copy_cached_range(const ParquetPageCacheRange& cached_range, int64_t copy_position,
                           int64_t copy_size, void* out, int64_t output_offset) {
        PageCacheHandle handle;
        if (!StoragePageCache::instance()->lookup(
                    page_cache_key(cached_range.offset, cached_range.size), &handle,
                    segment_v2::DATA_PAGE)) {
            unregister_cached_page_range(_page_cache_file_key, cached_range);
            return false;
        }
        Slice cached = handle.data();
        const int64_t cache_offset = copy_position - cached_range.offset;
        DORIS_CHECK(cache_offset >= 0);
        DORIS_CHECK(cached.size >= static_cast<size_t>(cache_offset + copy_size));
        memcpy(static_cast<uint8_t*>(out) + output_offset, cached.data + cache_offset,
               static_cast<size_t>(copy_size));
        return true;
    }

    bool try_read_from_cached_ranges(int64_t position, int64_t nbytes, void* out) {
        auto plan = detail::plan_page_cache_range_read(
                position, nbytes, cached_page_ranges_for_file(_page_cache_file_key));
        if (plan.empty()) {
            return false;
        }
        for (const auto& entry : plan) {
            if (!copy_cached_range(entry.cached_range,
                                   entry.cached_range.offset + entry.copy_offset_in_cache,
                                   entry.copy_size, out, entry.output_offset)) {
                return false;
            }
        }
        return true;
    }

    bool try_read_from_page_cache(int64_t position, int64_t nbytes, void* out) {
        std::lock_guard lock(_page_cache_mutex);
        if (!page_cache_enabled() || !range_in_page_cache_scope(position, nbytes)) {
            return false;
        }
        ++_page_cache_stats.read_count;
        // Fast path: Arrow issues the same ReadAt(offset, size) again, so the exact
        // StoragePageCache key matches.
        // Fallback path: Arrow may read a different but related byte range on another scan.
        // Examples:
        // - Current request [120, 150) can be served from cached [100, 200) by copying the
        //   30-byte subset starting at cached offset 20.
        // - Current request [100, 260) can be served by stitching cached [100, 180) and
        //   [180, 260). If any middle span is missing, it is a miss and the file reader fills
        //   the whole request from storage.
        if (!copy_cached_range(ParquetPageCacheRange {position, nbytes}, position, nbytes, out,
                               0) &&
            !try_read_from_cached_ranges(position, nbytes, out)) {
            ++_page_cache_stats.miss_count;
            return false;
        }
        ++_page_cache_stats.hit_count;
        ++_page_cache_stats.compressed_hit_count;
        return true;
    }

    void insert_page_cache(int64_t position, int64_t nbytes, const void* data, size_t bytes_read) {
        std::lock_guard lock(_page_cache_mutex);
        if (!page_cache_enabled() || !range_in_page_cache_scope(position, nbytes) ||
            bytes_read != static_cast<size_t>(nbytes)) {
            return;
        }
        auto* page = new DataPage(bytes_read, true, segment_v2::DATA_PAGE);
        memcpy(page->data(), data, bytes_read);
        PageCacheHandle handle;
        StoragePageCache::instance()->insert(page_cache_key(position, nbytes), page, &handle,
                                             segment_v2::DATA_PAGE);
        register_cached_page_range(_page_cache_file_key, position, nbytes);
        ++_page_cache_stats.write_count;
        ++_page_cache_stats.compressed_write_count;
    }

    std::shared_ptr<io::CachedRemoteFileReader> cached_remote_file_reader() {
        auto reader = _file_reader;
        if (reader == nullptr) {
            return nullptr;
        }
        // FileReader::init wraps the physical reader with TracingFileReader when scan IO stats are
        // enabled. Prefetch should target the physical cached reader below that tracing wrapper,
        // otherwise v2 scans with profiling would silently lose prefetch.
        if (auto tracing_reader = std::dynamic_pointer_cast<io::TracingFileReader>(reader)) {
            reader = tracing_reader->inner_reader();
        }
        return std::dynamic_pointer_cast<io::CachedRemoteFileReader>(reader);
    }

    io::FileReaderSPtr _file_reader;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
    bool _enable_page_cache = false;
    std::string _page_cache_file_key;
    mutable std::mutex _page_cache_mutex;
    std::vector<ParquetPageCacheRange> _page_cache_ranges;
    ParquetPageCacheStats _page_cache_stats;
};

} // namespace

Status arrow_status_to_doris_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    }
    if (status.IsIOError()) {
        return Status::IOError(status.ToString());
    }
    if (status.IsInvalid()) {
        return Status::InvalidArgument(status.ToString());
    }
    return Status::InternalError(status.ToString());
}

Status ParquetFileContext::open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx,
                                bool enable_page_cache,
                                const io::FileDescription& file_description) {
    DORIS_CHECK(input_file_reader != nullptr);
    auto page_cache_file_key = build_page_cache_file_key(*input_file_reader, file_description);
    arrow_file = std::make_shared<DorisRandomAccessFile>(std::move(input_file_reader), io_ctx,
                                                         enable_page_cache,
                                                         std::move(page_cache_file_key));
    try {
        // TODO: Cache parquet metadata in file system layer to avoid repeated metadata read for same file.
        this->file_reader = ::parquet::ParquetFileReader::Open(
                arrow_file, ::parquet::default_reader_properties());
        metadata = this->file_reader->metadata();
        schema = metadata != nullptr ? metadata->schema() : nullptr;
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption("Failed to open parquet file: {}", e.what());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open parquet file: {}", e.what());
    }

    if (metadata == nullptr || schema == nullptr) {
        return Status::Corruption("Failed to read parquet metadata");
    }
    return Status::OK();
}

void ParquetFileContext::register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges) {
    DORIS_CHECK(arrow_file != nullptr);
    static_cast<DorisRandomAccessFile*>(arrow_file.get())
            ->register_page_cache_ranges(std::move(ranges));
}

void ParquetFileContext::prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                         const io::IOContext* io_ctx) {
    DORIS_CHECK(arrow_file != nullptr);
    static_cast<DorisRandomAccessFile*>(arrow_file.get())->prefetch_ranges(ranges, io_ctx);
}

ParquetPageCacheStats ParquetFileContext::page_cache_stats() const {
    if (arrow_file == nullptr) {
        return {};
    }
    return static_cast<const DorisRandomAccessFile*>(arrow_file.get())->page_cache_stats();
}

Status ParquetFileContext::close() {
    if (file_reader != nullptr) {
        try {
            file_reader->Close();
        } catch (const std::exception&) {
        }
    }
    if (arrow_file != nullptr) {
        static_cast<void>(arrow_status_to_doris_status(arrow_file->Close()));
    }
    file_reader.reset();
    arrow_file.reset();
    return Status::OK();
}

} // namespace doris::format::parquet
