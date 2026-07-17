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
#include <parquet/page_index.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/config.h"
#include "format/parquet/parquet_thrift_util.h"
#include "format_v2/parquet/reader/native/column_chunk_reader.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/cache/page_cache.h"
#include "util/slice.h"

namespace doris::format::parquet {

namespace detail {

namespace {

bool page_cache_range_less(const ParquetPageCacheRange& lhs, const ParquetPageCacheRange& rhs) {
    return lhs.offset < rhs.offset || (lhs.offset == rhs.offset && lhs.size < rhs.size);
}

} // namespace

ParquetPageCacheRangeIndex::ParquetPageCacheRangeIndex(size_t max_ranges)
        : _max_ranges(max_ranges) {
    DORIS_CHECK(_max_ranges > 0);
}

void ParquetPageCacheRangeIndex::insert(ParquetPageCacheRange range) {
    std::lock_guard lock(_mutex);
    auto it = std::lower_bound(_ranges.begin(), _ranges.end(), range, page_cache_range_less);
    if (it != _ranges.end() && it->offset == range.offset && it->size == range.size) {
        return;
    }
    if (_ranges.size() >= _max_ranges) {
        _ranges.erase(_ranges.begin());
        it = std::lower_bound(_ranges.begin(), _ranges.end(), range, page_cache_range_less);
    }
    _ranges.insert(it, range);
}

void ParquetPageCacheRangeIndex::erase(ParquetPageCacheRange range) {
    std::lock_guard lock(_mutex);
    const auto it = std::lower_bound(_ranges.begin(), _ranges.end(), range, page_cache_range_less);
    if (it != _ranges.end() && it->offset == range.offset && it->size == range.size) {
        _ranges.erase(it);
    }
}

std::vector<ParquetPageCacheRange> ParquetPageCacheRangeIndex::ranges() const {
    std::lock_guard lock(_mutex);
    return _ranges;
}

size_t ParquetPageCacheRangeIndex::size() const {
    std::lock_guard lock(_mutex);
    return _ranges.size();
}

ParquetPageCacheRangeDirectory::ParquetPageCacheRangeDirectory(size_t max_files)
        : _max_files(max_files) {
    DORIS_CHECK(_max_files > 0);
}

std::shared_ptr<ParquetPageCacheRangeIndex> ParquetPageCacheRangeDirectory::get_or_create(
        const std::string& file_key) {
    DORIS_CHECK(!file_key.empty());
    std::lock_guard lock(_mutex);
    if (const auto it = _indexes.find(file_key); it != _indexes.end()) {
        return it->second;
    }
    if (_indexes.size() >= _max_files) {
        _indexes.erase(_indexes.begin());
    }
    auto index = std::make_shared<ParquetPageCacheRangeIndex>();
    _indexes.emplace(file_key, index);
    return index;
}

size_t ParquetPageCacheRangeDirectory::size() const {
    std::lock_guard lock(_mutex);
    return _indexes.size();
}

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

std::vector<ParquetPageCacheRange> valid_prefetch_ranges(
        const std::vector<ParquetPageCacheRange>& ranges) {
    std::vector<ParquetPageCacheRange> valid_ranges;
    valid_ranges.reserve(ranges.size());
    for (const auto& range : ranges) {
        if (range.offset < 0 || range.size <= 0 ||
            range.offset > std::numeric_limits<int64_t>::max() - range.size) {
            continue;
        }
        valid_ranges.push_back(range);
    }
    return valid_ranges;
}

size_t average_prefetch_range_size(const std::vector<ParquetPageCacheRange>& ranges) {
    const auto valid_ranges = valid_prefetch_ranges(ranges);
    if (valid_ranges.empty()) {
        return 0;
    }
    size_t total_size = 0;
    for (const auto& range : valid_ranges) {
        total_size += static_cast<size_t>(range.size);
    }
    return total_size / valid_ranges.size();
}

bool should_use_merge_range_reader(const std::vector<ParquetPageCacheRange>& ranges,
                                   size_t avg_io_size, bool is_in_memory_reader) {
    return !is_in_memory_reader && !valid_prefetch_ranges(ranges).empty() &&
           avg_io_size < io::MergeRangeFileReader::SMALL_IO;
}

} // namespace detail

namespace {

detail::ParquetPageCacheRangeDirectory& cached_page_range_directory() {
    // Directory lookup is paid once when a reader opens. ReadAt() then synchronizes only on the
    // shared index for this file, so unrelated Parquet files no longer serialize on a process-wide
    // hot-path mutex. Strong references deliberately keep range discovery alive after reader A
    // closes: reader B can reuse cached [100, 200) for a request [120, 150). The directory and each
    // per-file index are independently capped, bounding stale metadata left by page-cache eviction.
    static detail::ParquetPageCacheRangeDirectory directory;
    return directory;
}

std::string build_page_cache_file_key(const io::FileReader& file_reader,
                                      const io::FileDescription& file_description) {
    const int64_t mtime =
            file_description.mtime != 0 ? file_description.mtime : file_reader.mtime();
    if (mtime == 0 && !file_description.is_immutable) {
        // mtime == 0 means "unknown version", not the Unix epoch. V1 historically caches such a
        // file under path::0, but copying that behavior for every V2 file is unsafe: a mutable file
        // can be overwritten with different bytes while retaining both its path and size, causing
        // process-global page cache entries to return stale data. Only callers that explicitly
        // guarantee path immutability may use the mtime=0 cache key below.
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
              _base_file_reader(_file_reader),
              _io_ctx(io_ctx),
              _enable_page_cache(enable_page_cache),
              _page_cache_file_key(std::move(page_cache_file_key)),
              _cached_page_range_index(
                      _enable_page_cache && !_page_cache_file_key.empty()
                              ? cached_page_range_directory().get_or_create(_page_cache_file_key)
                              : nullptr) {
        DORIS_CHECK(_file_reader != nullptr);
        if (auto tracing_reader = std::dynamic_pointer_cast<io::TracingFileReader>(_file_reader)) {
            _file_reader_stats = tracing_reader->stats();
            _base_file_reader = tracing_reader->inner_reader();
        }
        DORIS_CHECK(_base_file_reader != nullptr);
        set_mode(arrow::io::FileMode::READ);
    }

    arrow::Status Close() override {
        if (!_closed) {
            collect_active_merge_range_profile();
            std::lock_guard lock(_page_cache_mutex);
            // Page payloads and their bounded per-file range index intentionally outlive this
            // reader for warm scans. Only reader-specific projected ranges are released here.
            std::vector<ParquetPageCacheRange>().swap(_page_cache_ranges);
            _closed = true;
        }
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
        if (_io_ctx != nullptr && _io_ctx->should_stop) {
            return arrow::Status::IOError("stop");
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
        if (_io_ctx != nullptr && _io_ctx->should_stop) {
            return arrow::Status::IOError("stop");
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

    bool set_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                  size_t avg_io_size, RuntimeProfile* profile,
                                  int64_t merge_read_slice_size) {
        reset_active_file_reader();
        const auto valid_ranges = detail::valid_prefetch_ranges(ranges);
        if (!detail::should_use_merge_range_reader(
                    valid_ranges, avg_io_size,
                    typeid_cast<io::InMemoryFileReader*>(_base_file_reader.get()) != nullptr)) {
            return false;
        }

        std::vector<io::PrefetchRange> random_access_ranges;
        random_access_ranges.reserve(valid_ranges.size());
        for (const auto& range : valid_ranges) {
            random_access_ranges.emplace_back(static_cast<size_t>(range.offset),
                                              static_cast<size_t>(range.end_offset()));
        }

        // This mirrors the v1 parquet reader for the migration metadata/index ReadAt path. Native
        // data-page decoding owns a separate BufferedFileStreamReader and v1-compatible page cache;
        // adjacent metadata/index requests here can still be coalesced and served from merge
        // buffers.
        // Example: a row group projects leaf chunks [1MB, 1.5MB) and [1.6MB, 2MB). Arrow later
        // issues page reads inside those chunks; MergeRangeFileReader can fetch a wider slice once
        // and satisfy the following ReadAt calls from its boxes, reducing remote request count.
        _merge_range_active = true;
        set_active_file_reader(std::make_shared<io::MergeRangeFileReader>(
                profile, _base_file_reader, random_access_ranges, merge_read_slice_size));
        return true;
    }

    void reset_random_access_ranges() { reset_active_file_reader(); }

    ParquetPageCacheStats page_cache_stats() const {
        std::lock_guard lock(_page_cache_mutex);
        return _page_cache_stats;
    }

private:
    bool page_cache_enabled() const {
        return _enable_page_cache && !config::disable_storage_page_cache &&
               StoragePageCache::instance() != nullptr && !_page_cache_file_key.empty() &&
               _cached_page_range_index != nullptr;
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
            _cached_page_range_index->erase(cached_range);
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
        auto plan = detail::plan_page_cache_range_read(position, nbytes,
                                                       _cached_page_range_index->ranges());
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
        _cached_page_range_index->insert(
                ParquetPageCacheRange {.offset = position, .size = nbytes});
        ++_page_cache_stats.write_count;
        ++_page_cache_stats.compressed_write_count;
    }

    void set_active_file_reader(io::FileReaderSPtr reader) {
        DORIS_CHECK(reader != nullptr);
        _file_reader = _file_reader_stats != nullptr
                               ? std::make_shared<io::TracingFileReader>(std::move(reader),
                                                                         _file_reader_stats)
                               : std::move(reader);
    }

    void reset_active_file_reader() {
        collect_active_merge_range_profile();
        _merge_range_active = false;
        set_active_file_reader(_base_file_reader);
    }

    void collect_active_merge_range_profile() {
        if (_merge_range_active && _file_reader != nullptr) {
            // MergeRangeFileReader writes its MergedSmallIO counters only from
            // collect_profile_before_close(). v2 replaces the active reader for every row group,
            // so collect before overwriting it; Close() handles the final row group. Example:
            // RG0 installs a merge reader, RG1 calls set_random_access_ranges() and resets the
            // active reader first, so RG0's RequestIO/MergedIO counters must be flushed here.
            _file_reader->collect_profile_before_close();
        }
    }

    std::shared_ptr<io::CachedRemoteFileReader> cached_remote_file_reader() {
        if (_merge_range_active) {
            return nullptr;
        }
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
    io::FileReaderSPtr _base_file_reader;
    io::FileReaderStats* _file_reader_stats = nullptr;
    io::IOContext* _io_ctx = nullptr;
    int64_t _pos = 0;
    bool _closed = false;
    bool _enable_page_cache = false;
    bool _merge_range_active = false;
    std::string _page_cache_file_key;
    mutable std::mutex _page_cache_mutex;
    std::vector<ParquetPageCacheRange> _page_cache_ranges;
    std::shared_ptr<detail::ParquetPageCacheRangeIndex> _cached_page_range_index;
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
                                bool enable_page_cache, const io::FileDescription& file_description,
                                bool enable_mapping_timestamp_tz) {
    DORIS_CHECK(input_file_reader != nullptr);
    native_file = input_file_reader;
    native_io_ctx = io_ctx;

    // Use the exact footer cache key and payload type used by v1. This deliberately happens before
    // Arrow metadata is opened: native readers can reuse a footer produced by a v1 scan (and vice
    // versa), and a cache miss performs one bounded tail read through the same Doris FileReader.
    auto* meta_cache = ExecEnv::GetInstance()->file_meta_cache();
    const auto meta_cache_key =
            FileMetaCache::get_key(native_file, file_description, /*enable_mapping_varbinary=*/true,
                                   enable_mapping_timestamp_tz);
    size_t native_footer_size = 0;
    if (meta_cache != nullptr && meta_cache->enabled() &&
        meta_cache->lookup(meta_cache_key, &native_meta_cache_handle)) {
        native_metadata = native_meta_cache_handle.data<FileMetaData>();
        ++native_footer_cache_hits;
    } else {
        RETURN_IF_ERROR(parse_thrift_footer(
                native_file, &native_metadata_owner, &native_footer_size, io_ctx,
                /*enable_mapping_varbinary=*/true, enable_mapping_timestamp_tz,
                /*retain_serialized_metadata=*/true));
        ++native_footer_read_calls;
        if (meta_cache != nullptr && meta_cache->enabled()) {
            meta_cache->insert(meta_cache_key, native_metadata_owner.release(),
                               &native_meta_cache_handle);
            native_metadata = native_meta_cache_handle.data<FileMetaData>();
        } else {
            native_metadata = native_metadata_owner.get();
        }
    }
    DORIS_CHECK(native_metadata != nullptr);

    auto page_cache_file_key = build_page_cache_file_key(*input_file_reader, file_description);
    native_page_cache_enabled = enable_page_cache && !page_cache_file_key.empty();
    // Native and Arrow readers must use the same FileDescription-derived immutable identity.
    native_page_cache_file_key = page_cache_file_key;
    arrow_file = std::make_shared<DorisRandomAccessFile>(
            input_file_reader, io_ctx, enable_page_cache, std::move(page_cache_file_key));
    try {
        std::shared_ptr<::parquet::FileMetaData> arrow_metadata;
        RETURN_IF_ERROR(native_metadata->get_arrow_metadata(&arrow_metadata));
        this->file_reader = ::parquet::ParquetFileReader::Open(
                arrow_file, ::parquet::default_reader_properties(), std::move(arrow_metadata));
        metadata = this->file_reader->metadata();
        schema = metadata != nullptr ? metadata->schema() : nullptr;
    } catch (const ::parquet::ParquetException& e) {
        if (io_ctx != nullptr && io_ctx->should_stop &&
            std::string_view(e.what()).find("stop") != std::string_view::npos) {
            return Status::EndOfFile("stop");
        }
        return Status::Corruption("Failed to open parquet file: {}", e.what());
    } catch (const std::exception& e) {
        if (io_ctx != nullptr && io_ctx->should_stop &&
            std::string_view(e.what()).find("stop") != std::string_view::npos) {
            return Status::EndOfFile("stop");
        }
        return Status::InternalError("Failed to open parquet file: {}", e.what());
    }

    if (metadata == nullptr || schema == nullptr) {
        return Status::Corruption("Failed to read parquet metadata");
    }
    return Status::OK();
}

Status ParquetFileContext::load_native_offset_indexes(
        int row_group_id, const std::unordered_set<int>& leaf_column_ids,
        std::unordered_map<int, tparquet::OffsetIndex>* offset_indexes) const {
    DORIS_CHECK(offset_indexes != nullptr);
    offset_indexes->clear();
    if (leaf_column_ids.empty() || file_reader == nullptr) {
        return Status::OK();
    }
    const auto& thrift_metadata = native_metadata->to_thrift();
    if (row_group_id < 0 || row_group_id >= static_cast<int>(thrift_metadata.row_groups.size())) {
        return Status::Corruption("Invalid Parquet row group {} for OffsetIndex", row_group_id);
    }
    const auto& native_row_group = thrift_metadata.row_groups[row_group_id];
    const auto compat = native::parquet_reader_compat(
            thrift_metadata.__isset.created_by ? thrift_metadata.created_by : "");
    try {
        auto page_index_reader = file_reader->GetPageIndexReader();
        if (page_index_reader == nullptr) {
            return Status::OK();
        }
        auto row_group_reader = page_index_reader->RowGroup(row_group_id);
        if (row_group_reader == nullptr) {
            return Status::OK();
        }
        for (const int leaf_column_id : leaf_column_ids) {
            if (leaf_column_id < 0 ||
                leaf_column_id >= static_cast<int>(native_row_group.columns.size())) {
                return Status::Corruption("Invalid Parquet leaf {} for OffsetIndex",
                                          leaf_column_id);
            }
            auto arrow_index = row_group_reader->GetOffsetIndex(leaf_column_id);
            if (arrow_index == nullptr || arrow_index->page_locations().empty()) {
                // An empty optional index is equivalent to no index. Publishing it would select
                // the indexed PageReader even though there is no first page to dereference.
                continue;
            }
            tparquet::OffsetIndex native_index;
            native_index.page_locations.reserve(arrow_index->page_locations().size());
            for (const auto& arrow_location : arrow_index->page_locations()) {
                tparquet::PageLocation native_location;
                native_location.__set_offset(arrow_location.offset);
                native_location.__set_compressed_page_size(arrow_location.compressed_page_size);
                native_location.__set_first_row_index(arrow_location.first_row_index);
                native_index.page_locations.push_back(std::move(native_location));
            }
            native::ColumnChunkRange chunk_range;
            RETURN_IF_ERROR(native::compute_column_chunk_range(
                    native_row_group.columns[leaf_column_id].meta_data, native_file->size(),
                    compat.parquet_816_padding, &chunk_range));
            if (!native::validate_offset_index(native_index, chunk_range,
                                               native_row_group.num_rows)) {
                // OffsetIndex is optional. Reject the complete index instead of letting one bad
                // location redirect an indexed reader outside its owning column chunk.
                continue;
            }
            offset_indexes->emplace(leaf_column_id, std::move(native_index));
        }
    } catch (const ::parquet::ParquetException&) {
        // OffsetIndex is optional. Selected logical ranges still enforce correctness, while the
        // native reader conservatively falls back to sequential page traversal.
        offset_indexes->clear();
    } catch (const std::exception&) {
        offset_indexes->clear();
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

bool ParquetFileContext::set_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                                  size_t avg_io_size, RuntimeProfile* profile,
                                                  int64_t merge_read_slice_size) {
    DORIS_CHECK(arrow_file != nullptr);
    return static_cast<DorisRandomAccessFile*>(arrow_file.get())
            ->set_random_access_ranges(ranges, avg_io_size, profile, merge_read_slice_size);
}

bool ParquetFileContext::set_native_random_access_ranges(
        const std::vector<ParquetPageCacheRange>& ranges, size_t avg_io_size,
        RuntimeProfile* profile, int64_t merge_read_slice_size) {
    DORIS_CHECK(native_file != nullptr);
    if (!detail::should_use_merge_range_reader(
                ranges, avg_io_size,
                typeid_cast<io::InMemoryFileReader*>(native_file.get()) != nullptr)) {
        native_row_group_file = native_file;
        return false;
    }

    const auto valid_ranges = detail::valid_prefetch_ranges(ranges);
    std::vector<io::PrefetchRange> native_ranges;
    native_ranges.reserve(valid_ranges.size());
    for (const auto& range : valid_ranges) {
        native_ranges.emplace_back(cast_set<size_t>(range.offset),
                                   cast_set<size_t>(range.end_offset()));
    }
    std::ranges::sort(native_ranges, {}, &io::PrefetchRange::start_offset);
    native_row_group_file = std::make_shared<io::MergeRangeFileReader>(
            profile, native_file, native_ranges, merge_read_slice_size);
    return true;
}

void ParquetFileContext::reset_random_access_ranges() {
    DORIS_CHECK(arrow_file != nullptr);
    static_cast<DorisRandomAccessFile*>(arrow_file.get())->reset_random_access_ranges();
    if (native_row_group_file != nullptr && native_row_group_file != native_file) {
        native_row_group_file->collect_profile_before_close();
    }
    native_row_group_file.reset();
}

ParquetPageCacheStats ParquetFileContext::page_cache_stats() const {
    if (arrow_file == nullptr) {
        return {};
    }
    return static_cast<const DorisRandomAccessFile*>(arrow_file.get())->page_cache_stats();
}

Status ParquetFileContext::close() {
    if (native_row_group_file != nullptr && native_row_group_file != native_file) {
        native_row_group_file->collect_profile_before_close();
    }
    native_row_group_file.reset();
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
    native_metadata = nullptr;
    native_metadata_owner.reset();
    native_meta_cache_handle = {};
    native_file.reset();
    native_io_ctx = nullptr;
    native_page_cache_enabled = false;
    native_page_cache_file_key.clear();
    return Status::OK();
}

} // namespace doris::format::parquet
