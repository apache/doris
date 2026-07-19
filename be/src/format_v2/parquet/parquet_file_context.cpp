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
#include <parquet/metadata.h>
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
#include "format_v2/parquet/parquet_statistics.h"
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
#include "util/coding.h"
#include "util/slice.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris::format::parquet {

constexpr size_t V2_PARQUET_FOOTER_SIZE = 8;

NativeParquetMetadata::NativeParquetMetadata(tparquet::FileMetaData metadata, size_t parsed_size)
        : _metadata(std::move(metadata)), _parsed_size(parsed_size) {
    ExecEnv::GetInstance()->parquet_meta_tracker()->consume(get_mem_size());
}

NativeParquetMetadata::~NativeParquetMetadata() {
    ExecEnv::GetInstance()->parquet_meta_tracker()->release(get_mem_size());
}

Status NativeParquetMetadata::init_schema(bool enable_mapping_varbinary,
                                          bool enable_mapping_timestamp_tz) {
    _schema.set_enable_mapping_varbinary(enable_mapping_varbinary);
    _schema.set_enable_mapping_timestamp_tz(enable_mapping_timestamp_tz);
    RETURN_IF_ERROR(_schema.parse_from_thrift(_metadata.schema));
    // Native readers address projected leaves by stable DFS IDs. Assign them only on the private
    // v2 schema object so v1's cached schema lifecycle and numbering remain untouched.
    _schema.assign_ids();
    for (size_t row_group_idx = 0; row_group_idx < _metadata.row_groups.size(); ++row_group_idx) {
        const auto& row_group = _metadata.row_groups[row_group_idx];
        if (row_group.num_rows < 0) {
            return Status::Corruption("Parquet row group {} has negative row count {}",
                                      row_group_idx, row_group.num_rows);
        }
        if (row_group.columns.size() != _schema.physical_fields_size()) {
            // All v2 planners index chunks by the native DFS leaf order, so validate cardinality
            // once before any projection, prefetch, or decoder can perform indexed access.
            return Status::Corruption(
                    "Parquet row group {} has {} column chunks but schema has {} physical fields",
                    row_group_idx, row_group.columns.size(), _schema.physical_fields_size());
        }
        for (size_t column_idx = 0; column_idx < row_group.columns.size(); ++column_idx) {
            const auto& chunk = row_group.columns[column_idx];
            if (!chunk.__isset.meta_data) {
                return Status::Corruption("Parquet row group {} column {} has no metadata",
                                          row_group_idx, column_idx);
            }
            if (chunk.meta_data.type != _schema.get_physical_field(column_idx)->physical_type) {
                return Status::Corruption(
                        "Parquet row group {} column {} physical type {} does not match schema {}",
                        row_group_idx, column_idx, tparquet::to_string(chunk.meta_data.type),
                        tparquet::to_string(_schema.get_physical_field(column_idx)->physical_type));
            }
        }
    }
    return Status::OK();
}

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

Status validate_native_footer_size(uint32_t serialized_size, size_t file_size,
                                   size_t metadata_size_limit) {
    if (file_size < V2_PARQUET_FOOTER_SIZE ||
        serialized_size > file_size - V2_PARQUET_FOOTER_SIZE) {
        return Status::Corruption("Parquet v2 footer size {} exceeds file size {}", serialized_size,
                                  file_size);
    }
    if (serialized_size > metadata_size_limit) {
        return Status::Corruption("Parquet v2 footer size {} exceeds metadata limit {}",
                                  serialized_size, metadata_size_limit);
    }
    return Status::OK();
}

std::string build_native_file_cache_key(std::string_view fs_name, std::string_view path,
                                        int64_t description_mtime, int64_t reader_mtime,
                                        int64_t description_file_size, int64_t reader_file_size,
                                        bool is_immutable) {
    const int64_t mtime = description_mtime != 0 ? description_mtime : reader_mtime;
    if (mtime == 0 && !is_immutable) {
        // Unknown version is not a stable identity: an overwrite can preserve path and size while
        // changing both footer semantics and page bytes.
        return {};
    }
    const int64_t file_size = description_file_size >= 0 ? description_file_size : reader_file_size;
    return fmt::format("{}::{}::mtime={}::size={}", fs_name, path, mtime, file_size);
}

bool is_serialized_index_range_safe(size_t file_size, int64_t offset, int64_t length) {
    if (offset < 0 || length <= 0 || length > MAX_SERIALIZED_PARQUET_INDEX_BYTES ||
        static_cast<uint64_t>(offset) > file_size) {
        return false;
    }
    return static_cast<uint64_t>(length) <= file_size - static_cast<uint64_t>(offset);
}

bool is_serialized_index_span_safe(int64_t span_offset, int64_t span_end) {
    return span_offset >= 0 && span_end >= span_offset &&
           span_end - span_offset <= MAX_SERIALIZED_PARQUET_INDEX_BYTES;
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

bool should_stage_small_http_file(std::string_view path, size_t file_size,
                                  size_t in_memory_file_size) {
    return file_size <= in_memory_file_size &&
           (path.starts_with("http://") || path.starts_with("https://"));
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

constexpr uint8_t V2_PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};
constexpr size_t V2_INITIAL_FOOTER_READ_SIZE = 48 * 1024;

Status parse_native_parquet_footer(io::FileReaderSPtr file,
                                   std::unique_ptr<NativeParquetMetadata>* metadata,
                                   size_t* footer_size, io::IOContext* io_ctx,
                                   bool enable_mapping_varbinary,
                                   bool enable_mapping_timestamp_tz) {
    DORIS_CHECK(file != nullptr);
    DORIS_CHECK(metadata != nullptr);
    DORIS_CHECK(footer_size != nullptr);
    const size_t file_size = file->size();
    if (file_size < V2_PARQUET_FOOTER_SIZE) {
        return Status::Corruption("Parquet v2 file is too small for a footer: {}", file_size);
    }

    const size_t tail_size = std::min(file_size, V2_INITIAL_FOOTER_READ_SIZE);
    std::vector<uint8_t> tail(tail_size);
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file->read_at(file_size - tail_size, Slice(tail.data(), tail.size()),
                                  &bytes_read, io_ctx));
    if (bytes_read != tail.size()) {
        return Status::Corruption("Short Parquet v2 footer read: expected {}, got {}", tail.size(),
                                  bytes_read);
    }
    const auto* magic = tail.data() + tail.size() - sizeof(V2_PARQUET_MAGIC);
    if (memcmp(magic, V2_PARQUET_MAGIC, sizeof(V2_PARQUET_MAGIC)) != 0) {
        return Status::Corruption("Invalid Parquet v2 footer magic in {}", file->path().native());
    }

    const uint32_t serialized_size =
            decode_fixed32_le(tail.data() + tail.size() - V2_PARQUET_FOOTER_SIZE);
    // The configured Thrift message ceiling also bounds this file-controlled allocation. Keep the
    // check before both allocation and the optional second read so a sparse file cannot force a
    // process-sized metadata buffer merely by advertising a large footer.
    const size_t metadata_size_limit =
            static_cast<size_t>(std::max(config::thrift_max_message_size, 0));
    RETURN_IF_ERROR(
            detail::validate_native_footer_size(serialized_size, file_size, metadata_size_limit));
    std::vector<uint8_t> serialized_metadata(serialized_size);
    if (serialized_size <= tail.size() - V2_PARQUET_FOOTER_SIZE) {
        const auto* metadata_start =
                tail.data() + tail.size() - V2_PARQUET_FOOTER_SIZE - serialized_size;
        memcpy(serialized_metadata.data(), metadata_start, serialized_size);
    } else {
        bytes_read = 0;
        RETURN_IF_ERROR(file->read_at(file_size - V2_PARQUET_FOOTER_SIZE - serialized_size,
                                      Slice(serialized_metadata.data(), serialized_metadata.size()),
                                      &bytes_read, io_ctx));
        if (bytes_read != serialized_metadata.size()) {
            return Status::Corruption("Short Parquet v2 metadata read: expected {}, got {}",
                                      serialized_metadata.size(), bytes_read);
        }
    }

    uint32_t thrift_size = serialized_size;
    tparquet::FileMetaData thrift_metadata;
    RETURN_IF_ERROR(deserialize_thrift_msg(serialized_metadata.data(), &thrift_size, true,
                                           &thrift_metadata));
    auto parsed =
            std::make_unique<NativeParquetMetadata>(std::move(thrift_metadata), serialized_size);
    RETURN_IF_ERROR(parsed->init_schema(enable_mapping_varbinary, enable_mapping_timestamp_tz));
    *footer_size = V2_PARQUET_FOOTER_SIZE + serialized_size;
    *metadata = std::move(parsed);
    return Status::OK();
}

std::string build_page_cache_file_key(const io::FileReader& file_reader,
                                      const io::FileDescription& file_description) {
    return detail::build_native_file_cache_key(
            file_description.fs_name, file_reader.path().native(), file_description.mtime,
            file_reader.mtime(), file_description.file_size,
            static_cast<int64_t>(file_reader.size()), file_description.is_immutable);
}

class DorisRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    [[maybe_unused]] DorisRandomAccessFile(io::FileReaderSPtr file_reader, io::IOContext* io_ctx,
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

    [[maybe_unused]] void register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges) {
        std::lock_guard lock(_page_cache_mutex);
        _page_cache_ranges = std::move(ranges);
    }

    [[maybe_unused]] void prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
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

    [[maybe_unused]] bool set_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
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

    [[maybe_unused]] void reset_random_access_ranges() { reset_active_file_reader(); }

    [[maybe_unused]] ParquetPageCacheStats page_cache_stats() const {
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
                                bool enable_mapping_timestamp_tz, bool enable_mapping_varbinary) {
    DORIS_CHECK(input_file_reader != nullptr);
    if (detail::should_stage_small_http_file(input_file_reader->path().native(),
                                             input_file_reader->size(),
                                             config::in_memory_file_size)) {
        // A metadata-cache hit can make the first physical read start inside a tiny HTTP file.
        // Read it from byte zero once so EOF-range quirks cannot make warm scans less reliable
        // than cold scans, while keeping this compatibility policy entirely inside v2.
        native_file = std::make_shared<io::InMemoryFileReader>(std::move(input_file_reader));
    } else {
        native_file = std::move(input_file_reader);
    }
    native_io_ctx = io_ctx;

    // Footer and page bytes must use the same stable identity. In particular, fs_name separates
    // identical HDFS paths from different nameservices, while an unknown mutable version bypasses
    // both caches rather than reusing an overwritten file of the same size.
    auto* meta_cache = ExecEnv::GetInstance()->file_meta_cache();
    auto meta_cache_key = build_page_cache_file_key(*native_file, file_description);
    const bool has_stable_meta_cache_identity = !meta_cache_key.empty();
    if (has_stable_meta_cache_identity) {
        // The discriminator prevents a v1 metadata value from being cast as the v2-owned type;
        // schema mapping flags participate because they change the cached native schema tree.
        meta_cache_key.append("\0v2", 3);
        meta_cache_key.push_back(static_cast<char>(enable_mapping_varbinary));
        meta_cache_key.push_back(static_cast<char>(enable_mapping_timestamp_tz));
    }
    size_t native_footer_size = 0;
    if (has_stable_meta_cache_identity && meta_cache != nullptr && meta_cache->enabled() &&
        meta_cache->lookup(meta_cache_key, &native_meta_cache_handle)) {
        native_metadata = native_meta_cache_handle.data<NativeParquetMetadata>();
        ++native_footer_cache_hits;
    } else {
        RETURN_IF_ERROR(parse_native_parquet_footer(
                native_file, &native_metadata_owner, &native_footer_size, io_ctx,
                enable_mapping_varbinary, enable_mapping_timestamp_tz));
        ++native_footer_read_calls;
        if (has_stable_meta_cache_identity && meta_cache != nullptr && meta_cache->enabled()) {
            meta_cache->insert(meta_cache_key, native_metadata_owner.release(),
                               &native_meta_cache_handle);
            native_metadata = native_meta_cache_handle.data<NativeParquetMetadata>();
        } else {
            native_metadata = native_metadata_owner.get();
        }
    }
    DORIS_CHECK(native_metadata != nullptr);

    auto page_cache_file_key = build_page_cache_file_key(*native_file, file_description);
    native_page_cache_enabled = enable_page_cache && !page_cache_file_key.empty();
    // Native page readers use the FileDescription-derived immutable identity directly.
    native_page_cache_file_key = page_cache_file_key;
    return Status::OK();
}

Status ParquetFileContext::load_native_offset_indexes(
        int row_group_id, const std::unordered_set<int>& leaf_column_ids,
        std::unordered_map<int, tparquet::OffsetIndex>* offset_indexes) const {
    DORIS_CHECK(offset_indexes != nullptr);
    offset_indexes->clear();
    if (leaf_column_ids.empty()) {
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
        for (const int leaf_column_id : leaf_column_ids) {
            if (leaf_column_id < 0 ||
                leaf_column_id >= static_cast<int>(native_row_group.columns.size())) {
                return Status::Corruption("Invalid Parquet leaf {} for OffsetIndex",
                                          leaf_column_id);
            }
            const auto& column_chunk = native_row_group.columns[leaf_column_id];
            if (!column_chunk.__isset.offset_index_offset ||
                !column_chunk.__isset.offset_index_length ||
                column_chunk.offset_index_length <= 0) {
                continue;
            }
            const int64_t index_offset = column_chunk.offset_index_offset;
            const int64_t index_length = column_chunk.offset_index_length;
            if (!detail::is_serialized_index_range_safe(native_file->size(), index_offset,
                                                        index_length)) {
                // OffsetIndex is optional. A malformed range must not allocate from untrusted
                // footer values or redirect the native reader outside the file.
                continue;
            }
            std::vector<uint8_t> serialized_index(static_cast<size_t>(index_length));
            Slice index_slice(serialized_index.data(), serialized_index.size());
            size_t bytes_read = 0;
            if (!native_file->read_at(index_offset, index_slice, &bytes_read, native_io_ctx).ok() ||
                bytes_read != serialized_index.size()) {
                continue;
            }
            uint32_t thrift_length = static_cast<uint32_t>(serialized_index.size());
            tparquet::OffsetIndex native_index;
            if (!deserialize_thrift_msg(serialized_index.data(), &thrift_length, true,
                                        &native_index)
                         .ok() ||
                native_index.page_locations.empty()) {
                continue;
            }
            native::ColumnChunkRange chunk_range;
            RETURN_IF_ERROR(native::compute_column_chunk_range(
                    native_row_group.columns[leaf_column_id].meta_data, native_file->size(),
                    compat.parquet_816_padding, &chunk_range));
            if (!native::validate_offset_index(
                        native_index, chunk_range,
                        native_row_group.columns[leaf_column_id].meta_data.data_page_offset,
                        native_row_group.num_rows)) {
                // OffsetIndex is optional. Reject the complete index instead of letting one bad
                // location redirect an indexed reader outside its owning column chunk.
                continue;
            }
            offset_indexes->emplace(leaf_column_id, std::move(native_index));
        }
    } catch (const std::exception&) {
        // OffsetIndex is optional. Selected logical ranges still enforce correctness, while the
        // native reader conservatively falls back to sequential page traversal.
        offset_indexes->clear();
    }
    return Status::OK();
}

Status ParquetFileContext::load_native_page_indexes(
        int row_group_id, const std::unordered_set<int>& leaf_column_ids,
        std::unordered_map<int, NativeParquetPageIndex>* page_indexes, int64_t* read_time,
        int64_t* parse_time) const {
    DORIS_CHECK(page_indexes != nullptr);
    page_indexes->clear();
    if (leaf_column_ids.empty()) {
        return Status::OK();
    }
    const auto& thrift_metadata = native_metadata->to_thrift();
    if (row_group_id < 0 || row_group_id >= static_cast<int>(thrift_metadata.row_groups.size())) {
        return Status::Corruption("Invalid Parquet row group {} for PageIndex", row_group_id);
    }
    const auto& row_group = thrift_metadata.row_groups[row_group_id];
    const auto compat = native::parquet_reader_compat(
            thrift_metadata.__isset.created_by ? thrift_metadata.created_by : "");

    struct SerializedIndexRange {
        int leaf_column_id;
        int64_t offset;
        int64_t length;
    };
    struct PendingPageIndex {
        NativeParquetPageIndex indexes;
        bool has_column_index = false;
        bool has_offset_index = false;
    };
    std::vector<SerializedIndexRange> column_index_ranges;
    std::vector<SerializedIndexRange> offset_index_ranges;
    std::unordered_map<int, PendingPageIndex> pending_indexes;

    auto valid_index_range = [&](int64_t offset, int64_t length) {
        return detail::is_serialized_index_range_safe(native_file->size(), offset, length);
    };

    for (const int leaf_column_id : leaf_column_ids) {
        if (leaf_column_id < 0 || leaf_column_id >= static_cast<int>(row_group.columns.size())) {
            return Status::Corruption("Invalid Parquet leaf {} for PageIndex", leaf_column_id);
        }
        const auto& chunk = row_group.columns[leaf_column_id];
        if (!chunk.__isset.column_index_offset || !chunk.__isset.column_index_length ||
            !chunk.__isset.offset_index_offset || !chunk.__isset.offset_index_length) {
            continue;
        }
        if (!valid_index_range(chunk.column_index_offset, chunk.column_index_length) ||
            !valid_index_range(chunk.offset_index_offset, chunk.offset_index_length)) {
            continue;
        }
        column_index_ranges.push_back(
                {leaf_column_id, chunk.column_index_offset, chunk.column_index_length});
        offset_index_ranges.push_back(
                {leaf_column_id, chunk.offset_index_offset, chunk.offset_index_length});
        pending_indexes.try_emplace(leaf_column_id);
    }

    auto read_coalesced_indexes = [&](std::vector<SerializedIndexRange>* ranges,
                                      bool column_index) {
        std::sort(ranges->begin(), ranges->end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.offset < rhs.offset; });
        size_t range_begin = 0;
        while (range_begin < ranges->size()) {
            size_t range_end = range_begin + 1;
            int64_t span_end = (*ranges)[range_begin].offset + (*ranges)[range_begin].length;
            while (range_end < ranges->size() && (*ranges)[range_end].offset <= span_end) {
                span_end = std::max(span_end,
                                    (*ranges)[range_end].offset + (*ranges)[range_end].length);
                ++range_end;
            }

            const int64_t span_offset = (*ranges)[range_begin].offset;
            if (!detail::is_serialized_index_span_safe(span_offset, span_end)) {
                // Optional indexes share one allocation per contiguous footer block. Skipping the
                // whole block prevents many individually small ranges from bypassing the budget.
                range_begin = range_end;
                continue;
            }
            const int64_t span_length = span_end - span_offset;
            std::vector<uint8_t> serialized(static_cast<size_t>(span_length));
            Slice slice(serialized.data(), serialized.size());
            size_t bytes_read = 0;
            Status read_status;
            int64_t read_time_sink = 0;
            {
                SCOPED_RAW_TIMER(read_time == nullptr ? &read_time_sink : read_time);
                read_status = native_file->read_at(span_offset, slice, &bytes_read, native_io_ctx);
            }
            if (read_status.ok() && bytes_read == serialized.size()) {
                for (size_t i = range_begin; i < range_end; ++i) {
                    const auto& range = (*ranges)[i];
                    auto pending = pending_indexes.find(range.leaf_column_id);
                    if (pending == pending_indexes.end()) {
                        continue;
                    }
                    uint32_t thrift_length = static_cast<uint32_t>(range.length);
                    const auto* thrift_data =
                            serialized.data() + static_cast<size_t>(range.offset - span_offset);
                    int64_t parse_time_sink = 0;
                    SCOPED_RAW_TIMER(parse_time == nullptr ? &parse_time_sink : parse_time);
                    if (column_index) {
                        pending->second.has_column_index =
                                deserialize_thrift_msg(thrift_data, &thrift_length, true,
                                                       &pending->second.indexes.column_index)
                                        .ok();
                    } else {
                        pending->second.has_offset_index =
                                deserialize_thrift_msg(thrift_data, &thrift_length, true,
                                                       &pending->second.indexes.offset_index)
                                        .ok();
                    }
                }
            }
            range_begin = range_end;
        }
    };

    // Parquet writers place each index kind in a contiguous block. Reading overlapping/adjacent
    // ranges as one span keeps cold small-file planning from paying two remote round trips per
    // projected leaf, while refusing gaps avoids amplifying reads from untrusted footer offsets.
    read_coalesced_indexes(&column_index_ranges, true);
    read_coalesced_indexes(&offset_index_ranges, false);

    for (auto& [leaf_column_id, pending] : pending_indexes) {
        const auto& chunk = row_group.columns[leaf_column_id];
        auto& indexes = pending.indexes;
        if (!pending.has_column_index || !pending.has_offset_index ||
            indexes.column_index.null_pages.size() != indexes.offset_index.page_locations.size()) {
            continue;
        }
        native::ColumnChunkRange chunk_range;
        RETURN_IF_ERROR(native::compute_column_chunk_range(
                chunk.meta_data, native_file->size(), compat.parquet_816_padding, &chunk_range));
        if (!native::validate_offset_index(indexes.offset_index, chunk_range,
                                           chunk.meta_data.data_page_offset, row_group.num_rows)) {
            continue;
        }
        page_indexes->emplace(leaf_column_id, std::move(indexes));
    }
    return Status::OK();
}

void ParquetFileContext::prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                         const io::IOContext* io_ctx) {
    io::FileReaderSPtr reader = native_file;
    if (auto tracing_reader = std::dynamic_pointer_cast<io::TracingFileReader>(reader)) {
        reader = tracing_reader->inner_reader();
    }
    auto cached_reader = std::dynamic_pointer_cast<io::CachedRemoteFileReader>(reader);
    if (cached_reader == nullptr) {
        return;
    }
    const auto* prefetch_io_ctx = io_ctx != nullptr ? io_ctx : native_io_ctx;
    for (const auto& range : detail::valid_prefetch_ranges(ranges)) {
        cached_reader->prefetch_range(cast_set<size_t>(range.offset), cast_set<size_t>(range.size),
                                      prefetch_io_ctx);
    }
}

bool ParquetFileContext::set_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                                  size_t avg_io_size, RuntimeProfile* profile,
                                                  int64_t merge_read_slice_size) {
    (void)ranges;
    (void)avg_io_size;
    (void)profile;
    (void)merge_read_slice_size;
    return false;
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
    if (native_row_group_file != nullptr && native_row_group_file != native_file) {
        native_row_group_file->collect_profile_before_close();
    }
    native_row_group_file.reset();
}

ParquetPageCacheStats ParquetFileContext::page_cache_stats() const {
    return {};
}

Status ParquetFileContext::close() {
    if (native_row_group_file != nullptr && native_row_group_file != native_file) {
        native_row_group_file->collect_profile_before_close();
    }
    native_row_group_file.reset();
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
