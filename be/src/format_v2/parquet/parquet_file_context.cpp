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

#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <limits>
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
            const auto* physical_field = _schema.get_physical_field(column_idx);
            if (chunk.meta_data.type != physical_field->physical_type) {
                return Status::Corruption(
                        "Parquet row group {} column {} physical type {} does not match schema {}",
                        row_group_idx, column_idx, tparquet::to_string(chunk.meta_data.type),
                        tparquet::to_string(physical_field->physical_type));
            }
            if (chunk.meta_data.num_values < 0) {
                return Status::Corruption(
                        "Parquet row group {} column {} has negative value count {}", row_group_idx,
                        column_idx, chunk.meta_data.num_values);
            }
            if (physical_field->repetition_level == 0 &&
                chunk.meta_data.num_values != row_group.num_rows) {
                // A flat leaf contributes one definition-level slot per row, including NULLs.
                // Reject a mismatched footer before metadata COUNT can treat it as cardinality.
                return Status::Corruption(
                        "Parquet row group {} flat column {} has {} values but {} rows",
                        row_group_idx, column_idx, chunk.meta_data.num_values, row_group.num_rows);
            }
        }
    }
    return Status::OK();
}

namespace detail {

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

} // namespace

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
