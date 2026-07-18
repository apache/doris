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

#pragma once

#include <arrow/io/interfaces.h>
#include <gen_cpp/parquet_types.h>
#include <parquet/api/reader.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "io/fs/file_reader.h"
#include "util/obj_lru_cache.h"

namespace doris::io {
struct FileDescription;
struct IOContext;
} // namespace doris::io

namespace doris {
class RuntimeProfile;
} // namespace doris

namespace doris::format::parquet {

struct NativeParquetPageIndex;

// V2-owned footer/schema tree. Production planning and decoding consume this object directly;
// Arrow metadata is intentionally not materialized from the serialized footer.
class NativeParquetMetadata {
public:
    NativeParquetMetadata(tparquet::FileMetaData metadata, size_t parsed_size);
    ~NativeParquetMetadata();

    Status init_schema(bool enable_mapping_varbinary, bool enable_mapping_timestamp_tz);
    const tparquet::FileMetaData& to_thrift() const { return _metadata; }
    const NativeFieldDescriptor& schema() const { return _schema; }
    size_t get_mem_size() const { return _parsed_size; }

private:
    tparquet::FileMetaData _metadata;
    NativeFieldDescriptor _schema;
    size_t _parsed_size = 0;
};

struct ParquetPageCacheRange {
    int64_t offset = 0;
    int64_t size = 0;

    int64_t end_offset() const { return offset + size; }
};

struct ParquetPageCacheReadPlanEntry {
    // The exact cached StoragePageCache entry. The final cache key is still exact-range based:
    // file key + cached_range.end_offset() + cached_range.offset.
    ParquetPageCacheRange cached_range;
    // Byte offset inside cached_range to start copying from.
    int64_t copy_offset_in_cache = 0;
    // Byte offset inside the current ReadAt output buffer to start writing to.
    int64_t output_offset = 0;
    int64_t copy_size = 0;
};

struct ParquetPageCacheStats {
    int64_t read_count = 0;
    int64_t write_count = 0;
    int64_t compressed_write_count = 0;
    int64_t hit_count = 0;
    int64_t miss_count = 0;
    int64_t compressed_hit_count = 0;
};

namespace detail {

class ParquetPageCacheRangeIndex {
public:
    static constexpr size_t DEFAULT_MAX_RANGES = 65536;

    explicit ParquetPageCacheRangeIndex(size_t max_ranges = DEFAULT_MAX_RANGES);

    void insert(ParquetPageCacheRange range);
    void erase(ParquetPageCacheRange range);

    std::vector<ParquetPageCacheRange> ranges() const;
    size_t size() const;

private:
    const size_t _max_ranges;
    mutable std::mutex _mutex;
    std::vector<ParquetPageCacheRange> _ranges;
};

class ParquetPageCacheRangeDirectory {
public:
    static constexpr size_t DEFAULT_MAX_FILES = 4096;

    explicit ParquetPageCacheRangeDirectory(size_t max_files = DEFAULT_MAX_FILES);

    std::shared_ptr<ParquetPageCacheRangeIndex> get_or_create(const std::string& file_key);
    size_t size() const;

private:
    const size_t _max_files;
    mutable std::mutex _mutex;
    std::unordered_map<std::string, std::shared_ptr<ParquetPageCacheRangeIndex>> _indexes;
};

inline constexpr int64_t MAX_SERIALIZED_PARQUET_INDEX_BYTES = 64LL << 20;

bool is_serialized_index_range_safe(size_t file_size, int64_t offset, int64_t length);

bool is_serialized_index_span_safe(int64_t span_offset, int64_t span_end);

// Build the copy plan for a ReadAt(position, nbytes) request from the range metadata of
// previously cached entries.
// StoragePageCache cannot do range lookup by itself; it can only lookup an exact key. The
// caller therefore keeps lightweight cached range metadata and uses this function to decide
// which exact cache entries to fetch and which byte spans to copy.
// Examples:
// 1. Subset hit:
//    request [120, 150), cached [100, 200) -> copy 30 bytes from cached offset 20.
// 2. Superset hit covered by multiple cached entries:
//    request [100, 260), cached [100, 180) and [180, 260)
//    -> two copies: [100, 180) to output offset 0, [180, 260) to output offset 80.
// 3. Partial overlap is a miss:
//    request [100, 260), cached [100, 180) only -> empty plan, caller reads from file.
std::vector<ParquetPageCacheReadPlanEntry> plan_page_cache_range_read(
        int64_t position, int64_t nbytes, const std::vector<ParquetPageCacheRange>& cached_ranges);

// Keep only byte ranges that are safe to hand to FileReader implementations. Parquet metadata is
// expected to contain non-negative offsets and positive compressed sizes, but tests and corrupted
// footers can still feed invalid values. Example: [100, 64) is kept, while [-1, 64), [100, 0) and
// an offset+size overflow are ignored.
std::vector<ParquetPageCacheRange> valid_prefetch_ranges(
        const std::vector<ParquetPageCacheRange>& ranges);

// Average projected column-chunk size for one row group. The v1 parquet path uses this value to
// decide whether a row group is dominated by small random IOs; v2 uses the same signal before
// installing MergeRangeFileReader. Example: chunks of 512KB and 1MB average below SMALL_IO and are
// good merge-reader candidates, while two 8MB chunks should stay on the raw random-access reader.
size_t average_prefetch_range_size(const std::vector<ParquetPageCacheRange>& ranges);

// Decide whether Arrow ReadAt() should be routed through MergeRangeFileReader for the current row
// group. This is intentionally stricter than the background warm-up path:
// - no valid projected chunks -> nothing to merge;
// - in-memory file readers already avoid remote random IO;
// - average chunk size >= MergeRangeFileReader::SMALL_IO would make merged reading wasteful.
bool should_use_merge_range_reader(const std::vector<ParquetPageCacheRange>& ranges,
                                   size_t avg_io_size, bool is_in_memory_reader);

// HTTP range servers may reject an overlong range near EOF even after accepting the capability
// probe. Staging a bounded small HTTP object also turns all native page reads into memory copies.
bool should_stage_small_http_file(std::string_view path, size_t file_size,
                                  size_t in_memory_file_size);

} // namespace detail

struct ParquetFileContext {
    // Native metadata, index, and data-page paths share Doris' FileReader without transferring
    // ownership to an external metadata tree.
    io::FileReaderSPtr native_file;
    // Row-group-scoped view of native_file. Small projected chunks use the same
    // MergeRangeFileReader policy as v1; large chunks and in-memory files keep native_file.
    io::FileReaderSPtr native_row_group_file;
    io::IOContext* native_io_ctx = nullptr;
    // V2-owned Thrift footer/schema used to construct native page/encoding readers. A cache hit is
    // owned by native_meta_cache_handle; a miss without cache is owned by native_metadata_owner.
    const NativeParquetMetadata* native_metadata = nullptr;
    std::unique_ptr<NativeParquetMetadata> native_metadata_owner;
    ObjLRUCache::CacheHandle native_meta_cache_handle;
    int64_t native_footer_read_calls = 0;
    int64_t native_footer_cache_hits = 0;
    bool native_page_cache_enabled = false;
    std::string native_page_cache_file_key;

    Status open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx, bool enable_page_cache,
                const io::FileDescription& file_description,
                bool enable_mapping_timestamp_tz = false);
    Status load_native_offset_indexes(
            int row_group_id, const std::unordered_set<int>& leaf_column_ids,
            std::unordered_map<int, tparquet::OffsetIndex>* offset_indexes) const;
    Status load_native_page_indexes(int row_group_id,
                                    const std::unordered_set<int>& leaf_column_ids,
                                    std::unordered_map<int, NativeParquetPageIndex>* page_indexes,
                                    int64_t* read_time = nullptr,
                                    int64_t* parse_time = nullptr) const;
    // Retained as a compatibility hook for callers; native readers admit exact page payloads.
    void register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges);
    // Best-effort asynchronous warm-up for Parquet column chunks. This only has an effect when
    // the underlying Doris file reader is a CachedRemoteFileReader; other readers keep the same
    // random-access behavior and simply skip prefetch.
    void prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                         const io::IOContext* io_ctx);
    // Deprecated adapter hook. Native readers use set_native_random_access_ranges().
    bool set_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                  size_t avg_io_size, RuntimeProfile* profile,
                                  int64_t merge_read_slice_size);
    // Install the v1-compatible MergeRangeFileReader on the native data-page path. Dictionary
    // probes must run before this method because their Arrow ReadAt order is independent of the
    // sequential projected chunk ranges consumed by MergeRangeFileReader.
    bool set_native_random_access_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                                         size_t avg_io_size, RuntimeProfile* profile,
                                         int64_t merge_read_slice_size);
    const io::FileReaderSPtr& native_data_file() const {
        return native_row_group_file != nullptr ? native_row_group_file : native_file;
    }
    // Restore native ReadAt() to the base Doris file reader and flush merge-reader counters.
    void reset_random_access_ranges();
    ParquetPageCacheStats page_cache_stats() const;
    Status close();
};

Status arrow_status_to_doris_status(const arrow::Status& status);

} // namespace doris::format::parquet
