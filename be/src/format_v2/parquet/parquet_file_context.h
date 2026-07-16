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
#include <parquet/api/reader.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "format/parquet/vparquet_file_metadata.h"
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

} // namespace detail

struct ParquetFileContext {
    // The native data-page path reads from Doris' FileReader directly. Keep this handle separate
    // from the Arrow RandomAccessFile used by the metadata/index migration path so opening Arrow
    // metadata never transfers ownership away from the native reader.
    io::FileReaderSPtr native_file;
    // Row-group-scoped view of native_file. Small projected chunks use the same
    // MergeRangeFileReader policy as v1; large chunks and in-memory files keep native_file.
    io::FileReaderSPtr native_row_group_file;
    io::IOContext* native_io_ctx = nullptr;
    // V1-compatible Thrift footer/schema used to construct Doris' native page/encoding readers.
    // A cache hit is owned by native_meta_cache_handle; a miss without cache is owned by
    // native_metadata_owner.
    const FileMetaData* native_metadata = nullptr;
    std::unique_ptr<FileMetaData> native_metadata_owner;
    ObjLRUCache::CacheHandle native_meta_cache_handle;
    int64_t native_footer_read_calls = 0;
    int64_t native_footer_cache_hits = 0;
    bool native_page_cache_enabled = false;

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;   // Arrow wrapper for Doris FileReader
    std::unique_ptr<::parquet::ParquetFileReader> file_reader; // Arrow Parquet file parser
    std::shared_ptr<::parquet::FileMetaData> metadata;   // footer metadata (RowGroup information)
    const ::parquet::SchemaDescriptor* schema = nullptr; // physical leaf column schema

    Status open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx, bool enable_page_cache,
                const io::FileDescription& file_description,
                bool enable_mapping_timestamp_tz = false);
    Status load_native_offset_indexes(
            int row_group_id, const std::unordered_set<int>& leaf_column_ids,
            std::unordered_map<int, tparquet::OffsetIndex>* offset_indexes) const;
    // Register ranges for the remaining Arrow metadata/index adapter. Native data pages use the
    // v1-compatible page cache owned by BufferedFileStreamReader instead.
    void register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges);
    // Best-effort asynchronous warm-up for Parquet column chunks. This only has an effect when
    // the underlying Doris file reader is a CachedRemoteFileReader; other readers keep the same
    // random-access behavior and simply skip prefetch.
    void prefetch_ranges(const std::vector<ParquetPageCacheRange>& ranges,
                         const io::IOContext* io_ctx);
    // Switch the active reader used by Arrow metadata/index ReadAt() to MergeRangeFileReader when
    // projected chunks are small random IOs. Native page decoding is intentionally independent.
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
    // Restore both Arrow and native ReadAt() to the base Doris file reader and flush active
    // merge-reader counters. Row-group setup uses this before dictionary-page probes.
    void reset_random_access_ranges();
    ParquetPageCacheStats page_cache_stats() const;
    Status close();
};

Status arrow_status_to_doris_status(const arrow::Status& status);

} // namespace doris::format::parquet
