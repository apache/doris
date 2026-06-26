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

#pragma once

#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"

namespace doris::io {
struct FileDescription;
} // namespace doris::io

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

// Build the copy plan for a ReadAt(position, nbytes) request from the range metadata of
// previously cached entries.
//
// StoragePageCache cannot do range lookup by itself; it can only lookup an exact key. The
// caller therefore keeps lightweight cached range metadata and uses this function to decide
// which exact cache entries to fetch and which byte spans to copy.
//
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

} // namespace detail

// Parquet 文件上下文 — 管理 Arrow 层文件对象和元数据的生命周期。
//
// 该类是 Doris 与 Arrow Parquet C++ library 的边界：
// - open():  将 Doris 的 io::FileReader 包装为 Arrow::RandomAccessFile，
//            然后用 Arrow 的 ParquetFileReader::Open() 解析 footer。
// - close(): 释放 Arrow 持有的文件句柄和 reader 资源。
//
// metadata 和 schema 在 open() 后可用，供 build_parquet_column_schema()、
// plan_parquet_row_groups() 等使用。ParquetColumnReaderFactory 通过
// file_reader->RowGroup(idx) 按需打开 RowGroupReader。
struct ParquetFileContext {
    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;   // Doris FileReader 的 Arrow 包装
    std::unique_ptr<::parquet::ParquetFileReader> file_reader; // Arrow Parquet 文件解析器
    std::shared_ptr<::parquet::FileMetaData> metadata;         // Footer metadata (RowGroup 信息)
    const ::parquet::SchemaDescriptor* schema = nullptr;       // 物理 leaf column schema

    Status open(io::FileReaderSPtr input_file_reader, io::IOContext* io_ctx, bool enable_page_cache,
                const io::FileDescription& file_description);
    // Register file ranges that belong to selected Parquet column chunks. Arrow still owns page
    // decoding, so v2 caches the serialized bytes read inside these ranges and excludes
    // footer/metadata reads that happen before registration.
    void register_page_cache_ranges(std::vector<ParquetPageCacheRange> ranges);
    ParquetPageCacheStats page_cache_stats() const;
    Status close();
};

Status arrow_status_to_doris_status(const arrow::Status& status);

} // namespace doris::format::parquet
