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

#include <gen_cpp/parquet_types.h>
#include <stdint.h>

#include <memory>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "olap/page_cache.h"
#include "util/block_compression.h"
#include "vec/exec/format/parquet/parquet_common.h"
namespace doris {
class BlockCompressionCodec;

namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io

} // namespace doris

namespace doris {
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io
struct Slice;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/**
 * Use to deserialize parquet page header, and get the page data in iterator interface.
 */

// Session-level options for parquet page reading/caching.
struct ParquetPageReadContext {
    bool enable_parquet_file_page_cache = true;
    std::string created_by; // Parquet file's created_by field for version checking
    ParquetPageReadContext() = default;
    ParquetPageReadContext(bool enable, const std::string& created_by_str = "")
            : enable_parquet_file_page_cache(enable), created_by(created_by_str) {}
};

template <bool IN_COLLECTION, bool OFFSET_INDEX>
class PageReader {
public:
    struct PageStatistics {
        int64_t decode_header_time = 0;
        int64_t skip_page_header_num = 0;
        int64_t parse_page_header_num = 0;
        int64_t read_page_header_time = 0;
        // page cache metrics
        // page_cache_hit_counter: number of times a cached page was used
        int64_t page_cache_hit_counter = 0;
        // page_cache_missing_counter: number of times a cached page was not found
        int64_t page_cache_missing_counter = 0;
        // page_cache_compressed_hit_counter: number of cache hits where the cached payload is compressed
        int64_t page_cache_compressed_hit_counter = 0;
        // page_cache_decompressed_hit_counter: number of cache hits where the cached payload is decompressed
        int64_t page_cache_decompressed_hit_counter = 0;
        // page_cache_write_counter: number of times this reader wrote an entry into the cache
        int64_t page_cache_write_counter = 0;
        // page_cache_compressed_write_counter: number of times this reader wrote compressed page into the cache
        int64_t page_cache_compressed_write_counter = 0;
        // page_cache_decompressed_write_counter: number of times this reader wrote decompressed page into the cache
        int64_t page_cache_decompressed_write_counter = 0;
        // page_read_counter: total pages read by this PageReader (includes cache hits and file reads)
        int64_t page_read_counter = 0;
    };

    PageReader(io::BufferedStreamReader* reader, io::IOContext* io_ctx, uint64_t offset,
               uint64_t length, size_t total_rows, const tparquet::ColumnMetaData& metadata,
               const tparquet::OffsetIndex* offset_index = nullptr,
               const ParquetPageReadContext& ctx = ParquetPageReadContext());
    ~PageReader() = default;

    bool has_next_page() const {
        if constexpr (OFFSET_INDEX) {
            return _page_index + 1 != _offset_index->page_locations.size();
        } else {
            // Deprecated
            // Parquet file may not be standardized,
            // _end_offset may exceed the actual data area.
            // ColumnChunkReader::has_next_page() use the number of parsed values for judgment
            // ref:https://github.com/duckdb/duckdb/issues/10829
            // [[deprecated]]
            LOG(FATAL) << "has_next_page should not be called when no offset index";
            return _offset < _end_offset;
        }
    }

    Status parse_page_header();

    Status next_page() {
        _page_statistics.skip_page_header_num += _state == INITIALIZED;
        if constexpr (OFFSET_INDEX) {
            _page_index++;
            _start_row = _offset_index->page_locations[_page_index].first_row_index;
            if (_page_index + 1 < _offset_index->page_locations.size()) {
                _end_row = _offset_index->page_locations[_page_index + 1].first_row_index;
            } else {
                _end_row = _total_rows;
            }
            int64_t next_page_offset = _offset_index->page_locations[_page_index].offset;
            _offset = next_page_offset;
            _next_header_offset = next_page_offset;
            _state = INITIALIZED;
        } else {
            if (UNLIKELY(_offset == _start_offset)) {
                return Status::Corruption("should parse first page.");
            }

            if (is_header_v2()) {
                _start_row += _cur_page_header.data_page_header_v2.num_rows;
            } else if constexpr (!IN_COLLECTION) {
                _start_row += _cur_page_header.data_page_header.num_values;
            }

            _offset = _next_header_offset;
            _state = INITIALIZED;
        }

        return Status::OK();
    }

    Status dict_next_page() {
        if constexpr (OFFSET_INDEX) {
            _state = INITIALIZED;
            return Status::OK();
        } else {
            return next_page();
        }
    }

    Status get_page_header(const tparquet::PageHeader** page_header) {
        if (UNLIKELY(_state != HEADER_PARSED)) {
            return Status::InternalError("Page header not parsed");
        }
        *page_header = &_cur_page_header;
        return Status::OK();
    }

    Status get_page_data(Slice& slice);

    // Skip page data and update offset (used when data is loaded from cache)
    void skip_page_data() {
        if (_state == HEADER_PARSED) {
            _offset += _cur_page_header.compressed_page_size;
            _state = DATA_LOADED;
        }
    }

    // Expose header bytes info for cache insertion
    uint32_t last_header_size() const { return _last_header_size; }
    const std::vector<uint8_t>& header_bytes() const { return _header_buf; }
    // header start offset for current page
    int64_t header_start_offset() const {
        return static_cast<int64_t>(_next_header_offset) - static_cast<int64_t>(_last_header_size) -
               static_cast<int64_t>(_cur_page_header.compressed_page_size);
    }
    uint64_t file_end_offset() const { return _end_offset; }
    bool cached_decompressed() const {
        return static_cast<double>(_cur_page_header.uncompressed_page_size) <=
               static_cast<double>(config::parquet_page_cache_decompress_threshold) *
                       static_cast<double>(_cur_page_header.compressed_page_size);
    }

    PageStatistics& page_statistics() { return _page_statistics; }

    bool is_header_v2() { return _cur_page_header.__isset.data_page_header_v2; }

    // Returns whether the current page's cache payload is decompressed
    bool is_cache_payload_decompressed() const { return _is_cache_payload_decompressed; }

    size_t start_row() const { return _start_row; }

    size_t end_row() const { return _end_row; }

    // Accessors for cache handle
    bool has_page_cache_handle() const { return _page_cache_handle.cache() != nullptr; }
    const doris::PageCacheHandle& page_cache_handle() const { return _page_cache_handle; }

    // Page cache members
    doris::PageCacheHandle _page_cache_handle;
    // stored header bytes when cache miss so we can insert header+payload into cache
    std::vector<uint8_t> _header_buf;
    // last parsed header size in bytes
    uint32_t _last_header_size = 0;

private:
    enum PageReaderState { INITIALIZED, HEADER_PARSED, DATA_LOADED };
    PageReaderState _state = INITIALIZED;
    PageStatistics _page_statistics;

    io::BufferedStreamReader* _reader = nullptr;
    io::IOContext* _io_ctx = nullptr;
    // current reader offset in file location.
    uint64_t _offset = 0;
    // this page offset in file location.
    uint64_t _start_offset = 0;
    uint64_t _end_offset = 0;
    uint64_t _next_header_offset = 0;
    // current page row range
    size_t _start_row = 0;
    size_t _end_row = 0;
    // total rows in this column chunk
    size_t _total_rows = 0;
    // Column metadata for this column chunk
    const tparquet::ColumnMetaData& _metadata;
    // for page index
    size_t _page_index = 0;
    const tparquet::OffsetIndex* _offset_index;

    // Session-level parquet page cache options
    ParquetPageReadContext _ctx;

    tparquet::PageHeader _cur_page_header;
    bool _is_cache_payload_decompressed = true;
};

template <bool IN_COLLECTION, bool OFFSET_INDEX>
std::unique_ptr<PageReader<IN_COLLECTION, OFFSET_INDEX>> create_page_reader(
        io::BufferedStreamReader* reader, io::IOContext* io_ctx, uint64_t offset, uint64_t length,
        size_t total_rows, const tparquet::ColumnMetaData& metadata,
        const tparquet::OffsetIndex* offset_index = nullptr,
        const ParquetPageReadContext& ctx = ParquetPageReadContext()) {
    return std::make_unique<PageReader<IN_COLLECTION, OFFSET_INDEX>>(
            reader, io_ctx, offset, length, total_rows, metadata, offset_index, ctx);
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
