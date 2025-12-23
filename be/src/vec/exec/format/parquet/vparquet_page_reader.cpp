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

#include "vparquet_page_reader.h"

#include <gen_cpp/parquet_types.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/fs/buffered_reader.h"
#include "olap/page_cache.h"
#include "parquet_common.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "util/thrift_util.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
static constexpr size_t INIT_PAGE_HEADER_SIZE = 128;

// // Check if the file was created by a version that always marks pages as compressed
// // regardless of the actual compression state (parquet-cpp < 2.0.0)
// static bool _is_always_compressed(const ParquetPageReadContext& ctx) {
//     if (ctx.created_by.empty()) {
//         return false;
//     }

//     // Parse the version string
//     std::unique_ptr<ParsedVersion> parsed_version;
//     Status status = VersionParser::parse(ctx.created_by, &parsed_version);
//     if (!status.ok()) {
//         return false;
//     }

//     // Check if it's parquet-cpp
//     if (parsed_version->application() != "parquet-cpp") {
//         return false;
//     }

//     // Check if version < 2.0.0
//     if (!parsed_version->version().has_value()) {
//         return false;
//     }

//     std::unique_ptr<SemanticVersion> semantic_version;
//     status = SemanticVersion::parse(parsed_version->version().value(), &semantic_version);
//     if (!status.ok()) {
//         return false;
//     }

//     // parquet-cpp versions < 2.0.0 always report compressed
//     static const SemanticVersion PARQUET_CPP_FIXED_VERSION(2, 0, 0);
//     return semantic_version->compare_to(PARQUET_CPP_FIXED_VERSION) < 0;
// }

template <bool IN_COLLECTION, bool OFFSET_INDEX>
PageReader<IN_COLLECTION, OFFSET_INDEX>::PageReader(io::BufferedStreamReader* reader,
                                                    io::IOContext* io_ctx, uint64_t offset,
                                                    uint64_t length, size_t total_rows,
                                                    const tparquet::ColumnMetaData& metadata,
                                                    const tparquet::OffsetIndex* offset_index,
                                                    const ParquetPageReadContext& ctx)
        : _reader(reader),
          _io_ctx(io_ctx),
          _offset(offset),
          _start_offset(offset),
          _end_offset(offset + length),
          _total_rows(total_rows),
          _metadata(metadata),
          _offset_index(offset_index),
          _ctx(ctx) {
    _next_header_offset = _offset;
    _state = INITIALIZED;

    if constexpr (OFFSET_INDEX) {
        _end_row = _offset_index->page_locations.size() >= 2
                           ? _offset_index->page_locations[1].first_row_index
                           : _total_rows;
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status PageReader<IN_COLLECTION, OFFSET_INDEX>::parse_page_header() {
    if (_state == HEADER_PARSED) {
        return Status::OK();
    }
    if (UNLIKELY(_offset < _start_offset || _offset >= _end_offset)) {
        return Status::IOError("Out-of-bounds Access");
    }
    if (UNLIKELY(_offset != _next_header_offset)) {
        return Status::IOError("Wrong header position, should seek to a page header first");
    }
    if (UNLIKELY(_state != INITIALIZED)) {
        return Status::IOError("Should skip or load current page to get next page");
    }

    _page_statistics.page_read_counter += 1;

    // Parse page header from file; header bytes are saved for possible cache insertion
    const uint8_t* page_header_buf = nullptr;
    size_t max_size = _end_offset - _offset;
    size_t header_size = std::min(INIT_PAGE_HEADER_SIZE, max_size);
    const size_t MAX_PAGE_HEADER_SIZE = config::parquet_header_max_size_mb << 20;
    uint32_t real_header_size = 0;

    // Try a header-only lookup in the page cache. Cached pages store
    // header + optional v2 levels + uncompressed payload, so we can
    // parse the page header directly from the cached bytes and avoid
    // a file read for the header.
    if (_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
        StoragePageCache::instance() != nullptr) {
        PageCacheHandle handle;
        StoragePageCache::CacheKey key(fmt::format("{}::{}", _reader->path(), _reader->mtime()),
                                       _end_offset, _offset);
        if (StoragePageCache::instance()->lookup(key, &handle, segment_v2::DATA_PAGE)) {
            // Parse header directly from cached data
            _page_cache_handle = std::move(handle);
            Slice s = _page_cache_handle.data();
            real_header_size = cast_set<uint32_t>(s.size);
            SCOPED_RAW_TIMER(&_page_statistics.decode_header_time);
            auto st = deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(s.data),
                                             &real_header_size, true, &_cur_page_header);
            if (!st.ok()) return st;
            // Increment page cache counters for a true cache hit on header+payload
            _page_statistics.page_cache_hit_counter += 1;
            // Detect whether the cached payload is compressed or decompressed and record
            // the appropriate counter. Cached layout is: header | optional levels | payload

            // Determine if payload is compressed using V2 is_compressed field if available
            // bool payload_is_compressed = true;
            // if (_cur_page_header.type == tparquet::PageType::DATA_PAGE_V2) {
            //     const auto& page_header_v2 = _cur_page_header.data_page_header_v2;
            //     if (page_header_v2.__isset.is_compressed) {
            //         payload_is_compressed = page_header_v2.is_compressed;
            //     }
            // }

            // // ARROW-17100: [C++][Parquet] Fix backwards compatibility for ParquetV2 data pages written prior to 3.0.0 per ARROW-10353 #13665
            // // https://github.com/apache/arrow/pull/13665/files
            // // Prior to parquet-cpp version 2.0.0, is_compressed was always set to false in column headers,
            // // even if compression was used. See ARROW-17100.
            // bool always_compressed = _is_always_compressed(_ctx);
            // payload_is_compressed |= always_compressed;

            // // Apply codec check: if codec is UNCOMPRESSED, payload cannot be compressed
            // payload_is_compressed = payload_is_compressed &&
            //                         (_metadata.codec != tparquet::CompressionCodec::UNCOMPRESSED);

            // // Save the computed result for use by ColumnChunkReader
            // _payload_is_compressed = payload_is_compressed;

            bool is_cache_payload_decompressed = true;
            if (_cur_page_header.compressed_page_size > 0) {
                is_cache_payload_decompressed =
                        (_metadata.codec == tparquet::CompressionCodec::UNCOMPRESSED) ||
                        (static_cast<double>(_cur_page_header.uncompressed_page_size) <=
                         static_cast<double>(config::parquet_page_cache_decompress_threshold) *
                                 static_cast<double>(_cur_page_header.compressed_page_size));
            }

            if (is_cache_payload_decompressed) {
                _page_statistics.page_cache_decompressed_hit_counter += 1;
            } else {
                _page_statistics.page_cache_compressed_hit_counter += 1;
            }

            _is_cache_payload_decompressed = is_cache_payload_decompressed;

            if constexpr (OFFSET_INDEX == false) {
                if (is_header_v2()) {
                    _end_row = _start_row + _cur_page_header.data_page_header_v2.num_rows;
                } else if constexpr (!IN_COLLECTION) {
                    _end_row = _start_row + _cur_page_header.data_page_header.num_values;
                }
            }

            // Save header bytes for later use (e.g., to insert updated cache entries)
            _header_buf.assign(s.data, s.data + real_header_size);
            _last_header_size = real_header_size;
            _page_statistics.parse_page_header_num++;
            _offset += real_header_size;
            _next_header_offset = _offset + _cur_page_header.compressed_page_size;
            _state = HEADER_PARSED;
            return Status::OK();
        } else {
            _page_statistics.page_cache_missing_counter += 1;
            // Clear any existing cache handle on miss to avoid holding stale handle
            _page_cache_handle = PageCacheHandle();
        }
    }
    // NOTE: page cache lookup for *decompressed* page data is handled in
    // ColumnChunkReader::load_page_data(). PageReader should only be
    // responsible for parsing the header bytes from the file and saving
    // them in `_header_buf` for possible later insertion into the cache.
    while (true) {
        if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
            return Status::EndOfFile("stop");
        }
        header_size = std::min(header_size, max_size);
        {
            SCOPED_RAW_TIMER(&_page_statistics.read_page_header_time);
            RETURN_IF_ERROR(_reader->read_bytes(&page_header_buf, _offset, header_size, _io_ctx));
        }
        real_header_size = cast_set<uint32_t>(header_size);
        SCOPED_RAW_TIMER(&_page_statistics.decode_header_time);
        auto st =
                deserialize_thrift_msg(page_header_buf, &real_header_size, true, &_cur_page_header);
        if (st.ok()) {
            break;
        }
        if (_offset + header_size >= _end_offset || real_header_size > MAX_PAGE_HEADER_SIZE) {
            return Status::IOError(
                    "Failed to deserialize parquet page header. offset: {}, "
                    "header size: {}, end offset: {}, real header size: {}",
                    _offset, header_size, _end_offset, real_header_size);
        }
        header_size <<= 2;
    }

    if constexpr (OFFSET_INDEX == false) {
        if (is_header_v2()) {
            _end_row = _start_row + _cur_page_header.data_page_header_v2.num_rows;
        } else if constexpr (!IN_COLLECTION) {
            _end_row = _start_row + _cur_page_header.data_page_header.num_values;
        }
    }

    // Save header bytes for possible cache insertion later
    _header_buf.assign(page_header_buf, page_header_buf + real_header_size);
    _last_header_size = real_header_size;
    _page_statistics.parse_page_header_num++;
    _offset += real_header_size;
    _next_header_offset = _offset + _cur_page_header.compressed_page_size;
    _state = HEADER_PARSED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status PageReader<IN_COLLECTION, OFFSET_INDEX>::get_page_data(Slice& slice) {
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::IOError("Should generate page header first to load current page data");
    }
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop");
    }
    slice.size = _cur_page_header.compressed_page_size;
    RETURN_IF_ERROR(_reader->read_bytes(slice, _offset, _io_ctx));
    _offset += slice.size;
    _state = DATA_LOADED;
    return Status::OK();
}

template class PageReader<true, true>;
template class PageReader<true, false>;
template class PageReader<false, true>;
template class PageReader<false, false>;
#include "common/compile_check_end.h"

} // namespace doris::vectorized
