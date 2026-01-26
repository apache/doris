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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "decoder.h"
#include "level_decoder.h"
#include "util/slice.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vparquet_page_reader.h"

namespace doris {
class BlockCompressionCodec;

namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io

} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
struct FieldSchema;
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;

struct ColumnChunkReaderStatistics {
    int64_t decompress_time = 0;
    int64_t decompress_cnt = 0;
    int64_t decode_header_time = 0;
    int64_t decode_value_time = 0;
    int64_t decode_dict_time = 0;
    int64_t decode_level_time = 0;
    int64_t skip_page_header_num = 0;
    int64_t parse_page_header_num = 0;
    int64_t read_page_header_time = 0;
    int64_t page_read_counter = 0;
    int64_t page_cache_write_counter = 0;
    int64_t page_cache_compressed_write_counter = 0;
    int64_t page_cache_decompressed_write_counter = 0;
    int64_t page_cache_hit_counter = 0;
    int64_t page_cache_missing_counter = 0;
    int64_t page_cache_compressed_hit_counter = 0;
    int64_t page_cache_decompressed_hit_counter = 0;
};

/**
 * Read and decode parquet column data into doris block column.
 * <p>Usage:</p>
 * // Create chunk reader
 * ColumnChunkReader chunk_reader(BufferedStreamReader* reader,
 *                                tparquet::ColumnChunk* column_chunk,
 *                                FieldSchema* fieldSchema);
 * // Initialize chunk reader
 * chunk_reader.init();
 * while (chunk_reader.has_next_page()) {
 *   // Seek to next page header.  Only read and parse the page header, not page data.
 *   chunk_reader.next_page();
 *   // Load data to decoder. Load the page data into underlying container.
 *   // Or, we can call the chunk_reader.skip_page() to skip current page.
 *   chunk_reader.load_page_data();
 *   // Decode values into column or slice.
 *   // Or, we can call chunk_reader.skip_values(num_values) to skip some values.
 *   chunk_reader.decode_values(slice, num_values);
 * }
 */
template <bool IN_COLLECTION, bool OFFSET_INDEX>
class ColumnChunkReader {
public:
    ColumnChunkReader(io::BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
                      FieldSchema* field_schema, const tparquet::OffsetIndex* offset_index,
                      size_t total_row, io::IOContext* io_ctx,
                      const ParquetPageReadContext& page_read_ctx);
    ~ColumnChunkReader() = default;

    // Initialize chunk reader, will generate the decoder and codec.
    Status init();

    // Whether the chunk reader has a more page to read.
    bool has_next_page() const {
        if constexpr (OFFSET_INDEX) {
            return _page_reader->has_next_page();
        } else {
            // no offset need parse all page header.
            return _chunk_parsed_values < _metadata.num_values;
        }
    }

    // Skip some values(will not read and parse) in current page if the values are filtered by predicates.
    // when skip_data = false, the underlying decoder will not skip data,
    // only used when maintaining the consistency of _remaining_num_values.
    Status skip_values(size_t num_values, bool skip_data = true);

    // Load page data into the underlying container,
    // and initialize the repetition and definition level decoder for current page data.
    Status load_page_data();
    Status load_page_data_idempotent() {
        if (_state == DATA_LOADED) {
            return Status::OK();
        }
        return load_page_data();
    }
    // The remaining number of values in current page(including null values). Decreased when reading or skipping.
    uint32_t remaining_num_values() const { return _remaining_num_values; }

    // Decode values in current page into doris column.
    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter);

    // Get the repetition level decoder of current page.
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }
    // Get the definition level decoder of current page.
    LevelDecoder& def_level_decoder() { return _def_level_decoder; }

    level_t max_rep_level() const { return _max_rep_level; }
    level_t max_def_level() const { return _max_def_level; }

    bool has_dict() const { return _has_dict; };

    // Get page decoder
    Decoder* get_page_decoder() { return _page_decoder; }

    ColumnChunkReaderStatistics& chunk_statistics() {
        _chunk_statistics.decode_header_time = _page_reader->page_statistics().decode_header_time;
        _chunk_statistics.skip_page_header_num =
                _page_reader->page_statistics().skip_page_header_num;
        _chunk_statistics.parse_page_header_num =
                _page_reader->page_statistics().parse_page_header_num;
        _chunk_statistics.read_page_header_time =
                _page_reader->page_statistics().read_page_header_time;
        _chunk_statistics.page_read_counter += _page_reader->page_statistics().page_read_counter;
        _chunk_statistics.page_cache_write_counter +=
                _page_reader->page_statistics().page_cache_write_counter;
        _chunk_statistics.page_cache_compressed_write_counter +=
                _page_reader->page_statistics().page_cache_compressed_write_counter;
        _chunk_statistics.page_cache_decompressed_write_counter +=
                _page_reader->page_statistics().page_cache_decompressed_write_counter;
        _chunk_statistics.page_cache_hit_counter +=
                _page_reader->page_statistics().page_cache_hit_counter;
        _chunk_statistics.page_cache_missing_counter +=
                _page_reader->page_statistics().page_cache_missing_counter;
        _chunk_statistics.page_cache_compressed_hit_counter +=
                _page_reader->page_statistics().page_cache_compressed_hit_counter;
        _chunk_statistics.page_cache_decompressed_hit_counter +=
                _page_reader->page_statistics().page_cache_decompressed_hit_counter;
        return _chunk_statistics;
    }

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) {
        return _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)]
                ->read_dict_values_to_column(doris_column);
    }

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) {
        return _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)]
                ->convert_dict_column_to_string_column(dict_column);
    }

    size_t page_start_row() const { return _page_reader->start_row(); }

    size_t page_end_row() const { return _page_reader->end_row(); }

    Status parse_page_header();
    Status next_page();

    Status seek_to_nested_row(size_t left_row);
    Status skip_nested_values(const std::vector<level_t>& def_levels);
    Status fill_def(std::vector<level_t>& def_values) {
        auto before_sz = def_values.size();
        auto append_sz = _remaining_def_nums - _remaining_rep_nums;
        def_values.resize(before_sz + append_sz, 0);
        if (max_def_level() != 0) {
            auto ptr = def_values.data() + before_sz;
            _def_level_decoder.get_levels(ptr, append_sz);
        }
        _remaining_def_nums -= append_sz;
        return Status::OK();
    }

    Status load_page_nested_rows(std::vector<level_t>& rep_levels, size_t max_rows,
                                 size_t* result_rows, bool* cross_page);
    Status load_cross_page_nested_row(std::vector<level_t>& rep_levels, bool* cross_page);

    Slice get_page_data() const { return _page_data; }
    const Slice& v2_rep_levels() const { return _v2_rep_levels; }
    const Slice& v2_def_levels() const { return _v2_def_levels; }
    ColumnChunkReaderStatistics& statistics() { return chunk_statistics(); }

private:
    enum ColumnChunkReaderState { NOT_INIT, INITIALIZED, HEADER_PARSED, DATA_LOADED, PAGE_SKIPPED };

    // for check dict page.
    Status _parse_first_page_header();
    Status _decode_dict_page();

    void _reserve_decompress_buf(size_t size);
    int32_t _get_type_length();
    void _insert_page_into_cache(const std::vector<uint8_t>& level_bytes, const Slice& payload);

    void _get_uncompressed_levels(const tparquet::DataPageHeaderV2& page_v2, Slice& page_data);
    Status _skip_nested_rows_in_page(size_t num_rows);

    level_t _rep_level_get_next() {
        if constexpr (IN_COLLECTION) {
            return _rep_level_decoder.get_next();
        }
        return 0;
    }

    void _rep_level_rewind_one() {
        if constexpr (IN_COLLECTION) {
            _rep_level_decoder.rewind_one();
        }
    }

    ColumnChunkReaderState _state = NOT_INIT;
    FieldSchema* _field_schema = nullptr;
    const level_t _max_rep_level;
    const level_t _max_def_level;

    io::BufferedStreamReader* _stream_reader = nullptr;
    tparquet::ColumnMetaData _metadata;
    const tparquet::OffsetIndex* _offset_index = nullptr;
    size_t _current_row = 0;
    size_t _total_rows = 0;
    io::IOContext* _io_ctx = nullptr;

    std::unique_ptr<PageReader<IN_COLLECTION, OFFSET_INDEX>> _page_reader;
    BlockCompressionCodec* _block_compress_codec = nullptr;

    ParquetPageReadContext _page_read_ctx;

    LevelDecoder _rep_level_decoder;
    LevelDecoder _def_level_decoder;
    size_t _chunk_parsed_values = 0;
    // this page remaining rep/def nums
    // if max_rep_level = 0 / max_def_level = 0, this value retail hava value.
    uint32_t _remaining_rep_nums = 0;
    uint32_t _remaining_def_nums = 0;
    // this page remaining values to be processed (for read/skip).
    // need parse this page header.
    uint32_t _remaining_num_values = 0;

    Slice _page_data;
    DorisUniqueBufferPtr<uint8_t> _decompress_buf;
    size_t _decompress_buf_size = 0;
    Slice _v2_rep_levels;
    Slice _v2_def_levels;
    bool _dict_checked = false;
    bool _has_dict = false;
    Decoder* _page_decoder = nullptr;
    // Map: encoding -> Decoder
    // Plain or Dictionary encoding. If the dictionary grows too big, the encoding will fall back to the plain encoding
    std::unordered_map<int, std::unique_ptr<Decoder>> _decoders;
    ColumnChunkReaderStatistics _chunk_statistics;
};
#include "common/compile_check_end.h"

bool has_dict_page(const tparquet::ColumnMetaData& column);

} // namespace doris::vectorized
