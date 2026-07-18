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
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/reader/native/common.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "format_v2/parquet/reader/native/page_reader.h"
#include "util/slice.h"

namespace doris {
class BlockCompressionCodec;
class DataTypeSerDe;
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io

} // namespace doris

namespace doris::format::parquet::native {
using ::doris::ColumnString;

struct ColumnChunkRange {
    size_t offset = 0;
    size_t length = 0;
};

struct ParquetReaderCompat {
    bool parquet_816_padding = false;
    bool data_page_v2_always_compressed = false;
};

ParquetReaderCompat parquet_reader_compat(const std::string& created_by);
Status compute_column_chunk_range(const tparquet::ColumnMetaData& metadata, size_t file_size,
                                  bool parquet_816_padding, ColumnChunkRange* range);
bool validate_offset_index(const tparquet::OffsetIndex& index, const ColumnChunkRange& chunk_range,
                           int64_t data_page_offset, int64_t row_count);
bool can_prepare_page_cache_payload(bool session_cache_enabled, bool storage_cache_disabled,
                                    bool cache_available, bool header_available);

struct ColumnChunkReaderStatistics {
    int64_t decompress_time = 0;
    int64_t decompress_cnt = 0;
    int64_t decode_header_time = 0;
    int64_t decode_value_time = 0;
    int64_t materialization_time = 0;
    int64_t hybrid_selection_batches = 0;
    int64_t hybrid_selection_ranges = 0;
    int64_t hybrid_selection_null_fallback_batches = 0;
    int64_t decode_dict_time = 0;
    int64_t decode_level_time = 0;
    int64_t skip_page_header_num = 0;
    int64_t parse_page_header_num = 0;
    int64_t read_page_header_time = 0;
    int64_t page_read_counter = 0;
    int64_t data_page_read_counter = 0;
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
 *                                NativeFieldSchema* fieldSchema);
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
 *   chunk_reader.materialize_values(column, serde, context, state, selection);
 * }
 */
template <bool IN_COLLECTION, bool OFFSET_INDEX>
class ColumnChunkReader {
public:
    ColumnChunkReader(io::BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
                      NativeFieldSchema* field_schema, const tparquet::OffsetIndex* offset_index,
                      size_t total_row, io::IOContext* io_ctx,
                      const ParquetPageReadContext& page_read_ctx,
                      const ColumnChunkRange* chunk_range = nullptr);
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

    // Apply one logical selection to the encoded page. Decoder advances physical payload cursors;
    // SerDe interprets each selected raw span and materializes the destination Doris column.
    Status materialize_values(MutableColumnPtr& doris_column, const DataTypeSerDe& serde,
                              ParquetDecodeContext& context, ParquetMaterializationState& state,
                              ColumnSelectVector& select_vector);

    // Get the repetition level decoder of current page.
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }
    // Get the definition level decoder of current page.
    LevelDecoder& def_level_decoder() { return _def_level_decoder; }

    level_t max_rep_level() const { return _max_rep_level; }
    level_t max_def_level() const { return _max_def_level; }

    bool has_dict() const { return _has_dict; };

    // Get page decoder
    Decoder* get_page_decoder() { return _page_decoder; }

    void release_decoder_scratch(size_t max_retained_bytes) {
        for (auto& [encoding, decoder] : _decoders) {
            decoder->release_scratch(max_retained_bytes);
        }
        // Level decoders may batch-convert unsigned RLE values into Doris' signed level_t.
        _rep_level_decoder.release_scratch(max_retained_bytes);
        _def_level_decoder.release_scratch(max_retained_bytes);
    }

    size_t retained_decoder_scratch_bytes() const {
        size_t bytes = _rep_level_decoder.retained_scratch_bytes() +
                       _def_level_decoder.retained_scratch_bytes();
        for (const auto& [encoding, decoder] : _decoders) {
            bytes += decoder->retained_scratch_bytes();
        }
        return bytes;
    }

    size_t active_decoder_scratch_bytes() const {
        // Only the current encoding is active. Old decoder instances retain reusable capacity but
        // must not make the high-water policy treat their last batch as current working memory.
        return (_page_decoder == nullptr ? 0 : _page_decoder->active_scratch_bytes()) +
               _rep_level_decoder.active_scratch_bytes() +
               _def_level_decoder.active_scratch_bytes();
    }

    tparquet::Encoding::type current_encoding() const { return _current_encoding; }

    ColumnChunkReaderStatistics& chunk_statistics() {
        _chunk_statistics.decode_header_time = _page_reader->page_statistics().decode_header_time;
        _chunk_statistics.skip_page_header_num =
                _page_reader->page_statistics().skip_page_header_num;
        _chunk_statistics.parse_page_header_num =
                _page_reader->page_statistics().parse_page_header_num;
        _chunk_statistics.read_page_header_time =
                _page_reader->page_statistics().read_page_header_time;
        // PageReader statistics are already cumulative. Snapshot them instead of folding the same
        // totals into the chunk on every FileScannerV2 batch. Cache write counters are owned by
        // ColumnChunkReader because insertion happens after decompression and therefore remain in
        // the chunk accumulator above.
        _chunk_statistics.page_read_counter = _page_reader->page_statistics().page_read_counter;
        _chunk_statistics.data_page_read_counter =
                _page_reader->page_statistics().data_page_read_counter;
        _chunk_statistics.page_cache_hit_counter =
                _page_reader->page_statistics().page_cache_hit_counter;
        _chunk_statistics.page_cache_missing_counter =
                _page_reader->page_statistics().page_cache_missing_counter;
        _chunk_statistics.page_cache_compressed_hit_counter =
                _page_reader->page_statistics().page_cache_compressed_hit_counter;
        _chunk_statistics.page_cache_decompressed_hit_counter =
                _page_reader->page_statistics().page_cache_decompressed_hit_counter;
        return _chunk_statistics;
    }

    Decoder* dictionary_decoder() {
        return _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)].get();
    }

    size_t page_start_row() const { return _page_reader->start_row(); }

    size_t page_end_row() const { return _page_reader->end_row(); }

    Status parse_page_header();
    Status next_page();

    Status seek_to_nested_row(size_t left_row);
    // Decode level slots without materializing their values. `read_levels` is the flat-column
    // counterpart of load_page_nested_rows()+fill_def(): both advance definition/repetition and
    // value streams together so a levels-only consumer cannot desynchronize the next data page.
    Status read_levels(size_t num_values, std::vector<level_t>* rep_levels,
                       std::vector<level_t>* def_levels);
    Status skip_nested_values(const std::vector<level_t>& def_levels, size_t start_index = 0);
    Status fill_def(std::vector<level_t>& def_values) {
        auto before_sz = def_values.size();
        auto append_sz = _remaining_def_nums - _remaining_rep_nums;
        def_values.resize(before_sz + append_sz, 0);
        if (max_def_level() != 0) {
            auto ptr = def_values.data() + before_sz;
            const size_t decoded = _def_level_decoder.get_levels(ptr, append_sz);
            if (UNLIKELY(decoded != append_sz)) {
                def_values.resize(before_sz);
                return Status::Corruption(
                        "Parquet definition level stream ended after {} of {} slots", decoded,
                        append_sz);
            }
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

    Status _get_uncompressed_levels(const tparquet::DataPageHeaderV2& page_v2, Slice& page_data);
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
    NativeFieldSchema* _field_schema = nullptr;
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
    ColumnChunkRange _chunk_range;
    bool _has_validated_chunk_range = false;

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
    bool _nested_row_started = false;
    Decoder* _page_decoder = nullptr;
    tparquet::Encoding::type _current_encoding = tparquet::Encoding::PLAIN;
    // Map: encoding -> Decoder
    // Plain or Dictionary encoding. If the dictionary grows too big, the encoding will fall back to the plain encoding
    std::unordered_map<int, std::unique_ptr<Decoder>> _decoders;
    ColumnChunkReaderStatistics _chunk_statistics;
};

bool has_dict_page(const tparquet::ColumnMetaData& column);

} // namespace doris::format::parquet::native
