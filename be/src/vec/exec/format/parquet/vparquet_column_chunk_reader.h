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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "io/buffered_reader.h"
#include "level_decoder.h"
#include "parquet_common.h"
#include "schema_desc.h"
#include "util/block_compression.h"
#include "vparquet_page_reader.h"

namespace doris::vectorized {

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
 *   // Or, we can call chunk_reader.slip_values(num_values) to skip some values.
 *   chunk_reader.decode_values(slice, num_values);
 * }
 */
class ColumnChunkReader {
public:
    struct Statistics {
        int64_t decompress_time = 0;
        int64_t decompress_cnt = 0;
        int64_t decode_header_time = 0;
        int64_t decode_value_time = 0;
        int64_t decode_dict_time = 0;
        int64_t decode_level_time = 0;
    };

    ColumnChunkReader(BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
                      FieldSchema* field_schema, cctz::time_zone* ctz);
    ~ColumnChunkReader() = default;

    // Initialize chunk reader, will generate the decoder and codec.
    Status init();

    // Whether the chunk reader has a more page to read.
    bool has_next_page() { return _page_reader->has_next_page(); }

    // Seek to the specific page, page_header_offset must be the start offset of the page header.
    void seek_to_page(int64_t page_header_offset) {
        _remaining_num_values = 0;
        _page_reader->seek_to_page(page_header_offset);
        _state = INITIALIZED;
    }

    // Seek to next page. Only read and parse the page header.
    Status next_page();

    // Skip current page(will not read and parse) if the page is filtered by predicates.
    Status skip_page() {
        _remaining_num_values = 0;
        if (_state == HEADER_PARSED) {
            return _page_reader->skip_page();
        }
        if (_state != DATA_LOADED) {
            return Status::Corruption("Should parse page header to skip page");
        }
        return Status::OK();
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
    uint32_t remaining_num_values() const { return _remaining_num_values; };
    // null values are generated from definition levels
    // the caller should maintain the consistency after analyzing null values from definition levels.
    void insert_null_values(MutableColumnPtr& doris_column, size_t num_values);
    // Get the raw data of current page.
    Slice& get_page_data() { return _page_data; }

    // Get the repetition levels
    size_t get_rep_levels(level_t* levels, size_t n);
    // Get the definition levels
    size_t get_def_levels(level_t* levels, size_t n);

    // Decode values in current page into doris column.
    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector);

    // Get the repetition level decoder of current page.
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }
    // Get the definition level decoder of current page.
    LevelDecoder& def_level_decoder() { return _def_level_decoder; }

    level_t max_rep_level() const { return _max_rep_level; }
    level_t max_def_level() const { return _max_def_level; }

    // Get page decoder
    Decoder* get_page_decoder() { return _page_decoder; }

    Statistics& statistics() {
        _statistics.decode_header_time = _page_reader->statistics().decode_header_time;
        return _statistics;
    }

private:
    enum ColumnChunkReaderState { NOT_INIT, INITIALIZED, HEADER_PARSED, DATA_LOADED };

    Status _decode_dict_page();
    void _reserve_decompress_buf(size_t size);
    int32_t _get_type_length();

    ColumnChunkReaderState _state = NOT_INIT;
    FieldSchema* _field_schema;
    level_t _max_rep_level;
    level_t _max_def_level;
    tparquet::LogicalType _parquet_logical_type;

    BufferedStreamReader* _stream_reader;
    tparquet::ColumnMetaData _metadata;
    cctz::time_zone* _ctz;

    std::unique_ptr<PageReader> _page_reader = nullptr;
    BlockCompressionCodec* _block_compress_codec = nullptr;

    LevelDecoder _rep_level_decoder;
    LevelDecoder _def_level_decoder;
    uint32_t _remaining_num_values = 0;
    Slice _page_data;
    std::unique_ptr<uint8_t[]> _decompress_buf;
    size_t _decompress_buf_size = 0;
    Decoder* _page_decoder = nullptr;
    // Map: encoding -> Decoder
    // Plain or Dictionary encoding. If the dictionary grows too big, the encoding will fall back to the plain encoding
    std::unordered_map<int, std::unique_ptr<Decoder>> _decoders;
    Statistics _statistics;
};

} // namespace doris::vectorized
