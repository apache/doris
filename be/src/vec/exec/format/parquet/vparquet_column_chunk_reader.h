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
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vparquet_page_reader.h"

namespace doris::vectorized {

/**
 * Read and decode parquet column data into doris block column.
 * <p>Usage:</p>
 * // Create chunk reader
 * ColumnChunkReader chunk_reader(BufferedStreamReader* reader,
 *                                tparquet::ColumnChunk* column_chunk,
 *                                FieldSchema* fieldSchema);
 * // Initialize chunk reader, we can set the type length if the length of column type is fixed.
 * // If not set, default value = -1, then the decoder will infer the type length.
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
    ColumnChunkReader(BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
                      FieldSchema* fieldSchema);
    ~ColumnChunkReader() = default;

    // Initialize chunk reader, will generate the decoder and codec.
    // We can set the type_length if the length of colum type if fixed,
    // or not set, the decoder will try to infer the type_length.
    Status init(size_t type_length = -1);

    // Whether the chunk reader has a more page to read.
    bool has_next_page() { return _page_reader->has_next_page(); }

    // Seek to the specific page, page_header_offset must be the start offset of the page header.
    void seek_to_page(int64_t page_header_offset) {
        _page_reader->seek_to_page(page_header_offset);
    }

    // Seek to next page. Only read and parse the page header.
    Status next_page();

    // Skip current page(will not read and parse) if the page is filtered by predicates.
    Status skip_page() { return _page_reader->skip_page(); }
    // Skip some values(will not read and parse) in current page if the values are filtered by predicates.
    Status skip_values(size_t num_values);

    // Load page data into the underlying container,
    // and initialize the repetition and definition level decoder for current page data.
    Status load_page_data();
    // The remaining number of values in current page. Decreased when reading or skipping.
    uint32_t num_values() const { return _num_values; };
    // Get the raw data of current page.
    Slice& get_page_data() { return _page_data; }

    // Get the repetition levels
    size_t get_rep_levels(level_t* levels, size_t n);
    // Get the definition levels
    size_t get_def_levels(level_t* levels, size_t n);

    // Decode values in current page into doris column.
    Status decode_values(ColumnPtr& doris_column, size_t num_values);
    // For test, Decode values in current page into slice.
    Status decode_values(Slice& slice, size_t num_values);

    // Get the repetition level decoder of current page.
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }
    // Get the definition level decoder of current page.
    LevelDecoder& def_level_decoder() { return _def_level_decoder; }

private:
    Status _decode_dict_page();
    void _reserve_decompress_buf(size_t size);

    level_t _max_rep_level;
    level_t _max_def_level;

    BufferedStreamReader* _stream_reader;
    // tparquet::ColumnChunk* _column_chunk;
    tparquet::ColumnMetaData& _metadata;
    // FieldSchema* _field_schema;

    std::unique_ptr<PageReader> _page_reader = nullptr;
    std::unique_ptr<BlockCompressionCodec> _block_compress_codec = nullptr;

    LevelDecoder _rep_level_decoder;
    LevelDecoder _def_level_decoder;
    uint32_t _num_values = 0;
    Slice _page_data;
    std::unique_ptr<uint8_t[]> _decompress_buf;
    size_t _decompress_buf_size = 0;
    std::unique_ptr<Decoder> _page_decoder = nullptr;
    size_t _type_length = -1;
};

} // namespace doris::vectorized
