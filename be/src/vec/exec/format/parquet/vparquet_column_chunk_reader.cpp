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

#include "vparquet_column_chunk_reader.h"

#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <string.h>

#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/bit_util.h"
#include "util/block_compression.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/level_decoder.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_page_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace io {
class BufferedStreamReader;
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {

ColumnChunkReader::ColumnChunkReader(io::BufferedStreamReader* reader,
                                     tparquet::ColumnChunk* column_chunk, FieldSchema* field_schema,
                                     cctz::time_zone* ctz, io::IOContext* io_ctx)
        : _field_schema(field_schema),
          _max_rep_level(field_schema->repetition_level),
          _max_def_level(field_schema->definition_level),
          _stream_reader(reader),
          _metadata(column_chunk->meta_data),
          //          _ctz(ctz),
          _io_ctx(io_ctx) {}

Status ColumnChunkReader::init() {
    size_t start_offset = _metadata.__isset.dictionary_page_offset
                                  ? _metadata.dictionary_page_offset
                                  : _metadata.data_page_offset;
    size_t chunk_size = _metadata.total_compressed_size;
    _page_reader = std::make_unique<PageReader>(_stream_reader, _io_ctx, start_offset, chunk_size);
    // get the block compression codec
    RETURN_IF_ERROR(get_block_compression_codec(_metadata.codec, &_block_compress_codec));
    if (_metadata.__isset.dictionary_page_offset) {
        // seek to the directory page
        _page_reader->seek_to_page(_metadata.dictionary_page_offset);
        // Parse dictionary data when reading
        // RETURN_IF_ERROR(_page_reader->next_page_header());
        // RETURN_IF_ERROR(_decode_dict_page());
    } else {
        // seek to the first data page
        _page_reader->seek_to_page(_metadata.data_page_offset);
    }
    _state = INITIALIZED;
    return Status::OK();
}

Status ColumnChunkReader::next_page() {
    if (_state == HEADER_PARSED) {
        return Status::OK();
    }
    if (UNLIKELY(_state == NOT_INIT)) {
        return Status::Corruption("Should initialize chunk reader");
    }
    if (UNLIKELY(_remaining_num_values != 0)) {
        return Status::Corruption("Should skip current page");
    }
    RETURN_IF_ERROR(_page_reader->next_page_header());
    if (_page_reader->get_page_header()->type == tparquet::PageType::DICTIONARY_PAGE) {
        // the first page maybe directory page even if _metadata.__isset.dictionary_page_offset == false,
        // so we should parse the directory page in next_page()
        RETURN_IF_ERROR(_decode_dict_page());
        // parse the real first data page
        return next_page();
    } else if (_page_reader->get_page_header()->type == tparquet::PageType::DATA_PAGE_V2) {
        _remaining_num_values = _page_reader->get_page_header()->data_page_header_v2.num_values;
        _state = HEADER_PARSED;
        return Status::OK();
    } else {
        _remaining_num_values = _page_reader->get_page_header()->data_page_header.num_values;
        _state = HEADER_PARSED;
        return Status::OK();
    }
}

void ColumnChunkReader::_get_uncompressed_levels(const tparquet::DataPageHeaderV2& page_v2,
                                                 Slice& page_data) {
    int32_t rl = page_v2.repetition_levels_byte_length;
    int32_t dl = page_v2.definition_levels_byte_length;
    _v2_rep_levels = Slice(page_data.data, rl);
    _v2_def_levels = Slice(page_data.data + rl, dl);
    page_data.data += dl + rl;
    page_data.size -= dl + rl;
}

Status ColumnChunkReader::load_page_data() {
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::Corruption("Should parse page header");
    }
    const auto& header = *_page_reader->get_page_header();
    // int32_t compressed_size = header.compressed_page_size;
    int32_t uncompressed_size = header.uncompressed_page_size;

    if (_block_compress_codec != nullptr) {
        Slice compressed_data;
        RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
        if (header.__isset.data_page_header_v2) {
            tparquet::DataPageHeaderV2 header_v2 = header.data_page_header_v2;
            uncompressed_size -= header_v2.repetition_levels_byte_length +
                                 header_v2.definition_levels_byte_length;
            _get_uncompressed_levels(header_v2, compressed_data);
        }
        bool is_v2_compressed =
                header.__isset.data_page_header_v2 && header.data_page_header_v2.is_compressed;
        if (header.__isset.data_page_header || is_v2_compressed) {
            // check decompressed buffer size
            _reserve_decompress_buf(uncompressed_size);
            _page_data = Slice(_decompress_buf.get(), uncompressed_size);
            SCOPED_RAW_TIMER(&_statistics.decompress_time);
            _statistics.decompress_cnt++;
            RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &_page_data));
        } else {
            // Don't need decompress
            _page_data = Slice(compressed_data.data, compressed_data.size);
        }
    } else {
        RETURN_IF_ERROR(_page_reader->get_page_data(_page_data));
        if (header.__isset.data_page_header_v2) {
            tparquet::DataPageHeaderV2 header_v2 = header.data_page_header_v2;
            _get_uncompressed_levels(header_v2, _page_data);
        }
    }

    // Initialize repetition level and definition level. Skip when level = 0, which means required field.
    if (_max_rep_level > 0) {
        SCOPED_RAW_TIMER(&_statistics.decode_level_time);
        if (header.__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_rep_level_decoder.init_v2(_v2_rep_levels, _max_rep_level,
                                                       _remaining_num_values));
        } else {
            RETURN_IF_ERROR(_rep_level_decoder.init(
                    &_page_data, header.data_page_header.repetition_level_encoding, _max_rep_level,
                    _remaining_num_values));
        }
    }
    if (_max_def_level > 0) {
        SCOPED_RAW_TIMER(&_statistics.decode_level_time);
        if (header.__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_def_level_decoder.init_v2(_v2_def_levels, _max_def_level,
                                                       _remaining_num_values));
        } else {
            RETURN_IF_ERROR(_def_level_decoder.init(
                    &_page_data, header.data_page_header.definition_level_encoding, _max_def_level,
                    _remaining_num_values));
        }
    }
    auto encoding = header.__isset.data_page_header_v2 ? header.data_page_header_v2.encoding
                                                       : header.data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        encoding = tparquet::Encoding::RLE_DICTIONARY;
    }

    // Reuse page decoder
    if (_decoders.find(static_cast<int>(encoding)) != _decoders.end()) {
        _page_decoder = _decoders[static_cast<int>(encoding)].get();
    } else {
        std::unique_ptr<Decoder> page_decoder;
        RETURN_IF_ERROR(Decoder::get_decoder(_metadata.type, encoding, page_decoder));
        // Set type length
        page_decoder->set_type_length(_get_type_length());
        // Initialize the time convert context
        //        page_decoder->init(_field_schema, _ctz);
        _decoders[static_cast<int>(encoding)] = std::move(page_decoder);
        _page_decoder = _decoders[static_cast<int>(encoding)].get();
    }
    // Reset page data for each page
    _page_decoder->set_data(&_page_data);

    _state = DATA_LOADED;
    return Status::OK();
}

Status ColumnChunkReader::_decode_dict_page() {
    const tparquet::PageHeader& header = *_page_reader->get_page_header();
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header.type);
    SCOPED_RAW_TIMER(&_statistics.decode_dict_time);

    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    tparquet::Encoding::type dict_encoding = header.dictionary_page_header.encoding;
    if (dict_encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
        dict_encoding != tparquet::Encoding::PLAIN) {
        return Status::InternalError("Unsupported dictionary encoding {}",
                                     tparquet::to_string(dict_encoding));
    }

    // Prepare dictionary data
    int32_t uncompressed_size = header.uncompressed_page_size;
    std::unique_ptr<uint8_t[]> dict_data(new uint8_t[uncompressed_size]);
    if (_block_compress_codec != nullptr) {
        Slice compressed_data;
        RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
        Slice dict_slice(dict_data.get(), uncompressed_size);
        RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &dict_slice));
    } else {
        Slice dict_slice;
        RETURN_IF_ERROR(_page_reader->get_page_data(dict_slice));
        // The data is stored by BufferedStreamReader, we should copy it out
        memcpy(dict_data.get(), dict_slice.data, dict_slice.size);
    }

    // Cache page decoder
    std::unique_ptr<Decoder> page_decoder;
    RETURN_IF_ERROR(
            Decoder::get_decoder(_metadata.type, tparquet::Encoding::RLE_DICTIONARY, page_decoder));
    // Set type length
    page_decoder->set_type_length(_get_type_length());
    // Initialize the time convert context
    //    page_decoder->init(_field_schema, _ctz);
    // Set the dictionary data
    RETURN_IF_ERROR(page_decoder->set_dict(dict_data, uncompressed_size,
                                           header.dictionary_page_header.num_values));
    _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)] = std::move(page_decoder);

    _has_dict = true;
    return Status::OK();
}

void ColumnChunkReader::_reserve_decompress_buf(size_t size) {
    if (size > _decompress_buf_size) {
        _decompress_buf_size = BitUtil::next_power_of_two(size);
        _decompress_buf.reset(new uint8_t[_decompress_buf_size]);
    }
}

Status ColumnChunkReader::skip_values(size_t num_values, bool skip_data) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Skip too many values in current page. {} vs. {}",
                               _remaining_num_values, num_values);
    }
    _remaining_num_values -= num_values;
    if (skip_data) {
        SCOPED_RAW_TIMER(&_statistics.decode_value_time);
        return _page_decoder->skip_values(num_values);
    } else {
        return Status::OK();
    }
}

void ColumnChunkReader::insert_null_values(MutableColumnPtr& doris_column, size_t num_values) {
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    doris_column->insert_many_defaults(num_values);
    _remaining_num_values -= num_values;
}

size_t ColumnChunkReader::get_rep_levels(level_t* levels, size_t n) {
    DCHECK_GT(_max_rep_level, 0);
    return _rep_level_decoder.get_levels(levels, n);
}

size_t ColumnChunkReader::get_def_levels(level_t* levels, size_t n) {
    DCHECK_GT(_max_def_level, 0);
    return _def_level_decoder.get_levels(levels, n);
}

Status ColumnChunkReader::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                        ColumnSelectVector& select_vector, bool is_dict_filter) {
    if (select_vector.num_values() == 0) {
        return Status::OK();
    }
    SCOPED_RAW_TIMER(&_statistics.decode_value_time);
    if (UNLIKELY((doris_column->is_column_dictionary() || is_dict_filter) && !_has_dict)) {
        return Status::IOError("Not dictionary coded");
    }
    if (UNLIKELY(_remaining_num_values < select_vector.num_values())) {
        return Status::IOError("Decode too many values in current page");
    }
    _remaining_num_values -= select_vector.num_values();
    return _page_decoder->decode_values(doris_column, data_type, select_vector, is_dict_filter);
}

int32_t ColumnChunkReader::_get_type_length() {
    switch (_field_schema->physical_type) {
    case tparquet::Type::INT32:
        [[fallthrough]];
    case tparquet::Type::FLOAT:
        return 4;
    case tparquet::Type::INT64:
        [[fallthrough]];
    case tparquet::Type::DOUBLE:
        return 8;
    case tparquet::Type::INT96:
        return 12;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        return _field_schema->parquet_schema.type_length;
    default:
        return -1;
    }
}
} // namespace doris::vectorized