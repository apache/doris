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

namespace doris::vectorized {

ColumnChunkReader::ColumnChunkReader(BufferedStreamReader* reader,
                                     tparquet::ColumnChunk* column_chunk, FieldSchema* field_schema)
        : _field_schema(field_schema),
          _max_rep_level(field_schema->repetition_level),
          _max_def_level(field_schema->definition_level),
          _stream_reader(reader),
          _metadata(column_chunk->meta_data) {}

Status ColumnChunkReader::init() {
    size_t start_offset = _metadata.__isset.dictionary_page_offset
                                  ? _metadata.dictionary_page_offset
                                  : _metadata.data_page_offset;
    size_t chunk_size = _metadata.total_compressed_size;
    VLOG_DEBUG << "create _page_reader";
    _page_reader = std::make_unique<PageReader>(_stream_reader, start_offset, chunk_size);

    if (_metadata.__isset.dictionary_page_offset) {
        RETURN_IF_ERROR(_decode_dict_page());
    }
    // seek to the first data page
    _page_reader->seek_to_page(_metadata.data_page_offset);

    // get the block compression codec
    RETURN_IF_ERROR(get_block_compression_codec(_metadata.codec, _block_compress_codec));

    VLOG_DEBUG << "initColumnChunkReader finish";
    return Status::OK();
}

Status ColumnChunkReader::next_page() {
    RETURN_IF_ERROR(_page_reader->next_page_header());
    _remaining_num_values = _page_reader->get_page_header()->data_page_header.num_values;
    return Status::OK();
}

Status ColumnChunkReader::load_page_data() {
    const auto& header = *_page_reader->get_page_header();
    // int32_t compressed_size = header.compressed_page_size;
    int32_t uncompressed_size = header.uncompressed_page_size;

    if (_block_compress_codec != nullptr) {
        Slice compressed_data;
        RETURN_IF_ERROR(_page_reader->get_page_date(compressed_data));
        // check decompressed buffer size
        _reserve_decompress_buf(uncompressed_size);
        _page_data = Slice(_decompress_buf.get(), uncompressed_size);
        RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &_page_data));
    } else {
        RETURN_IF_ERROR(_page_reader->get_page_date(_page_data));
    }

    // Initialize repetition level and definition level. Skip when level = 0, which means required field.
    if (_max_rep_level > 0) {
        RETURN_IF_ERROR(_rep_level_decoder.init(&_page_data,
                                                header.data_page_header.repetition_level_encoding,
                                                _max_rep_level, _remaining_num_values));
    }
    if (_max_def_level > 0) {
        RETURN_IF_ERROR(_def_level_decoder.init(&_page_data,
                                                header.data_page_header.definition_level_encoding,
                                                _max_def_level, _remaining_num_values));
    }

    auto encoding = header.data_page_header.encoding;
    // change the deprecated encoding to RLE_DICTIONARY
    if (encoding == tparquet::Encoding::PLAIN_DICTIONARY) {
        encoding = tparquet::Encoding::RLE_DICTIONARY;
    }

    // Reuse page decoder
    if (_decoders.find(static_cast<int>(encoding)) != _decoders.end()) {
        _page_decoder = _decoders[static_cast<int>(encoding)].get();
    } else {
        std::unique_ptr<Decoder> page_decoder;
        Decoder::get_decoder(_metadata.type, encoding, page_decoder);
        _decoders[static_cast<int>(encoding)] = std::move(page_decoder);
        _page_decoder = _decoders[static_cast<int>(encoding)].get();
    }
    _page_decoder->set_data(&_page_data);
    // Set type length
    _page_decoder->set_type_length(_get_type_length());

    return Status::OK();
}

Status ColumnChunkReader::_decode_dict_page() {
    int64_t dict_offset = _metadata.dictionary_page_offset;
    _page_reader->seek_to_page(dict_offset);
    _page_reader->next_page_header();
    const tparquet::PageHeader& header = *_page_reader->get_page_header();
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header.type);
    // TODO(gaoxin): decode dictionary page
    return Status::OK();
}

void ColumnChunkReader::_reserve_decompress_buf(size_t size) {
    if (size > _decompress_buf_size) {
        _decompress_buf_size = BitUtil::next_power_of_two(size);
        _decompress_buf.reset(new uint8_t[_decompress_buf_size]);
    }
}

Status ColumnChunkReader::skip_values(size_t num_values) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Skip too many values in current page");
    }
    _remaining_num_values -= num_values;
    return _page_decoder->skip_values(num_values);
}

size_t ColumnChunkReader::get_rep_levels(level_t* levels, size_t n) {
    DCHECK_GT(_max_rep_level, 0);
    return _rep_level_decoder.get_levels(levels, n);
}

size_t ColumnChunkReader::get_def_levels(level_t* levels, size_t n) {
    DCHECK_GT(_max_def_level, 0);
    return _def_level_decoder.get_levels(levels, n);
}

Status ColumnChunkReader::decode_values(ColumnPtr& doris_column, DataTypePtr& data_type,
                                        size_t num_values) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Decode too many values in current page");
    }
    _remaining_num_values -= num_values;
    return _page_decoder->decode_values(doris_column, data_type, num_values);
}

Status ColumnChunkReader::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                        size_t num_values) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Decode too many values in current page");
    }
    _remaining_num_values -= num_values;
    return _page_decoder->decode_values(doris_column, data_type, num_values);
}

Status ColumnChunkReader::decode_values(Slice& slice, size_t num_values) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Decode too many values in current page");
    }
    _remaining_num_values -= num_values;
    return _page_decoder->decode_values(slice, num_values);
}

int32_t ColumnChunkReader::_get_type_length() {
    switch (_field_schema->physical_type) {
    case tparquet::Type::INT32:
    case tparquet::Type::FLOAT:
        return 4;
    case tparquet::Type::INT64:
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
