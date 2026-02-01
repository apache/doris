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

#include <cstdint>
#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "io/fs/buffered_reader.h"
#include "olap/page_cache.h"
#include "util/bit_util.h"
#include "util/block_compression.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/common/custom_allocator.h"
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
#include "common/compile_check_begin.h"
template <bool IN_COLLECTION, bool OFFSET_INDEX>
ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::ColumnChunkReader(
        io::BufferedStreamReader* reader, tparquet::ColumnChunk* column_chunk,
        FieldSchema* field_schema, const tparquet::OffsetIndex* offset_index, size_t total_rows,
        io::IOContext* io_ctx, const ParquetPageReadContext& page_read_ctx)
        : _field_schema(field_schema),
          _max_rep_level(field_schema->repetition_level),
          _max_def_level(field_schema->definition_level),
          _stream_reader(reader),
          _metadata(column_chunk->meta_data),
          _offset_index(offset_index),
          _total_rows(total_rows),
          _io_ctx(io_ctx),
          _page_read_ctx(page_read_ctx) {}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::init() {
    size_t start_offset = has_dict_page(_metadata) ? _metadata.dictionary_page_offset
                                                   : _metadata.data_page_offset;
    size_t chunk_size = _metadata.total_compressed_size;
    // create page reader
    _page_reader = create_page_reader<IN_COLLECTION, OFFSET_INDEX>(
            _stream_reader, _io_ctx, start_offset, chunk_size, _total_rows, _metadata,
            _page_read_ctx, _offset_index);
    // get the block compression codec
    RETURN_IF_ERROR(get_block_compression_codec(_metadata.codec, &_block_compress_codec));
    _state = INITIALIZED;
    RETURN_IF_ERROR(_parse_first_page_header());
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::skip_nested_values(
        const std::vector<level_t>& def_levels) {
    size_t no_value_cnt = 0;
    size_t value_cnt = 0;

    for (size_t idx = 0; idx < def_levels.size(); idx++) {
        level_t def_level = def_levels[idx];
        if (IN_COLLECTION && def_level < _field_schema->repeated_parent_def_level) {
            no_value_cnt++;
        } else if (def_level < _field_schema->definition_level) {
            no_value_cnt++;
        } else {
            value_cnt++;
        }
    }

    RETURN_IF_ERROR(skip_values(value_cnt, true));
    RETURN_IF_ERROR(skip_values(no_value_cnt, false));
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_parse_first_page_header() {
    RETURN_IF_ERROR(parse_page_header());

    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    if (header->type == tparquet::PageType::DICTIONARY_PAGE) {
        // the first page maybe directory page even if _metadata.__isset.dictionary_page_offset == false,
        // so we should parse the directory page in next_page()
        RETURN_IF_ERROR(_decode_dict_page());
        // parse the real first data page
        RETURN_IF_ERROR(_page_reader->dict_next_page());
        _state = INITIALIZED;
    }

    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::parse_page_header() {
    if (_state == HEADER_PARSED || _state == DATA_LOADED) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_page_reader->parse_page_header());

    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    int32_t page_num_values = _page_reader->is_header_v2() ? header->data_page_header_v2.num_values
                                                           : header->data_page_header.num_values;
    _remaining_rep_nums = page_num_values;
    _remaining_def_nums = page_num_values;
    _remaining_num_values = page_num_values;

    // no offset will parse all header.
    if constexpr (OFFSET_INDEX == false) {
        _chunk_parsed_values += _remaining_num_values;
    }
    _state = HEADER_PARSED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::next_page() {
    _state = INITIALIZED;
    RETURN_IF_ERROR(_page_reader->next_page());
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_get_uncompressed_levels(
        const tparquet::DataPageHeaderV2& page_v2, Slice& page_data) {
    int32_t rl = page_v2.repetition_levels_byte_length;
    int32_t dl = page_v2.definition_levels_byte_length;
    _v2_rep_levels = Slice(page_data.data, rl);
    _v2_def_levels = Slice(page_data.data + rl, dl);
    page_data.data += dl + rl;
    page_data.size -= dl + rl;
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_page_data() {
    if (_state == DATA_LOADED) {
        return Status::OK();
    }
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::Corruption("Should parse page header");
    }

    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    int32_t uncompressed_size = header->uncompressed_page_size;
    bool page_loaded = false;

    // First, try to reuse a cache handle previously discovered by PageReader
    // (header-only lookup) to avoid a second lookup here.
    if (_page_read_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
        StoragePageCache::instance() != nullptr) {
        if (_page_reader->has_page_cache_handle()) {
            const PageCacheHandle& handle = _page_reader->page_cache_handle();
            Slice cached = handle.data();
            size_t header_size = _page_reader->header_bytes().size();
            size_t levels_size = 0;
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                levels_size = rl + dl;
                _v2_rep_levels =
                        Slice(reinterpret_cast<const uint8_t*>(cached.data) + header_size, rl);
                _v2_def_levels =
                        Slice(reinterpret_cast<const uint8_t*>(cached.data) + header_size + rl, dl);
            }
            // payload_slice points to the bytes after header and levels
            Slice payload_slice(cached.data + header_size + levels_size,
                                cached.size - header_size - levels_size);

            bool cache_payload_is_decompressed = _page_reader->is_cache_payload_decompressed();

            if (cache_payload_is_decompressed) {
                // Cached payload is already uncompressed
                _page_data = payload_slice;
            } else {
                CHECK(_block_compress_codec);
                // Decompress cached payload into _decompress_buf for decoding
                size_t uncompressed_payload_size =
                        header->__isset.data_page_header_v2
                                ? static_cast<size_t>(header->uncompressed_page_size) - levels_size
                                : static_cast<size_t>(header->uncompressed_page_size);
                _reserve_decompress_buf(uncompressed_payload_size);
                _page_data = Slice(_decompress_buf.get(), uncompressed_payload_size);
                SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                _chunk_statistics.decompress_cnt++;
                RETURN_IF_ERROR(_block_compress_codec->decompress(payload_slice, &_page_data));
            }
            // page cache counters were incremented when PageReader did the header-only
            // cache lookup. Do not increment again to avoid double-counting.
            page_loaded = true;
        }
    }

    if (!page_loaded) {
        if (_block_compress_codec != nullptr) {
            Slice compressed_data;
            RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
            std::vector<uint8_t> level_bytes;
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                // uncompressed_size = rl + dl + uncompressed_data_size
                // compressed_size = rl + dl + compressed_data_size
                uncompressed_size -= header_v2.repetition_levels_byte_length +
                                     header_v2.definition_levels_byte_length;
                // copy level bytes (rl + dl) so that we can cache header + levels + uncompressed payload
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                size_t level_sz = rl + dl;
                if (level_sz > 0) {
                    level_bytes.resize(level_sz);
                    memcpy(level_bytes.data(), compressed_data.data, level_sz);
                }
                // now remove levels from compressed_data for decompression
                _get_uncompressed_levels(header_v2, compressed_data);
            }
            bool is_v2_compressed = header->__isset.data_page_header_v2 &&
                                    header->data_page_header_v2.is_compressed;
            bool page_has_compression = header->__isset.data_page_header || is_v2_compressed;

            if (page_has_compression) {
                // Decompress payload for immediate decoding
                _reserve_decompress_buf(uncompressed_size);
                _page_data = Slice(_decompress_buf.get(), uncompressed_size);
                SCOPED_RAW_TIMER(&_chunk_statistics.decompress_time);
                _chunk_statistics.decompress_cnt++;
                RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &_page_data));

                // Decide whether to cache decompressed payload or compressed payload based on threshold
                bool cache_payload_decompressed = should_cache_decompressed(header, _metadata);

                if (_page_read_ctx.enable_parquet_file_page_cache &&
                    !config::disable_storage_page_cache &&
                    StoragePageCache::instance() != nullptr &&
                    !_page_reader->header_bytes().empty()) {
                    if (cache_payload_decompressed) {
                        _insert_page_into_cache(level_bytes, _page_data);
                        _chunk_statistics.page_cache_decompressed_write_counter += 1;
                    } else {
                        if (config::enable_parquet_cache_compressed_pages) {
                            // cache the compressed payload as-is (header | levels | compressed_payload)
                            _insert_page_into_cache(
                                    level_bytes, Slice(compressed_data.data, compressed_data.size));
                            _chunk_statistics.page_cache_compressed_write_counter += 1;
                        }
                    }
                }
            } else {
                // no compression on this page, use the data directly
                _page_data = Slice(compressed_data.data, compressed_data.size);
                if (_page_read_ctx.enable_parquet_file_page_cache &&
                    !config::disable_storage_page_cache &&
                    StoragePageCache::instance() != nullptr) {
                    _insert_page_into_cache(level_bytes, _page_data);
                    _chunk_statistics.page_cache_decompressed_write_counter += 1;
                }
            }
        } else {
            // For uncompressed page, we may still need to extract v2 levels
            std::vector<uint8_t> level_bytes;
            Slice uncompressed_data;
            RETURN_IF_ERROR(_page_reader->get_page_data(uncompressed_data));
            if (header->__isset.data_page_header_v2) {
                const tparquet::DataPageHeaderV2& header_v2 = header->data_page_header_v2;
                size_t rl = header_v2.repetition_levels_byte_length;
                size_t dl = header_v2.definition_levels_byte_length;
                size_t level_sz = rl + dl;
                if (level_sz > 0) {
                    level_bytes.resize(level_sz);
                    memcpy(level_bytes.data(), uncompressed_data.data, level_sz);
                }
                _get_uncompressed_levels(header_v2, uncompressed_data);
            }
            // copy page data out
            _page_data = Slice(uncompressed_data.data, uncompressed_data.size);
            // Optionally cache uncompressed data for uncompressed pages
            if (_page_read_ctx.enable_parquet_file_page_cache &&
                !config::disable_storage_page_cache && StoragePageCache::instance() != nullptr) {
                _insert_page_into_cache(level_bytes, _page_data);
                _chunk_statistics.page_cache_decompressed_write_counter += 1;
            }
        }
    }

    // Initialize repetition level and definition level. Skip when level = 0, which means required field.
    if (_max_rep_level > 0) {
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_level_time);
        if (header->__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_rep_level_decoder.init_v2(_v2_rep_levels, _max_rep_level,
                                                       _remaining_rep_nums));
        } else {
            RETURN_IF_ERROR(_rep_level_decoder.init(
                    &_page_data, header->data_page_header.repetition_level_encoding, _max_rep_level,
                    _remaining_rep_nums));
        }
    }
    if (_max_def_level > 0) {
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_level_time);
        if (header->__isset.data_page_header_v2) {
            RETURN_IF_ERROR(_def_level_decoder.init_v2(_v2_def_levels, _max_def_level,
                                                       _remaining_def_nums));
        } else {
            RETURN_IF_ERROR(_def_level_decoder.init(
                    &_page_data, header->data_page_header.definition_level_encoding, _max_def_level,
                    _remaining_def_nums));
        }
    }
    auto encoding = header->__isset.data_page_header_v2 ? header->data_page_header_v2.encoding
                                                        : header->data_page_header.encoding;
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
        _decoders[static_cast<int>(encoding)] = std::move(page_decoder);
        _page_decoder = _decoders[static_cast<int>(encoding)].get();
    }
    RETURN_IF_ERROR(_page_decoder->set_data(&_page_data));

    _state = DATA_LOADED;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_decode_dict_page() {
    const tparquet::PageHeader* header = nullptr;
    RETURN_IF_ERROR(_page_reader->get_page_header(&header));
    DCHECK_EQ(tparquet::PageType::DICTIONARY_PAGE, header->type);
    SCOPED_RAW_TIMER(&_chunk_statistics.decode_dict_time);

    // Using the PLAIN_DICTIONARY enum value is deprecated in the Parquet 2.0 specification.
    // Prefer using RLE_DICTIONARY in a data page and PLAIN in a dictionary page for Parquet 2.0+ files.
    // refer: https://github.com/apache/parquet-format/blob/master/Encodings.md
    tparquet::Encoding::type dict_encoding = header->dictionary_page_header.encoding;
    if (dict_encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
        dict_encoding != tparquet::Encoding::PLAIN) {
        return Status::InternalError("Unsupported dictionary encoding {}",
                                     tparquet::to_string(dict_encoding));
    }

    // Prepare dictionary data
    int32_t uncompressed_size = header->uncompressed_page_size;
    auto dict_data = make_unique_buffer<uint8_t>(uncompressed_size);
    bool dict_loaded = false;

    // Try to load dictionary page from cache
    if (_page_read_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
        StoragePageCache::instance() != nullptr) {
        if (_page_reader->has_page_cache_handle()) {
            const PageCacheHandle& handle = _page_reader->page_cache_handle();
            Slice cached = handle.data();
            size_t header_size = _page_reader->header_bytes().size();
            // Dictionary page layout in cache: header | payload (compressed or uncompressed)
            Slice payload_slice(cached.data + header_size, cached.size - header_size);

            bool cache_payload_is_decompressed = _page_reader->is_cache_payload_decompressed();

            if (cache_payload_is_decompressed) {
                // Use cached decompressed dictionary data
                memcpy(dict_data.get(), payload_slice.data, payload_slice.size);
                dict_loaded = true;
            } else {
                CHECK(_block_compress_codec);
                // Decompress cached compressed dictionary data
                Slice dict_slice(dict_data.get(), uncompressed_size);
                RETURN_IF_ERROR(_block_compress_codec->decompress(payload_slice, &dict_slice));
                dict_loaded = true;
            }

            // When dictionary page is loaded from cache, we need to skip the page data
            // to update the offset correctly (similar to calling get_page_data())
            if (dict_loaded) {
                _page_reader->skip_page_data();
            }
        }
    }

    if (!dict_loaded) {
        // Load and decompress dictionary page from file
        if (_block_compress_codec != nullptr) {
            Slice compressed_data;
            RETURN_IF_ERROR(_page_reader->get_page_data(compressed_data));
            Slice dict_slice(dict_data.get(), uncompressed_size);
            RETURN_IF_ERROR(_block_compress_codec->decompress(compressed_data, &dict_slice));

            // Decide whether to cache decompressed or compressed dictionary based on threshold
            bool cache_payload_decompressed = should_cache_decompressed(header, _metadata);

            if (_page_read_ctx.enable_parquet_file_page_cache &&
                !config::disable_storage_page_cache && StoragePageCache::instance() != nullptr &&
                !_page_reader->header_bytes().empty()) {
                std::vector<uint8_t> empty_levels; // Dictionary pages don't have levels
                if (cache_payload_decompressed) {
                    // Cache the decompressed dictionary page
                    _insert_page_into_cache(empty_levels, dict_slice);
                    _chunk_statistics.page_cache_decompressed_write_counter += 1;
                } else {
                    if (config::enable_parquet_cache_compressed_pages) {
                        // Cache the compressed dictionary page
                        _insert_page_into_cache(empty_levels,
                                                Slice(compressed_data.data, compressed_data.size));
                        _chunk_statistics.page_cache_compressed_write_counter += 1;
                    }
                }
            }
        } else {
            Slice dict_slice;
            RETURN_IF_ERROR(_page_reader->get_page_data(dict_slice));
            // The data is stored by BufferedStreamReader, we should copy it out
            memcpy(dict_data.get(), dict_slice.data, dict_slice.size);

            // Cache the uncompressed dictionary page
            if (_page_read_ctx.enable_parquet_file_page_cache &&
                !config::disable_storage_page_cache && StoragePageCache::instance() != nullptr &&
                !_page_reader->header_bytes().empty()) {
                std::vector<uint8_t> empty_levels;
                Slice payload(dict_data.get(), uncompressed_size);
                _insert_page_into_cache(empty_levels, payload);
                _chunk_statistics.page_cache_decompressed_write_counter += 1;
            }
        }
    }

    // Cache page decoder
    std::unique_ptr<Decoder> page_decoder;
    RETURN_IF_ERROR(
            Decoder::get_decoder(_metadata.type, tparquet::Encoding::RLE_DICTIONARY, page_decoder));
    // Set type length
    page_decoder->set_type_length(_get_type_length());
    // Set the dictionary data
    RETURN_IF_ERROR(page_decoder->set_dict(dict_data, uncompressed_size,
                                           header->dictionary_page_header.num_values));
    _decoders[static_cast<int>(tparquet::Encoding::RLE_DICTIONARY)] = std::move(page_decoder);

    _has_dict = true;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_reserve_decompress_buf(size_t size) {
    if (size > _decompress_buf_size) {
        _decompress_buf_size = BitUtil::next_power_of_two(size);
        _decompress_buf = make_unique_buffer<uint8_t>(_decompress_buf_size);
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_insert_page_into_cache(
        const std::vector<uint8_t>& level_bytes, const Slice& payload) {
    StoragePageCache::CacheKey key =
            _page_reader->make_page_cache_key(_page_reader->header_start_offset());
    const std::vector<uint8_t>& header_bytes = _page_reader->header_bytes();
    size_t total = header_bytes.size() + level_bytes.size() + payload.size;
    auto page = std::make_unique<DataPage>(total, true, segment_v2::DATA_PAGE);
    size_t pos = 0;
    memcpy(page->data() + pos, header_bytes.data(), header_bytes.size());
    pos += header_bytes.size();
    if (!level_bytes.empty()) {
        memcpy(page->data() + pos, level_bytes.data(), level_bytes.size());
        pos += level_bytes.size();
    }
    if (payload.size > 0) {
        memcpy(page->data() + pos, payload.data, payload.size);
        pos += payload.size;
    }
    page->reset_size(total);
    PageCacheHandle handle;
    StoragePageCache::instance()->insert(key, page.get(), &handle, segment_v2::DATA_PAGE);
    page.release();
    _chunk_statistics.page_cache_write_counter += 1;
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::skip_values(size_t num_values,
                                                                   bool skip_data) {
    if (UNLIKELY(_remaining_num_values < num_values)) {
        return Status::IOError("Skip too many values in current page. {} vs. {}",
                               _remaining_num_values, num_values);
    }
    _remaining_num_values -= num_values;
    if (skip_data) {
        SCOPED_RAW_TIMER(&_chunk_statistics.decode_value_time);
        return _page_decoder->skip_values(num_values);
    } else {
        return Status::OK();
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::decode_values(
        MutableColumnPtr& doris_column, DataTypePtr& data_type, ColumnSelectVector& select_vector,
        bool is_dict_filter) {
    if (select_vector.num_values() == 0) {
        return Status::OK();
    }
    SCOPED_RAW_TIMER(&_chunk_statistics.decode_value_time);
    if (UNLIKELY((doris_column->is_column_dictionary() || is_dict_filter) && !_has_dict)) {
        return Status::IOError("Not dictionary coded");
    }
    if (UNLIKELY(_remaining_num_values < select_vector.num_values())) {
        return Status::IOError("Decode too many values in current page");
    }
    _remaining_num_values -= select_vector.num_values();
    return _page_decoder->decode_values(doris_column, data_type, select_vector, is_dict_filter);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::seek_to_nested_row(size_t left_row) {
    if constexpr (OFFSET_INDEX) {
        while (true) {
            if (_page_reader->start_row() <= left_row && left_row < _page_reader->end_row()) {
                break;
            } else if (has_next_page()) {
                RETURN_IF_ERROR(next_page());
                _current_row = _page_reader->start_row();
            } else [[unlikely]] {
                return Status::InternalError("no match seek row {}, current row {}", left_row,
                                             _current_row);
            }
        };

        RETURN_IF_ERROR(parse_page_header());
        RETURN_IF_ERROR(load_page_data());
        RETURN_IF_ERROR(_skip_nested_rows_in_page(left_row - _current_row));
        _current_row = left_row;
    } else {
        while (true) {
            RETURN_IF_ERROR(parse_page_header());
            if (_page_reader->is_header_v2() || !IN_COLLECTION) {
                if (_page_reader->start_row() <= left_row && left_row < _page_reader->end_row()) {
                    RETURN_IF_ERROR(load_page_data());
                    // this page contain this row.
                    RETURN_IF_ERROR(_skip_nested_rows_in_page(left_row - _current_row));
                    _current_row = left_row;
                    break;
                }

                _current_row = _page_reader->end_row();
                if (has_next_page()) [[likely]] {
                    RETURN_IF_ERROR(next_page());
                } else {
                    return Status::InternalError("no match seek row {}, current row {}", left_row,
                                                 _current_row);
                }
            } else {
                RETURN_IF_ERROR(load_page_data());
                std::vector<level_t> rep_levels;
                std::vector<level_t> def_levels;
                bool cross_page = false;

                size_t result_rows = 0;
                RETURN_IF_ERROR(load_page_nested_rows(rep_levels, left_row - _current_row,
                                                      &result_rows, &cross_page));
                RETURN_IF_ERROR(fill_def(def_levels));
                RETURN_IF_ERROR(skip_nested_values(def_levels));
                bool need_load_next_page = true;
                while (cross_page) {
                    need_load_next_page = false;
                    rep_levels.clear();
                    def_levels.clear();
                    RETURN_IF_ERROR(load_cross_page_nested_row(rep_levels, &cross_page));
                    RETURN_IF_ERROR(fill_def(def_levels));
                    RETURN_IF_ERROR(skip_nested_values(def_levels));
                }
                if (left_row == _current_row) {
                    break;
                }
                if (need_load_next_page) {
                    if (has_next_page()) [[likely]] {
                        RETURN_IF_ERROR(next_page());
                    } else {
                        return Status::InternalError("no match seek row {}, current row {}",
                                                     left_row, _current_row);
                    }
                }
            }
        };
    }

    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_skip_nested_rows_in_page(size_t num_rows) {
    if (num_rows == 0) {
        return Status::OK();
    }

    std::vector<level_t> rep_levels;
    std::vector<level_t> def_levels;

    bool cross_page = false;
    size_t result_rows = 0;
    RETURN_IF_ERROR(load_page_nested_rows(rep_levels, num_rows, &result_rows, &cross_page));
    RETURN_IF_ERROR(fill_def(def_levels));
    RETURN_IF_ERROR(skip_nested_values(def_levels));
    DCHECK(cross_page == false);
    if (num_rows != result_rows) [[unlikely]] {
        return Status::InternalError("no match skip rows, expect {} vs. real {}", num_rows,
                                     result_rows);
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_page_nested_rows(
        std::vector<level_t>& rep_levels, size_t max_rows, size_t* result_rows, bool* cross_page) {
    if (_state != DATA_LOADED) [[unlikely]] {
        return Status::IOError("Should load page data first to load nested rows");
    }
    *cross_page = false;
    *result_rows = 0;
    rep_levels.reserve(rep_levels.size() + _remaining_rep_nums);
    while (_remaining_rep_nums) {
        level_t rep_level = _rep_level_get_next();
        if (rep_level == 0) {               // rep_level 0 indicates start of new row
            if (*result_rows == max_rows) { // this page contain max_rows, page no end.
                _current_row += max_rows;
                _rep_level_rewind_one();
                return Status::OK();
            }
            (*result_rows)++;
        }
        _remaining_rep_nums--;
        rep_levels.emplace_back(rep_level);
    }
    _current_row += *result_rows;

    auto need_check_cross_page = [&]() -> bool {
        return !OFFSET_INDEX && IN_COLLECTION && _remaining_rep_nums == 0 &&
               !_page_reader->is_header_v2() && has_next_page();
    };
    *cross_page = need_check_cross_page();
    return Status::OK();
};

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::load_cross_page_nested_row(
        std::vector<level_t>& rep_levels, bool* cross_page) {
    RETURN_IF_ERROR(next_page());
    RETURN_IF_ERROR(parse_page_header());
    RETURN_IF_ERROR(load_page_data());

    *cross_page = has_next_page();
    while (_remaining_rep_nums) {
        level_t rep_level = _rep_level_get_next();
        if (rep_level == 0) { // rep_level 0 indicates start of new row
            *cross_page = false;
            _rep_level_rewind_one();
            break;
        }
        _remaining_rep_nums--;
        rep_levels.emplace_back(rep_level);
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
int32_t ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::_get_type_length() {
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

/**
 * Checks if the given column has a dictionary page.
 *
 * This function determines the presence of a dictionary page by checking the
 * dictionary_page_offset field in the column metadata. The dictionary_page_offset
 * must be set and greater than 0, and it must be less than the data_page_offset.
 *
 * The reason for these checks is based on the implementation in the Java version
 * of ORC, where dictionary_page_offset is used to indicate the absence of a dictionary.
 * Additionally, Parquet may write an empty row group, in which case the dictionary page
 * content would be empty, and thus the dictionary page should not be read.
 *
 * See https://github.com/apache/arrow/pull/2667/files
 */
bool has_dict_page(const tparquet::ColumnMetaData& column) {
    return column.__isset.dictionary_page_offset && column.dictionary_page_offset > 0 &&
           column.dictionary_page_offset < column.data_page_offset;
}

template class ColumnChunkReader<true, true>;
template class ColumnChunkReader<true, false>;
template class ColumnChunkReader<false, true>;
template class ColumnChunkReader<false, false>;

#include "common/compile_check_end.h"
} // namespace doris::vectorized
