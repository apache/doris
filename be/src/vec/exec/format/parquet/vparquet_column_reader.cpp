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

#include "vparquet_column_reader.h"

#include <common/status.h>
#include <gen_cpp/parquet_types.h>
#include <vec/columns/columns_number.h>

#include "schema_desc.h"
#include "vec/data_types/data_type_array.h"
#include "vparquet_column_chunk_reader.h"

namespace doris::vectorized {

Status ParquetColumnReader::create(FileReader* file, FieldSchema* field,
                                   const ParquetReadColumn& column,
                                   const tparquet::RowGroup& row_group,
                                   std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                                   std::unique_ptr<ParquetColumnReader>& reader,
                                   size_t max_buf_size) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::Corruption("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        tparquet::ColumnChunk chunk = row_group.columns[field->children[0].physical_column_index];
        ArrayColumnReader* array_reader = new ArrayColumnReader(ctz);
        array_reader->init_column_metadata(chunk);
        RETURN_IF_ERROR(array_reader->init(file, field, &chunk, row_ranges, max_buf_size));
        reader.reset(array_reader);
    } else {
        tparquet::ColumnChunk chunk = row_group.columns[field->physical_column_index];
        ScalarColumnReader* scalar_reader = new ScalarColumnReader(ctz);
        scalar_reader->init_column_metadata(chunk);
        RETURN_IF_ERROR(scalar_reader->init(file, field, &chunk, row_ranges, max_buf_size));
        reader.reset(scalar_reader);
    }
    return Status::OK();
}

void ParquetColumnReader::init_column_metadata(const tparquet::ColumnChunk& chunk) {
    auto chunk_meta = chunk.meta_data;
    int64_t chunk_start = chunk_meta.__isset.dictionary_page_offset
                                  ? chunk_meta.dictionary_page_offset
                                  : chunk_meta.data_page_offset;
    size_t chunk_len = chunk_meta.total_compressed_size;
    _metadata.reset(new ParquetColumnMetadata(chunk_start, chunk_len, chunk_meta));
}

void ParquetColumnReader::_generate_read_ranges(int64_t start_index, int64_t end_index,
                                                std::list<RowRange>& read_ranges) {
    if (_row_ranges.size() == 0) {
        read_ranges.emplace_back(start_index, end_index);
        return;
    }
    int index = _row_range_index;
    while (index < _row_ranges.size()) {
        RowRange& read_range = _row_ranges[index];
        if (read_range.last_row <= start_index) {
            index++;
            _row_range_index++;
            continue;
        }
        if (read_range.first_row >= end_index) {
            break;
        }
        int64_t start = read_range.first_row < start_index ? start_index : read_range.first_row;
        int64_t end = read_range.last_row < end_index ? read_range.last_row : end_index;
        read_ranges.emplace_back(start, end);
        index++;
    }
}

Status ScalarColumnReader::init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                                std::vector<RowRange>& row_ranges, size_t max_buf_size) {
    _stream_reader =
            new BufferedFileStreamReader(file, _metadata->start_offset(), _metadata->size(),
                                         std::min((size_t)_metadata->size(), max_buf_size));
    _row_ranges = row_ranges;
    _chunk_reader.reset(new ColumnChunkReader(_stream_reader, chunk, field, _ctz));
    RETURN_IF_ERROR(_chunk_reader->init());
    if (_chunk_reader->max_def_level() > 1) {
        return Status::Corruption("Max definition level in scalar column can't exceed 1");
    }
    if (_chunk_reader->max_rep_level() != 0) {
        return Status::Corruption("Max repetition level in scalar column should be 0");
    }
    return Status::OK();
}

Status ScalarColumnReader::_skip_values(size_t num_values) {
    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t skipped = 0;
        while (skipped < num_values) {
            level_t def_level;
            size_t loop_skip = def_decoder.get_next_run(&def_level, num_values - skipped);
            RETURN_IF_ERROR(_chunk_reader->skip_values(loop_skip, def_level == 1));
            skipped += loop_skip;
        }
    } else {
        RETURN_IF_ERROR(_chunk_reader->skip_values(num_values));
    }
    return Status::OK();
}

Status ScalarColumnReader::_read_values(size_t num_values, ColumnPtr& doris_column,
                                        DataTypePtr& type) {
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    NullMap& map_data_column = nullable_column->get_null_map_data();
    auto origin_size = map_data_column.size();
    {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        map_data_column.resize(origin_size + num_values);
    }

    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t has_read = 0;
        while (has_read < num_values) {
            level_t def_level;
            size_t loop_read = def_decoder.get_next_run(&def_level, num_values - has_read);
            bool is_null = def_level == 0;
            // fill NullMap
            {
                SCOPED_RAW_TIMER(&_decode_null_map_time);
                for (int i = 0; i < loop_read; ++i) {
                    map_data_column[origin_size + has_read + i] = (UInt8)is_null;
                }
            }
            // decode data
            if (is_null) {
                // null values
                _chunk_reader->insert_null_values(data_column, loop_read);
            } else {
                RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, loop_read));
            }
            has_read += loop_read;
        }
    } else {
        for (int i = 0; i < num_values; ++i) {
            map_data_column[origin_size + i] = (UInt8) false;
        }
        RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, num_values));
    }
    return Status::OK();
}

Status ScalarColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                            size_t batch_size, size_t* read_rows, bool* eof) {
    if (_chunk_reader->remaining_num_values() == 0) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(_chunk_reader->next_page());
    }

    // generate the row ranges that should be read
    std::list<RowRange> read_ranges;
    _generate_read_ranges(_current_row_index,
                          _current_row_index + _chunk_reader->remaining_num_values(), read_ranges);
    if (read_ranges.size() == 0) {
        // skip the whole page
        _current_row_index += _chunk_reader->remaining_num_values();
        RETURN_IF_ERROR(_chunk_reader->skip_page());
        *read_rows = 0;
    } else {
        // load page data to decode or skip values
        RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
        size_t has_read = 0;
        for (auto& range : read_ranges) {
            // generate the skipped values
            size_t skip_values = range.first_row - _current_row_index;
            RETURN_IF_ERROR(_skip_values(skip_values));
            _current_row_index += skip_values;
            // generate the read values
            size_t read_values =
                    std::min((size_t)(range.last_row - range.first_row), batch_size - has_read);
            RETURN_IF_ERROR(_read_values(read_values, doris_column, type));
            has_read += read_values;
            _current_row_index += read_values;
            if (has_read == batch_size) {
                break;
            }
        }
        *read_rows = has_read;
    }

    if (_chunk_reader->remaining_num_values() == 0 && !_chunk_reader->has_next_page()) {
        *eof = true;
    }
    return Status::OK();
}

void ScalarColumnReader::close() {}

void ArrayColumnReader::_reserve_def_levels_buf(size_t size) {
    if (size > _def_levels_buf_size || _def_levels_buf == nullptr) {
        _def_levels_buf_size = BitUtil::next_power_of_two(size);
        _def_levels_buf.reset(new level_t[_def_levels_buf_size]);
    }
}

Status ArrayColumnReader::init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                               std::vector<RowRange>& row_ranges, size_t max_buf_size) {
    _stream_reader =
            new BufferedFileStreamReader(file, _metadata->start_offset(), _metadata->size(),
                                         std::min((size_t)_metadata->size(), max_buf_size));
    _row_ranges = row_ranges;
    _chunk_reader.reset(new ColumnChunkReader(_stream_reader, chunk, &field->children[0], _ctz));
    RETURN_IF_ERROR(_chunk_reader->init());
    if (_chunk_reader->max_def_level() > 4) {
        return Status::Corruption("Max definition level in array column can't exceed 4");
    } else {
        FieldSchema& child = field->children[0];
        _CONCRETE_ELEMENT = child.definition_level;
        if (child.is_nullable) {
            _NULL_ELEMENT = _CONCRETE_ELEMENT - 1;
        }
        _EMPTY_ARRAY = field->definition_level;
        if (field->is_nullable) {
            _NULL_ARRAY = _EMPTY_ARRAY - 1;
        }
    }
    if (_chunk_reader->max_rep_level() != 1) {
        return Status::Corruption("Max repetition level in array column should be 1");
    }
    return Status::OK();
}

Status ArrayColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                           size_t batch_size, size_t* read_rows, bool* eof) {
    if (_chunk_reader->remaining_num_values() == 0) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(_chunk_reader->next_page());
        // array should load data to analyze row range
        RETURN_IF_ERROR(_chunk_reader->load_page_data());
        _init_rep_levels_buf();
    }

    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    NullMap& map_data_column = nullable_column->get_null_map_data();
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();

    // generate array offset
    size_t real_batch_size = 0;
    size_t num_values = 0;
    std::vector<size_t> element_offsets;
    RETURN_IF_ERROR(
            _generate_array_offset(element_offsets, batch_size, &real_batch_size, &num_values));
    _reserve_def_levels_buf(num_values);
    level_t* definitions = _def_levels_buf.get();
    _chunk_reader->get_def_levels(definitions, num_values);
    _def_offset = 0;

    // generate the row ranges that should be read
    std::list<RowRange> read_ranges;
    _generate_read_ranges(_current_row_index, _current_row_index + real_batch_size, read_ranges);
    if (read_ranges.size() == 0) {
        // skip the current batch
        _current_row_index += real_batch_size;
        _skip_values(num_values);
        *read_rows = 0;
    } else {
        size_t has_read = 0;
        int offset_index = 0;
        for (auto& range : read_ranges) {
            // generate the skipped values
            size_t skip_rows = range.first_row - _current_row_index;
            size_t skip_values =
                    element_offsets[offset_index + skip_rows] - element_offsets[offset_index];
            RETURN_IF_ERROR(_skip_values(skip_values));
            offset_index += skip_rows;
            _current_row_index += skip_rows;
            real_batch_size -= skip_rows;

            // generate the read values
            size_t scan_rows = range.last_row - range.first_row;
            size_t scan_values =
                    element_offsets[offset_index + scan_rows] - element_offsets[offset_index];
            // null array, should ignore the last offset in element_offsets
            {
                SCOPED_RAW_TIMER(&_decode_null_map_time);
                auto origin_size = map_data_column.size();
                map_data_column.resize(origin_size + scan_rows);
                for (int i = offset_index; i < offset_index + scan_rows; ++i) {
                    map_data_column[origin_size + i] =
                            (UInt8)(definitions[element_offsets[i]] == _NULL_ARRAY);
                }
            }
            // fill array offset, should skip a value when parsing null array
            _fill_array_offset(data_column, element_offsets, offset_index, scan_rows);
            // fill nested array elements
            if (LIKELY(scan_values > 0)) {
                RETURN_IF_ERROR(_load_nested_column(
                        static_cast<ColumnArray&>(*data_column).get_data_ptr(),
                        const_cast<DataTypePtr&>((reinterpret_cast<const DataTypeArray*>(
                                                          remove_nullable(type).get()))
                                                         ->get_nested_type()),
                        scan_values));
            }
            offset_index += scan_rows;
            has_read += scan_rows;
            _current_row_index += scan_rows;
        }
        if (offset_index != element_offsets.size() - 1) {
            size_t skip_rows = element_offsets.size() - 1 - offset_index;
            size_t skip_values = element_offsets.back() - element_offsets[offset_index];
            RETURN_IF_ERROR(_skip_values(skip_values));
            offset_index += skip_rows;
            _current_row_index += skip_rows;
            real_batch_size -= skip_rows;
        }
        DCHECK_EQ(real_batch_size, has_read);
        *read_rows = has_read;
    }

    if (_chunk_reader->remaining_num_values() == 0 && !_chunk_reader->has_next_page()) {
        *eof = true;
    }
    return Status::OK();
}

Status ArrayColumnReader::_load_nested_column(ColumnPtr& doris_column, DataTypePtr& type,
                                              size_t read_values) {
    // fill NullMap
    CHECK(doris_column->is_nullable());
    level_t* definitions = _def_levels_buf.get();
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        NullMap& map_data_column = nullable_column->get_null_map_data();
        size_t null_map_size = 0;
        for (int i = _def_offset; i < _def_offset + read_values; ++i) {
            // should skip _EMPTY_ARRAY and _NULL_ARRAY
            if (definitions[i] == _CONCRETE_ELEMENT || definitions[i] == _NULL_ELEMENT) {
                null_map_size++;
            }
        }
        auto origin_size = map_data_column.size();
        map_data_column.resize(origin_size + null_map_size);
        size_t null_map_idx = origin_size;
        for (int i = _def_offset; i < _def_offset + read_values; ++i) {
            if (definitions[i] == _CONCRETE_ELEMENT) {
                map_data_column[null_map_idx++] = (UInt8) false;
            } else if (definitions[i] == _NULL_ELEMENT) {
                map_data_column[null_map_idx++] = (UInt8) true;
            }
        }
    }
    // column with null values
    int start_idx = _def_offset;
    while (start_idx < read_values) {
        if (definitions[start_idx] == _CONCRETE_ELEMENT ||
            definitions[start_idx] == _NULL_ELEMENT) {
            break;
        } else {
            // only decrease _remaining_num_values
            _chunk_reader->skip_values(1, false);
            start_idx++;
        }
    }
    if (start_idx == _def_offset + read_values) {
        // all values are empty array or null array
        return Status::OK();
    }
    bool prev_is_null = definitions[start_idx] == _NULL_ELEMENT;
    int num_values = 1;
    for (int i = start_idx + 1; i < _def_offset + read_values; ++i) {
        if (definitions[i] == _EMPTY_ARRAY || definitions[i] == _NULL_ARRAY) {
            // only decrease _remaining_num_values
            _chunk_reader->skip_values(1, false);
            continue;
        }
        bool curr_is_null = definitions[i] == _NULL_ELEMENT;
        if (prev_is_null ^ curr_is_null) {
            if (prev_is_null) {
                // null values
                _chunk_reader->insert_null_values(data_column, num_values);
            } else {
                RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, num_values));
            }
            prev_is_null = curr_is_null;
            num_values = 1;
        } else {
            num_values++;
        }
    }
    if (prev_is_null) {
        // null values
        _chunk_reader->insert_null_values(data_column, num_values);
    } else {
        RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, num_values));
    }
    _def_offset += read_values;
    return Status::OK();
}

void ArrayColumnReader::close() {}

void ArrayColumnReader::_init_rep_levels_buf() {
    size_t max_buf_size = _chunk_reader->remaining_num_values() < 4096
                                  ? _chunk_reader->remaining_num_values()
                                  : 4096;
    if (_rep_levels_buf_size < max_buf_size || _rep_levels_buf == nullptr) {
        _rep_levels_buf.reset(new level_t[max_buf_size]);
        _rep_levels_buf_size = max_buf_size;
    }
    _remaining_rep_levels = _chunk_reader->remaining_num_values();
    _start_offset = 0;
}

void ArrayColumnReader::_load_rep_levels() {
    _start_offset += _rep_size;
    _rep_size = _remaining_rep_levels < _rep_levels_buf_size ? _remaining_rep_levels
                                                             : _rep_levels_buf_size;
    _chunk_reader->get_rep_levels(_rep_levels_buf.get(), _rep_size);
    _rep_offset = 0;
}

Status ArrayColumnReader::_generate_array_offset(std::vector<size_t>& element_offsets,
                                                 size_t pre_batch_size, size_t* real_batch_size,
                                                 size_t* num_values) {
    if (_remaining_rep_levels == 0) {
        *real_batch_size = 0;
        *num_values = 0;
        return Status::OK();
    }

    size_t num_array = 0;
    size_t remaining_values = _remaining_rep_levels;
    level_t* repetitions = _rep_levels_buf.get();
    if (_rep_offset == _rep_size) {
        _load_rep_levels();
    }
    size_t prev_rep_offset = _start_offset + _rep_offset;
    _remaining_rep_levels--;
    element_offsets.emplace_back(0);
    if (repetitions[_rep_offset++] != 0) {
        return Status::Corruption("Wrong repetition level in array column");
    }
    while (_remaining_rep_levels > 0) {
        if (_rep_offset == _rep_size) {
            _load_rep_levels();
        }
        if (repetitions[_rep_offset] != 1) { // new array
            element_offsets.emplace_back(_start_offset + _rep_offset - prev_rep_offset);
            if (++num_array >= pre_batch_size) {
                break;
            }
        }
        _rep_offset++;
        _remaining_rep_levels--;
    }
    if (_remaining_rep_levels == 0) { // resolve the last array
        element_offsets.emplace_back(_start_offset + _rep_offset - prev_rep_offset);
        num_array++;
    }

    *real_batch_size = num_array;
    *num_values = remaining_values - _remaining_rep_levels;
    return Status::OK();
}

void ArrayColumnReader::_fill_array_offset(MutableColumnPtr& doris_column,
                                           std::vector<size_t>& element_offsets, int offset_index,
                                           size_t num_rows) {
    if (LIKELY(num_rows > 0)) {
        auto& offsets_data = static_cast<ColumnArray&>(*doris_column).get_offsets();
        level_t* definitions = _def_levels_buf.get();
        auto prev_offset = offsets_data.back();
        auto base_offset = element_offsets[offset_index];
        size_t null_empty_array = 0;
        for (int i = offset_index; i < offset_index + num_rows + 1; ++i) {
            auto& offset = element_offsets[i];
            if (i != offset_index) {
                offsets_data.emplace_back(prev_offset + offset - base_offset - null_empty_array);
            }
            if ((i != offset_index + num_rows) &&
                (definitions[offset] == _EMPTY_ARRAY || definitions[offset] == _NULL_ARRAY)) {
                null_empty_array++;
            }
        }
    }
}

Status ArrayColumnReader::_skip_values(size_t num_values) {
    if (LIKELY(num_values > 0)) {
        level_t* definitions = _def_levels_buf.get();
        bool prev_is_null = definitions[_def_offset] != _CONCRETE_ELEMENT;
        int cnt = 1;
        for (int i = _def_offset + 1; i < _def_offset + num_values; ++i) {
            bool curr_is_null = definitions[i] != _CONCRETE_ELEMENT;
            if (prev_is_null ^ curr_is_null) {
                RETURN_IF_ERROR(_chunk_reader->skip_values(cnt, !prev_is_null));
                prev_is_null = curr_is_null;
                cnt = 1;
            } else {
                cnt++;
            }
        }
        RETURN_IF_ERROR(_chunk_reader->skip_values(cnt, !prev_is_null));
        _def_offset += num_values;
    }
    return Status::OK();
}
}; // namespace doris::vectorized
