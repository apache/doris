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
                                   std::unique_ptr<ParquetColumnReader>& reader) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::Corruption("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        tparquet::ColumnChunk chunk = row_group.columns[field->children[0].physical_column_index];
        ArrayColumnReader* array_reader = new ArrayColumnReader(column, ctz);
        array_reader->init_column_metadata(chunk);
        RETURN_IF_ERROR(array_reader->init(file, field, &chunk, row_ranges));
        reader.reset(array_reader);
    } else {
        tparquet::ColumnChunk chunk = row_group.columns[field->physical_column_index];
        ScalarColumnReader* scalar_reader = new ScalarColumnReader(column, ctz);
        scalar_reader->init_column_metadata(chunk);
        RETURN_IF_ERROR(scalar_reader->init(file, field, &chunk, row_ranges));
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

void ParquetColumnReader::_reserve_def_levels_buf(size_t size) {
    if (size > _def_levels_buf_size || _def_levels_buf == nullptr) {
        _def_levels_buf_size = BitUtil::next_power_of_two(size);
        _def_levels_buf.reset(new level_t[_def_levels_buf_size]);
    }
}

void ParquetColumnReader::_skipped_pages() {}

Status ScalarColumnReader::init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                                std::vector<RowRange>& row_ranges) {
    _stream_reader =
            new BufferedFileStreamReader(file, _metadata->start_offset(), _metadata->size());
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

Status ScalarColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                            size_t batch_size, size_t* read_rows, bool* eof) {
    if (_chunk_reader->remaining_num_values() == 0) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
            *read_rows = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(_chunk_reader->next_page());
        if (_row_ranges.size() != 0) {
            _skipped_pages();
        }
        RETURN_IF_ERROR(_chunk_reader->load_page_data());
    }
    size_t read_values = _chunk_reader->remaining_num_values() < batch_size
                                 ? _chunk_reader->remaining_num_values()
                                 : batch_size;
    // get definition levels, and generate null values
    _reserve_def_levels_buf(read_values);
    level_t* definitions = _def_levels_buf.get();
    if (_chunk_reader->max_def_level() == 0) { // required field
        std::fill(definitions, definitions + read_values, 1);
    } else {
        _chunk_reader->get_def_levels(definitions, read_values);
    }
    // fill NullMap
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    NullMap& map_data = nullable_column->get_null_map_data();
    for (int i = 0; i < read_values; ++i) {
        map_data.emplace_back(definitions[i] == 0);
    }
    // decode data
    if (_chunk_reader->max_def_level() == 0) {
        RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, read_values));
    } else if (read_values > 0) {
        // column with null values
        level_t level_type = definitions[0];
        int num_values = 1;
        for (int i = 1; i < read_values; ++i) {
            if (definitions[i] != level_type) {
                if (level_type == 0) {
                    // null values
                    _chunk_reader->insert_null_values(data_column, num_values);
                } else {
                    RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, num_values));
                }
                level_type = definitions[i];
                num_values = 1;
            } else {
                num_values++;
            }
        }
        if (level_type == 0) {
            // null values
            _chunk_reader->insert_null_values(data_column, num_values);
        } else {
            RETURN_IF_ERROR(_chunk_reader->decode_values(data_column, type, num_values));
        }
    }
    *read_rows = read_values;
    if (_chunk_reader->remaining_num_values() == 0 && !_chunk_reader->has_next_page()) {
        *eof = true;
    }
    return Status::OK();
}

void ScalarColumnReader::close() {}

Status ArrayColumnReader::init(FileReader* file, FieldSchema* field, tparquet::ColumnChunk* chunk,
                               std::vector<RowRange>& row_ranges) {
    _stream_reader =
            new BufferedFileStreamReader(file, _metadata->start_offset(), _metadata->size());
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
        return Status::Corruption("Max repetition level in scalar column should be 1");
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
        if (_row_ranges.size() != 0) {
            _skipped_pages();
        }
        RETURN_IF_ERROR(_chunk_reader->load_page_data());
        _init_rep_levels_buf();
    }

    // fill NullMap
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    NullMap& map_data = nullable_column->get_null_map_data();
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
    // null array, should ignore the last offset in element_offsets
    for (int i = 0; i < element_offsets.size() - 1; ++i) {
        map_data.emplace_back(definitions[element_offsets[i]] == _NULL_ARRAY);
    }
    // fill array offset, should skip a value when parsing null array
    _fill_array_offset(data_column, element_offsets);
    // fill nested array elements
    if (num_values > 0) {
        _load_nested_column(static_cast<ColumnArray&>(*data_column).get_data_ptr(),
                            const_cast<DataTypePtr&>((reinterpret_cast<const DataTypeArray*>(
                                                              remove_nullable(type).get()))
                                                             ->get_nested_type()),
                            num_values);
    }
    *read_rows = real_batch_size;
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
    NullMap& map_data = nullable_column->get_null_map_data();
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    for (int i = 0; i < read_values; ++i) {
        // should skip _EMPTY_ARRAY and _NULL_ARRAY
        if (definitions[i] == _CONCRETE_ELEMENT) {
            map_data.emplace_back(false);
        } else if (definitions[i] == _NULL_ELEMENT) {
            map_data.emplace_back(true);
        }
    }
    // column with null values
    int start_idx = 0;
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
    if (start_idx == read_values) {
        // all values are empty array or null array
        return Status::OK();
    }
    bool prev_is_null = definitions[start_idx] == _NULL_ELEMENT;
    int num_values = 1;
    for (int i = start_idx + 1; i < read_values; ++i) {
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
    return Status::OK();
}

void ArrayColumnReader::close() {}

void ArrayColumnReader::_init_rep_levels_buf() {
    size_t max_buf_size = _chunk_reader->remaining_num_values() < 1024
                                  ? _chunk_reader->remaining_num_values()
                                  : 1024;
    if (_rep_levels_buf_size < max_buf_size || _rep_levels_buf == nullptr) {
        _rep_levels_buf.reset(new level_t[max_buf_size]);
        _rep_levels_buf_size = max_buf_size;
    }
    _remaining_rep_levels = _chunk_reader->remaining_num_values();
    _start_offset = 0;
}

void ArrayColumnReader::_load_rep_levels() {
    _rep_size = _chunk_reader->remaining_num_values() < _rep_levels_buf_size
                        ? _chunk_reader->remaining_num_values()
                        : _rep_levels_buf_size;
    _chunk_reader->get_rep_levels(_rep_levels_buf.get(), _rep_size);
    _start_offset += _rep_size;
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
                                           std::vector<size_t>& element_offsets) {
    auto& offsets_data = static_cast<ColumnArray&>(*doris_column).get_offsets();
    level_t* definitions = _def_levels_buf.get();
    auto prev_offset = offsets_data.back();
    size_t null_empty_array = 0;
    for (int i = 0; i < element_offsets.size(); ++i) {
        auto& offset = element_offsets[i];
        if (i != 0) {
            offsets_data.emplace_back(prev_offset + offset - null_empty_array);
        }
        if ((i != element_offsets.size() - 1) &&
            (definitions[offset] == _EMPTY_ARRAY || definitions[offset] == _NULL_ARRAY)) {
            null_empty_array++;
        }
    }
}
}; // namespace doris::vectorized