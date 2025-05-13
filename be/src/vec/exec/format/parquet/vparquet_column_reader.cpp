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
#include <limits.h>
#include <sys/types.h>

#include <algorithm>
#include <utility>

#include "runtime/define_primitive_type.h"
#include "schema_desc.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/parquet/level_decoder.h"
#include "vparquet_column_chunk_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
static void fill_struct_null_map(FieldSchema* field, NullMap& null_map,
                                 const std::vector<level_t>& rep_levels,
                                 const std::vector<level_t>& def_levels) {
    size_t num_levels = def_levels.size();
    DCHECK_EQ(num_levels, rep_levels.size());
    size_t origin_size = null_map.size();
    null_map.resize(origin_size + num_levels);
    size_t pos = origin_size;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (def_levels[i] >= field->definition_level) {
            null_map[pos++] = 0;
        } else {
            null_map[pos++] = 1;
        }
    }
    null_map.resize(pos);
}

static void fill_array_offset(FieldSchema* field, ColumnArray::Offsets64& offsets_data,
                              NullMap* null_map_ptr, const std::vector<level_t>& rep_levels,
                              const std::vector<level_t>& def_levels) {
    size_t num_levels = rep_levels.size();
    DCHECK_EQ(num_levels, def_levels.size());
    size_t origin_size = offsets_data.size();
    offsets_data.resize(origin_size + num_levels);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(origin_size + num_levels);
    }
    size_t offset_pos = origin_size - 1;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (rep_levels[i] == field->repetition_level) {
            offsets_data[offset_pos]++;
            continue;
        }
        offset_pos++;
        offsets_data[offset_pos] = offsets_data[offset_pos - 1];
        if (def_levels[i] >= field->definition_level) {
            offsets_data[offset_pos]++;
        }
        if (def_levels[i] >= field->definition_level - 1) {
            (*null_map_ptr)[offset_pos] = 0;
        } else {
            (*null_map_ptr)[offset_pos] = 1;
        }
    }
    offsets_data.resize(offset_pos + 1);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(offset_pos + 1);
    }
}

Status ParquetColumnReader::create(io::FileReaderSPtr file, FieldSchema* field,
                                   const tparquet::RowGroup& row_group,
                                   const std::vector<RowRange>& row_ranges, cctz::time_zone* ctz,
                                   io::IOContext* io_ctx,
                                   std::unique_ptr<ParquetColumnReader>& reader,
                                   size_t max_buf_size, const tparquet::OffsetIndex* offset_index) {
    if (field->data_type->get_primitive_type() == TYPE_ARRAY) {
        std::unique_ptr<ParquetColumnReader> element_reader;
        RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                               element_reader, max_buf_size));
        element_reader->set_nested_column();
        auto array_reader = ArrayColumnReader::create_unique(row_ranges, ctz, io_ctx);
        RETURN_IF_ERROR(array_reader->init(std::move(element_reader), field));
        reader.reset(array_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_MAP) {
        std::unique_ptr<ParquetColumnReader> key_reader;
        std::unique_ptr<ParquetColumnReader> value_reader;
        RETURN_IF_ERROR(create(file, &field->children[0].children[0], row_group, row_ranges, ctz,
                               io_ctx, key_reader, max_buf_size));
        RETURN_IF_ERROR(create(file, &field->children[0].children[1], row_group, row_ranges, ctz,
                               io_ctx, value_reader, max_buf_size));
        key_reader->set_nested_column();
        value_reader->set_nested_column();
        auto map_reader = MapColumnReader::create_unique(row_ranges, ctz, io_ctx);
        RETURN_IF_ERROR(map_reader->init(std::move(key_reader), std::move(value_reader), field));
        reader.reset(map_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_STRUCT) {
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> child_readers;
        child_readers.reserve(field->children.size());
        for (int i = 0; i < field->children.size(); ++i) {
            std::unique_ptr<ParquetColumnReader> child_reader;
            RETURN_IF_ERROR(create(file, &field->children[i], row_group, row_ranges, ctz, io_ctx,
                                   child_reader, max_buf_size));
            child_reader->set_nested_column();
            child_readers[field->children[i].name] = std::move(child_reader);
        }
        auto struct_reader = StructColumnReader::create_unique(row_ranges, ctz, io_ctx);
        RETURN_IF_ERROR(struct_reader->init(std::move(child_readers), field));
        reader.reset(struct_reader.release());
    } else {
        const tparquet::ColumnChunk& chunk = row_group.columns[field->physical_column_index];
        auto scalar_reader =
                ScalarColumnReader::create_unique(row_ranges, chunk, offset_index, ctz, io_ctx);
        RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size));
        reader.reset(scalar_reader.release());
    }
    return Status::OK();
}

void ParquetColumnReader::_generate_read_ranges(int64_t start_index, int64_t end_index,
                                                std::list<RowRange>& read_ranges) {
    if (_nested_column) {
        read_ranges.emplace_back(start_index, end_index);
        return;
    }
    int index = _row_range_index;
    while (index < _row_ranges.size()) {
        const RowRange& read_range = _row_ranges[index];
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

Status ScalarColumnReader::init(io::FileReaderSPtr file, FieldSchema* field, size_t max_buf_size) {
    _field_schema = field;
    auto& chunk_meta = _chunk_meta.meta_data;
    int64_t chunk_start = has_dict_page(chunk_meta) ? chunk_meta.dictionary_page_offset
                                                    : chunk_meta.data_page_offset;
    size_t chunk_len = chunk_meta.total_compressed_size;
    size_t prefetch_buffer_size = std::min(chunk_len, max_buf_size);
    if (typeid_cast<io::MergeRangeFileReader*>(file.get())) {
        // turn off prefetch data when using MergeRangeFileReader
        prefetch_buffer_size = 0;
    }
    _stream_reader = std::make_unique<io::BufferedFileStreamReader>(file, chunk_start, chunk_len,
                                                                    prefetch_buffer_size);
    _chunk_reader = std::make_unique<ColumnChunkReader>(_stream_reader.get(), &_chunk_meta, field,
                                                        _offset_index, _ctz, _io_ctx);
    RETURN_IF_ERROR(_chunk_reader->init());
    return Status::OK();
}

Status ScalarColumnReader::_skip_values(size_t num_values) {
    if (num_values == 0) {
        return Status::OK();
    }
    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t skipped = 0;
        size_t null_size = 0;
        size_t nonnull_size = 0;
        while (skipped < num_values) {
            level_t def_level = -1;
            size_t loop_skip = def_decoder.get_next_run(&def_level, num_values - skipped);
            if (loop_skip == 0) {
                std::stringstream ss;
                auto& bit_reader = def_decoder.rle_decoder().bit_reader();
                ss << "def_decoder buffer (hex): ";
                for (size_t i = 0; i < bit_reader.max_bytes(); ++i) {
                    ss << std::hex << std::setw(2) << std::setfill('0')
                       << static_cast<int>(bit_reader.buffer()[i]) << " ";
                }
                LOG(WARNING) << ss.str();
                return Status::InternalError("Failed to decode definition level.");
            }
            if (def_level == 0) {
                null_size += loop_skip;
            } else {
                nonnull_size += loop_skip;
            }
            skipped += loop_skip;
        }
        if (null_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(null_size, false));
        }
        if (nonnull_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(nonnull_size, true));
        }
    } else {
        RETURN_IF_ERROR(_chunk_reader->skip_values(num_values));
    }
    return Status::OK();
}

Status ScalarColumnReader::_read_values(size_t num_values, ColumnPtr& doris_column,
                                        DataTypePtr& type, FilterMap& filter_map,
                                        bool is_dict_filter) {
    if (num_values == 0) {
        return Status::OK();
    }
    MutableColumnPtr data_column;
    std::vector<uint16_t> null_map;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        auto* nullable_column =
                assert_cast<vectorized::ColumnNullable*>(const_cast<IColumn*>(doris_column.get()));

        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
        if (_chunk_reader->max_def_level() > 0) {
            LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
            size_t has_read = 0;
            bool prev_is_null = true;
            while (has_read < num_values) {
                level_t def_level;
                size_t loop_read = def_decoder.get_next_run(&def_level, num_values - has_read);
                if (loop_read == 0) {
                    std::stringstream ss;
                    auto& bit_reader = def_decoder.rle_decoder().bit_reader();
                    ss << "def_decoder buffer (hex): ";
                    for (size_t i = 0; i < bit_reader.max_bytes(); ++i) {
                        ss << std::hex << std::setw(2) << std::setfill('0')
                           << static_cast<int>(bit_reader.buffer()[i]) << " ";
                    }
                    LOG(WARNING) << ss.str();
                    return Status::InternalError("Failed to decode definition level.");
                }
                bool is_null = def_level == 0;
                if (!(prev_is_null ^ is_null)) {
                    null_map.emplace_back(0);
                }
                size_t remaining = loop_read;
                while (remaining > USHRT_MAX) {
                    null_map.emplace_back(USHRT_MAX);
                    null_map.emplace_back(0);
                    remaining -= USHRT_MAX;
                }
                null_map.emplace_back((u_short)remaining);
                prev_is_null = is_null;
                has_read += loop_read;
            }
        }
    } else {
        if (_chunk_reader->max_def_level() > 0) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (null_map.size() == 0) {
        size_t remaining = num_values;
        while (remaining > USHRT_MAX) {
            null_map.emplace_back(USHRT_MAX);
            null_map.emplace_back(0);
            remaining -= USHRT_MAX;
        }
        null_map.emplace_back((u_short)remaining);
    }
    ColumnSelectVector select_vector;
    {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        RETURN_IF_ERROR(select_vector.init(null_map, num_values, map_data_column, &filter_map,
                                           _filter_map_index));
        _filter_map_index += num_values;
    }
    return _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter);
}

/**
 * Load the nested column data of complex type.
 * A row of complex type may be stored across two(or more) pages, and the parameter `align_rows` indicates that
 * whether the reader should read the remaining value of the last row in previous page.
 */
Status ScalarColumnReader::_read_nested_column(ColumnPtr& doris_column, DataTypePtr& type,
                                               FilterMap& filter_map, size_t batch_size,
                                               size_t* read_rows, bool* eof, bool is_dict_filter,
                                               bool align_rows) {
    std::unique_ptr<FilterMap> nested_filter_map;

    FilterMap* current_filter_map = &filter_map;
    size_t origin_size = 0;
    if (align_rows) {
        origin_size = _rep_levels.size();
        // just read the remaining values of the last row in previous page,
        // so there's no a new row should be read.
        batch_size = 0;
        /*
         * Since the function is repeatedly called to fetch data for the batch size,
         * it causes `_rep_levels.resize(0); _def_levels.resize(0);`, resulting in the
         * definition and repetition levels of the reader only containing the latter
         * part of the batch (i.e., missing some parts). Therefore, when using the
         * definition and repetition levels to fill the null_map for structs and maps,
         * the function should not be called multiple times before filling.
        * todo:
         * We may need to consider reading the entire batch of data at once, as this approach
         * would be more user-friendly in terms of function usage. However, we must consider that if the
         * data spans multiple pages, memory usage may increase significantly.
         */
    } else {
        _rep_levels.resize(0);
        _def_levels.resize(0);
        if (_nested_filter_map_data) {
            _nested_filter_map_data->resize(0);
        }
    }
    size_t parsed_rows = 0;
    size_t remaining_values = _chunk_reader->remaining_num_values();
    bool has_rep_level = _chunk_reader->max_rep_level() > 0;
    bool has_def_level = _chunk_reader->max_def_level() > 0;

    // Handle repetition levels (indicates nesting structure)
    if (has_rep_level) {
        LevelDecoder& rep_decoder = _chunk_reader->rep_level_decoder();
        // Read repetition levels until batch is full or no more values
        while (parsed_rows <= batch_size && remaining_values > 0) {
            level_t rep_level = rep_decoder.get_next();
            if (rep_level == 0) { // rep_level 0 indicates start of new row
                if (parsed_rows == batch_size) {
                    rep_decoder.rewind_one();
                    break;
                }
                parsed_rows++;
            }
            _rep_levels.emplace_back(rep_level);
            remaining_values--;
        }

        // Generate nested filter map
        if (filter_map.has_filter() && (!filter_map.filter_all())) {
            if (_nested_filter_map_data == nullptr) {
                _nested_filter_map_data.reset(new std::vector<uint8_t>());
            }
            RETURN_IF_ERROR(filter_map.generate_nested_filter_map(
                    _rep_levels, *_nested_filter_map_data, &nested_filter_map,
                    &_orig_filter_map_index, origin_size));
            // Update current_filter_map to nested_filter_map
            current_filter_map = nested_filter_map.get();
        }
    } else if (!align_rows) {
        // case : required child columns in struct type
        parsed_rows = std::min(remaining_values, batch_size);
        remaining_values -= parsed_rows;
        _rep_levels.resize(parsed_rows, 0);
    }

    // Process definition levels (indicates null values)
    size_t parsed_values = _chunk_reader->remaining_num_values() - remaining_values;
    _def_levels.resize(origin_size + parsed_values);
    if (has_def_level) {
        _chunk_reader->def_level_decoder().get_levels(&_def_levels[origin_size], parsed_values);
    } else {
        std::fill(_def_levels.begin() + origin_size, _def_levels.end(), 0);
    }

    // Handle nullable columns
    MutableColumnPtr data_column;
    std::vector<uint16_t> null_map;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        auto* nullable_column = const_cast<vectorized::ColumnNullable*>(
                assert_cast<const vectorized::ColumnNullable*>(doris_column.get()));
        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }

    // Process definition levels to build null map
    size_t has_read = origin_size;
    size_t ancestor_nulls = 0;
    size_t null_size = 0;
    size_t nonnull_size = 0;
    null_map.emplace_back(0);
    bool prev_is_null = false;
    std::unordered_set<size_t> ancestor_null_indices;

    while (has_read < origin_size + parsed_values) {
        level_t def_level = _def_levels[has_read++];
        size_t loop_read = 1;
        while (has_read < origin_size + parsed_values && _def_levels[has_read] == def_level) {
            has_read++;
            loop_read++;
        }

        if (def_level < _field_schema->repeated_parent_def_level) {
            for (size_t i = 0; i < loop_read; i++) {
                ancestor_null_indices.insert(has_read - loop_read + i);
            }
            ancestor_nulls += loop_read;
            continue;
        }

        bool is_null = def_level < _field_schema->definition_level;
        if (is_null) {
            null_size += loop_read;
        } else {
            nonnull_size += loop_read;
        }

        if (prev_is_null == is_null && (USHRT_MAX - null_map.back() >= loop_read)) {
            null_map.back() += loop_read;
        } else {
            if (!(prev_is_null ^ is_null)) {
                null_map.emplace_back(0);
            }
            size_t remaining = loop_read;
            while (remaining > USHRT_MAX) {
                null_map.emplace_back(USHRT_MAX);
                null_map.emplace_back(0);
                remaining -= USHRT_MAX;
            }
            null_map.emplace_back((u_short)remaining);
            prev_is_null = is_null;
        }
    }

    size_t num_values = parsed_values - ancestor_nulls;

    // Handle filtered values
    if (current_filter_map->filter_all()) {
        // Skip all values if everything is filtered
        if (null_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(null_size, false));
        }
        if (nonnull_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(nonnull_size, true));
        }
        if (ancestor_nulls != 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(ancestor_nulls, false));
        }
    } else {
        ColumnSelectVector select_vector;
        {
            SCOPED_RAW_TIMER(&_decode_null_map_time);
            RETURN_IF_ERROR(
                    select_vector.init(null_map, num_values, map_data_column, current_filter_map,
                                       _nested_filter_map_data ? origin_size : _filter_map_index,
                                       &ancestor_null_indices));
        }

        RETURN_IF_ERROR(
                _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter));
        if (ancestor_nulls != 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(ancestor_nulls, false));
        }
    }
    *read_rows += parsed_rows;
    _filter_map_index += parsed_values;

    // Handle cross-page reading
    if (_chunk_reader->remaining_num_values() == 0) {
        if (_chunk_reader->has_next_page()) {
            RETURN_IF_ERROR(_chunk_reader->next_page());
            RETURN_IF_ERROR(_chunk_reader->load_page_data());
            return _read_nested_column(doris_column, type, filter_map, 0, read_rows, eof,
                                       is_dict_filter, true);
        } else {
            *eof = true;
        }
    }

    // Apply filtering to repetition and definition levels
    if (current_filter_map->has_filter()) {
        if (current_filter_map->filter_all()) {
            _rep_levels.resize(0);
            _def_levels.resize(0);
        } else {
            std::vector<level_t> filtered_rep_levels;
            std::vector<level_t> filtered_def_levels;
            filtered_rep_levels.reserve(_rep_levels.size());
            filtered_def_levels.reserve(_def_levels.size());

            const uint8_t* filter_map_data = current_filter_map->filter_map_data();

            for (size_t i = 0; i < _rep_levels.size(); i++) {
                if (filter_map_data[i]) {
                    filtered_rep_levels.push_back(_rep_levels[i]);
                    filtered_def_levels.push_back(_def_levels[i]);
                }
            }

            _rep_levels = std::move(filtered_rep_levels);
            _def_levels = std::move(filtered_def_levels);
        }
    }

    // Prepare for next row
    ++_orig_filter_map_index;

    if (_rep_levels.size() > 0) {
        // make sure the rows of complex type are aligned correctly,
        // so the repetition level of first element should be 0, meaning a new row is started.
        DCHECK_EQ(_rep_levels[0], 0);
    }
    return Status::OK();
}

Status ScalarColumnReader::read_dict_values_to_column(MutableColumnPtr& doris_column,
                                                      bool* has_dict) {
    bool loaded;
    RETURN_IF_ERROR(_try_load_dict_page(&loaded, has_dict));
    if (loaded && *has_dict) {
        return _chunk_reader->read_dict_values_to_column(doris_column);
    }
    return Status::OK();
}

MutableColumnPtr ScalarColumnReader::convert_dict_column_to_string_column(
        const ColumnInt32* dict_column) {
    return _chunk_reader->convert_dict_column_to_string_column(dict_column);
}

Status ScalarColumnReader::_try_load_dict_page(bool* loaded, bool* has_dict) {
    *loaded = false;
    *has_dict = false;
    if (_chunk_reader->remaining_num_values() == 0) {
        if (!_chunk_reader->has_next_page()) {
            *loaded = false;
            return Status::OK();
        }
        RETURN_IF_ERROR(_chunk_reader->next_page());
        *loaded = true;
        *has_dict = _chunk_reader->has_dict();
    }
    return Status::OK();
}

Status ScalarColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                            FilterMap& filter_map, size_t batch_size,
                                            size_t* read_rows, bool* eof, bool is_dict_filter) {
    if (_converter == nullptr) {
        _converter = parquet::PhysicalToLogicalConverter::get_converter(
                _field_schema, _field_schema->data_type, type, _ctz, is_dict_filter);
        if (!_converter->support()) {
            return Status::InternalError(
                    "The column type of '{}' is not supported: {}, is_dict_filter: {}, "
                    "src_logical_type: {}, dst_logical_type: {}",
                    _field_schema->name, _converter->get_error_msg(), is_dict_filter,
                    _field_schema->data_type->get_name(), type->get_name());
        }
    }
    ColumnPtr resolved_column =
            _converter->get_physical_column(_field_schema->physical_type, _field_schema->data_type,
                                            doris_column, type, is_dict_filter);
    DataTypePtr& resolved_type = _converter->get_physical_type();

    do {
        if (_chunk_reader->remaining_num_values() == 0) {
            if (!_chunk_reader->has_next_page()) {
                *eof = true;
                *read_rows = 0;
                return Status::OK();
            }
            RETURN_IF_ERROR(_chunk_reader->next_page());
        }
        if (_nested_column) {
            RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
            RETURN_IF_ERROR(_read_nested_column(resolved_column, resolved_type, filter_map,
                                                batch_size, read_rows, eof, is_dict_filter, false));
            break;
        }

        // generate the row ranges that should be read
        std::list<RowRange> read_ranges;
        _generate_read_ranges(_current_row_index,
                              _current_row_index + _chunk_reader->remaining_num_values(),
                              read_ranges);
        if (read_ranges.size() == 0) {
            // skip the whole page
            _current_row_index += _chunk_reader->remaining_num_values();
            RETURN_IF_ERROR(_chunk_reader->skip_page());
            *read_rows = 0;
        } else {
            bool skip_whole_batch = false;
            // Determining whether to skip page or batch will increase the calculation time.
            // When the filtering effect is greater than 60%, it is possible to skip the page or batch.
            if (filter_map.has_filter() && filter_map.filter_ratio() > 0.6) {
                // lazy read
                size_t remaining_num_values = 0;
                for (auto& range : read_ranges) {
                    remaining_num_values += range.last_row - range.first_row;
                }
                if (batch_size >= remaining_num_values &&
                    filter_map.can_filter_all(remaining_num_values, _filter_map_index)) {
                    // We can skip the whole page if the remaining values is filtered by predicate columns
                    _filter_map_index += remaining_num_values;
                    _current_row_index += _chunk_reader->remaining_num_values();
                    RETURN_IF_ERROR(_chunk_reader->skip_page());
                    *read_rows = remaining_num_values;
                    if (!_chunk_reader->has_next_page()) {
                        *eof = true;
                    }
                    break;
                }
                skip_whole_batch = batch_size <= remaining_num_values &&
                                   filter_map.can_filter_all(batch_size, _filter_map_index);
                if (skip_whole_batch) {
                    _filter_map_index += batch_size;
                }
            }
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
                if (skip_whole_batch) {
                    RETURN_IF_ERROR(_skip_values(read_values));
                } else {
                    RETURN_IF_ERROR(_read_values(read_values, resolved_column, resolved_type,
                                                 filter_map, is_dict_filter));
                }
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
    } while (false);

    return _converter->convert(resolved_column, _field_schema->data_type, type, doris_column,
                               is_dict_filter);
}

Status ArrayColumnReader::init(std::unique_ptr<ParquetColumnReader> element_reader,
                               FieldSchema* field) {
    _field_schema = field;
    _element_reader = std::move(element_reader);
    return Status::OK();
}

Status ArrayColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                           FilterMap& filter_map, size_t batch_size,
                                           size_t* read_rows, bool* eof, bool is_dict_filter) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_ARRAY) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Array type, actual type: {}.",
                _field_schema->name, type->get_name());
    }

    ColumnPtr& element_column = assert_cast<ColumnArray&>(*data_column).get_data_ptr();
    auto& element_type = const_cast<DataTypePtr&>(
            (assert_cast<const DataTypeArray*>(remove_nullable(type).get()))->get_nested_type());
    // read nested column
    RETURN_IF_ERROR(_element_reader->read_column_data(element_column, element_type, filter_map,
                                                      batch_size, read_rows, eof, is_dict_filter));
    if (*read_rows == 0) {
        return Status::OK();
    }

    ColumnArray::Offsets64& offsets_data = assert_cast<ColumnArray&>(*data_column).get_offsets();
    // fill offset and null map
    fill_array_offset(_field_schema, offsets_data, null_map_ptr, _element_reader->get_rep_level(),
                      _element_reader->get_def_level());
    DCHECK_EQ(element_column->size(), offsets_data.back());

    return Status::OK();
}

Status MapColumnReader::init(std::unique_ptr<ParquetColumnReader> key_reader,
                             std::unique_ptr<ParquetColumnReader> value_reader,
                             FieldSchema* field) {
    _field_schema = field;
    _key_reader = std::move(key_reader);
    _value_reader = std::move(value_reader);
    return Status::OK();
}

Status MapColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                         FilterMap& filter_map, size_t batch_size,
                                         size_t* read_rows, bool* eof, bool is_dict_filter) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (remove_nullable(type)->get_primitive_type() != PrimitiveType::TYPE_MAP) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Map type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& map = assert_cast<ColumnMap&>(*data_column);
    auto& key_type = const_cast<DataTypePtr&>(
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_key_type());
    auto& value_type = const_cast<DataTypePtr&>(
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_value_type());
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();

    size_t key_rows = 0;
    size_t value_rows = 0;
    bool key_eof = false;
    bool value_eof = false;
    RETURN_IF_ERROR(_key_reader->read_column_data(key_column, key_type, filter_map, batch_size,
                                                  &key_rows, &key_eof, is_dict_filter));

    while (value_rows < key_rows && !value_eof) {
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_value_reader->read_column_data(value_column, value_type, filter_map,
                                                        key_rows - value_rows, &loop_rows,
                                                        &value_eof, is_dict_filter));
        value_rows += loop_rows;
    }
    DCHECK_EQ(key_rows, value_rows);
    DCHECK_EQ(key_eof, value_eof);
    *read_rows = key_rows;
    *eof = key_eof;

    if (*read_rows == 0) {
        return Status::OK();
    }

    DCHECK_EQ(key_column->size(), value_column->size());
    // fill offset and null map
    fill_array_offset(_field_schema, map.get_offsets(), null_map_ptr, _key_reader->get_rep_level(),
                      _key_reader->get_def_level());
    RETURN_IF_ERROR(map.deduplicate_keys());
    DCHECK_EQ(key_column->size(), map.get_offsets().back());

    return Status::OK();
}

Status StructColumnReader::init(
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>>&& child_readers,
        FieldSchema* field) {
    _field_schema = field;
    _child_readers = std::move(child_readers);
    return Status::OK();
}
Status StructColumnReader::read_column_data(ColumnPtr& doris_column, DataTypePtr& type,
                                            FilterMap& filter_map, size_t batch_size,
                                            size_t* read_rows, bool* eof, bool is_dict_filter) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    if (doris_column->is_nullable()) {
        auto mutable_column = doris_column->assume_mutable();
        auto* nullable_column = assert_cast<vectorized::ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assume_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_STRUCT) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Struct type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& doris_struct = assert_cast<ColumnStruct&>(*data_column);
    const auto* doris_struct_type = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());

    int64_t not_missing_column_id = -1;
    std::vector<size_t> missing_column_idxs {};

    _read_column_names.clear();

    for (size_t i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& doris_field = doris_struct.get_column_ptr(i);
        auto& doris_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(i));
        auto& doris_name = const_cast<String&>(doris_struct_type->get_element_name(i));

        // remember the missing column index
        if (_child_readers.find(doris_name) == _child_readers.end()) {
            missing_column_idxs.push_back(i);
            continue;
        }

        _read_column_names.emplace_back(doris_name);

        //        select_vector.reset();
        size_t field_rows = 0;
        bool field_eof = false;
        if (not_missing_column_id == -1) {
            not_missing_column_id = i;
            RETURN_IF_ERROR(_child_readers[doris_name]->read_column_data(
                    doris_field, doris_type, filter_map, batch_size, &field_rows, &field_eof,
                    is_dict_filter));
            *read_rows = field_rows;
            *eof = field_eof;
            /*
             * Considering the issue in the `_read_nested_column` function where data may span across pages, leading
             * to missing definition and repetition levels, when filling the null_map of the struct later, it is
             * crucial to use the definition and repetition levels from the first read column
             * (since `_read_nested_column` is not called repeatedly).
             *
             *  It is worth mentioning that, theoretically, any sub-column can be chosen to fill the null_map,
             *  and selecting the shortest one will offer better performance
             */
        } else {
            while (field_rows < *read_rows && !field_eof) {
                size_t loop_rows = 0;
                RETURN_IF_ERROR(_child_readers[doris_name]->read_column_data(
                        doris_field, doris_type, filter_map, *read_rows - field_rows, &loop_rows,
                        &field_eof, is_dict_filter));
                field_rows += loop_rows;
            }
            DCHECK_EQ(*read_rows, field_rows);
            DCHECK_EQ(*eof, field_eof);
        }
    }

    if (not_missing_column_id == -1) {
        // TODO: support read struct which columns are all missing
        return Status::Corruption("Not support read struct '{}' which columns are all missing",
                                  _field_schema->name);
    }

    //  This missing_column_sz is not *read_rows. Because read_rows returns the number of rows.
    //  For example: suppose we have a column array<struct<a:int,b:string>>,
    //  where b is a newly added column, that is, a missing column.
    //  There are two rows of data in this column,
    //      [{1,null},{2,null},{3,null}]
    //      [{4,null},{5,null}]
    //  When you first read subcolumn a, you read 5 data items and the value of *read_rows is 2.
    //  You should insert 5 records into subcolumn b instead of 2.
    auto missing_column_sz = doris_struct.get_column(not_missing_column_id).size();
    // fill missing column with null or default value
    for (auto idx : missing_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(idx));
        DCHECK(doris_type->is_nullable());
        auto mutable_column = doris_field->assume_mutable();
        auto* nullable_column = static_cast<vectorized::ColumnNullable*>(mutable_column.get());
        nullable_column->insert_many_defaults(missing_column_sz);
    }

    if (null_map_ptr != nullptr) {
        fill_struct_null_map(_field_schema, *null_map_ptr, this->get_rep_level(),
                             this->get_def_level());
    }

    return Status::OK();
}
#include "common/compile_check_end.h"

}; // namespace doris::vectorized
