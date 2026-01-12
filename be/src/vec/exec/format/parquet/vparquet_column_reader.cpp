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

#include "io/fs/tracing_file_reader.h"
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
                                   const tparquet::RowGroup& row_group, const RowRanges& row_ranges,
                                   const cctz::time_zone* ctz, io::IOContext* io_ctx,
                                   std::unique_ptr<ParquetColumnReader>& reader,
                                   size_t max_buf_size,
                                   std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                                   RuntimeState* state, bool in_collection,
                                   const std::set<uint64_t>& column_ids,
                                   const std::set<uint64_t>& filter_column_ids) {
    size_t total_rows = row_group.num_rows;
    if (field->data_type->get_primitive_type() == TYPE_ARRAY) {
        std::unique_ptr<ParquetColumnReader> element_reader;
        RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                               element_reader, max_buf_size, col_offsets, state, true, column_ids,
                               filter_column_ids));
        auto array_reader = ArrayColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        element_reader->set_column_in_nested();
        RETURN_IF_ERROR(array_reader->init(std::move(element_reader), field));
        array_reader->_filter_column_ids = filter_column_ids;
        reader.reset(array_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_MAP) {
        std::unique_ptr<ParquetColumnReader> key_reader;
        std::unique_ptr<ParquetColumnReader> value_reader;

        if (column_ids.empty() ||
            column_ids.find(field->children[0].get_column_id()) != column_ids.end()) {
            // Create key reader
            RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                                   key_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, &field->children[0]);
            key_reader = std::move(skip_reader);
        }

        if (column_ids.empty() ||
            column_ids.find(field->children[1].get_column_id()) != column_ids.end()) {
            // Create value reader
            RETURN_IF_ERROR(create(file, &field->children[1], row_group, row_ranges, ctz, io_ctx,
                                   value_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, &field->children[0]);
            value_reader = std::move(skip_reader);
        }

        auto map_reader = MapColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        key_reader->set_column_in_nested();
        value_reader->set_column_in_nested();
        RETURN_IF_ERROR(map_reader->init(std::move(key_reader), std::move(value_reader), field));
        map_reader->_filter_column_ids = filter_column_ids;
        reader.reset(map_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_STRUCT) {
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> child_readers;
        child_readers.reserve(field->children.size());
        int non_skip_reader_idx = -1;
        for (int i = 0; i < field->children.size(); ++i) {
            auto& child = field->children[i];
            std::unique_ptr<ParquetColumnReader> child_reader;
            if (column_ids.empty() || column_ids.find(child.get_column_id()) != column_ids.end()) {
                RETURN_IF_ERROR(create(file, &child, row_group, row_ranges, ctz, io_ctx,
                                       child_reader, max_buf_size, col_offsets, state,
                                       in_collection, column_ids, filter_column_ids));
                child_readers[child.name] = std::move(child_reader);
                // Record the first non-SkippingReader
                if (non_skip_reader_idx == -1) {
                    non_skip_reader_idx = i;
                }
            } else {
                auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                       io_ctx, &child);
                skip_reader->_filter_column_ids = filter_column_ids;
                child_readers[child.name] = std::move(skip_reader);
            }
            child_readers[child.name]->set_column_in_nested();
        }
        // If all children are SkipReadingReader, force the first child to call create
        if (non_skip_reader_idx == -1) {
            std::unique_ptr<ParquetColumnReader> child_reader;
            RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                                   child_reader, max_buf_size, col_offsets, state, in_collection,
                                   column_ids, filter_column_ids));
            child_reader->set_column_in_nested();
            child_readers[field->children[0].name] = std::move(child_reader);
        }
        auto struct_reader = StructColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        RETURN_IF_ERROR(struct_reader->init(std::move(child_readers), field));
        struct_reader->_filter_column_ids = filter_column_ids;
        reader.reset(struct_reader.release());
    } else {
        auto physical_index = field->physical_column_index;
        const tparquet::OffsetIndex* offset_index =
                col_offsets.find(physical_index) != col_offsets.end() ? &col_offsets[physical_index]
                                                                      : nullptr;

        const tparquet::ColumnChunk& chunk = row_group.columns[physical_index];
        if (in_collection) {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<true, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<true, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        } else {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<false, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<false, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        }
    }
    return Status::OK();
}

void ParquetColumnReader::_generate_read_ranges(RowRange page_row_range,
                                                RowRanges* result_ranges) const {
    result_ranges->add(page_row_range);
    RowRanges::ranges_intersection(*result_ranges, _row_ranges, result_ranges);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::init(io::FileReaderSPtr file,
                                                             FieldSchema* field,
                                                             size_t max_buf_size,
                                                             RuntimeState* state) {
    _field_schema = field;
    auto& chunk_meta = _chunk_meta.meta_data;
    int64_t chunk_start = has_dict_page(chunk_meta) ? chunk_meta.dictionary_page_offset
                                                    : chunk_meta.data_page_offset;
    size_t chunk_len = chunk_meta.total_compressed_size;
    size_t prefetch_buffer_size = std::min(chunk_len, max_buf_size);
    if ((typeid_cast<doris::io::TracingFileReader*>(file.get()) &&
         typeid_cast<io::MergeRangeFileReader*>(
                 ((doris::io::TracingFileReader*)(file.get()))->inner_reader().get())) ||
        typeid_cast<io::MergeRangeFileReader*>(file.get())) {
        // turn off prefetch data when using MergeRangeFileReader
        prefetch_buffer_size = 0;
    }
    _stream_reader = std::make_unique<io::BufferedFileStreamReader>(file, chunk_start, chunk_len,
                                                                    prefetch_buffer_size);
    ParquetPageReadContext ctx(
            (state == nullptr) ? true : state->query_options().enable_parquet_file_page_cache);

    _chunk_reader = std::make_unique<ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>>(
            _stream_reader.get(), &_chunk_meta, field, _offset_index, _total_rows, _io_ctx, ctx);
    RETURN_IF_ERROR(_chunk_reader->init());
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_skip_values(size_t num_values) {
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
            if (def_level < _field_schema->definition_level) {
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

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_values(size_t num_values,
                                                                     ColumnPtr& doris_column,
                                                                     DataTypePtr& type,
                                                                     FilterMap& filter_map,
                                                                     bool is_dict_filter) {
    if (num_values == 0) {
        return Status::OK();
    }
    MutableColumnPtr data_column;
    std::vector<uint16_t> null_map;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        // doris_column either originates from a mutable block in vparquet_group_reader
        // or is a newly created ColumnPtr, and therefore can be modified.
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

                bool is_null = def_level < _field_schema->definition_level;
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
template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_nested_column(
        ColumnPtr& doris_column, DataTypePtr& type, FilterMap& filter_map, size_t batch_size,
        size_t* read_rows, bool* eof, bool is_dict_filter) {
    _rep_levels.clear();
    _def_levels.clear();

    // Handle nullable columns
    MutableColumnPtr data_column;
    NullMap* map_data_column = nullptr;
    if (doris_column->is_nullable()) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        // doris_column either originates from a mutable block in vparquet_group_reader
        // or is a newly created ColumnPtr, and therefore can be modified.
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

    std::vector<uint16_t> null_map;
    std::unordered_set<size_t> ancestor_null_indices;
    std::vector<uint8_t> nested_filter_map_data;

    auto read_and_fill_data = [&](size_t before_rep_level_sz, size_t filter_map_index) {
        RETURN_IF_ERROR(_chunk_reader->fill_def(_def_levels));
        std::unique_ptr<FilterMap> nested_filter_map = std::make_unique<FilterMap>();
        if (filter_map.has_filter()) {
            RETURN_IF_ERROR(gen_filter_map(filter_map, filter_map_index, before_rep_level_sz,
                                           _rep_levels.size(), nested_filter_map_data,
                                           &nested_filter_map));
        }

        null_map.clear();
        ancestor_null_indices.clear();
        RETURN_IF_ERROR(gen_nested_null_map(before_rep_level_sz, _rep_levels.size(), null_map,
                                            ancestor_null_indices));

        ColumnSelectVector select_vector;
        {
            SCOPED_RAW_TIMER(&_decode_null_map_time);
            RETURN_IF_ERROR(select_vector.init(
                    null_map,
                    _rep_levels.size() - before_rep_level_sz - ancestor_null_indices.size(),
                    map_data_column, nested_filter_map.get(), 0, &ancestor_null_indices));
        }

        RETURN_IF_ERROR(
                _chunk_reader->decode_values(data_column, type, select_vector, is_dict_filter));
        if (ancestor_null_indices.size() != 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(ancestor_null_indices.size(), false));
        }
        if (filter_map.has_filter()) {
            auto new_rep_sz = before_rep_level_sz;
            for (size_t idx = before_rep_level_sz; idx < _rep_levels.size(); idx++) {
                if (nested_filter_map_data[idx - before_rep_level_sz]) {
                    _rep_levels[new_rep_sz] = _rep_levels[idx];
                    _def_levels[new_rep_sz] = _def_levels[idx];
                    new_rep_sz++;
                }
            }
            _rep_levels.resize(new_rep_sz);
            _def_levels.resize(new_rep_sz);
        }
        return Status::OK();
    };

    while (_current_range_idx < _row_ranges.range_size()) {
        size_t left_row =
                std::max(_current_row_index, _row_ranges.get_range_from(_current_range_idx));
        size_t right_row = std::min(left_row + batch_size - *read_rows,
                                    (size_t)_row_ranges.get_range_to(_current_range_idx));
        _current_row_index = left_row;
        RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(left_row));
        size_t load_rows = 0;
        bool cross_page = false;
        size_t before_rep_level_sz = _rep_levels.size();
        RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(_rep_levels, right_row - left_row,
                                                             &load_rows, &cross_page));
        RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index));
        _filter_map_index += load_rows;
        while (cross_page) {
            before_rep_level_sz = _rep_levels.size();
            RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(_rep_levels, &cross_page));
            RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index - 1));
        }
        *read_rows += load_rows;
        _current_row_index += load_rows;
        _current_range_idx += (_current_row_index == _row_ranges.get_range_to(_current_range_idx));
        if (*read_rows == batch_size) {
            break;
        }
    }
    *eof = _current_range_idx == _row_ranges.range_size();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_dict_values_to_column(
        MutableColumnPtr& doris_column, bool* has_dict) {
    bool loaded;
    RETURN_IF_ERROR(_try_load_dict_page(&loaded, has_dict));
    if (loaded && *has_dict) {
        return _chunk_reader->read_dict_values_to_column(doris_column);
    }
    return Status::OK();
}
template <bool IN_COLLECTION, bool OFFSET_INDEX>
MutableColumnPtr
ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::convert_dict_column_to_string_column(
        const ColumnInt32* dict_column) {
    return _chunk_reader->convert_dict_column_to_string_column(dict_column);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_try_load_dict_page(bool* loaded,
                                                                            bool* has_dict) {
    // _chunk_reader init will load first page header to check whether has dict page
    *loaded = true;
    *has_dict = _chunk_reader->has_dict();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
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
    // !FIXME: We should verify whether the get_physical_column logic is correct, why do we return a doris_column?
    ColumnPtr resolved_column =
            _converter->get_physical_column(_field_schema->physical_type, _field_schema->data_type,
                                            doris_column, type, is_dict_filter);
    DataTypePtr& resolved_type = _converter->get_physical_type();

    _def_levels.clear();
    _rep_levels.clear();
    *read_rows = 0;

    if (_in_nested) {
        RETURN_IF_ERROR(_read_nested_column(resolved_column, resolved_type, filter_map, batch_size,
                                            read_rows, eof, is_dict_filter));
        return _converter->convert(resolved_column, _field_schema->data_type, type, doris_column,
                                   is_dict_filter);
    }

    int64_t right_row = 0;
    if constexpr (OFFSET_INDEX == false) {
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());
        right_row = _chunk_reader->page_end_row();
    } else {
        right_row = _chunk_reader->page_end_row();
    }

    do {
        // generate the row ranges that should be read
        RowRanges read_ranges;
        _generate_read_ranges(RowRange {_current_row_index, right_row}, &read_ranges);
        if (read_ranges.count() == 0) {
            // skip the whole page
            _current_row_index = right_row;
        } else {
            bool skip_whole_batch = false;
            // Determining whether to skip page or batch will increase the calculation time.
            // When the filtering effect is greater than 60%, it is possible to skip the page or batch.
            if (filter_map.has_filter() && filter_map.filter_ratio() > 0.6) {
                // lazy read
                size_t remaining_num_values = read_ranges.count();
                if (batch_size >= remaining_num_values &&
                    filter_map.can_filter_all(remaining_num_values, _filter_map_index)) {
                    // We can skip the whole page if the remaining values are filtered by predicate columns
                    _filter_map_index += remaining_num_values;
                    _current_row_index = right_row;
                    *read_rows = remaining_num_values;
                    break;
                }
                skip_whole_batch = batch_size <= remaining_num_values &&
                                   filter_map.can_filter_all(batch_size, _filter_map_index);
                if (skip_whole_batch) {
                    _filter_map_index += batch_size;
                }
            }
            // load page data to decode or skip values
            RETURN_IF_ERROR(_chunk_reader->parse_page_header());
            RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
            size_t has_read = 0;
            for (size_t idx = 0; idx < read_ranges.range_size(); idx++) {
                auto range = read_ranges.get_range(idx);
                // generate the skipped values
                size_t skip_values = range.from() - _current_row_index;
                RETURN_IF_ERROR(_skip_values(skip_values));
                _current_row_index += skip_values;
                // generate the read values
                size_t read_values =
                        std::min((size_t)(range.to() - range.from()), batch_size - has_read);
                if (skip_whole_batch) {
                    RETURN_IF_ERROR(_skip_values(read_values));
                } else {
                    RETURN_IF_ERROR(_read_values(read_values, resolved_column, resolved_type,
                                                 filter_map, is_dict_filter));
                }
                has_read += read_values;
                *read_rows += read_values;
                _current_row_index += read_values;
                if (has_read == batch_size) {
                    break;
                }
            }
        }
    } while (false);

    if (right_row == _current_row_index) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
        } else {
            RETURN_IF_ERROR(_chunk_reader->next_page());
        }
    }

    return _converter->convert(resolved_column, _field_schema->data_type, type, doris_column,
                               is_dict_filter);
}

Status ArrayColumnReader::init(std::unique_ptr<ParquetColumnReader> element_reader,
                               FieldSchema* field) {
    _field_schema = field;
    _element_reader = std::move(element_reader);
    return Status::OK();
}

Status ArrayColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
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
    const DataTypePtr& element_type =
            (assert_cast<const DataTypeArray*>(remove_nullable(type).get()))->get_nested_type();
    // read nested column
    RETURN_IF_ERROR(_element_reader->read_column_data(element_column, element_type,
                                                      root_node->get_element_node(), filter_map,
                                                      batch_size, read_rows, eof, is_dict_filter));
    if (*read_rows == 0) {
        return Status::OK();
    }

    ColumnArray::Offsets64& offsets_data = assert_cast<ColumnArray&>(*data_column).get_offsets();
    // fill offset and null map
    fill_array_offset(_field_schema, offsets_data, null_map_ptr, _element_reader->get_rep_level(),
                      _element_reader->get_def_level());
    DCHECK_EQ(element_column->size(), offsets_data.back());
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
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

Status MapColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
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
    const DataTypePtr& key_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_key_type();
    const DataTypePtr& value_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_value_type();
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();

    size_t key_rows = 0;
    size_t value_rows = 0;
    bool key_eof = false;
    bool value_eof = false;
    int64_t orig_col_column_size = key_column->size();

    RETURN_IF_ERROR(_key_reader->read_column_data(key_column, key_type, root_node->get_key_node(),
                                                  filter_map, batch_size, &key_rows, &key_eof,
                                                  is_dict_filter));

    while (value_rows < key_rows && !value_eof) {
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_value_reader->read_column_data(
                value_column, value_type, root_node->get_value_node(), filter_map,
                key_rows - value_rows, &loop_rows, &value_eof, is_dict_filter,
                key_column->size() - orig_col_column_size));
        value_rows += loop_rows;
    }
    DCHECK_EQ(key_rows, value_rows);
    *read_rows = key_rows;
    *eof = key_eof;

    if (*read_rows == 0) {
        return Status::OK();
    }

    DCHECK_EQ(key_column->size(), value_column->size());
    // fill offset and null map
    fill_array_offset(_field_schema, map.get_offsets(), null_map_ptr, _key_reader->get_rep_level(),
                      _key_reader->get_def_level());
    DCHECK_EQ(key_column->size(), map.get_offsets().back());
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status StructColumnReader::init(
        std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>>&& child_readers,
        FieldSchema* field) {
    _field_schema = field;
    _child_readers = std::move(child_readers);
    return Status::OK();
}
Status StructColumnReader::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<TableSchemaChangeHelper::Node>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
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
    size_t not_missing_orig_column_size = 0;
    std::vector<size_t> missing_column_idxs {};
    std::vector<size_t> skip_reading_column_idxs {};

    _read_column_names.clear();

    for (size_t i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& doris_field = doris_struct.get_column_ptr(i);
        auto& doris_type = doris_struct_type->get_element(i);
        auto& doris_name = doris_struct_type->get_element_name(i);
        if (!root_node->children_column_exists(doris_name)) {
            missing_column_idxs.push_back(i);
            VLOG_DEBUG << "[ParquetReader] Missing column in schema: column_idx[" << i
                       << "], doris_name: " << doris_name << " (column not exists in root node)";
            continue;
        }
        auto file_name = root_node->children_file_column_name(doris_name);

        // Check if this is a SkipReadingReader - we should skip it when choosing reference column
        // because SkipReadingReader doesn't know the actual data size in nested context
        bool is_skip_reader =
                dynamic_cast<SkipReadingReader*>(_child_readers[file_name].get()) != nullptr;

        if (is_skip_reader) {
            // Store SkipReadingReader columns to fill them later based on reference column size
            skip_reading_column_idxs.push_back(i);
            continue;
        }

        // Only add non-SkipReadingReader columns to _read_column_names
        // This ensures get_rep_level() and get_def_level() return valid levels
        _read_column_names.emplace_back(file_name);

        size_t field_rows = 0;
        bool field_eof = false;
        if (not_missing_column_id == -1) {
            not_missing_column_id = i;
            not_missing_orig_column_size = doris_field->size();
            RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                    doris_field, doris_type, root_node->get_children_node(doris_name), filter_map,
                    batch_size, &field_rows, &field_eof, is_dict_filter));
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
                RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                        doris_field, doris_type, root_node->get_children_node(doris_name),
                        filter_map, *read_rows - field_rows, &loop_rows, &field_eof,
                        is_dict_filter));
                field_rows += loop_rows;
            }
            DCHECK_EQ(*read_rows, field_rows);
            //            DCHECK_EQ(*eof, field_eof);
        }
    }

    int64_t missing_column_sz = -1;

    if (not_missing_column_id == -1) {
        // All queried columns are missing in the file (e.g., all added after schema change)
        // We need to pick a column from _field_schema children that exists in the file for RL/DL reference
        std::string reference_file_column_name;
        std::unique_ptr<ParquetColumnReader>* reference_reader = nullptr;

        for (const auto& child : _field_schema->children) {
            auto it = _child_readers.find(child.name);
            if (it != _child_readers.end()) {
                // Skip SkipReadingReader as they don't have valid RL/DL
                bool is_skip_reader = dynamic_cast<SkipReadingReader*>(it->second.get()) != nullptr;
                if (!is_skip_reader) {
                    reference_file_column_name = child.name;
                    reference_reader = &(it->second);
                    break;
                }
            }
        }

        if (reference_reader != nullptr) {
            // Read the reference column to get correct RL/DL information
            // TODO: Optimize by only reading RL/DL without actual data decoding

            // We need to find the FieldSchema for the reference column from _field_schema children
            FieldSchema* ref_field_schema = nullptr;
            for (auto& child : _field_schema->children) {
                if (child.name == reference_file_column_name) {
                    ref_field_schema = &child;
                    break;
                }
            }

            if (ref_field_schema == nullptr) {
                return Status::InternalError(
                        "Cannot find field schema for reference column '{}' in struct '{}'",
                        reference_file_column_name, _field_schema->name);
            }

            // Create a temporary column to hold the data (we'll use its size for missing_column_sz)
            ColumnPtr temp_column = ref_field_schema->data_type->create_column();
            auto temp_type = ref_field_schema->data_type;

            size_t field_rows = 0;
            bool field_eof = false;

            // Use ConstNode for the reference column instead of looking up from root_node.
            // The reference column is only used to get RL/DL information for determining the number
            // of elements in the struct. It may be a column that has been dropped from the table
            // schema (e.g., 'removed' field), but still exists in older parquet files.
            // Since we don't need schema mapping for this column (we just need its RL/DL levels),
            // using ConstNode is safe and avoids the issue where the reference column doesn't exist
            // in root_node (because it was dropped from table schema).
            auto ref_child_node = TableSchemaChangeHelper::ConstNode::get_instance();
            not_missing_orig_column_size = temp_column->size();

            RETURN_IF_ERROR((*reference_reader)
                                    ->read_column_data(temp_column, temp_type, ref_child_node,
                                                       filter_map, batch_size, &field_rows,
                                                       &field_eof, is_dict_filter));

            *read_rows = field_rows;
            *eof = field_eof;

            // Store this reference column name for get_rep_level/get_def_level to use
            _read_column_names.emplace_back(reference_file_column_name);

            missing_column_sz = temp_column->size() - not_missing_orig_column_size;
        } else {
            return Status::Corruption(
                    "Cannot read struct '{}': all queried columns are missing and no reference "
                    "column found in file",
                    _field_schema->name);
        }
    }

    //  This missing_column_sz is not *read_rows. Because read_rows returns the number of rows.
    //  For example: suppose we have a column array<struct<a:int,b:string>>,
    //  where b is a newly added column, that is, a missing column.
    //  There are two rows of data in this column,
    //      [{1,null},{2,null},{3,null}]
    //      [{4,null},{5,null}]
    //  When you first read subcolumn a, you read 5 data items and the value of *read_rows is 2.
    //  You should insert 5 records into subcolumn b instead of 2.
    if (missing_column_sz == -1) {
        missing_column_sz = doris_struct.get_column(not_missing_column_id).size() -
                            not_missing_orig_column_size;
    }

    // Fill SkipReadingReader columns with the correct amount of data based on reference column
    // Let SkipReadingReader handle the data filling through its read_column_data method
    for (auto idx : skip_reading_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(idx));
        auto& doris_name = const_cast<String&>(doris_struct_type->get_element_name(idx));
        auto file_name = root_node->children_file_column_name(doris_name);

        size_t field_rows = 0;
        bool field_eof = false;
        RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                doris_field, doris_type, root_node->get_children_node(doris_name), filter_map,
                missing_column_sz, &field_rows, &field_eof, is_dict_filter, missing_column_sz));
    }

    // Fill truly missing columns (not in root_node) with null or default value
    for (auto idx : missing_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = doris_struct_type->get_element(idx);
        DCHECK(doris_type->is_nullable());
        auto mutable_column = doris_field->assume_mutable();
        auto* nullable_column = static_cast<vectorized::ColumnNullable*>(mutable_column.get());
        nullable_column->insert_many_defaults(missing_column_sz);
    }

    if (null_map_ptr != nullptr) {
        fill_struct_null_map(_field_schema, *null_map_ptr, this->get_rep_level(),
                             this->get_def_level());
    }
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

template class ScalarColumnReader<true, true>;
template class ScalarColumnReader<true, false>;
template class ScalarColumnReader<false, true>;
template class ScalarColumnReader<false, false>;

#include "common/compile_check_end.h"

}; // namespace doris::vectorized
