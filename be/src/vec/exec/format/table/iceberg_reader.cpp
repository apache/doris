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

#include "iceberg_reader.h"

#include <ctype.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <memory>
#include <mutex>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
IcebergTableReader::IcebergTableReader(std::unique_ptr<GenericReader> file_format_reader,
                                       RuntimeProfile* profile, RuntimeState* state,
                                       const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range, ShardedKVCache* kv_cache,
                                       io::IOContext* io_ctx, int64_t push_down_count)
        : TableFormatReader(std::move(file_format_reader)),
          _profile(profile),
          _state(state),
          _params(params),
          _range(range),
          _kv_cache(kv_cache),
          _io_ctx(io_ctx),
          _remaining_push_down_count(push_down_count) {
    static const char* iceberg_profile = "IcebergProfile";
    ADD_TIMER(_profile, iceberg_profile);
    _iceberg_profile.num_delete_files =
            ADD_CHILD_COUNTER(_profile, "NumDeleteFiles", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", iceberg_profile);
    _iceberg_profile.delete_rows_sort_time =
            ADD_CHILD_TIMER(_profile, "DeleteRowsSortTime", iceberg_profile);
}

Status IcebergTableReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    // already get rows from be
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_push_down_count > 0) {
        auto rows =
                std::min(_remaining_push_down_count, (int64_t)_state->query_options().batch_size);
        _remaining_push_down_count -= rows;
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));
        *read_rows = rows;
        if (_remaining_push_down_count == 0) {
            *eof = true;
        }

        return Status::OK();
    }
    RETURN_IF_ERROR(_expand_block_if_need(block));

    // To support iceberg schema evolution. We change the column name in block to
    // make it match with the column name in parquet file before reading data. and
    // Set the name back to table column name before return this block.
    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _table_col_to_file_col.find(col.name);
            if (iter != _table_col_to_file_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }

    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    // Set the name back to table column name before return this block.
    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _file_col_to_table_col.find(col.name);
            if (iter != _file_col_to_table_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }

    if (_equality_delete_impl != nullptr) {
        RETURN_IF_ERROR(_equality_delete_impl->filter_data_block(block));
        *read_rows = block->rows();
    }
    return _shrink_block_if_need(block);
}

Status IcebergTableReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    return _file_format_reader->get_columns(name_to_type, missing_cols);
}

Status IcebergTableReader::init_row_filters(const TFileRangeDesc& range, io::IOContext* io_ctx) {
    // We get the count value by doris's be, so we don't need to read the delete file
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_push_down_count > 0) {
        return Status::OK();
    }

    auto& table_desc = range.table_format_params.iceberg_params;
    auto& version = table_desc.format_version;
    if (version < MIN_SUPPORT_DELETE_FILES_VERSION) {
        return Status::OK();
    }

    std::vector<TIcebergDeleteFileDesc> position_delete_files;
    std::vector<TIcebergDeleteFileDesc> equality_delete_files;
    for (const TIcebergDeleteFileDesc& desc : table_desc.delete_files) {
        if (desc.content == POSITION_DELETE) {
            position_delete_files.emplace_back(desc);
        } else if (desc.content == EQUALITY_DELETE) {
            equality_delete_files.emplace_back(desc);
        }
    }

    if (position_delete_files.size() > 0) {
        RETURN_IF_ERROR(
                _position_delete_base(table_desc.original_file_path, position_delete_files));
    }
    if (equality_delete_files.size() > 0) {
        RETURN_IF_ERROR(_equality_delete_base(equality_delete_files));
    }

    COUNTER_UPDATE(_iceberg_profile.num_delete_files, table_desc.delete_files.size());
    return Status::OK();
}

Status IcebergTableReader::_equality_delete_base(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    bool init_schema = false;
    std::vector<std::string> equality_delete_col_names;
    std::vector<TypeDescriptor> equality_delete_col_types;
    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    std::vector<std::string> not_in_file_col_names;

    for (auto& delete_file : delete_files) {
        TFileRangeDesc delete_desc;
        // must use __set() method to make sure __isset is true
        delete_desc.__set_fs_name(_range.fs_name);
        delete_desc.path = delete_file.path;
        delete_desc.start_offset = 0;
        delete_desc.size = -1;
        delete_desc.file_size = -1;
        std::unique_ptr<GenericReader> delete_reader = _create_equality_reader(delete_desc);
        if (!init_schema) {
            RETURN_IF_ERROR(delete_reader->get_parsed_schema(&equality_delete_col_names,
                                                             &equality_delete_col_types));
            _generate_equality_delete_block(&_equality_delete_block, equality_delete_col_names,
                                            equality_delete_col_types);
            init_schema = true;
        }
        if (auto* parquet_reader = typeid_cast<ParquetReader*>(delete_reader.get())) {
            RETURN_IF_ERROR(parquet_reader->open());
            RETURN_IF_ERROR(parquet_reader->init_reader(equality_delete_col_names,
                                                        not_in_file_col_names, nullptr, {}, nullptr,
                                                        nullptr, nullptr, nullptr, nullptr, false));
        } else if (auto* orc_reader = typeid_cast<OrcReader*>(delete_reader.get())) {
            RETURN_IF_ERROR(orc_reader->init_reader(&equality_delete_col_names, nullptr, {}, false,
                                                    {}, {}, nullptr, nullptr));
        } else {
            return Status::InternalError("Unsupported format of delete file");
        }

        RETURN_IF_ERROR(delete_reader->set_fill_columns(partition_columns, missing_columns));

        bool eof = false;
        while (!eof) {
            Block block;
            _generate_equality_delete_block(&block, equality_delete_col_names,
                                            equality_delete_col_types);
            size_t read_rows = 0;
            RETURN_IF_ERROR(delete_reader->get_next_block(&block, &read_rows, &eof));
            if (read_rows > 0) {
                MutableBlock mutable_block(&_equality_delete_block);
                RETURN_IF_ERROR(mutable_block.merge(block));
            }
        }
    }
    for (int i = 0; i < equality_delete_col_names.size(); ++i) {
        const std::string& delete_col = equality_delete_col_names[i];
        if (std::find(_all_required_col_names.begin(), _all_required_col_names.end(), delete_col) ==
            _all_required_col_names.end()) {
            _expand_col_names.emplace_back(delete_col);
            DataTypePtr data_type = DataTypeFactory::instance().create_data_type(
                    equality_delete_col_types[i], true);
            MutableColumnPtr data_column = data_type->create_column();
            _expand_columns.emplace_back(
                    ColumnWithTypeAndName(std::move(data_column), data_type, delete_col));
        }
    }
    for (const std::string& delete_col : _expand_col_names) {
        _all_required_col_names.emplace_back(delete_col);
    }
    _equality_delete_impl = EqualityDeleteBase::get_delete_impl(&_equality_delete_block);
    return _equality_delete_impl->init(_profile);
}

void IcebergTableReader::_generate_equality_delete_block(
        Block* block, const std::vector<std::string>& equality_delete_col_names,
        const std::vector<TypeDescriptor>& equality_delete_col_types) {
    for (int i = 0; i < equality_delete_col_names.size(); ++i) {
        DataTypePtr data_type =
                DataTypeFactory::instance().create_data_type(equality_delete_col_types[i], true);
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                            equality_delete_col_names[i]));
    }
}

Status IcebergTableReader::_expand_block_if_need(Block* block) {
    for (auto& col : _expand_columns) {
        col.column->assume_mutable()->clear();
        if (block->try_get_by_name(col.name)) {
            return Status::InternalError("Wrong expand column '{}'", col.name);
        }
        block->insert(col);
    }
    return Status::OK();
}

Status IcebergTableReader::_shrink_block_if_need(Block* block) {
    for (const std::string& expand_col : _expand_col_names) {
        block->erase(expand_col);
    }
    return Status::OK();
}

Status IcebergTableReader::_position_delete_base(
        const std::string data_file_path, const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::vector<DeleteRows*> delete_rows_array;
    int64_t num_delete_rows = 0;
    std::vector<DeleteFile*> erase_data;
    for (auto& delete_file : delete_files) {
        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        Status create_status = Status::OK();
        auto* delete_file_cache = _kv_cache->get<DeleteFile>(
                _delet_file_cache_key(delete_file.path), [&]() -> DeleteFile* {
                    auto* position_delete = new DeleteFile;
                    TFileRangeDesc delete_file_range;
                    // must use __set() method to make sure __isset is true
                    delete_file_range.__set_fs_name(_range.fs_name);
                    delete_file_range.path = delete_file.path;
                    delete_file_range.start_offset = 0;
                    delete_file_range.size = -1;
                    delete_file_range.file_size = -1;
                    //read position delete file base on delete_file_range , generate DeleteFile , add DeleteFile to kv_cache
                    create_status = _read_position_delete_file(&delete_file_range, position_delete);

                    if (!create_status) {
                        return nullptr;
                    }

                    return position_delete;
                });
        if (create_status.is<ErrorCode::END_OF_FILE>()) {
            continue;
        } else if (!create_status.ok()) {
            return create_status;
        }

        DeleteFile& delete_file_map = *((DeleteFile*)delete_file_cache);
        auto get_value = [&](const auto& v) {
            DeleteRows* row_ids = v.second.get();
            if (row_ids->size() > 0) {
                delete_rows_array.emplace_back(row_ids);
                num_delete_rows += row_ids->size();
                erase_data.emplace_back(delete_file_cache);
            }
        };
        delete_file_map.if_contains(data_file_path, get_value);
    }
    if (num_delete_rows > 0) {
        SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
        _sort_delete_rows(delete_rows_array, num_delete_rows);
        this->set_delete_rows();
        COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
    }
    // the deleted rows are copy out, we can erase them.
    for (auto& erase_item : erase_data) {
        erase_item->erase(data_file_path);
    }
    return Status::OK();
}

IcebergTableReader::PositionDeleteRange IcebergTableReader::_get_range(
        const ColumnDictI32& file_path_column) {
    IcebergTableReader::PositionDeleteRange range;
    int read_rows = file_path_column.get_data().size();
    int* code_path = const_cast<int*>(file_path_column.get_data().data());
    int* code_path_start = code_path;
    int* code_path_end = code_path + read_rows;
    while (code_path < code_path_end) {
        int code = code_path[0];
        int* code_end = std::upper_bound(code_path, code_path_end, code);
        range.data_file_path.emplace_back(file_path_column.get_value(code).to_string());
        range.range.emplace_back(
                std::make_pair(code_path - code_path_start, code_end - code_path_start));
        code_path = code_end;
    }
    return range;
}

IcebergTableReader::PositionDeleteRange IcebergTableReader::_get_range(
        const ColumnString& file_path_column) {
    IcebergTableReader::PositionDeleteRange range;
    int read_rows = file_path_column.size();
    int index = 0;
    while (index < read_rows) {
        StringRef data_path = file_path_column.get_data_at(index);
        int left = index - 1;
        int right = read_rows;
        while (left + 1 != right) {
            int mid = left + (right - left) / 2;
            if (file_path_column.get_data_at(mid) > data_path) {
                right = mid;
            } else {
                left = mid;
            }
        }
        range.data_file_path.emplace_back(data_path.to_string());
        range.range.emplace_back(std::make_pair(index, left + 1));
        index = left + 1;
    }
    return range;
}

void IcebergTableReader::_sort_delete_rows(std::vector<std::vector<int64_t>*>& delete_rows_array,
                                           int64_t num_delete_rows) {
    if (delete_rows_array.empty()) {
        return;
    }
    if (delete_rows_array.size() == 1) {
        _iceberg_delete_rows.resize(num_delete_rows);
        memcpy(&_iceberg_delete_rows[0], &((*delete_rows_array.front())[0]),
               sizeof(int64_t) * num_delete_rows);
        return;
    }
    if (delete_rows_array.size() == 2) {
        _iceberg_delete_rows.resize(num_delete_rows);
        std::merge(delete_rows_array.front()->begin(), delete_rows_array.front()->end(),
                   delete_rows_array.back()->begin(), delete_rows_array.back()->end(),
                   _iceberg_delete_rows.begin());
        return;
    }

    using vec_pair = std::pair<std::vector<int64_t>::iterator, std::vector<int64_t>::iterator>;
    _iceberg_delete_rows.resize(num_delete_rows);
    auto row_id_iter = _iceberg_delete_rows.begin();
    auto iter_end = _iceberg_delete_rows.end();
    std::vector<vec_pair> rows_array;
    for (auto rows : delete_rows_array) {
        if (rows->size() > 0) {
            rows_array.push_back({rows->begin(), rows->end()});
        }
    }
    int array_size = rows_array.size();
    while (row_id_iter != iter_end) {
        int min_index = 0;
        int min = *rows_array[0].first;
        for (int i = 0; i < array_size; ++i) {
            if (*rows_array[i].first < min) {
                min_index = i;
                min = *rows_array[i].first;
            }
        }
        *row_id_iter++ = min;
        rows_array[min_index].first++;
        if (UNLIKELY(rows_array[min_index].first == rows_array[min_index].second)) {
            rows_array.erase(rows_array.begin() + min_index);
            array_size--;
        }
    }
}

/*
 * Generate _all_required_col_names and _not_in_file_col_names.
 *
 * _all_required_col_names is all the columns required by user sql.
 * If the column name has been modified after the data file was written,
 * put the old name in data file to _all_required_col_names.
 *
 * _not_in_file_col_names is all the columns required by user sql but not in the data file.
 * e.g. New columns added after this data file was written.
 * The columns added with names used by old dropped columns should consider as a missing column,
 * which should be in _not_in_file_col_names.
 */
void IcebergTableReader::_gen_file_col_names() {
    _all_required_col_names.clear();
    _not_in_file_col_names.clear();
    for (int i = 0; i < _file_col_names.size(); ++i) {
        auto name = _file_col_names[i];
        auto iter = _table_col_to_file_col.find(name);
        if (iter == _table_col_to_file_col.end()) {
            // If the user creates the iceberg table, directly append the parquet file that already exists,
            // there is no 'iceberg.schema' field in the footer of parquet, the '_table_col_to_file_col' may be empty.
            // Because we are ignoring case, so, it is converted to lowercase here
            auto name_low = to_lower(name);
            _all_required_col_names.emplace_back(name_low);
            if (_has_iceberg_schema) {
                _not_in_file_col_names.emplace_back(name);
            } else {
                _table_col_to_file_col.emplace(name, name_low);
                _file_col_to_table_col.emplace(name_low, name);
                if (name != name_low) {
                    _has_schema_change = true;
                }
            }
        } else {
            _all_required_col_names.emplace_back(iter->second);
        }
    }
}

/*
 * Generate _new_colname_to_value_range, by replacing the column name in
 * _colname_to_value_range with column name in data file.
 */
void IcebergTableReader::_gen_new_colname_to_value_range() {
    for (auto it = _colname_to_value_range->begin(); it != _colname_to_value_range->end(); it++) {
        auto iter = _table_col_to_file_col.find(it->first);
        if (iter == _table_col_to_file_col.end()) {
            _new_colname_to_value_range.emplace(it->first, it->second);
        } else {
            _new_colname_to_value_range.emplace(iter->second, it->second);
        }
    }
}

void IcebergTableReader::_gen_position_delete_file_range(Block& block, DeleteFile* position_delete,
                                                         size_t read_rows,
                                                         bool file_path_column_dictionary_coded) {
    ColumnPtr path_column = block.get_by_name(ICEBERG_FILE_PATH).column;
    DCHECK_EQ(path_column->size(), read_rows);
    ColumnPtr pos_column = block.get_by_name(ICEBERG_ROW_POS).column;
    using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
    const int64_t* src_data = assert_cast<const ColumnType&>(*pos_column).get_data().data();
    IcebergTableReader::PositionDeleteRange range;
    if (file_path_column_dictionary_coded) {
        range = _get_range(assert_cast<const ColumnDictI32&>(*path_column));
    } else {
        range = _get_range(assert_cast<const ColumnString&>(*path_column));
    }
    for (int i = 0; i < range.range.size(); ++i) {
        std::string key = range.data_file_path[i];
        auto iter = position_delete->find(key);
        DeleteRows* delete_rows;
        if (iter == position_delete->end()) {
            delete_rows = new DeleteRows;
            std::unique_ptr<DeleteRows> delete_rows_ptr(delete_rows);
            (*position_delete)[key] = std::move(delete_rows_ptr);
        } else {
            delete_rows = iter->second.get();
        }
        const int64_t* cpy_start = src_data + range.range[i].first;
        const int64_t cpy_count = range.range[i].second - range.range[i].first;
        int64_t origin_size = delete_rows->size();
        delete_rows->resize(origin_size + cpy_count);
        int64_t* dest_position = &(*delete_rows)[origin_size];
        memcpy(dest_position, cpy_start, cpy_count * sizeof(int64_t));
    }
}

Status IcebergParquetReader::init_reader(
        const std::vector<std::string>& file_col_names,
        const std::unordered_map<int, std::string>& col_id_name_map,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::PARQUET;
    ParquetReader* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    _col_id_name_map = col_id_name_map;
    _file_col_names = file_col_names;
    _colname_to_value_range = colname_to_value_range;
    auto parquet_meta_kv = parquet_reader->get_metadata_key_values();
    RETURN_IF_ERROR(_gen_col_name_maps(parquet_meta_kv));
    _gen_file_col_names();
    _gen_new_colname_to_value_range();
    parquet_reader->set_table_to_file_col_map(_table_col_to_file_col);
    parquet_reader->iceberg_sanitize(_all_required_col_names);
    RETURN_IF_ERROR(init_row_filters(_range, _io_ctx));
    return parquet_reader->init_reader(
            _all_required_col_names, _not_in_file_col_names, &_new_colname_to_value_range,
            conjuncts, tuple_descriptor, row_descriptor, colname_to_slot_id,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
}

Status IcebergParquetReader ::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                         DeleteFile* position_delete) {
    ParquetReader parquet_delete_reader(
            _profile, _params, *delete_range, READ_DELETE_FILE_BATCH_SIZE,
            const_cast<cctz::time_zone*>(&_state->timezone_obj()), _io_ctx, _state);

    RETURN_IF_ERROR(parquet_delete_reader.open());
    RETURN_IF_ERROR(parquet_delete_reader.init_reader(delete_file_col_names, {}, nullptr, {},
                                                      nullptr, nullptr, nullptr, nullptr, nullptr,
                                                      false));

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    RETURN_IF_ERROR(parquet_delete_reader.set_fill_columns(partition_columns, missing_columns));

    const tparquet::FileMetaData* meta_data = parquet_delete_reader.get_meta_data();
    bool dictionary_coded = true;
    for (int j = 0; j < meta_data->row_groups.size(); ++j) {
        auto& column_chunk = meta_data->row_groups[j].columns[ICEBERG_FILE_PATH_INDEX];
        if (!(column_chunk.__isset.meta_data &&
              column_chunk.meta_data.__isset.dictionary_page_offset)) {
            dictionary_coded = false;
            break;
        }
    }
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    bool eof = false;
    while (!eof) {
        Block block = {dictionary_coded
                               ? ColumnWithTypeAndName {ColumnDictI32::create(),
                                                        data_type_file_path, ICEBERG_FILE_PATH}
                               : ColumnWithTypeAndName {data_type_file_path, ICEBERG_FILE_PATH},

                       {data_type_pos, ICEBERG_ROW_POS}};
        size_t read_rows = 0;
        RETURN_IF_ERROR(parquet_delete_reader.get_next_block(&block, &read_rows, &eof));

        if (read_rows <= 0) {
            break;
        }
        _gen_position_delete_file_range(block, position_delete, read_rows, dictionary_coded);
    }
    return Status::OK();
};

Status IcebergOrcReader::init_reader(
        const std::vector<std::string>& file_col_names,
        const std::unordered_map<int, std::string>& col_id_name_map,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _file_format = Fileformat::ORC;
    auto* orc_reader = static_cast<OrcReader*>(_file_format_reader.get());
    _col_id_name_map = col_id_name_map;
    _file_col_names = file_col_names;
    _colname_to_value_range = colname_to_value_range;

    RETURN_IF_ERROR(_gen_col_name_maps(orc_reader));
    _gen_file_col_names();
    _gen_new_colname_to_value_range();
    orc_reader->set_table_col_to_file_col(_table_col_to_file_col);
    RETURN_IF_ERROR(init_row_filters(_range, _io_ctx));
    return orc_reader->init_reader(&_all_required_col_names, &_new_colname_to_value_range,
                                   conjuncts, false, tuple_descriptor, row_descriptor,
                                   not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);
}

Status IcebergOrcReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                    DeleteFile* position_delete) {
    OrcReader orc_delete_reader(_profile, _state, _params, *delete_range,
                                READ_DELETE_FILE_BATCH_SIZE, _state->timezone(), _io_ctx);
    std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    RETURN_IF_ERROR(orc_delete_reader.init_reader(&delete_file_col_names, &colname_to_value_range,
                                                  {}, false, {}, {}, nullptr, nullptr));

    std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
            partition_columns;
    std::unordered_map<std::string, VExprContextSPtr> missing_columns;
    RETURN_IF_ERROR(orc_delete_reader.set_fill_columns(partition_columns, missing_columns));

    bool eof = false;
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    while (!eof) {
        Block block = {{data_type_file_path, ICEBERG_FILE_PATH}, {data_type_pos, ICEBERG_ROW_POS}};

        size_t read_rows = 0;
        RETURN_IF_ERROR(orc_delete_reader.get_next_block(&block, &read_rows, &eof));

        _gen_position_delete_file_range(block, position_delete, read_rows, false);
    }
    return Status::OK();
}

/*
 * To support schema evolution, Iceberg write the column id to column name map to
 * parquet file key_value_metadata.
 * This function is to compare the table schema from FE (_col_id_name_map) with
 * the schema in key_value_metadata for the current parquet file and generate two maps
 * for future use:
 * 1. table column name to parquet column name.
 * 2. parquet column name to table column name.
 * For example, parquet file has a column 'col1',
 * after this file was written, iceberg changed the column name to 'col1_new'.
 * The two maps would contain:
 * 1. col1_new -> col1
 * 2. col1 -> col1_new
 */
Status IcebergParquetReader::_gen_col_name_maps(std::vector<tparquet::KeyValue> parquet_meta_kv) {
    for (int i = 0; i < parquet_meta_kv.size(); ++i) {
        tparquet::KeyValue kv = parquet_meta_kv[i];
        if (kv.key == "iceberg.schema") {
            _has_iceberg_schema = true;
            std::string schema = kv.value;
            rapidjson::Document json;
            json.Parse(schema.c_str());

            if (json.HasMember("fields")) {
                rapidjson::Value& fields = json["fields"];
                if (fields.IsArray()) {
                    for (int j = 0; j < fields.Size(); j++) {
                        rapidjson::Value& e = fields[j];
                        rapidjson::Value& id = e["id"];
                        rapidjson::Value& name = e["name"];
                        std::string name_string = name.GetString();
                        transform(name_string.begin(), name_string.end(), name_string.begin(),
                                  ::tolower);
                        auto iter = _col_id_name_map.find(id.GetInt());
                        if (iter != _col_id_name_map.end()) {
                            _table_col_to_file_col.emplace(iter->second, name_string);
                            _file_col_to_table_col.emplace(name_string, iter->second);
                            if (name_string != iter->second) {
                                _has_schema_change = true;
                            }
                        } else {
                            _has_schema_change = true;
                        }
                    }
                }
            }
            break;
        }
    }
    return Status::OK();
}

Status IcebergOrcReader::_gen_col_name_maps(OrcReader* orc_reader) {
    std::vector<std::string> col_names;
    std::vector<uint64_t> col_ids;
    RETURN_IF_ERROR(
            orc_reader->get_schema_col_name_attribute(&col_names, &col_ids, ICEBERG_ORC_ATTRIBUTE));
    _has_iceberg_schema = true;
    _table_col_to_file_col.clear();
    _file_col_to_table_col.clear();
    for (size_t i = 0; i < col_ids.size(); i++) {
        auto col_id = col_ids[i];
        auto& file_col_name = col_names[i];

        if (_col_id_name_map.find(col_id) == _col_id_name_map.end()) {
            _has_schema_change = true;
            continue;
        }
        auto& table_col_name = _col_id_name_map[col_id];
        _table_col_to_file_col.emplace(table_col_name, file_col_name);
        _file_col_to_table_col.emplace(file_col_name, table_col_name);
        if (table_col_name != file_col_name) {
            _has_schema_change = true;
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized
