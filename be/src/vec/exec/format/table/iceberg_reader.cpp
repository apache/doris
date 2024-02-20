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
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
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

using DeleteRows = std::vector<int64_t>;
using DeleteFile = phmap::parallel_flat_hash_map<
        std::string, std::unique_ptr<DeleteRows>, std::hash<std::string>,
        std::equal_to<std::string>,
        std::allocator<std::pair<const std::string, std::unique_ptr<DeleteRows>>>, 8, std::mutex>;

const int64_t MIN_SUPPORT_DELETE_FILES_VERSION = 2;
const std::string ICEBERG_ROW_POS = "pos";
const std::string ICEBERG_FILE_PATH = "file_path";

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

Status IcebergTableReader::init_reader(
        const std::vector<std::string>& file_col_names,
        const std::unordered_map<int, std::string>& col_id_name_map,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    ParquetReader* parquet_reader = static_cast<ParquetReader*>(_file_format_reader.get());
    _col_id_name_map = col_id_name_map;
    _file_col_names = file_col_names;
    _colname_to_value_range = colname_to_value_range;
    auto parquet_meta_kv = parquet_reader->get_metadata_key_values();
    static_cast<void>(_gen_col_name_maps(parquet_meta_kv));
    _gen_file_col_names();
    _gen_new_colname_to_value_range();
    parquet_reader->set_table_to_file_col_map(_table_col_to_file_col);
    parquet_reader->iceberg_sanitize(_all_required_col_names);
    Status status = parquet_reader->init_reader(
            _all_required_col_names, _not_in_file_col_names, &_new_colname_to_value_range,
            conjuncts, tuple_descriptor, row_descriptor, colname_to_slot_id,
            not_single_slot_filter_conjuncts, slot_id_to_filter_conjuncts);

    return status;
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

    auto res = _file_format_reader->get_next_block(block, read_rows, eof);
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
    return res;
}

Status IcebergTableReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    return _file_format_reader->set_fill_columns(partition_columns, missing_columns);
}

bool IcebergTableReader::fill_all_columns() const {
    return _file_format_reader->fill_all_columns();
};

Status IcebergTableReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    return _file_format_reader->get_columns(name_to_type, missing_cols);
}

Status IcebergTableReader::init_row_filters(const TFileRangeDesc& range) {
    // We get the count value by doris's be, so we don't need to read the delete file
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_push_down_count > 0) {
        return Status::OK();
    }

    auto& table_desc = range.table_format_params.iceberg_params;
    auto& version = table_desc.format_version;
    if (version < MIN_SUPPORT_DELETE_FILES_VERSION) {
        return Status::OK();
    }
    auto& delete_file_type = table_desc.content;
    const std::vector<TIcebergDeleteFileDesc>& files = table_desc.delete_files;
    if (files.empty()) {
        return Status::OK();
    }

    if (delete_file_type == POSITION_DELETE) {
        RETURN_IF_ERROR(_position_delete(files));
    }

    // todo: equality delete
    //       If it is a count operation and it has equality delete file kind,
    //       the push down operation of the count for this split needs to be canceled.

    COUNTER_UPDATE(_iceberg_profile.num_delete_files, files.size());
    return Status::OK();
}

Status IcebergTableReader::_position_delete(
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    std::string data_file_path = _range.path;
    // the path in _range is remove the namenode prefix,
    // and the file_path in delete file is full path, so we should add it back.
    if (_params.__isset.hdfs_params && _params.hdfs_params.__isset.fs_name) {
        std::string fs_name = _params.hdfs_params.fs_name;
        if (!starts_with(data_file_path, fs_name)) {
            data_file_path = fs_name + data_file_path;
        }
    }

    // position delete
    ParquetReader* parquet_reader = (ParquetReader*)(_file_format_reader.get());
    RowRange whole_range = parquet_reader->get_whole_range();
    bool init_schema = false;
    std::vector<std::string> delete_file_col_names;
    std::vector<TypeDescriptor> delete_file_col_types;
    std::vector<DeleteRows*> delete_rows_array;
    int64_t num_delete_rows = 0;
    std::vector<DeleteFile*> erase_data;
    for (auto& delete_file : delete_files) {
        if (whole_range.last_row <= delete_file.position_lower_bound ||
            whole_range.first_row > delete_file.position_upper_bound) {
            continue;
        }

        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        Status create_status = Status::OK();
        DeleteFile* delete_file_cache = _kv_cache->get<
                DeleteFile>(_delet_file_cache_key(delete_file.path), [&]() -> DeleteFile* {
            TFileRangeDesc delete_range;
            // must use __set() method to make sure __isset is true
            delete_range.__set_fs_name(_range.fs_name);
            delete_range.path = delete_file.path;
            delete_range.start_offset = 0;
            delete_range.size = -1;
            delete_range.file_size = -1;
            ParquetReader delete_reader(_profile, _params, delete_range, 102400,
                                        const_cast<cctz::time_zone*>(&_state->timezone_obj()),
                                        _io_ctx, _state);
            if (!init_schema) {
                static_cast<void>(delete_reader.get_parsed_schema(&delete_file_col_names,
                                                                  &delete_file_col_types));
                init_schema = true;
            }
            create_status = delete_reader.open();
            if (!create_status.ok()) {
                return nullptr;
            }
            create_status = delete_reader.init_reader(delete_file_col_names, _not_in_file_col_names,
                                                      nullptr, {}, nullptr, nullptr, nullptr,
                                                      nullptr, nullptr, false);
            if (!create_status.ok()) {
                return nullptr;
            }

            std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                    partition_columns;
            std::unordered_map<std::string, VExprContextSPtr> missing_columns;
            static_cast<void>(delete_reader.set_fill_columns(partition_columns, missing_columns));

            bool dictionary_coded = true;
            const tparquet::FileMetaData* meta_data = delete_reader.get_meta_data();
            for (int i = 0; i < delete_file_col_names.size(); ++i) {
                if (delete_file_col_names[i] == ICEBERG_FILE_PATH) {
                    for (int j = 0; j < meta_data->row_groups.size(); ++j) {
                        auto& column_chunk = meta_data->row_groups[j].columns[i];
                        if (!(column_chunk.__isset.meta_data &&
                              column_chunk.meta_data.__isset.dictionary_page_offset)) {
                            dictionary_coded = false;
                            break;
                        }
                    }
                    break;
                }
            }

            DeleteFile* position_delete = new DeleteFile;
            bool eof = false;
            while (!eof) {
                Block block = Block();
                for (int i = 0; i < delete_file_col_names.size(); ++i) {
                    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(
                            delete_file_col_types[i], false);
                    if (delete_file_col_names[i] == ICEBERG_FILE_PATH && dictionary_coded) {
                        // the dictionary data in ColumnDictI32 is referenced by StringValue, it does keep
                        // the dictionary data in its life circle, so the upper caller should keep the
                        // dictionary data alive after ColumnDictI32.
                        MutableColumnPtr dict_column = ColumnDictI32::create();
                        block.insert(ColumnWithTypeAndName(std::move(dict_column), data_type,
                                                           delete_file_col_names[i]));
                    } else {
                        MutableColumnPtr data_column = data_type->create_column();
                        block.insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                           delete_file_col_names[i]));
                    }
                }
                eof = false;
                size_t read_rows = 0;
                create_status = delete_reader.get_next_block(&block, &read_rows, &eof);
                if (!create_status.ok()) {
                    return nullptr;
                }
                if (read_rows > 0) {
                    ColumnPtr path_column = block.get_by_name(ICEBERG_FILE_PATH).column;
                    DCHECK_EQ(path_column->size(), read_rows);
                    ColumnPtr pos_column = block.get_by_name(ICEBERG_ROW_POS).column;
                    using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
                    const int64_t* src_data =
                            assert_cast<const ColumnType&>(*pos_column).get_data().data();
                    IcebergTableReader::PositionDeleteRange range;
                    if (dictionary_coded) {
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
                if (row_ids->front() >= whole_range.first_row &&
                    row_ids->back() < whole_range.last_row) {
                    erase_data.emplace_back(delete_file_cache);
                }
            }
        };
        delete_file_map.if_contains(data_file_path, get_value);
    }
    if (num_delete_rows > 0) {
        SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
        _sort_delete_rows(delete_rows_array, num_delete_rows);
        parquet_reader->set_delete_rows(&_delete_rows);
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
        int left = index;
        int right = read_rows - 1;
        while (left < right) {
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
        _delete_rows.resize(num_delete_rows);
        memcpy(&_delete_rows[0], &((*delete_rows_array.front())[0]),
               sizeof(int64_t) * num_delete_rows);
        return;
    }
    if (delete_rows_array.size() == 2) {
        _delete_rows.resize(num_delete_rows);
        std::merge(delete_rows_array.front()->begin(), delete_rows_array.front()->end(),
                   delete_rows_array.back()->begin(), delete_rows_array.back()->end(),
                   _delete_rows.begin());
        return;
    }

    using vec_pair = std::pair<std::vector<int64_t>::iterator, std::vector<int64_t>::iterator>;
    _delete_rows.resize(num_delete_rows);
    auto row_id_iter = _delete_rows.begin();
    auto iter_end = _delete_rows.end();
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
Status IcebergTableReader::_gen_col_name_maps(std::vector<tparquet::KeyValue> parquet_meta_kv) {
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

} // namespace doris::vectorized
