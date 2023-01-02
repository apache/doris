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

#include "common/status.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris::vectorized {

const int64_t MIN_SUPPORT_DELETE_FILES_VERSION = 2;
const std::string ICEBERG_ROW_POS = "pos";
const std::string ICEBERG_FILE_PATH = "file_path";

IcebergTableReader::IcebergTableReader(GenericReader* file_format_reader, RuntimeProfile* profile,
                                       RuntimeState* state, const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range)
        : TableFormatReader(file_format_reader),
          _profile(profile),
          _state(state),
          _params(params),
          _range(range) {
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
    return _file_format_reader->get_next_block(block, read_rows, eof);
}

Status IcebergTableReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContext*>& missing_columns) {
    return _file_format_reader->set_fill_columns(partition_columns, missing_columns);
}

Status IcebergTableReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    return _file_format_reader->get_columns(name_to_type, missing_cols);
}

Status IcebergTableReader::init_row_filters(const TFileRangeDesc& range) {
    auto& table_desc = range.table_format_params.iceberg_params;
    auto& version = table_desc.format_version;
    if (version < MIN_SUPPORT_DELETE_FILES_VERSION) {
        return Status::OK();
    }
    auto& delete_file_type = table_desc.content;
    auto files = table_desc.delete_files;
    if (files.empty()) {
        return Status::OK();
    }
    if (delete_file_type == POSITION_DELETE) {
        // position delete
        ParquetReader* parquet_reader = (ParquetReader*)(_file_format_reader.get());
        RowRange whole_range = parquet_reader->get_whole_range();
        bool init_schema = false;
        std::vector<std::string> delete_file_col_names;
        std::vector<TypeDescriptor> delete_file_col_types;
        std::list<std::vector<int64_t>> delete_rows_list;
        delete_rows_list.resize(files.size());
        int64_t num_delete_rows = 0;
        auto delete_rows_iter = delete_rows_list.begin();
        for (auto& delete_file : files) {
            if (whole_range.last_row <= delete_file.position_lower_bound ||
                whole_range.first_row > delete_file.position_upper_bound) {
                delete_rows_iter++;
                continue;
            }
            SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
            std::vector<int64_t>& delete_rows = *delete_rows_iter;
            TFileRangeDesc delete_range;
            delete_range.path = delete_file.path;
            delete_range.start_offset = 0;
            delete_range.size = -1;
            delete_range.file_size = -1;
            ParquetReader delete_reader(_profile, _params, delete_range, 102400,
                                        const_cast<cctz::time_zone*>(&_state->timezone_obj()));
            if (!init_schema) {
                delete_reader.get_parsed_schema(&delete_file_col_names, &delete_file_col_types);
                init_schema = true;
            }
            std::string data_file_path = _range.path;
            // the path in _range is remove the namenode prefix,
            // and the file_path in delete file is full path, so we should add it back.
            if (_params.__isset.hdfs_params && _params.hdfs_params.__isset.fs_name) {
                std::string fs_name = _params.hdfs_params.fs_name;
                if (!starts_with(data_file_path, fs_name)) {
                    data_file_path = fs_name + data_file_path;
                }
            }
            Status init_st =
                    delete_reader.init_reader(delete_file_col_names, nullptr, nullptr, false);
            if (init_st.is<ErrorCode::END_OF_FILE>()) {
                continue;
            } else if (!init_st.ok()) {
                return init_st;
            }
            std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                    partition_columns;
            std::unordered_map<std::string, VExprContext*> missing_columns;
            delete_reader.set_fill_columns(partition_columns, missing_columns);

            bool dictionary_coded = false;
            const tparquet::FileMetaData* meta_data = delete_reader.get_meta_data();
            for (int i = 0; i < delete_file_col_names.size(); ++i) {
                if (delete_file_col_names[i] == ICEBERG_FILE_PATH) {
                    // ParquetReader wil return EndOfFile if there's no row group
                    auto& column_chunk = meta_data->row_groups[0].columns[i];
                    if (column_chunk.__isset.meta_data &&
                        column_chunk.meta_data.__isset.dictionary_page_offset) {
                        dictionary_coded = true;
                    }
                    break;
                }
            }

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
                RETURN_IF_ERROR(delete_reader.get_next_block(&block, &read_rows, &eof));
                if (read_rows > 0) {
                    ColumnPtr path_column = block.get_by_name(ICEBERG_FILE_PATH).column;
                    DCHECK_EQ(path_column->size(), read_rows);
                    std::pair<int, int> path_range;
                    if (dictionary_coded) {
                        path_range = _binary_search(assert_cast<const ColumnDictI32&>(*path_column),
                                                    data_file_path);
                    } else {
                        path_range = _binary_search(assert_cast<const ColumnString&>(*path_column),
                                                    data_file_path);
                    }

                    int skip_count = path_range.first;
                    int valid_count = path_range.second;
                    if (valid_count > 0) {
                        // delete position
                        ColumnPtr pos_column = block.get_by_name(ICEBERG_ROW_POS).column;
                        CHECK_EQ(pos_column->size(), read_rows);
                        using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
                        const int64_t* src_data =
                                assert_cast<const ColumnType&>(*pos_column).get_data().data() +
                                skip_count;
                        const int64_t* src_data_end = src_data + valid_count;
                        const int64_t* cpy_start =
                                std::lower_bound(src_data, src_data_end, whole_range.first_row);
                        const int64_t* cpy_end =
                                std::lower_bound(cpy_start, src_data_end, whole_range.last_row);
                        int64_t cpy_count = cpy_end - cpy_start;

                        if (cpy_count > 0) {
                            int64_t origin_size = delete_rows.size();
                            delete_rows.resize(origin_size + cpy_count);
                            int64_t* dest_position = &delete_rows[origin_size];
                            memcpy(dest_position, cpy_start, cpy_count * sizeof(int64_t));
                            num_delete_rows += cpy_count;
                        }
                    }
                }
            }
            delete_rows_iter++;
        }
        if (num_delete_rows > 0) {
            for (auto iter = delete_rows_list.begin(); iter != delete_rows_list.end();) {
                if (iter->empty()) {
                    delete_rows_list.erase(iter++);
                } else {
                    iter++;
                }
            }
            SCOPED_TIMER(_iceberg_profile.delete_rows_sort_time);
            _merge_sort(delete_rows_list, num_delete_rows);
            parquet_reader->set_delete_rows(&_delete_rows);
            COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
        }
    }
    // todo: equality delete
    COUNTER_UPDATE(_iceberg_profile.num_delete_files, files.size());
    return Status::OK();
}

std::pair<int, int> IcebergTableReader::_binary_search(const ColumnDictI32& file_path_column,
                                                       const std::string& data_file_path) {
    size_t read_rows = file_path_column.get_data().size();

    int data_file_code = file_path_column.find_code(StringValue(data_file_path));
    if (data_file_code == -2) { // -1 is null code
        return std::make_pair(read_rows, 0);
    }

    const int* coded_path = file_path_column.get_data().data();
    const int* coded_path_end = coded_path + read_rows;
    const int* path_start = std::lower_bound(coded_path, coded_path_end, data_file_code);
    const int* path_end = std::lower_bound(path_start, coded_path_end, data_file_code + 1);
    int skip_count = path_start - coded_path;
    int valid_count = path_end - path_start;

    return std::make_pair(skip_count, valid_count);
}

std::pair<int, int> IcebergTableReader::_binary_search(const ColumnString& file_path_column,
                                                       const std::string& data_file_path) {
    const int read_rows = file_path_column.size();
    if (read_rows == 0) {
        return std::make_pair(0, 0);
    }
    StringRef data_file(data_file_path);

    int left = 0;
    int right = read_rows - 1;
    if (file_path_column.get_data_at(left) > data_file ||
        file_path_column.get_data_at(right) < data_file) {
        return std::make_pair(read_rows, 0);
    }
    while (left < right) {
        int mid = (left + right) / 2;
        if (file_path_column.get_data_at(mid) < data_file) {
            left = mid;
        } else {
            right = mid;
        }
    }
    if (file_path_column.get_data_at(left) == data_file) {
        int start = left;
        int end = read_rows - 1;
        while (start < end) {
            int pivot = (start + end) / 2;
            if (file_path_column.get_data_at(pivot) > data_file) {
                end = pivot;
            } else {
                start = pivot;
            }
        }
        return std::make_pair(left, end - left + 1);
    } else {
        return std::make_pair(read_rows, 0);
    }
}

void IcebergTableReader::_merge_sort(std::list<std::vector<int64_t>>& delete_rows_list,
                                     int64_t num_delete_rows) {
    if (delete_rows_list.empty()) {
        return;
    }
    if (delete_rows_list.size() == 1) {
        _delete_rows.resize(num_delete_rows);
        memcpy(&_delete_rows[0], &(delete_rows_list.front()[0]), sizeof(int64_t) * num_delete_rows);
        return;
    }
    if (delete_rows_list.size() == 2) {
        _delete_rows.resize(num_delete_rows);
        std::merge(delete_rows_list.front().begin(), delete_rows_list.front().end(),
                   delete_rows_list.back().begin(), delete_rows_list.back().end(),
                   _delete_rows.begin());
        return;
    }

    // merge sort
    using vec_pair =
            std::pair<std::vector<int64_t>::iterator, std::vector<int64_t>::const_iterator>;
    auto cmp = [](const vec_pair& a, const vec_pair& b) { return *a.first > *b.first; };
    std::priority_queue<vec_pair, vector<vec_pair>, decltype(cmp)> pq(cmp);
    for (auto iter = delete_rows_list.begin(); iter != delete_rows_list.end(); ++iter) {
        if (iter->size() > 0) {
            pq.push({iter->begin(), iter->end()});
        }
    }
    _delete_rows.reserve(num_delete_rows);
    while (!pq.empty()) {
        vec_pair p = pq.top();
        pq.pop();
        _delete_rows.emplace_back(*p.first);
        p.first++;
        if (p.first != p.second) {
            pq.push(p);
        }
    }
}

} // namespace doris::vectorized
