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

#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris::vectorized {

const int64_t MIN_SUPPORT_DELETE_FILES_VERSION = 2;
const std::string ICEBERG_ROW_POS = "pos";

IcebergTableReader::IcebergTableReader(GenericReader* file_format_reader, RuntimeProfile* profile,
                                       RuntimeState* state, const TFileScanRangeParams& params)
        : TableFormatReader(file_format_reader), _profile(profile), _state(state), _params(params) {
    static const char* iceberg_profile = "IcebergProfile";
    ADD_TIMER(_profile, iceberg_profile);
    _iceberg_profile.num_delete_files =
            ADD_CHILD_COUNTER(_profile, "NumDeleteFiles", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, iceberg_profile);
    _iceberg_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", iceberg_profile);
}

IcebergTableReader::~IcebergTableReader() {
    if (_data_path_conjunct_ctx != nullptr) {
        _data_path_conjunct_ctx->close(_state);
    }
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
        SCOPED_TIMER(_iceberg_profile.delete_files_read_time);
        auto row_desc = RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({table_desc.delete_table_tuple_id}),
                                      std::vector<bool>({false}));
        RETURN_IF_ERROR(VExpr::create_expr_tree(_state->obj_pool(), table_desc.file_select_conjunct,
                                                &_data_path_conjunct_ctx));
        RETURN_IF_ERROR(_data_path_conjunct_ctx->prepare(_state, row_desc));
        RETURN_IF_ERROR(_data_path_conjunct_ctx->open(_state));

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
            RETURN_IF_ERROR(delete_reader.init_reader(delete_file_col_names, nullptr,
                                                      _data_path_conjunct_ctx, false));
            std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                    partition_columns;
            std::unordered_map<std::string, VExprContext*> missing_columns;
            delete_reader.set_fill_columns(partition_columns, missing_columns);

            bool eof = false;
            while (!eof) {
                Block block = Block();
                for (int i = 0; i < delete_file_col_names.size(); ++i) {
                    DataTypePtr data_type = DataTypeFactory::instance().create_data_type(
                            delete_file_col_types[i], true);
                    MutableColumnPtr data_column = data_type->create_column();
                    block.insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                       delete_file_col_names[i]));
                }
                eof = false;
                size_t read_rows = 0;
                RETURN_IF_ERROR(delete_reader.get_next_block(&block, &read_rows, &eof));
                if (read_rows > 0) {
                    auto& pos_type_column = block.get_by_name(ICEBERG_ROW_POS);
                    ColumnPtr pos_column = pos_type_column.column;
                    using ColumnType = typename PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType;
                    if (pos_type_column.type->is_nullable()) {
                        pos_column = assert_cast<const ColumnNullable&>(*pos_column)
                                             .get_nested_column_ptr();
                    }
                    const int64_t* src_data =
                            assert_cast<const ColumnType&>(*pos_column).get_data().data();
                    const int64_t* src_data_end = src_data + read_rows;
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
            _merge_sort(delete_rows_list, num_delete_rows);
            parquet_reader->set_delete_rows(&_delete_rows);
            COUNTER_UPDATE(_iceberg_profile.num_delete_rows, num_delete_rows);
        }
    }
    // todo: equality delete
    COUNTER_UPDATE(_iceberg_profile.num_delete_files, files.size());
    return Status::OK();
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
