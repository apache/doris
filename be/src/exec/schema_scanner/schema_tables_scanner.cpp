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

#include "exec/schema_scanner/schema_tables_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <stdint.h>

#include <string>

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"
#include "vec/common/string_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaTablesScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTO_INCREMENT", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"TABLE_COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_OPTIONS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_COMMENT", TYPE_VARCHAR, sizeof(StringRef), false},
};

SchemaTablesScanner::SchemaTablesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_TABLES), _db_index(0) {}

SchemaTablesScanner::~SchemaTablesScanner() {}

Status SchemaTablesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    SCOPED_TIMER(_get_db_timer);
    TGetDbsParams db_params;
    if (nullptr != _param->common_param->db) {
        db_params.__set_pattern(*(_param->common_param->db));
    }
    if (nullptr != _param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            db_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            db_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablesScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index]);
    if (_db_result.__isset.catalogs) {
        table_params.__set_catalog(_db_result.catalogs[_db_index]);
    }
    _db_index++;
    if (nullptr != _param->common_param->wild) {
        table_params.__set_pattern(*(_param->common_param->wild));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            table_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            table_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->common_param->ip),
                                                        _param->common_param->port, table_params,
                                                        &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto table_num = _table_result.tables.size();
    if (table_num == 0) {
        return Status::OK();
    }
    std::vector<void*> null_datas(table_num, nullptr);
    std::vector<void*> datas(table_num);

    // catalog
    {
        if (_db_result.__isset.catalogs) {
            std::string catalog_name = _db_result.catalogs[_db_index - 1];
            StringRef str_slot = StringRef(catalog_name.c_str(), catalog_name.size());
            for (int i = 0; i < table_num; ++i) {
                datas[i] = &str_slot;
            }
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
        } else {
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, null_datas));
        }
    }
    // schema
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str_slot = StringRef(db_name.c_str(), db_name.size());
        for (int i = 0; i < table_num; ++i) {
            datas[i] = &str_slot;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // name
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const std::string* src = &_table_result.tables[i].name;
            strs[i] = StringRef(src->c_str(), src->size());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // type
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const std::string* src = &_table_result.tables[i].type;
            strs[i] = StringRef(src->c_str(), src->size());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // engine
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.engine) {
                const std::string* src = &tbl_status.engine;
                strs[i] = StringRef(src->c_str(), src->size());
                datas[i] = strs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // version
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, null_datas)); }
    // row_format
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, null_datas)); }
    // rows
    {
        int64_t srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.rows) {
                srcs[i] = tbl_status.rows;
                datas[i] = srcs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // avg_row_length
    {
        int64_t srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.avg_row_length) {
                srcs[i] = tbl_status.avg_row_length;
                datas[i] = srcs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // data_length
    {
        int64_t srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.avg_row_length) {
                srcs[i] = tbl_status.data_length;
                datas[i] = srcs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));
    }
    // max_data_length
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, null_datas)); }
    // index_length
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, null_datas)); }
    // data_free
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 12, null_datas)); }
    // auto_increment
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, null_datas)); }
    // creation_time
    {
        vectorized::VecDateTimeValue srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.create_time) {
                int64_t create_time = tbl_status.create_time;
                if (create_time <= 0) {
                    datas[i] = nullptr;
                } else {
                    srcs[i].from_unixtime(create_time, TimezoneUtils::default_time_zone);
                    datas[i] = srcs + i;
                }
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, datas));
    }
    // update_time
    {
        vectorized::VecDateTimeValue srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.update_time) {
                int64_t update_time = tbl_status.update_time;
                if (update_time <= 0) {
                    datas[i] = nullptr;
                } else {
                    srcs[i].from_unixtime(update_time, TimezoneUtils::default_time_zone);
                    datas[i] = srcs + i;
                }
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));
    }
    // check_time
    {
        vectorized::VecDateTimeValue srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.last_check_time) {
                int64_t check_time = tbl_status.last_check_time;
                if (check_time <= 0) {
                    datas[i] = nullptr;
                } else {
                    srcs[i].from_unixtime(check_time, TimezoneUtils::default_time_zone);
                    datas[i] = srcs + i;
                }
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));
    }
    // collation
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            if (tbl_status.__isset.collation) {
                const std::string* src = &tbl_status.collation;
                strs[i] = StringRef(src->c_str(), src->size());
                datas[i] = strs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));
    }
    // checksum
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, null_datas)); }
    // create_options
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 19, null_datas)); }
    // create_comment
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            const std::string* src = &_table_result.tables[i].comment;
            strs[i] = StringRef(src->c_str(), src->size());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 20, datas));
    }
    return Status::OK();
}

Status SchemaTablesScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_db_index < _db_result.dbs.size()) {
        RETURN_IF_ERROR(_get_new_table());
    } else {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_block_impl(block);
}

} // namespace doris
