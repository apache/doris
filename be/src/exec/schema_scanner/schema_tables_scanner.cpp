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

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_complex.h"
#include "vec/common/string_ref.h"

namespace doris {

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
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
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
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->catalog) {
        db_params.__set_catalog(*(_param->catalog));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
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
    if (nullptr != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (nullptr != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params,
                                                        &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablesScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto table_num = _table_result.tables.size();
    // catalog
    if (_db_result.__isset.catalogs) {
        std::string catalog_name = _db_result.catalogs[_db_index - 1];
        StringRef str_slot = StringRef(catalog_name.c_str(), catalog_name.size());
        for (int i = 0; i < table_num; ++i) {
            fill_dest_column(block, &str_slot, _s_tbls_columns[0]);
        }
    } else {
        for (int i = 0; i < table_num; ++i) {
            fill_dest_column(block, nullptr, _s_tbls_columns[0]);
        }
    }
    // schema
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str_slot = StringRef(db_name.c_str(), db_name.size());
        for (int i = 0; i < table_num; ++i) {
            fill_dest_column(block, &str_slot, _s_tbls_columns[1]);
        }
    }
    // name
    for (int i = 0; i < table_num; ++i) {
        const std::string* src = &_table_result.tables[i].name;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _s_tbls_columns[2]);
    }
    // type
    for (int i = 0; i < table_num; ++i) {
        const std::string* src = &_table_result.tables[i].type;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _s_tbls_columns[3]);
    }
    // engine
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.engine) {
            const std::string* src = &tbl_status.engine;
            StringRef str_slot = StringRef(src->c_str(), src->size());
            fill_dest_column(block, &str_slot, _s_tbls_columns[4]);
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[4]);
        }
    }
    // version
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[5]);
    }
    // row_format
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[6]);
    }
    // rows
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.rows) {
            int64_t src = tbl_status.rows;
            fill_dest_column(block, &src, _s_tbls_columns[7]);
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[7]);
        }
    }
    // avg_row_length
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.avg_row_length) {
            int64_t src = tbl_status.avg_row_length;
            fill_dest_column(block, &src, _s_tbls_columns[8]);
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[8]);
        }
    }
    // data_length
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.avg_row_length) {
            int64_t src = tbl_status.data_length;
            fill_dest_column(block, &src, _s_tbls_columns[9]);
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[9]);
        }
    }
    // max_data_length
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[10]);
    }
    // index_length
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[11]);
    }
    // data_free
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[12]);
    }
    // auto_increment
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[13]);
    }
    // creation_time
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.create_time) {
            int64_t create_time = tbl_status.create_time;
            if (create_time <= 0) {
                fill_dest_column(block, nullptr, _s_tbls_columns[14]);
            } else {
                DateTimeValue time_slot;
                time_slot.from_unixtime(create_time, TimezoneUtils::default_time_zone);
                fill_dest_column(block, &time_slot, _s_tbls_columns[14]);
            }
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[14]);
        }
    }
    // update_time
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.update_time) {
            int64_t update_time = tbl_status.update_time;
            if (update_time <= 0) {
                fill_dest_column(block, nullptr, _s_tbls_columns[15]);
            } else {
                DateTimeValue time_slot;
                time_slot.from_unixtime(update_time, TimezoneUtils::default_time_zone);
                fill_dest_column(block, &time_slot, _s_tbls_columns[15]);
            }
        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[15]);
        }
    }
    // check_time
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.last_check_time) {
            int64_t check_time = tbl_status.last_check_time;
            if (check_time <= 0) {
                fill_dest_column(block, nullptr, _s_tbls_columns[16]);
            } else {
                DateTimeValue time_slot;
                time_slot.from_unixtime(check_time, TimezoneUtils::default_time_zone);
                fill_dest_column(block, &time_slot, _s_tbls_columns[16]);
            }
        }
    }
    // collation
    for (int i = 0; i < table_num; ++i) {
        const TTableStatus& tbl_status = _table_result.tables[i];
        if (tbl_status.__isset.collation) {
            const std::string* src = &tbl_status.collation;
            StringRef str_slot = StringRef(src->c_str(), src->size());
            fill_dest_column(block, &str_slot, _s_tbls_columns[17]);

        } else {
            fill_dest_column(block, nullptr, _s_tbls_columns[17]);
        }
    }
    // checksum
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[18]);
    }
    // create_options
    for (int i = 0; i < table_num; ++i) {
        fill_dest_column(block, nullptr, _s_tbls_columns[19]);
    }
    // create_comment
    for (int i = 0; i < table_num; ++i) {
        const std::string* src = &_table_result.tables[i].comment;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _s_tbls_columns[20]);
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
