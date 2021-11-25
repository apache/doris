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

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
namespace doris {

SchemaScanner::ColumnDesc SchemaTablesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringValue), true},
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
        {"TABLE_COLLATION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_OPTIONS", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablesScanner::SchemaTablesScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaTablesScanner::~SchemaTablesScanner() {}

Status SchemaTablesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
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

Status SchemaTablesScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // catalog
    { tuple->set_null(_tuple_desc->slots()[0]->null_indicator_offset()); }
    // schema
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        str_slot->ptr = (char*)pool->allocate(db_name.size());
        str_slot->len = db_name.size();
        memcpy(str_slot->ptr, db_name.c_str(), str_slot->len);
    }
    // name
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.name;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // type
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.type;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // engine
    if (tbl_status.__isset.engine) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.engine;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    } else {
        tuple->set_null(_tuple_desc->slots()[4]->null_indicator_offset());
    }
    // version
    { tuple->set_null(_tuple_desc->slots()[5]->null_indicator_offset()); }
    // row_format
    { tuple->set_null(_tuple_desc->slots()[6]->null_indicator_offset()); }
    // rows
    if (tbl_status.__isset.rows) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[7]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.rows;
    } else {
        tuple->set_null(_tuple_desc->slots()[7]->null_indicator_offset());
    }
    // avg_row_length
    if (tbl_status.__isset.avg_row_length) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[8]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.avg_row_length;
    } else {
        tuple->set_null(_tuple_desc->slots()[8]->null_indicator_offset());
    }
    // data_length
    if (tbl_status.__isset.avg_row_length) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[9]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.data_length;
    } else {
        tuple->set_null(_tuple_desc->slots()[9]->null_indicator_offset());
    } // max_data_length
    { tuple->set_null(_tuple_desc->slots()[10]->null_indicator_offset()); }
    // index_length
    { tuple->set_null(_tuple_desc->slots()[11]->null_indicator_offset()); }
    // data_free
    { tuple->set_null(_tuple_desc->slots()[12]->null_indicator_offset()); }
    // auto_increment
    { tuple->set_null(_tuple_desc->slots()[13]->null_indicator_offset()); }
    // creation_time
    if (tbl_status.__isset.create_time) {
        int64_t create_time = tbl_status.create_time;
        if (create_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[14]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[14]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[14]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(create_time, TimezoneUtils::default_time_zone);
        }
    }
    // update_time
    if (tbl_status.__isset.update_time) {
        int64_t update_time = tbl_status.update_time;
        if (update_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[15]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[15]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[15]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(update_time, TimezoneUtils::default_time_zone);
        }
    }
    // check_time
    if (tbl_status.__isset.last_check_time) {
        int64_t check_time = tbl_status.last_check_time;
        if (check_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[16]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[16]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[16]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(check_time, TimezoneUtils::default_time_zone);
        }
    }
    // collation
    if (tbl_status.__isset.collation) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[17]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.collation;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (nullptr == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    } else {
        tuple->set_null(_tuple_desc->slots()[17]->null_indicator_offset());
    }
    // checksum
    { tuple->set_null(_tuple_desc->slots()[18]->null_indicator_offset()); }
    // create_options
    { tuple->set_null(_tuple_desc->slots()[19]->null_indicator_offset()); }
    // create_comment
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[20]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.comment;
        str_slot->len = src->length();
        if (str_slot->len == 0) {
            str_slot->ptr = nullptr;
        } else {
            str_slot->ptr = (char*)pool->allocate(str_slot->len);
            if (nullptr == str_slot->ptr) {
                return Status::InternalError("Allocate memcpy failed.");
            }
            memcpy(str_slot->ptr, src->c_str(), str_slot->len);
        }
    }
    _table_index++;
    return Status::OK();
}

Status SchemaTablesScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
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
    _table_index = 0;
    return Status::OK();
}

Status SchemaTablesScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_one_row(tuple, pool);
}

} // namespace doris
