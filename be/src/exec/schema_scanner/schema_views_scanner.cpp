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

#include "exec/schema_scanner/schema_views_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaViewsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"VIEW_DEFINITION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHECK_OPTION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"IS_UPDATABLE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SECURITY_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaViewsScanner::SchemaViewsScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaViewsScanner::~SchemaViewsScanner() {}

Status SchemaViewsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
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

Status SchemaViewsScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // catalog
    { tuple->set_null(_tuple_desc->slots()[0]->null_indicator_offset()); }
    // schema
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        str_slot->data = (char*)pool->allocate(db_name.size());
        str_slot->size = db_name.size();
        memcpy(const_cast<char*>(str_slot->data), db_name.c_str(), str_slot->size);
    }
    // name
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.name;
        str_slot->size = src->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
    }
    // definition
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* ddl_sql = &tbl_status.ddl_sql;
        str_slot->size = ddl_sql->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), ddl_sql->c_str(), str_slot->size);
    }
    // check_option
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        // This is from views in mysql
        const std::string check_option = "NONE";
        str_slot->size = check_option.length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), check_option.c_str(), str_slot->size);
    }
    // is_updatable
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[5]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        // This is from views in mysql
        const std::string is_updatable = "NO";
        str_slot->size = is_updatable.length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), is_updatable.c_str(), str_slot->size);
    }
    // definer
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[6]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        // This is from views in mysql
        const std::string definer = "root@%";
        str_slot->size = definer.length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), definer.c_str(), str_slot->size);
    }
    // security_type
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[7]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        // This is from views in mysql
        const std::string security_type = "DEFINER";
        str_slot->size = security_type.length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), security_type.c_str(), str_slot->size);
    }
    // character_set_client
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[8]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        // This is from views in mysql
        const std::string encoding = "utf8";
        str_slot->size = encoding.length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), encoding.c_str(), str_slot->size);
    }
    // collation_connection
    { tuple->set_null(_tuple_desc->slots()[9]->null_indicator_offset()); }
    _table_index++;
    return Status::OK();
}

Status SchemaViewsScanner::get_new_table() {
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
    table_params.__set_type("VIEW");

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params,
                                                        &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaViewsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
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
