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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>

#include <string>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaViewsScanner::_s_tbls_columns = {
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
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_VIEWS), _db_index(0) {}

SchemaViewsScanner::~SchemaViewsScanner() {}

Status SchemaViewsScanner::start(RuntimeState* state) {
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

Status SchemaViewsScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
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
    table_params.__set_type("VIEW");

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->common_param->ip),
                                                        _param->common_param->port, table_params,
                                                        &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaViewsScanner::get_next_block(vectorized::Block* block, bool* eos) {
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

Status SchemaViewsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto tables_num = _table_result.tables.size();
    if (tables_num == 0) {
        return Status::OK();
    }
    std::vector<void*> null_datas(tables_num, nullptr);
    std::vector<void*> datas(tables_num);

    // catalog
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, null_datas)); }
    // schema
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str = StringRef(db_name.c_str(), db_name.size());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // name
    {
        StringRef strs[tables_num];
        for (int i = 0; i < tables_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            const std::string* src = &tbl_status.name;
            strs[i] = StringRef(src->c_str(), src->size());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // definition
    {
        StringRef strs[tables_num];
        for (int i = 0; i < tables_num; ++i) {
            const TTableStatus& tbl_status = _table_result.tables[i];
            const std::string* src = &tbl_status.ddl_sql;
            strs[i] = StringRef(src->c_str(), src->length());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // check_option
    {
        const std::string check_option = "NONE";
        StringRef str = StringRef(check_option.c_str(), check_option.length());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // is_updatable
    {
        // This is from views in mysql
        const std::string is_updatable = "NO";
        StringRef str = StringRef(is_updatable.c_str(), is_updatable.length());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    // definer
    {
        // This is from views in mysql
        const std::string definer = "root@%";
        StringRef str = StringRef(definer.c_str(), definer.length());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }
    // security_type
    {
        // This is from views in mysql
        const std::string security_type = "DEFINER";
        StringRef str = StringRef(security_type.c_str(), security_type.length());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // character_set_client
    {
        // This is from views in mysql
        const std::string encoding = "utf8";
        StringRef str = StringRef(encoding.c_str(), encoding.length());
        for (int i = 0; i < tables_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // collation_connection
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, null_datas)); }
    return Status::OK();
}

} // namespace doris
