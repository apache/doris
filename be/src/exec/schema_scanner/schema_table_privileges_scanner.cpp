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

#include "exec/schema_scanner/schema_table_privileges_scanner.h"

#include <string>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaTablePrivilegesScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaTablePrivilegesScanner::SchemaTablePrivilegesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_TABLE_PRIVILEGES) {}

SchemaTablePrivilegesScanner::~SchemaTablePrivilegesScanner() {}

Status SchemaTablePrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_get_new_table());
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::_get_new_table() {
    TGetTablesParams table_params;
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
        RETURN_IF_ERROR(SchemaHelper::list_table_privilege_status(*(_param->ip), _param->port,
                                                                  table_params, &_priv_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (!_priv_result.privileges.size()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaTablePrivilegesScanner::_fill_block_impl(vectorized::Block* block) {
    auto privileges_num = _priv_result.privileges.size();

    // grantee
    {
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            StringRef str = StringRef(priv_status.grantee.c_str(), priv_status.grantee.size());
            fill_dest_column(block, &str, _s_tbls_columns[0]);
        }
    }
    // catalog
    // This value is always def.
    {
        std::string definer = "def";
        StringRef str = StringRef(definer.c_str(), definer.size());
        for (int i = 0; i < privileges_num; ++i) {
            fill_dest_column(block, &str, _s_tbls_columns[1]);
        }
    }
    // schema
    {
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            StringRef str = StringRef(priv_status.schema.c_str(), priv_status.schema.size());
            fill_dest_column(block, &str, _s_tbls_columns[2]);
        }
    }
    // table name
    {
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            StringRef str =
                    StringRef(priv_status.table_name.c_str(), priv_status.table_name.size());
            fill_dest_column(block, &str, _s_tbls_columns[3]);
        }
    }
    // privilege type
    {
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            StringRef str = StringRef(priv_status.privilege_type.c_str(),
                                      priv_status.privilege_type.size());
            fill_dest_column(block, &str, _s_tbls_columns[4]);
        }
    }
    // is grantable
    {
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            StringRef str =
                    StringRef(priv_status.is_grantable.c_str(), priv_status.is_grantable.size());
            fill_dest_column(block, &str, _s_tbls_columns[5]);
        }
    }
    return Status::OK();
}

} // namespace doris