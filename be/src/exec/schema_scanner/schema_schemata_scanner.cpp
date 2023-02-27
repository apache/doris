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

#include "exec/schema_scanner/schema_schemata_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaSchemataScanner::_s_columns = {
        //   name,       type,          size
        {"CATALOG_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SCHEMA_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DEFAULT_CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DEFAULT_COLLATION_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SQL_PATH", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaSchemataScanner::SchemaSchemataScanner()
        : SchemaScanner(_s_columns, TSchemaTableType::SCH_SCHEMATA) {}

SchemaSchemataScanner::~SchemaSchemataScanner() = default;

Status SchemaSchemataScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initial.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->wild) {
        db_params.__set_pattern(*(_param->wild));
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

Status SchemaSchemataScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before Initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (!_db_result.dbs.size()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaSchemataScanner::_fill_block_impl(vectorized::Block* block) {
    auto dbs_num = _db_result.dbs.size();

    // catalog
    {
        for (int i = 0; i < dbs_num; ++i) {
            if (!_db_result.__isset.catalogs) {
                fill_dest_column(block, nullptr, _s_columns[0]);
            } else {
                std::string catalog_name = _db_result.catalogs[i];
                StringRef str = StringRef(catalog_name.c_str(), catalog_name.size());
                fill_dest_column(block, &str, _s_columns[0]);
            }
        }
    }
    // schema
    {
        for (int i = 0; i < dbs_num; ++i) {
            std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[i]);
            StringRef str = StringRef(db_name.c_str(), db_name.size());
            fill_dest_column(block, &str, _s_columns[1]);
        }
    }
    // DEFAULT_CHARACTER_SET_NAME
    {
        for (int i = 0; i < dbs_num; ++i) {
            std::string src = "utf8";
            StringRef str = StringRef(src.c_str(), src.size());
            fill_dest_column(block, &str, _s_columns[2]);
        }
    }
    // DEFAULT_COLLATION_NAME
    {
        for (int i = 0; i < dbs_num; ++i) {
            std::string src = "utf8_general_ci";
            StringRef str = StringRef(src.c_str(), src.size());
            fill_dest_column(block, &str, _s_columns[3]);
        }
    }
    // SQL_PATH
    {
        for (int i = 0; i < dbs_num; ++i) {
            fill_dest_column(block, nullptr, _s_columns[4]);
        }
    }
    return Status::OK();
}

} // namespace doris
