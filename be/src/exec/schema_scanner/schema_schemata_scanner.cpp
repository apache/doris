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
    SCOPED_TIMER(_get_db_timer);
    if (!_is_init) {
        return Status::InternalError("used before initial.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->common_param->wild) {
        db_params.__set_pattern(*(_param->common_param->wild));
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
    SCOPED_TIMER(_fill_block_timer);
    auto dbs_num = _db_result.dbs.size();
    std::vector<void*> null_datas(dbs_num, nullptr);
    std::vector<void*> datas(dbs_num);

    // catalog
    {
        if (!_db_result.__isset.catalogs) {
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, null_datas));
        } else {
            StringRef strs[dbs_num];
            for (int i = 0; i < dbs_num; ++i) {
                strs[i] = StringRef(_db_result.catalogs[i].c_str(), _db_result.catalogs[i].size());
                datas[i] = strs + i;
            }
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
        }
    }
    // schema
    {
        std::string db_names[dbs_num];
        StringRef strs[dbs_num];
        for (int i = 0; i < dbs_num; ++i) {
            db_names[i] = SchemaHelper::extract_db_name(_db_result.dbs[i]);
            strs[i] = StringRef(db_names[i].c_str(), db_names[i].size());
            datas[i] = strs + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // DEFAULT_CHARACTER_SET_NAME
    {
        std::string src = "utf8";
        StringRef str = StringRef(src.c_str(), src.size());
        for (int i = 0; i < dbs_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // DEFAULT_COLLATION_NAME
    {
        std::string src = "utf8_general_ci";
        StringRef str = StringRef(src.c_str(), src.size());
        for (int i = 0; i < dbs_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // SQL_PATH
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, null_datas)); }
    return Status::OK();
}

} // namespace doris
