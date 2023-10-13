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

#include "exec/schema_scanner/schema_metadata_name_ids_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaMetadataNameIdsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"CATALOG_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"CATALOG_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DATABASE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaMetadataNameIdsScanner::SchemaMetadataNameIdsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_METADATA_NAME_IDS), _db_index(0) {}

SchemaMetadataNameIdsScanner::~SchemaMetadataNameIdsScanner() {}

Status SchemaMetadataNameIdsScanner::start(RuntimeState* state) {
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
    db_params.__set_get_null_catalog(true);
    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaMetadataNameIdsScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    if (_db_result.db_ids[_db_index] == -1 &&
        _db_result.dbs[_db_index] == "NULL") { //catalog is empty.
        _db_index++;
        _table_result.tables.clear();
        _table_result.tables.push_back(TTableMetadataNameIds());

        return Status::OK();
    }
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
        RETURN_IF_ERROR(SchemaHelper::list_table_metadata_name_ids(*(_param->common_param->ip),
                                                                   _param->common_param->port,
                                                                   table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaMetadataNameIdsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto table_num = _table_result.tables.size();
    if (table_num == 0) { //database is null
        table_num = 1;
        _table_result.tables.push_back(TTableMetadataNameIds());
    }
    std::vector<void*> null_datas(table_num, nullptr);
    std::vector<void*> datas(table_num);

    // catalog_id
    {
        int64_t srcs[table_num];
        if (_db_result.__isset.catalog_ids) {
            int64_t id = _db_result.catalog_ids[_db_index - 1];
            for (int i = 0; i < table_num; ++i) {
                srcs[i] = id;
                datas[i] = srcs + i;
            }
            static_cast<void>(fill_dest_column_for_range(block, 0, datas));
        } else {
            static_cast<void>(fill_dest_column_for_range(block, 0, null_datas));
        }
    }

    // catalog_name
    {
        if (_db_result.__isset.catalogs) {
            std::string catalog_name = _db_result.catalogs[_db_index - 1];
            StringRef str_slot = StringRef(catalog_name.c_str(), catalog_name.size());
            for (int i = 0; i < table_num; ++i) {
                datas[i] = &str_slot;
            }
            static_cast<void>(fill_dest_column_for_range(block, 1, datas));
        } else {
            static_cast<void>(fill_dest_column_for_range(block, 1, null_datas));
        }
    }

    // database_id
    {
        int64_t srcs[table_num];
        if (_db_result.__isset.db_ids) {
            int64_t id = _db_result.db_ids[_db_index - 1];
            for (int i = 0; i < table_num; ++i) {
                srcs[i] = id;
                datas[i] = srcs + i;
            }
            static_cast<void>(fill_dest_column_for_range(block, 2, datas));
        } else {
            static_cast<void>(fill_dest_column_for_range(block, 2, null_datas));
        }
    }

    // database_name
    {
        if (_db_result.__isset.dbs) {
            std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
            StringRef str_slot = StringRef(db_name.c_str(), db_name.size());
            for (int i = 0; i < table_num; ++i) {
                datas[i] = &str_slot;
            }
            static_cast<void>(fill_dest_column_for_range(block, 3, datas));
        } else {
            static_cast<void>(fill_dest_column_for_range(block, 3, null_datas));
        }
    }
    //     table_id
    {
        int64_t srcs[table_num];
        for (int i = 0; i < table_num; ++i) {
            if (_table_result.tables[i].__isset.id) {
                srcs[i] = _table_result.tables[i].id;
                datas[i] = &srcs;
            } else {
                datas[i] = nullptr;
            }
        }
        static_cast<void>(fill_dest_column_for_range(block, 4, datas));
    }

    //table_name
    {
        StringRef strs[table_num];
        for (int i = 0; i < table_num; ++i) {
            if (_table_result.tables[i].__isset.name) {
                const std::string* src = &_table_result.tables[i].name;
                strs[i] = StringRef(src->c_str(), src->size());
                datas[i] = strs + i;
            } else {
                datas[i] = nullptr;
            }
        }
        static_cast<void>(fill_dest_column_for_range(block, 5, datas));
    }

    return Status::OK();
}

Status SchemaMetadataNameIdsScanner::get_next_block(vectorized::Block* block, bool* eos) {
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
