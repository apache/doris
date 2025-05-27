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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>

#include <string>

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaBackendsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"BACKENDID", TYPE_BIGINT, sizeof(int64_t), true},
        {"HOST", TYPE_STRING, sizeof(StringRef), true},
        {"HEARTBEATPORT", TYPE_INT, sizeof(int32_t), true},
        {"BEPORT", TYPE_INT, sizeof(int32_t), true},
        {"HTTPPORT", TYPE_INT, sizeof(int32_t), true},
        {"BRPCPORT", TYPE_INT, sizeof(int32_t), true},
        {"ARROWFLIGHTSQLPORT", TYPE_INT, sizeof(int32_t), true},
        {"LASTSTARTTIME", TYPE_STRING, sizeof(StringRef), true},
        {"LASTHEARTBEAT", TYPE_STRING, sizeof(StringRef), true},
        {"ALIVE", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"SYSTEMDECOMMISSIONED", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"TABLETNUM", TYPE_INT, sizeof(int32_t), true},
        {"DATAUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TRASHUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVAILCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TOTALCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"USEDPCT", TYPE_DOUBLE, sizeof(double), true},
        {"MAXDISKUSEDPCT", TYPE_DOUBLE, sizeof(double), true},
        {"REMOTEUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TAG", TYPE_STRING, sizeof(StringRef), true},
        {"ERRMSG", TYPE_STRING, sizeof(StringRef), true},
        {"VERSION", TYPE_STRING, sizeof(StringRef), true},
        {"STATUS", TYPE_STRING, sizeof(StringRef), true},
        {"HEARTBEATFAILURECOUNTER", TYPE_INT, sizeof(int32_t), true},
        {"NODEROLE", TYPE_STRING, sizeof(StringRef), true},
        {"CPUCORES", TYPE_INT, sizeof(int32_t), true},
        {"MEMORY", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaBackendsScanner::SchemaBackendsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKENDS) {}

SchemaBackendsScanner::~SchemaBackendsScanner() {}

Status SchemaBackendsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_get_new_table());
    return Status::OK();
}

Status SchemaBackendsScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    TGetTablesParams table_params;
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
        RETURN_IF_ERROR(SchemaHelper::list_table_privilege_status(*(_param->common_param->ip),
                                                                  _param->common_param->port,
                                                                  table_params, &_priv_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaBackendsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (_priv_result.privileges.empty()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaBackendsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto privileges_num = _priv_result.privileges.size();
    std::vector<void*> datas(privileges_num);

    // grantee
    {
        std::vector<StringRef> strs(privileges_num);
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            strs[i] = StringRef(priv_status.grantee.c_str(), priv_status.grantee.size());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
    }
    // catalog
    // This value is always def.
    {
        std::string definer = "def";
        StringRef str = StringRef(definer.c_str(), definer.size());
        for (int i = 0; i < privileges_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // schema
    {
        std::vector<StringRef> strs(privileges_num);
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            strs[i] = StringRef(priv_status.schema.c_str(), priv_status.schema.size());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // table name
    {
        std::vector<StringRef> strs(privileges_num);
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            strs[i] = StringRef(priv_status.table_name.c_str(), priv_status.table_name.size());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // privilege type
    {
        std::vector<StringRef> strs(privileges_num);
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            strs[i] = StringRef(priv_status.privilege_type.c_str(),
                                priv_status.privilege_type.size());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // is grantable
    {
        std::vector<StringRef> strs(privileges_num);
        for (int i = 0; i < privileges_num; ++i) {
            const TPrivilegeStatus& priv_status = _priv_result.privileges[i];
            strs[i] = StringRef(priv_status.is_grantable.c_str(), priv_status.is_grantable.size());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));
    }
    return Status::OK();
}

} // namespace doris