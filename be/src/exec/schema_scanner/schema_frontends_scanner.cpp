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

#include "exec/schema_scanner/schema_frontends_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaFrontendsScanner::_s_frontends_columns = {
        //   name,       type,          size,     is_null
        {"NAME", TYPE_STRING, sizeof(StringRef), true},
        {"HOST", TYPE_STRING, sizeof(StringRef), true},
        {"EDITLOGPORT", TYPE_INT, sizeof(int32_t), true},
        {"HTTPPORT", TYPE_INT, sizeof(int32_t), true},
        {"QUERYPORT", TYPE_INT, sizeof(int32_t), true},
        {"RPCPORT", TYPE_INT, sizeof(int32_t), true},
        {"ARROWFLIGHTSQLPORT", TYPE_INT, sizeof(int32_t), true},
        {"ROLE", TYPE_STRING, sizeof(StringRef), true},
        {"ISMASTER", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"CLUSTERID", TYPE_INT, sizeof(int32_t), true},
        {"JOIN", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"ALIVE", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"REPLAYEDJOURNALID", TYPE_BIGINT, sizeof(int64_t), true},
        {"LASTSTARTTIME", TYPE_STRING, sizeof(StringRef), true},
        {"LASTHEARTBEAT", TYPE_STRING, sizeof(StringRef), true},
        {"ISHELPER", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"ERRMSG", TYPE_STRING, sizeof(StringRef), true},
        {"VERSION", TYPE_STRING, sizeof(StringRef), true},
        {"CURRENTCONNECTED", TYPE_BOOLEAN, sizeof(int8_t), true},
};

SchemaFrontendsScanner::SchemaFrontendsScanner()
        : SchemaScanner(_s_frontends_columns, TSchemaTableType::SCH_FRONTENDS) {}

SchemaFrontendsScanner::~SchemaFrontendsScanner() {}

Status SchemaFrontendsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_get_new_table());
    return Status::OK();
}

Status SchemaFrontendsScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    TFetchFrontendsRequest request;
    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::fetch_frontends(*(_param->common_param->ip),
                                                     _param->common_param->port,
                                                     request, &_frontends_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaFrontendsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (_frontends_result.backends.empty()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaFrontendsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    const std::vector<TFrontendDetailInfo>& frontends = _frontends_result.frontends;
    size_t row_num = frontends.size();
    if (row_num == 0) {
        return Status::OK();
    }

    std::vector<StringRef> str_refs(row_num);
    std::vector<int32_t> int32_refs(row_num);
    std::vector<int64_t> int64_refs(row_num);
    std::vector<char> bool_refs(row_num);
    std::vector<double> double_refs(row_num);
    std::vector<void*> null_datas(row_num, nullptr);
    std::vector<void*> datas(row_num);

    // NAME
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].name.c_str(), frontends[row_idx].name.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));

    // HOST
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].host.c_str(), frontends[row_idx].host.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));

    // EDITLOGPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].edit_log_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));

    // HTTPPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].http_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));

    // QUERYPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].query_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));

    // RPCPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].rpc_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));

    // ARROWFLIGHTSQLPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].arrowflightsqlport;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));

    // ROLE
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].role.c_str(), frontends[row_idx].role.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));

    // ISMASTER
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(frontends[row_idx].is_master);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));

    // CLUSTERID
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = frontends[row_idx].cluster_id;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));

    // JOIN
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(frontends[row_idx].join);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));

    // ALIVE
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(frontends[row_idx].alive);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));

    // REPLAYEDJOURNALID
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = frontends[row_idx].replayed_journal_id ;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 12, datas));

    // LASTSTARTTIME
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].last_start_time.c_str(), frontends[row_idx].last_start_time.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, datas));

    // LASTHEARTBEAT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].last_heartbeat.c_str(), frontends[row_idx].last_heartbeat.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, datas));

    // ISHELPER
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(frontends[row_idx].is_helper);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));

    // ERRMSG
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].err_msg.c_str(), frontends[row_idx].err_msg.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));

    // VERSION
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(frontends[row_idx].version.c_str(), frontends[row_idx].version.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));

    // CURRENTCONNECTED
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(frontends[row_idx].current_connected);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, datas));

    return Status::OK();
}

} // namespace doris