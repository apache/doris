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

#include "exec/schema_scanner/schema_backends_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaBackendsScanner::_s_backends_columns = {
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
        : SchemaScanner(_s_backends_columns, TSchemaTableType::SCH_BACKENDS) {}

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
    TFetchBackendsRequest request;
    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::fetch_backends(*(_param->common_param->ip),
                                                     _param->common_param->port, request,
                                                     &_backends_result));
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
    if (_backends_result.backends.empty()) {
        return Status::OK();
    }
    return _fill_block_impl(block);
}

Status SchemaBackendsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    const std::vector<TBackendDetailInfo>& backends = _backends_result.backends;
    size_t row_num = backends.size();
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

    // BACKENDID
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].backend_id;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));

    // HOST
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] =
                StringRef(backends[row_idx].host.c_str(), backends[row_idx].host.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));

    // HEARTBEATPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].heartbeat_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));

    // BEPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].be_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));

    // HTTPPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].http_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));

    // BRPCPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].brpc_port;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, datas));

    // ARROWFLIGHTSQLPORT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].arrowflightsqlport;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));

    // LASTSTARTTIME
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(backends[row_idx].last_start_time.c_str(),
                                      backends[row_idx].last_start_time.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));

    // LASTHEARTBEAT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(backends[row_idx].last_heartbeat.c_str(),
                                      backends[row_idx].last_heartbeat.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));

    // ALIVE
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(backends[row_idx].alive);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));

    // SYSTEMDECOMMISSIONED
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        bool_refs[row_idx] = static_cast<char>(backends[row_idx].system_decommissioned);
        datas[row_idx] = bool_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));

    // TABLETNUM
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].tablet_num;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));

    // DATAUSEDCAPACITY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].data_used_capacity;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 12, datas));

    // TRASHUSEDCAPACITY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].trash_used_capacity;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, datas));

    // AVAILCAPACITY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].avail_capacity;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, datas));

    // TOTALCAPACITY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].total_capacity;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));

    // USEDPCT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        double_refs[row_idx] = backends[row_idx].used_pct;
        datas[row_idx] = double_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));

    // MAXDISKUSEDPCT
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        double_refs[row_idx] = backends[row_idx].max_disk_used_pct;
        datas[row_idx] = double_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));

    // REMOTEUSEDCAPACITY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].remote_used_capacity;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, datas));

    // TAG
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] =
                StringRef(backends[row_idx].tag.c_str(), backends[row_idx].tag.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 19, datas));

    // ERRMSG
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] =
                StringRef(backends[row_idx].errmsg.c_str(), backends[row_idx].errmsg.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 20, datas));

    // VERSION
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] =
                StringRef(backends[row_idx].version.c_str(), backends[row_idx].version.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 21, datas));

    // STATUS
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] =
                StringRef(backends[row_idx].status.c_str(), backends[row_idx].status.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 22, datas));

    // HEARTBEATFAILURECOUNTER
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].heartbeat_failure_counter;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 23, datas));

    // NODEROLE
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        str_refs[row_idx] = StringRef(backends[row_idx].node_role.c_str(),
                                      backends[row_idx].node_role.length());
        datas[row_idx] = str_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 24, datas));

    // CPUCORES
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int32_refs[row_idx] = backends[row_idx].cpu_cores;
        datas[row_idx] = int32_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 25, datas));

    // MEMORY
    for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
        int64_refs[row_idx] = backends[row_idx].memory;
        datas[row_idx] = int64_refs.data() + row_idx;
    }
    RETURN_IF_ERROR(fill_dest_column_for_range(block, 26, datas));

    return Status::OK();
}

} // namespace doris
