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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
SchemaScanner::ColumnDesc SchemaBackendsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"BackendId", TYPE_BIGINT, sizeof(int64_t), true},
        {"Cluster", TYPE_VARCHAR, sizeof(StringValue), true},
        {"IP", TYPE_VARCHAR, sizeof(StringValue), true},
        {"HeartbeatPort", TYPE_INT, sizeof(int32_t), true},
        {"BePort", TYPE_INT, sizeof(int32_t), true},
        {"HttpPort", TYPE_INT, sizeof(int32_t), true},
        {"BrpcPort", TYPE_INT, sizeof(int32_t), true},
        {"LastStartTime", TYPE_VARCHAR, sizeof(StringValue), true},
        {"LastHeartbeat", TYPE_VARCHAR, sizeof(StringValue), true},
        {"Alive", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SystemDecommissioned", TYPE_VARCHAR, sizeof(StringValue), true},
        {"ClusterDecommissioned", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TabletNum", TYPE_BIGINT, sizeof(int64_t), true},
        {"DataUsedCapacity", TYPE_VARCHAR, sizeof(StringValue), true},
        {"AvailCapacity", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TotalCapacity", TYPE_VARCHAR, sizeof(StringValue), true},
        {"UsedPct", TYPE_VARCHAR, sizeof(StringValue), true},
        {"MaxDiskUsedPct", TYPE_VARCHAR, sizeof(StringValue), true},
        {"RemoteUsedCapacity", TYPE_VARCHAR, sizeof(StringValue), true},
        {"Tag", TYPE_VARCHAR, sizeof(StringValue), true},
        {"ErrMsg", TYPE_VARCHAR, sizeof(StringValue), true},
        {"Version", TYPE_VARCHAR, sizeof(StringValue), true},
        {"Status", TYPE_VARCHAR, sizeof(StringValue), true},
};

SchemaBackendsScanner::SchemaBackendsScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _row_idx(0) {}

Status SchemaBackendsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_fetch_backends_info());
    for (int i = 0; i < _backends_info.size(); ++i) {
        for (int j = 0; j < _backends_info[i].size(); ++j) {
            LOG(INFO) << j << " : " << _backends_info[i][j];
        }
    }
    return Status::OK();
}

Status SchemaBackendsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_row_idx >= _backends_info.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_one_row(tuple, pool);
}

Status SchemaBackendsScanner::_fill_one_row(Tuple* tuple, MemPool* pool) {
#define _FILL_ONE_STRINGVALUE_COLUMN()                                               \
    void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());     \
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);                    \
    str_slot->ptr = (char*)pool->allocate(_backends_info[_row_idx][col_idx].size()); \
    str_slot->len = _backends_info[_row_idx][col_idx].size();                        \
    memcpy(str_slot->ptr, _backends_info[_row_idx][col_idx].c_str(), str_slot->len);

    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    uint32_t col_idx = 0;
    // BackendId
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = std::stoll(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // Cluster
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // IP
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // HeartbeatPort
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int32_t*>(slot)) = std::stoi(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // BePort
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int32_t*>(slot)) = std::stoi(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // HttpPort
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int32_t*>(slot)) = std::stoi(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // BrpcPort
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int32_t*>(slot)) = std::stoi(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // LastStartTime
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // LastHeartbeat
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // Alive
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // SystemDecommissioned
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // ClusterDecommissioned
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // TabletNum
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = std::stoll(_backends_info[_row_idx][col_idx]);
        ++col_idx;
    }
    // DataUsedCapacity
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // AvailCapacity
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // TotalCapacity
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // UsedPct
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // MaxDiskUsedPct
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // RemoteUsedCapacity
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // Tag
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // ErrMsg
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // Version
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    // Status
    {
        _FILL_ONE_STRINGVALUE_COLUMN();
        ++col_idx;
    }
    ++_row_idx;
    return Status::OK();
}

Status SchemaBackendsScanner::_fetch_backends_info() {
    TFetchBackendsInfoRequest request;
    request.cluster_name = "";
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    // TODO(ftw): if result will too large?
    TFetchBackendsInfoResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchBackendInfo(result, request);
            },
            config::txn_commit_rpc_timeout_ms));

    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "fetch backends info from master failed, errmsg=" << status.get_error_msg();
        return status;
    }
    _backends_info = std::move(result.backend_info);
    return Status::OK();
}
} // namespace doris
