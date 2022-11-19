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

SchemaBackendsScanner::SchemaBackendsScanner()
        : SchemaScanner(nullptr, 0, TSchemaTableType::SCH_BACKENDS), _row_idx(0) {}

Status SchemaBackendsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_fetch_backends_info());
    RETURN_IF_ERROR(_set_col_name_to_type());
    return Status::OK();
}

Status SchemaBackendsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_row_idx >= _batch_data.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_one_row(tuple, pool);
}

Status SchemaBackendsScanner::_fill_one_row(Tuple* tuple, MemPool* pool) {
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    for (size_t col_idx = 0; col_idx < _column_num; ++col_idx) {
        RETURN_IF_ERROR(_fill_one_col(tuple, pool, col_idx));
    }
    ++_row_idx;
    return Status::OK();
}

Status SchemaBackendsScanner::_fill_one_col(Tuple* tuple, MemPool* pool, size_t col_idx) {
    auto it = _col_name_to_type.find(_columns[col_idx].name);

    // if this column is not exist in BE, we fill it with `NULL`.
    if (it == _col_name_to_type.end()) {
        if (_columns[col_idx].is_null) {
            tuple->set_null(_tuple_desc->slots()[col_idx]->null_indicator_offset());
        } else {
            return Status::InternalError("column {} is not found in BE, and {} is not nullable.",
                                         _columns[col_idx].name, _columns[col_idx].name);
        }
    } else if (it->second == TYPE_BIGINT) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = _batch_data[_row_idx].column_value[col_idx].longVal;
    } else if (it->second == TYPE_INT) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<int32_t*>(slot)) = _batch_data[_row_idx].column_value[col_idx].intVal;
    } else if (it->second == TYPE_VARCHAR) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr =
                (char*)pool->allocate(_batch_data[_row_idx].column_value[col_idx].stringVal.size());
        str_slot->len = _batch_data[_row_idx].column_value[col_idx].stringVal.size();
        memcpy(str_slot->ptr, _batch_data[_row_idx].column_value[col_idx].stringVal.c_str(),
               str_slot->len);
    } else if (it->second == TYPE_DOUBLE) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[col_idx]->tuple_offset());
        *(reinterpret_cast<double_t*>(slot)) =
                _batch_data[_row_idx].column_value[col_idx].doubleVal;
    } else {
        // other type
    }
    return Status::OK();
}

Status SchemaBackendsScanner::_fetch_backends_info() {
    TFetchSchemaTableDataRequest request;
    request.cluster_name = "";
    request.__isset.cluster_name = true;
    request.schema_table_name = TSchemaTableName::BACKENDS;
    request.__isset.schema_table_name = true;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    // TODO(ftw): if result will too large?
    TFetchSchemaTableDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            config::txn_commit_rpc_timeout_ms));

    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "fetch schema table data from master failed, errmsg="
                     << status.get_error_msg();
        return status;
    }
    _batch_data = std::move(result.data_batch);
    return Status::OK();
}

Status SchemaBackendsScanner::_set_col_name_to_type() {
    _col_name_to_type.emplace("BackendId", TYPE_BIGINT);
    _col_name_to_type.emplace("TabletNum", TYPE_BIGINT);

    _col_name_to_type.emplace("HeartbeatPort", TYPE_INT);
    _col_name_to_type.emplace("BePort", TYPE_INT);
    _col_name_to_type.emplace("HttpPort", TYPE_INT);
    _col_name_to_type.emplace("BrpcPort", TYPE_INT);

    _col_name_to_type.emplace("Cluster", TYPE_VARCHAR);
    _col_name_to_type.emplace("IP", TYPE_VARCHAR);
    _col_name_to_type.emplace("LastStartTime", TYPE_VARCHAR);
    _col_name_to_type.emplace("LastHeartbeat", TYPE_VARCHAR);
    _col_name_to_type.emplace("Alive", TYPE_VARCHAR);
    _col_name_to_type.emplace("SystemDecommissioned", TYPE_VARCHAR);
    _col_name_to_type.emplace("ClusterDecommissioned", TYPE_VARCHAR);

    _col_name_to_type.emplace("DataUsedCapacity", TYPE_BIGINT);
    _col_name_to_type.emplace("AvailCapacity", TYPE_BIGINT);
    _col_name_to_type.emplace("TotalCapacity", TYPE_BIGINT);

    _col_name_to_type.emplace("UsedPct", TYPE_DOUBLE);
    _col_name_to_type.emplace("MaxDiskUsedPct", TYPE_DOUBLE);

    _col_name_to_type.emplace("RemoteUsedCapacity", TYPE_BIGINT);

    _col_name_to_type.emplace("Tag", TYPE_VARCHAR);
    _col_name_to_type.emplace("ErrMsg", TYPE_VARCHAR);
    _col_name_to_type.emplace("Version", TYPE_VARCHAR);
    _col_name_to_type.emplace("Status", TYPE_VARCHAR);
    return Status::OK();
}
} // namespace doris
