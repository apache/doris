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

#include "common/status.h"
#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaBackendsScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"BackendId", TYPE_BIGINT, sizeof(StringRef), false},
        {"TabletNum", TYPE_BIGINT, sizeof(StringRef), false},
        {"HeartbeatPort", TYPE_INT, sizeof(int), false},
        {"BePort", TYPE_INT, sizeof(int), false},
        {"HttpPort", TYPE_INT, sizeof(int), false},
        {"BrpcPort", TYPE_INT, sizeof(int), false},
        {"Cluster", TYPE_VARCHAR, sizeof(StringRef), false},
        {"IP", TYPE_VARCHAR, sizeof(StringRef), false},
        {"LastStartTime", TYPE_VARCHAR, sizeof(StringRef), false},
        {"LastHeartbeat", TYPE_VARCHAR, sizeof(StringRef), false},
        {"Alive", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SystemDecommissioned", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ClusterDecommissioned", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DataUsedCapacity", TYPE_BIGINT, sizeof(int64_t), false},
        {"AvailCapacity", TYPE_BIGINT, sizeof(int64_t), false},
        {"TotalCapacity", TYPE_BIGINT, sizeof(int64_t), false},
        {"UsedPct", TYPE_DOUBLE, sizeof(double), false},
        {"MaxDiskUsedPct", TYPE_DOUBLE, sizeof(double), false},
        {"RemoteUsedCapacity", TYPE_BIGINT, sizeof(int64_t), false},
        {"Tag", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ErrMsg", TYPE_VARCHAR, sizeof(StringRef), false},
        {"Version", TYPE_VARCHAR, sizeof(StringRef), false},
        {"Status", TYPE_VARCHAR, sizeof(StringRef), false},
};

SchemaBackendsScanner::SchemaBackendsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKENDS) {}

Status SchemaBackendsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(_fetch_backends_info());
    RETURN_IF_ERROR(_set_col_name_to_type());
    return Status::OK();
}

Status SchemaBackendsScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaBackendsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto row_num = _batch_data.size();
    std::vector<void*> null_datas(row_num, nullptr);
    std::vector<void*> datas(row_num);

    for (size_t col_idx = 0; col_idx < _columns.size(); ++col_idx) {
        auto it = _col_name_to_type.find(_columns[col_idx].name);
        if (it == _col_name_to_type.end()) {
            if (_columns[col_idx].is_null) {
                fill_dest_column_for_range(block, col_idx, null_datas);
            } else {
                return Status::InternalError(
                        "column {} is not found in BE, and {} is not nullable.",
                        _columns[col_idx].name, _columns[col_idx].name);
            }
        } else if (it->second == TYPE_BIGINT) {
            for (int row_idx = 0; row_idx < row_num; ++row_idx) {
                datas[row_idx] = &_batch_data[row_idx].column_value[col_idx].longVal;
            }
            fill_dest_column_for_range(block, col_idx, datas);
        } else if (it->second == TYPE_INT) {
            for (int row_idx = 0; row_idx < row_num; ++row_idx) {
                datas[row_idx] = &_batch_data[row_idx].column_value[col_idx].intVal;
            }
            fill_dest_column_for_range(block, col_idx, datas);
        } else if (it->second == TYPE_VARCHAR) {
            for (int row_idx = 0; row_idx < row_num; ++row_idx) {
                datas[row_idx] = &_batch_data[row_idx].column_value[col_idx].stringVal;
            }
            fill_dest_column_for_range(block, col_idx, datas);
        } else if (it->second == TYPE_DOUBLE) {
            for (int row_idx = 0; row_idx < row_num; ++row_idx) {
                datas[row_idx] = &_batch_data[row_idx].column_value[col_idx].doubleVal;
            }
            fill_dest_column_for_range(block, col_idx, datas);
        } else {
            // other type
        }
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
        LOG(WARNING) << "fetch schema table data from master failed, errmsg=" << status;
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
