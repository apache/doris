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

#include "runtime/auto_batch_load_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <memory>
#include <sstream>

#include "client_cache.h"
#include "common/object_pool.h"
#include "olap/storage_engine.h"
#include "olap/wal_reader.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "util/file_utils.h"
#include "util/path_util.h"
#include "util/url_coding.h"

namespace doris {

using apache::thrift::TException;
using apache::thrift::TProcessor;
using apache::thrift::transport::TTransportException;

class RuntimeProfile;

AutoBatchLoadTable::AutoBatchLoadTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id), _wal_id(UnixMillis()) {
    std::string path = _exec_env->storage_engine()->auto_batch_load_dir();
    path = path_util::join_path_segments(path, std::to_string(_db_id));
    path = path_util::join_path_segments(path, std::to_string(_table_id));
    // create auto batch load table directory if it does not exist
    if (!FileUtils::check_exist(path)) {
        Status st = FileUtils::create_dir(path);
        CHECK(st.ok()) << st.to_string();
    }
    _table_load_dir = path;
}

AutoBatchLoadTable::~AutoBatchLoadTable() {}

Status AutoBatchLoadTable::auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                                           int64_t& txn_id) {
    std::lock_guard<std::mutex> lock(_lock);
    // 1. create wal and begin txn if needed
    if (UNLIKELY(!_begin)) {
        // create wal writer
        _wal_writer = std::make_shared<WalWriter>(_wal_path(_next_wal_id()));
        RETURN_NOT_OK_STATUS_WITH_WARN(_wal_writer->init(), "init wal failed");
        // begin a txn if needed
        RETURN_IF_ERROR(_begin_auto_batch_load(_wal_id, _label, _fragment_instance_id, _txn_id));
        VLOG(1) << "begin auto batch load, wal_id=" << _wal_id << ", label=" << _label
                << ", fragment id=" << _fragment_instance_id << ", txn id=" << _txn_id;
        _begin = true;
    }
    // 2. get pip
    auto pipe = _exec_env->fragment_mgr()->get_pipe(_fragment_instance_id);
    if (UNLIKELY(pipe == nullptr)) {
        _begin = false;
        return Status::InternalError("pip is null");
    }
    // 3. append to wal
    RETURN_IF_ERROR(_wal_writer->append_rows(request->data()));
    // 4. write row to pip
    for (int i = 0; i < request->data_size(); ++i) {
        PDataRow* row = new PDataRow();
        row->CopyFrom(request->data(i));
        RETURN_IF_ERROR(pipe->append_and_flush(reinterpret_cast<char*>(&row), sizeof(row),
                                               sizeof(row) + row->ByteSizeLong()));
    }
    label = _label;
    txn_id = _txn_id;
    return Status::OK();
}

bool AutoBatchLoadTable::need_commit() {
    return false;
}

Status AutoBatchLoadTable::commit(int64_t& wal_id, std::string& wal_path) {
    return Status::OK();
}

Status AutoBatchLoadTable::recovery_wal(const int64_t& wal_id, const std::string& wal_path) {
    return Status::OK();
}

int64_t AutoBatchLoadTable::_next_wal_id() {
    ++_wal_id;
    return _wal_id;
}

std::string AutoBatchLoadTable::_wal_path(int64_t wal_id) {
    return path_util::join_path_segments(_table_load_dir, std::to_string(wal_id));
}

Status AutoBatchLoadTable::_begin_auto_batch_load(const int64_t& wal_id, std::string& label,
                                                  TUniqueId& fragment_instance_id,
                                                  int64_t& txn_id) {
    Status status;
    TBeginAutoBatchLoadRequest request;
    label = "auto_batch_load_" + std::to_string(_table_id) + "_" + std::to_string(wal_id);
    request.__set_db_id(_db_id);
    request.__set_table_id(_table_id);
    request.__set_label(label);
    TBeginAutoBatchLoadResult result;
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    if (!status.ok()) {
        LOG(WARNING) << "fail to get master client from cache. "
                     << "host=" << master_address.hostname << ", port=" << master_address.port
                     << ", code=" << status.code();
        return Status::InternalError("Failed to get master client");
    }
    try {
        try {
            client->beginAutoBatchLoad(result, request);
        } catch (TTransportException& e) {
            // reopen the client
            Status master_status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!master_status.ok()) {
                LOG(WARNING) << "Reopen to get frontend client failed, with address:"
                             << _exec_env->master_info()->network_address.hostname << ":"
                             << _exec_env->master_info()->network_address.port;
                return Status::InternalError(
                        "Reopen to get frontend client failed, with address:" +
                        _exec_env->master_info()->network_address.hostname + ":" +
                        std::to_string(_exec_env->master_info()->network_address.port));
            }
            LOG(WARNING) << "begin auto batch load failed, retry!";
            client->beginAutoBatchLoad(result, request);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Fail to begin auto batch load to master(" << master_address.hostname << ":"
           << master_address.port << "). reason: " << e.what();
        return Status::ThriftRpcError(ss.str());
    }
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed begin auto batch load"
                     << ", status code=" << result.status.status_code
                     << ", error=" << result.status.error_msgs;
        return result.status;
    }
    fragment_instance_id = result.fragment_instance_id;
    txn_id = result.txn_id;
    return status;
}

Status AutoBatchLoadTable::_abort_txn(std::string& label, std::string& reason) {
    Status status = Status::OK();
    TAbortAutoBatchLoadRequest request;
    request.__set_db_id(_db_id);
    request.__set_label(label);
    request.__set_reason(reason);
    TAbortAutoBatchLoadResult result;
    const TNetworkAddress& master_address = _exec_env->master_info()->network_address;
    FrontendServiceConnection client(_exec_env->frontend_client_cache(), master_address,
                                     config::thrift_rpc_timeout_ms, &status);
    if (!status.ok()) {
        LOG(WARNING) << "fail to get master client from cache. "
                     << "host=" << master_address.hostname << ", port=" << master_address.port
                     << ", code=" << status.code();
        return Status::InternalError("Failed to get master client");
    }
    try {
        try {
            client->abortAutoBatchLoad(result, request);
        } catch (TTransportException& e) {
            // reopen the client
            Status master_status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!master_status.ok()) {
                return Status::InternalError(
                        "Reopen to get frontend client failed, with address:" +
                        _exec_env->master_info()->network_address.hostname + ":" +
                        std::to_string(_exec_env->master_info()->network_address.port));
            }
            client->abortAutoBatchLoad(result, request);
        }
    } catch (apache::thrift::TException& e) {
        // failed when retry
        // reopen to disable this connection
        client.reopen(config::thrift_rpc_timeout_ms);
        std::stringstream ss;
        ss << "Fail to abort auto batch load to master(" << master_address.hostname << ":"
           << master_address.port << "). reason: " << e.what();
        return Status::ThriftRpcError(ss.str());
    }
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed abort txn with label=" << label
                     << ", status code=" << result.status.status_code
                     << ", error=" << result.status.error_msgs;
        return result.status;
    }
    return status;
}
} // namespace doris
