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

#include "olap/wal_table.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "http/action/http_stream.h"
#include "http/action/stream_load.h"
#include "http/ev_http_server.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "olap/wal_manager.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "util/path_util.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/format/wal/wal_reader.h"

namespace doris {

WalTable::WalTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id), _stop(false) {}
WalTable::~WalTable() {}

#ifdef BE_TEST
std::string k_request_line;
#endif

void WalTable::add_wals(std::vector<std::string> wals) {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    for (const auto& wal : wals) {
        LOG(INFO) << "add replay wal " << wal;
        auto wal_info = std::make_shared<WalInfo>(wal, 0, UnixMillis(), false);
        _replay_wal_map.emplace(wal, wal_info);
    }
}
void WalTable::pick_relay_wals() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    std::vector<std::string> need_replay_wals;
    std::vector<std::string> need_erase_wals;
    for (auto it = _replay_wal_map.begin(); it != _replay_wal_map.end(); it++) {
        auto wal_info = it->second;
        if (wal_info->get_retry_num() >= config::group_commit_replay_wal_retry_num) {
            LOG(WARNING) << "All replay wal failed, db=" << _db_id << ", table=" << _table_id
                         << ", wal=" << it->first << ", retry_num=" << wal_info->get_retry_num();
            if (!_rename_to_tmp_path(it->first)) {
                LOG(WARNING) << "rename " << it->first << " fail";
            }
            need_erase_wals.push_back(it->first);
            continue;
        }
        if (_need_replay(wal_info)) {
            need_replay_wals.push_back(it->first);
        }
    }
    for (const auto& wal : need_erase_wals) {
        _replay_wal_map.erase(wal);
    }
    std::sort(need_replay_wals.begin(), need_replay_wals.end());
    for (const auto& wal : need_replay_wals) {
        _replaying_queue.emplace_back(_replay_wal_map[wal]);
        _replay_wal_map.erase(wal);
    }
}
Status WalTable::replay_wals() {
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        if (_replay_wal_map.empty()) {
            LOG(INFO) << "_replay_wal_map is empty,skip relaying";
            return Status::OK();
        }
        if (!_replaying_queue.empty()) {
            LOG(INFO) << "_replaying_queue is not empty,skip relaying";
            return Status::OK();
        }
    }
    VLOG_DEBUG << "Start replay wals for db=" << _db_id << ", table=" << _table_id
               << ", wal size=" << _replay_wal_map.size();
    pick_relay_wals();
    while (!_replaying_queue.empty()) {
        auto wal_info = _replaying_queue.front();
        wal_info->add_retry_num();
        auto st = _replay_wal_internal(wal_info->get_wal_path());
        if (!st.ok()) {
            LOG(WARNING) << "failed replay wal, db=" << _db_id << ", table=" << _table_id
                         << ", wal=" << wal_info->get_wal_path() << ", st=" << st.to_string();
            wal_info->add_retry_num();
            if (!st.is<ErrorCode::NOT_FOUND>()) {
                std::lock_guard<std::mutex> lock(_replay_wal_lock);
                _replay_wal_map.emplace(wal_info->get_wal_path(), wal_info);
            } else {
                std::shared_ptr<std::pair<int64_t, std::string>> pair = nullptr;
                RETURN_IF_ERROR(_parse_wal_path(wal_info->get_wal_path(), pair));
                auto wal_id = pair->first;
                RETURN_IF_ERROR(_delete_wal(wal_id));
            }
        }
        {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            _replaying_queue.pop_front();
        }
        VLOG_NOTICE << "replay wal, db=" << _db_id << ", table=" << _table_id
                    << ", wal=" << wal_info->get_wal_path() << ", st=" << st.to_string();
    }
    return Status::OK();
}

bool WalTable::_rename_to_tmp_path(const std::string wal) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    std::stringstream ss;
    int index = 0;
    while (index < path_element.size()) {
        ss << path_element[index] << "/";
        if (index == path_element.size() - 4) {
            ss << "tmp/";
        }
        index++;
    }
    LOG(INFO) << "rename wal from " << wal << " to " << ss.str();
    return std::rename(wal.c_str(), ss.str().c_str());
}

bool WalTable::_need_replay(std::shared_ptr<WalInfo> wal_info) {
#ifndef BE_TEST
    auto replay_interval = pow(2, wal_info->get_retry_num()) *
                           config::group_commit_replay_wal_retry_interval_seconds * 1000;
    return UnixMillis() - wal_info->get_start_time_ms() >= replay_interval;
#else
    return true;
#endif
}

Status WalTable::_try_abort_txn(int64_t db_id, int64_t wal_id) {
    TLoadTxnRollbackRequest request;
    request.__set_auth_code(0); // this is a fake, fe not check it now
    request.__set_db_id(db_id);
    request.__set_txnId(wal_id);
    std::string reason = "relay wal " + std::to_string(wal_id);
    request.__set_reason(reason);
    TLoadTxnRollbackResult result;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->loadTxnRollback(result, request);
            },
            10000L);
    auto result_status = Status::create(result.status);
    LOG(INFO) << "abort txn " << wal_id << ",st:" << st << ",result_status:" << result_status;
    return result_status;
}

Status WalTable::_replay_wal_internal(const std::string& wal) {
    LOG(INFO) << "Start replay wal for db=" << _db_id << ", table=" << _table_id << ", wal=" << wal;
    std::shared_ptr<std::pair<int64_t, std::string>> pair = nullptr;
    RETURN_IF_ERROR(_parse_wal_path(wal, pair));
    auto wal_id = pair->first;
    auto label = pair->second;
#ifndef BE_TEST
    auto st = _try_abort_txn(_db_id, wal_id);
    if (!st.ok()) {
        LOG(WARNING) << "abort txn " << wal_id << " fail";
    }
    RETURN_IF_ERROR(_get_column_info(_db_id, _table_id));
#endif
    RETURN_IF_ERROR(_replay_one_txn_with_stremaload(wal_id, wal, label));
    return Status::OK();
}

Status WalTable::_parse_wal_path(const std::string& wal,
                                 std::shared_ptr<std::pair<int64_t, std::string>>& pair) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    auto pos = path_element[path_element.size() - 1].find("_");
    try {
        int64_t wal_id = std::strtoll(path_element[path_element.size() - 1].substr(0, pos).c_str(),
                                      NULL, 10);
        auto label = path_element[path_element.size() - 1].substr(pos + 1);
        pair = std::make_shared<std::pair<int64_t, std::string>>(std::make_pair(wal_id, label));
    } catch (const std::invalid_argument& e) {
        return Status::InvalidArgument("Invalid format, {}", e.what());
    }
    return Status::OK();
}

Status WalTable::_construct_sql_str(const std::string& wal, const std::string& label,
                                    std::string& sql_str, std::vector<size_t>& index_vector) {
    std::string columns;
    RETURN_IF_ERROR(_read_wal_header(wal, columns));
    std::vector<std::string> column_id_element;
    doris::vectorized::WalReader::string_split(columns, ",", column_id_element);
    std::stringstream ss_name;
    std::stringstream ss_id;
    int index_raw = 0;
    for (auto column_id_str : column_id_element) {
        try {
            int64_t column_id = std::strtoll(column_id_str.c_str(), NULL, 10);
            auto it = _column_id_info_map.find(column_id);
            if (it != _column_id_info_map.end()) {
                ss_name << "`" << it->second->first << "`,";
                ss_id << "c" << std::to_string(it->second->second) << ",";
                index_vector.emplace_back(index_raw);
            }
            index_raw++;
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid format, {}", e.what());
        }
    }
    auto name = ss_name.str().substr(0, ss_name.str().size() - 1);
    auto id = ss_id.str().substr(0, ss_id.str().size() - 1);
    std::stringstream ss;
    ss << "insert into doris_internal_table_id(" << _table_id << ") WITH LABEL " << label << " ("
       << name << ") select " << id << " from http_stream(\"format\" = \"wal\", \"table_id\" = \""
       << std::to_string(_table_id) << "\")";
    sql_str = ss.str().data();
    return Status::OK();
}
Status WalTable::_handle_stream_load(int64_t wal_id, const std::string& wal,
                                     const std::string& label) {
    std::string sql_str;
    std::vector<size_t> index_vector;
    RETURN_IF_ERROR(_construct_sql_str(wal, label, sql_str, index_vector));
    _exec_env->wal_mgr()->add_wal_column_index(wal_id, index_vector);
    auto st = HttpStreamAction::process_wal_relay(_exec_env, wal_id, sql_str, label);
    _exec_env->wal_mgr()->erase_wal_column_index(wal_id);
    LOG(INFO) << "relay wal id=" << wal_id << ",st=" << st.to_string();
    return st;
}

Status WalTable::_replay_one_txn_with_stremaload(int64_t wal_id, const std::string& wal,
                                                 const std::string& label) {
    bool success;
#ifndef BE_TEST
    auto st = _handle_stream_load(wal_id, wal, label);
    auto msg = st.msg();
    success = st.ok() || st.is<ErrorCode::PUBLISH_TIMEOUT>() ||
              msg.find("LabelAlreadyUsedException") != msg.npos;
#endif
    if (success) {
        LOG(INFO) << "success to replay wal =" << wal;
        RETURN_IF_ERROR(_delete_wal(wal_id));
    } else {
        LOG(INFO) << "fail to replay wal =" << wal;
        return Status::InternalError("fail to replay wal =" + wal);
    }
    return Status::OK();
}

void WalTable::stop() {
    do {
        {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            if (!this->_stop.load()) {
                this->_stop.store(true);
            }
            if (_replay_wal_map.empty() && _replaying_queue.empty()) {
                break;
            }
            LOG(INFO) << "stopping wal_table,wait for relay wal task done, now "
                      << _replay_wal_map.size() << " wals wait to replay, "
                      << _replaying_queue.size() << " wals are replaying";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    } while (true);
}

size_t WalTable::size() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    return _replay_wal_map.size();
}

Status WalTable::_get_column_info(int64_t db_id, int64_t tb_id) {
    TGetColumnInfoRequest request;
    request.__set_db_id(db_id);
    request.__set_table_id(tb_id);
    TGetColumnInfoResult result;
    Status status;
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    if (master_addr.hostname.empty() || master_addr.port == 0) {
        status = Status::InternalError("Have not get FE Master heartbeat yet");
    } else {
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->getColumnInfo(result, request);
                }));
        status = Status::create(result.status);
        if (!status.ok()) {
            return status;
        }
        std::vector<TColumnInfo> column_element = result.columns;
        int64_t column_index = 1;
        _column_id_info_map.clear();
        for (auto column : column_element) {
            auto column_name = column.column_name;
            auto column_id = column.column_id;
            std::shared_ptr<column_info> column_pair =
                    std::make_shared<column_info>(std::make_pair(column_name, column_index));
            _column_id_info_map.emplace(column_id, column_pair);
            column_index++;
        }
    }
    return status;
}

Status WalTable::_read_wal_header(const std::string& wal_path, std::string& columns) {
    std::shared_ptr<doris::WalReader> wal_reader;
    RETURN_IF_ERROR(_exec_env->wal_mgr()->create_wal_reader(wal_path, wal_reader));
    uint32_t version = 0;
    RETURN_IF_ERROR(wal_reader->read_header(version, columns));
    RETURN_IF_ERROR(wal_reader->finalize());
    return Status::OK();
}

Status WalTable::_delete_wal(int64_t wal_id) {
    RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(wal_id));
    RETURN_IF_ERROR(_exec_env->wal_mgr()->erase_wal_status_queue(_table_id, wal_id));
    return Status::OK();
}

} // namespace doris