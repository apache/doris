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
        _replay_wal_map.emplace(wal, replay_wal_info {0, UnixMillis(), false});
    }
}
Status WalTable::replay_wals() {
    std::vector<std::string> need_replay_wals;
    std::vector<std::string> need_erase_wals;
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        if (_replay_wal_map.empty()) {
            return Status::OK();
        }
        VLOG_DEBUG << "Start replay wals for db=" << _db_id << ", table=" << _table_id
                   << ", wal size=" << _replay_wal_map.size();
        for (auto& [wal, info] : _replay_wal_map) {
            auto& [retry_num, start_ts, replaying] = info;
            if (replaying) {
                LOG(INFO) << wal << " is replaying, skip this round";
                return Status::OK();
            }
            if (retry_num >= config::group_commit_replay_wal_retry_num) {
                LOG(WARNING) << "All replay wal failed, db=" << _db_id << ", table=" << _table_id
                             << ", wal=" << wal
                             << ", retry_num=" << config::group_commit_replay_wal_retry_num;
                std::string rename_path = _get_tmp_path(wal);
                LOG(INFO) << "rename wal from " << wal << " to " << rename_path;
                std::rename(wal.c_str(), rename_path.c_str());
                need_erase_wals.push_back(wal);
                continue;
            }
            if (_need_replay(info)) {
                need_replay_wals.push_back(wal);
            }
        }
        std::sort(need_replay_wals.begin(), need_replay_wals.end());
        for (const auto& wal : need_erase_wals) {
            if (_replay_wal_map.erase(wal)) {
                LOG(INFO) << "erase wal " << wal << " from _replay_wal_map";
            } else {
                LOG(WARNING) << "fail to erase wal " << wal << " from _replay_wal_map";
            }
        }
    }
    for (const auto& wal : need_replay_wals) {
        {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            if (_stop.load()) {
                break;
            } else {
                auto it = _replay_wal_map.find(wal);
                if (it != _replay_wal_map.end()) {
                    auto& [retry_num, start_time, replaying] = it->second;
                    replaying = true;
                }
            }
        }
        auto st = _replay_wal_internal(wal);
        if (!st.ok()) {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            auto it = _replay_wal_map.find(wal);
            if (it != _replay_wal_map.end()) {
                auto& [retry_num, start_time, replaying] = it->second;
                replaying = false;
            }
            LOG(WARNING) << "failed replay wal, drop this round, db=" << _db_id
                         << ", table=" << _table_id << ", wal=" << wal << ", st=" << st.to_string();
            if (st.is<ErrorCode::RUNTIME_ERROR>()) {
                auto msg = st.msg();
                if (msg.find("tableId") != msg.npos && msg.find("not exists") != msg.npos) {
                    std::shared_ptr<std::pair<int64_t, std::string>> pair = nullptr;
                    RETURN_IF_ERROR(_get_wal_info(wal, pair));
                    auto wal_id = pair->first;
                    RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(wal_id));
                    RETURN_IF_ERROR(
                            _exec_env->wal_mgr()->erase_wal_status_queue(_table_id, wal_id));
                    if (_replay_wal_map.erase(wal)) {
                        LOG(INFO) << "erase " << wal << " from _replay_wal_map";
                    } else {
                        LOG(WARNING) << "fail to erase " << wal << " from _replay_wal_map";
                    }
                }
            }
            break;
        }
        VLOG_NOTICE << "replay wal, db=" << _db_id << ", table=" << _table_id << ", label=" << wal
                    << ", st=" << st.to_string();
    }
    return Status::OK();
}

std::string WalTable::_get_tmp_path(const std::string wal) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    std::stringstream ss;
    int index = 0;
    while (index < path_element.size() - 3) {
        ss << path_element[index] << "/";
        index++;
    }
    ss << "tmp/";
    while (index < path_element.size()) {
        if (index != path_element.size() - 1) {
            ss << path_element[index] << "_";
        } else {
            ss << path_element[index];
        }
        index++;
    }
    return ss.str();
}

bool WalTable::_need_replay(const doris::WalTable::replay_wal_info& info) {
#ifndef BE_TEST
    auto& [retry_num, start_ts, replaying] = info;
    auto replay_interval =
            pow(2, retry_num) * config::group_commit_replay_wal_retry_interval_seconds * 1000;
    return UnixMillis() - start_ts >= replay_interval;
#else
    return true;
#endif
}

Status WalTable::_abort_txn(int64_t db_id, int64_t wal_id) {
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
    // start a new stream load
    {
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        auto it = _replay_wal_map.find(wal);
        if (it != _replay_wal_map.end()) {
            auto& [retry_num, start_time, replaying] = it->second;
            ++retry_num;
            replaying = true;
        } else {
            LOG(WARNING) << "can not find wal in stream load replay map. db=" << _db_id
                         << ", table=" << _table_id << ", wal=" << wal;
            return Status::OK();
        }
    }
    std::shared_ptr<std::pair<int64_t, std::string>> pair = nullptr;
    RETURN_IF_ERROR(_get_wal_info(wal, pair));
    auto wal_id = pair->first;
    auto label = pair->second;
#ifndef BE_TEST
    auto st = _abort_txn(_db_id, wal_id);
    if (!st.ok()) {
        LOG(WARNING) << "abort txn " << wal_id << " fail";
    }
    RETURN_IF_ERROR(_get_column_info(_db_id, _table_id));
#endif
    RETURN_IF_ERROR(_send_request(wal_id, wal, label));
    return Status::OK();
}

Status WalTable::_get_wal_info(const std::string& wal,
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

Status WalTable::_parse_sql(int64_t wal_id, const std::string& wal, const std::string& label,
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
            auto it = _column_id_name_map.find(column_id);
            auto it2 = _column_id_index_map.find(column_id);
            if (it != _column_id_name_map.end() && it2 != _column_id_index_map.end()) {
                ss_name << "`" << it->second << "`,";
                ss_id << "c" << std::to_string(_column_id_index_map[column_id]) << ",";
                index_vector.emplace_back(index_raw);
                _column_id_name_map.erase(column_id);
                _column_id_index_map.erase(column_id);
            }
            index_raw++;
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid format, {}", e.what());
        }
    }
    _exec_env->wal_mgr()->add_wal_column_index(wal_id, index_vector);
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
    RETURN_IF_ERROR(_parse_sql(wal_id, wal, label, sql_str, index_vector));
    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    ctx->wal_id = wal_id;
    ctx->auth.auth_code = wal_id;
    UniqueId load_id = UniqueId::gen_uid();
    TUniqueId tload_id;
    tload_id.__set_hi(load_id.hi);
    tload_id.__set_lo(load_id.lo);
    TStreamLoadPutRequest request;
    request.__set_auth_code(ctx->auth.auth_code);
    request.__set_load_sql(sql_str);
    request.__set_loadId(tload_id);
    request.__set_label(label);
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }
    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        auto msg = plan_status.msg();
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    } else {
        ctx->db = ctx->put_result.params.db_name;
        ctx->table = ctx->put_result.params.table_name;
        ctx->txn_id = ctx->put_result.params.txn_conf.txn_id;
        ctx->label = ctx->put_result.params.import_label;
        ctx->put_result.params.__set_wal_id(ctx->wal_id);
        auto st = _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
        if (st.ok()) {
            // wait stream load finish
            RETURN_IF_ERROR(ctx->future.get());
            if (ctx->status.ok()) {
                auto commit_st = _exec_env->stream_load_executor()->commit_txn(ctx.get());
                return commit_st;
            } else if (!ctx->status.ok()) {
                LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                             << ", errmsg=" << ctx->status;
                _exec_env->stream_load_executor()->rollback_txn(ctx.get());
                return ctx->status;
            }
        } else {
            LOG(WARNING) << "execute_plan_fragment fail, id=" << ctx->id << ", errmsg=" << st;
            return st;
        }
    }
    return Status::OK();
}
Status WalTable::_send_request(int64_t wal_id, const std::string& wal, const std::string& label) {
    bool success = false;
#ifndef BE_TEST
    auto st = _handle_stream_load(wal_id, wal, label);
    auto msg = st.msg();
    success = st.ok() || st.is<ErrorCode::PUBLISH_TIMEOUT>() ||
              msg.find("LabelAlreadyUsedException") != msg.npos;
#endif
    if (success) {
        LOG(INFO) << "success to replay wal =" << wal;
        RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(wal_id));
        RETURN_IF_ERROR(_exec_env->wal_mgr()->erase_wal_status_queue(_table_id, wal_id));
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        if (_replay_wal_map.erase(wal)) {
            LOG(INFO) << "erase " << wal << " from _replay_wal_map";
        } else {
            LOG(WARNING) << "fail to erase " << wal << " from _replay_wal_map";
        }
    } else {
        LOG(INFO) << "fail to replay wal =" << wal;
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        auto it = _replay_wal_map.find(wal);
        if (it != _replay_wal_map.end()) {
            auto& [retry_num, start_time, replaying] = it->second;
            replaying = false;
        } else {
            _replay_wal_map.emplace(wal, replay_wal_info {0, UnixMillis(), false});
        }
    }
    _exec_env->wal_mgr()->erase_wal_column_index(wal_id);
    return Status::OK();
}

void WalTable::stop() {
    bool done = true;
    do {
        {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            if (!this->_stop.load()) {
                this->_stop.store(true);
            }
            auto it = _replay_wal_map.begin();
            for (; it != _replay_wal_map.end(); it++) {
                auto& [retry_num, start_time, replaying] = it->second;
                if (replaying) {
                    break;
                }
            }
            if (it != _replay_wal_map.end()) {
                done = false;
            } else {
                done = true;
            }
        }
        if (!done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    } while (!done);
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
        std::string columns_str = result.column_info;
        std::vector<std::string> column_element;
        doris::vectorized::WalReader::string_split(columns_str, ",", column_element);
        int64_t column_index = 1;
        _column_id_name_map.clear();
        _column_id_index_map.clear();
        for (auto column : column_element) {
            auto pos = column.find(":");
            try {
                auto column_name = column.substr(0, pos);
                int64_t column_id = std::strtoll(column.substr(pos + 1).c_str(), NULL, 10);
                _column_id_name_map.emplace(column_id, column_name);
                _column_id_index_map.emplace(column_id, column_index);
                column_index++;
            } catch (const std::invalid_argument& e) {
                return Status::InvalidArgument("Invalid format, {}", e.what());
            }
        }

        status = Status::create(result.status);
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

} // namespace doris