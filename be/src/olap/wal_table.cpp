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

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/http.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "evhttp.h"
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
        : _exec_env(exec_env), _db_id(db_id), _table_id(table_id) {}
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
                continue;
            }
            if (retry_num >= config::group_commit_replay_wal_retry_num) {
                LOG(WARNING) << "All replay wal failed, db=" << _db_id << ", table=" << _table_id
                             << ", wal=" << wal
                             << ", retry_num=" << config::group_commit_replay_wal_retry_num;
                std::string rename_path = get_tmp_path(wal);
                LOG(INFO) << "rename wal from " << wal << " to " << rename_path;
                std::rename(wal.c_str(), rename_path.c_str());
                _replay_wal_map.erase(wal);
                continue;
            }
            if (need_replay(info)) {
                replaying = true;
                need_replay_wals.push_back(wal);
            }
        }
        std::sort(need_replay_wals.begin(), need_replay_wals.end());
    }
    for (const auto& wal : need_replay_wals) {
        auto st = replay_wal_internal(wal);
        if (!st.ok()) {
            std::lock_guard<std::mutex> lock(_replay_wal_lock);
            auto it = _replay_wal_map.find(wal);
            if (it != _replay_wal_map.end()) {
                auto& [retry_num, start_time, replaying] = it->second;
                replaying = false;
            }
            LOG(WARNING) << "failed replay wal, drop this round, db=" << _db_id
                         << ", table=" << _table_id << ", wal=" << wal << ", st=" << st.to_string();
            break;
        }
        VLOG_NOTICE << "replay wal, db=" << _db_id << ", table=" << _table_id << ", label=" << wal
                    << ", st=" << st.to_string();
    }
    return Status::OK();
}

std::string WalTable::get_tmp_path(const std::string wal) {
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

bool WalTable::need_replay(const doris::WalTable::replay_wal_info& info) {
#ifndef BE_TEST
    auto& [retry_num, start_ts, replaying] = info;
    auto replay_interval =
            pow(2, retry_num) * config::group_commit_replay_wal_retry_interval_seconds * 1000;
    return UnixMillis() - start_ts >= replay_interval;
#else
    return true;
#endif
}

Status WalTable::replay_wal_internal(const std::string& wal) {
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
    auto pair = get_wal_info(wal);
    auto wal_id = pair.first;
    auto label = pair.second;
    RETURN_IF_ERROR(send_request(wal_id, wal, label));
    return Status::OK();
}

std::pair<int64_t, std::string> WalTable::get_wal_info(const std::string& wal) {
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal, "/", path_element);
    auto pos = path_element[path_element.size() - 1].find("_");
    int64_t wal_id =
            std::strtoll(path_element[path_element.size() - 1].substr(0, pos).c_str(), NULL, 10);
    auto label = path_element[path_element.size() - 1].substr(pos + 1);
    return std::make_pair(wal_id, label);
}

void http_request_done(struct evhttp_request* req, void* arg) {
    event_base_loopbreak((struct event_base*)arg);
}

Status WalTable::send_request(int64_t wal_id, const std::string& wal, const std::string& label) {
#ifndef BE_TEST
    struct event_base* base = nullptr;
    struct evhttp_connection* conn = nullptr;
    struct evhttp_request* req = nullptr;
    event_init();
    base = event_base_new();
    conn = evhttp_connection_new("127.0.0.1", doris::config::webserver_port);
    evhttp_connection_set_base(conn, base);
    req = evhttp_request_new(http_request_done, base);
    evhttp_add_header(req->output_headers, HTTP_LABEL_KEY.c_str(), label.c_str());
    evhttp_add_header(req->output_headers, HTTP_AUTH_CODE.c_str(), std::to_string(wal_id).c_str());
    evhttp_add_header(req->output_headers, HTTP_WAL_ID_KY.c_str(), std::to_string(wal_id).c_str());
    std::stringstream ss;
    ss << "insert into doris_internal_table_id(" << _table_id << ") WITH LABEL " << label
       << " select * from http_stream(\"format\" = \"wal\", \"table_id\" = \""
       << std::to_string(_table_id) << "\")";
    evhttp_add_header(req->output_headers, HTTP_SQL.c_str(), ss.str().c_str());
    evbuffer* output = evhttp_request_get_output_buffer(req);
    evbuffer_add_printf(output, "replay wal %s", std::to_string(wal_id).c_str());

    evhttp_make_request(conn, req, EVHTTP_REQ_PUT, "/api/_http_stream");
    evhttp_connection_set_timeout(req->evcon, 300);

    event_base_dispatch(base);
    evhttp_connection_free(conn);
    event_base_free(base);

#endif
    bool retry = false;
    std::string status;
    std::string msg;
    std::stringstream out;
    rapidjson::Document doc;
#ifndef BE_TEST
    size_t len = 0;
    auto input = evhttp_request_get_input_buffer(req);
    char* request_line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF);
    while (request_line != nullptr) {
        std::string s(request_line);
        out << request_line;
        request_line = evbuffer_readln(input, &len, EVBUFFER_EOL_CRLF);
    }
#else
    out << k_request_line;
#endif
    auto out_str = out.str();
    if (!out_str.empty()) {
        doc.Parse(out.str().c_str());
        status = std::string(doc["Status"].GetString());
        msg = std::string(doc["Message"].GetString());
        LOG(INFO) << "replay wal " << wal_id << " status:" << status << ",msg:" << msg;
        if (status.find("Fail") != status.npos) {
            if (msg.find("Label") != msg.npos && msg.find("has already been used") != msg.npos) {
                retry = false;
            } else {
                retry = true;
            }
        } else {
            retry = false;
        }
    } else {
        retry = true;
    }
    if (retry) {
        LOG(INFO) << "fail to replay wal =" << wal << ",status:" << status << ",msg:" << msg;
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        auto it = _replay_wal_map.find(wal);
        if (it != _replay_wal_map.end()) {
            auto& [retry_num, start_time, replaying] = it->second;
            replaying = false;
        } else {
            _replay_wal_map.emplace(wal, replay_wal_info {0, UnixMillis(), false});
        }
    } else {
        LOG(INFO) << "success to replay wal =" << wal << ",status:" << status << ",msg:" << msg;
        RETURN_IF_ERROR(_exec_env->wal_mgr()->delete_wal(wal_id));
        std::lock_guard<std::mutex> lock(_replay_wal_lock);
        _replay_wal_map.erase(wal);
    }
    return Status::OK();
}

size_t WalTable::size() {
    std::lock_guard<std::mutex> lock(_replay_wal_lock);
    return _replay_wal_map.size();
}
} // namespace doris