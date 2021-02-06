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

#include "http/action/snapshot_action.h"

#include <boost/lexical_cast.hpp>
#include <sstream>
#include <string>

#include "agent/cgroups_mgr.h"
#include "common/logging.h"
#include "gen_cpp/AgentService_types.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "olap/olap_define.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

SnapshotAction::SnapshotAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void SnapshotAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get tablet id
    const std::string& tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string("parameter " + TABLET_ID + " not specified in url.");

        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // Get schema hash
    const std::string& schema_hash_str = req->param(SCHEMA_HASH);
    if (schema_hash_str.empty()) {
        std::string error_msg = std::string("parameter " + SCHEMA_HASH + " not specified in url.");
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // valid str format
    int64_t tablet_id;
    int32_t schema_hash;
    try {
        tablet_id = boost::lexical_cast<int64_t>(tablet_id_str);
        schema_hash = boost::lexical_cast<int64_t>(schema_hash_str);
    } catch (boost::bad_lexical_cast& e) {
        std::string error_msg = std::string("param format is invalid: ") + std::string(e.what());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    VLOG_ROW << "get make snapshot tablet info: " << tablet_id << "-" << schema_hash;

    std::string snapshot_path;
    int64_t ret = make_snapshot(tablet_id, schema_hash, &snapshot_path);
    if (ret != 0L) {
        std::string error_msg = std::string("make snapshot failed");
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
        return;
    } else {
        std::stringstream result;
        result << snapshot_path;
        std::string result_str = result.str();
        HttpChannel::send_reply(req, result_str);
    }

    LOG(INFO) << "deal with snapshot request finished! tablet id: " << tablet_id;
}

int64_t SnapshotAction::make_snapshot(int64_t tablet_id, int32_t schema_hash,
                                      std::string* snapshot_path) {
    TSnapshotRequest request;
    request.tablet_id = tablet_id;
    request.schema_hash = schema_hash;

    OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
    bool allow_incremental_clone; // not used
    res = SnapshotManager::instance()->make_snapshot(request, snapshot_path, &allow_incremental_clone);
    if (res != OLAPStatus::OLAP_SUCCESS) {
        LOG(WARNING) << "make snapshot failed. status: " << res << ", signature: " << tablet_id;
        return -1L;
    } else {
        LOG(INFO) << "make snapshot success. status: " << res << ", signature: " << tablet_id
                  << ". path: " << *snapshot_path;
    }

    return 0L;
}

} // end namespace doris
