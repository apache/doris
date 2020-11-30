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

#include "http/action/reload_tablet_action.h"

#include <sstream>
#include <string>

#include "agent/cgroups_mgr.h"
#include "boost/lexical_cast.hpp"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

const std::string PATH = "path";
const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

ReloadTabletAction::ReloadTabletAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void ReloadTabletAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();

    // Get path
    const std::string& path = req->param(PATH);
    if (path.empty()) {
        std::string error_msg = std::string("parameter " + PATH + " not specified in url.");
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

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

    VLOG_ROW << "get reload tablet request: " << tablet_id << "-" << schema_hash;

    reload(path, tablet_id, schema_hash, req);

    LOG(INFO) << "deal with reload tablet request finished! tablet id: " << tablet_id;
}

void ReloadTabletAction::reload(const std::string& path, int64_t tablet_id, int32_t schema_hash,
                                HttpRequest* req) {
    TCloneReq clone_req;
    clone_req.__set_tablet_id(tablet_id);
    clone_req.__set_schema_hash(schema_hash);

    OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
    res = _exec_env->storage_engine()->load_header(path, clone_req);
    if (res != OLAPStatus::OLAP_SUCCESS) {
        LOG(WARNING) << "load header failed. status: " << res << ", signature: " << tablet_id;
        std::string error_msg = std::string("load header failed");
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
        return;
    } else {
        LOG(INFO) << "load header success. status: " << res << ", signature: " << tablet_id;
        std::string result_msg = std::string("load header succeed");
        HttpChannel::send_reply(req, result_msg);
        return;
    }
}

} // end namespace doris
