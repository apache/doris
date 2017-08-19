// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include <string>
#include <sstream>

#include "boost/lexical_cast.hpp"

#include "agent/cgroups_mgr.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"

namespace palo {

const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

SnapshotAction::SnapshotAction(ExecEnv* exec_env) :
        _exec_env(exec_env) {
    _command_executor = new CommandExecutor();
}

void SnapshotAction::handle(HttpRequest *req, HttpChannel *channel) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get tablet id
    const std::string& tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + TABLET_ID + " not specified in url.");
        HttpResponse response(HttpStatus::BAD_REQUEST, &error_msg);
        channel->send_response(response);
        return;
    }

    // Get schema hash
    const std::string& schema_hash_str = req->param(SCHEMA_HASH);
    if (schema_hash_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + SCHEMA_HASH + " not specified in url.");
        HttpResponse response(HttpStatus::BAD_REQUEST, &error_msg);
        channel->send_response(response);
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
        HttpResponse response(HttpStatus::BAD_REQUEST, &error_msg);
        channel->send_response(response);
        return;
    }

    VLOG_ROW << "get make snapshot tablet info: " << tablet_id << "-" << schema_hash;

    std::string snapshot_path;
    int64_t ret = make_snapshot(tablet_id, schema_hash, &snapshot_path);
    if (ret != 0L) {
        std::string error_msg = std::string("make snapshot falied");
        HttpResponse response(HttpStatus::INTERNAL_SERVER_ERROR, &error_msg);
        channel->send_response(response);
        return;
    } else {
        std::stringstream result;
        result << snapshot_path;
        std::string result_str = result.str();
        HttpResponse response(HttpStatus::OK, &result_str);
        channel->send_response(response);
    }

    LOG(INFO) << "deal with snapshot request finished! tablet id: " << tablet_id;
}

int64_t SnapshotAction::make_snapshot(int64_t tablet_id, int32_t schema_hash,
                                      std::string* snapshot_path) {

    OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
    res = _command_executor->make_snapshot(tablet_id, schema_hash, snapshot_path);
    if (res != OLAPStatus::OLAP_SUCCESS) {
        LOG(WARNING) << "make snapshot failed. status: " << res
                     << ", signature: " << tablet_id;
        return -1L;
    } else {
        LOG(INFO) << "make snapshot success. status: " << res
                  << ", signature: " << tablet_id << ". path: " << *snapshot_path;
    }

    return 0L;
} 

SnapshotAction::~SnapshotAction() {
    if (_command_executor != NULL) {
        delete _command_executor;
    }
}

} // end namespace palo
