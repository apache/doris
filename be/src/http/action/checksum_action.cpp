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

#include "http/action/checksum_action.h"

#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <sstream>
#include <string>

#include "boost/lexical_cast.hpp"
#include "common/logging.h"
#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/task/engine_checksum_task.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";
// do not use name "VERSION",
// or will be conflict with "VERSION" in thrift/config.h
const std::string TABLET_VERSION = "version";
const std::string SCHEMA_HASH = "schema_hash";

ChecksumAction::ChecksumAction(ExecEnv* exec_env, StorageEngine& engine, TPrivilegeHier::type hier,
                               TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type), _engine(engine) {}

void ChecksumAction::handle(HttpRequest* req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    // Get tablet id
    const std::string& tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string("parameter " + TABLET_ID + " not specified in url.");

        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    // Get version
    const std::string& version_str = req->param(TABLET_VERSION);
    if (version_str.empty()) {
        std::string error_msg =
                std::string("parameter " + TABLET_VERSION + " not specified in url.");
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
    int64_t version;
    int32_t schema_hash;
    try {
        tablet_id = boost::lexical_cast<int64_t>(tablet_id_str);
        version = boost::lexical_cast<int64_t>(version_str);
        schema_hash = boost::lexical_cast<int64_t>(schema_hash_str);
    } catch (boost::bad_lexical_cast& e) {
        std::string error_msg = std::string("param format is invalid: ") + std::string(e.what());
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, error_msg);
        return;
    }

    VLOG_ROW << "get checksum tablet info: " << tablet_id << "-" << version << "-" << schema_hash;

    int64_t checksum = do_checksum(tablet_id, version, schema_hash, req);
    if (checksum == -1L) {
        std::string error_msg = std::string("checksum failed");
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, error_msg);
        return;
    } else {
        std::stringstream result;
        result << checksum;
        std::string result_str = result.str();
        HttpChannel::send_reply(req, result_str);
    }

    LOG(INFO) << "deal with checksum request finished! tablet id: " << tablet_id;
}

int64_t ChecksumAction::do_checksum(int64_t tablet_id, int64_t version, int32_t schema_hash,
                                    HttpRequest* req) {
    Status res = Status::OK();
    uint32_t checksum;
    EngineChecksumTask engine_task(_engine, tablet_id, schema_hash, version, &checksum);
    SCOPED_ATTACH_TASK(engine_task.mem_tracker());
    res = engine_task.execute();
    if (!res.ok()) {
        LOG(WARNING) << "checksum failed. status: " << res << ", signature: " << tablet_id;
        return -1L;
    } else {
        LOG(INFO) << "checksum success. status: " << res << ", signature: " << tablet_id
                  << ". checksum: " << checksum;
    }

    return static_cast<int64_t>(checksum);
}

} // end namespace doris
