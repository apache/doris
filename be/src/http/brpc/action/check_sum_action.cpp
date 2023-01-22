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
#include "check_sum_action.h"

#include "agent/cgroups_mgr.h"
#include "boost/lexical_cast.hpp"
#include "olap/task/engine_checksum_task.h"

namespace doris {
const std::string TABLET_ID = "tablet_id";
// do not use name "VERSION",
// or will be conflict with "VERSION" in thrift/config.h
const std::string TABLET_VERSION = "version";
const std::string SCHEMA_HASH = "schema_hash";

CheckSumAction::CheckSumAction() : BaseHttpHandler("check_sum") {}

void CheckSumAction::handle_sync(brpc::Controller* cntl) {
    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get tablet id
    const std::string& tablet_id_str = *get_param(cntl, TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string("parameter " + TABLET_ID + " not specified in url.");
        on_bad_req(cntl, error_msg);
        return;
    }

    // Get version
    const std::string& version_str = *get_param(cntl, TABLET_VERSION);
    if (version_str.empty()) {
        std::string error_msg =
                std::string("parameter " + TABLET_VERSION + " not specified in url.");
        on_bad_req(cntl, error_msg);
        return;
    }

    // Get schema hash
    const std::string& schema_hash_str = *get_param(cntl, SCHEMA_HASH);
    if (schema_hash_str.empty()) {
        std::string error_msg = std::string("parameter " + SCHEMA_HASH + " not specified in url.");
        on_bad_req(cntl, error_msg);
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
        on_bad_req(cntl, error_msg);
        return;
    }

    VLOG_ROW << "get checksum tablet info: " << tablet_id << "-" << version << "-" << schema_hash;

    int64_t checksum = _do_check_sum(tablet_id, version, schema_hash);
    if (checksum == -1L) {
        std::string error_msg = std::string("checksum failed");
        on_error(cntl, error_msg);
        return;
    } else {
        std::stringstream result;
        result << checksum;
        std::string result_str = result.str();
        on_succ(cntl, result_str);
    }

    LOG(INFO) << "deal with checksum request finished! tablet id: " << tablet_id;
}

int64_t CheckSumAction::_do_check_sum(int64_t tablet_id, int64_t version, int32_t schema_hash) {
    Status res = Status::OK();
    uint32_t checksum;
    EngineChecksumTask engine_task(tablet_id, schema_hash, version, &checksum);
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
} // namespace doris