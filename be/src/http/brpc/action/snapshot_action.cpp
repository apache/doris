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

#include "snapshot_action.h"

#include <boost/lexical_cast.hpp>

#include "agent/cgroups_mgr.h"
#include "olap/snapshot_manager.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

SnapshotHandler::SnapshotHandler() : BaseHttpHandler("snapshot") {}

void SnapshotHandler::handle_sync(brpc::Controller* cntl) {
    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get tablet id
    const std::string& tablet_id_str = *get_param(cntl, TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string("parameter " + TABLET_ID + " not specified in url.");

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
    int32_t schema_hash;
    try {
        tablet_id = boost::lexical_cast<int64_t>(tablet_id_str);
        schema_hash = boost::lexical_cast<int64_t>(schema_hash_str);
    } catch (boost::bad_lexical_cast& e) {
        std::string error_msg = std::string("param format is invalid: ") + std::string(e.what());
        on_bad_req(cntl, error_msg);
        return;
    }

    VLOG_ROW << "get make snapshot tablet info: " << tablet_id << "-" << schema_hash;

    std::string snapshot_path;
    int64_t ret = _make_snapshot(tablet_id, schema_hash, &snapshot_path);
    if (ret != 0L) {
        std::string error_msg = std::string("make snapshot failed");
        on_error(cntl, error_msg);
        return;
    } else {
        std::stringstream result;
        result << snapshot_path;
        std::string result_str = result.str();
        on_succ(cntl, result_str);
    }

    LOG(INFO) << "deal with snapshot request finished! tablet id: " << tablet_id;
}

int64_t SnapshotHandler::_make_snapshot(int64_t tablet_id, int schema_hash,
                                        std::string* snapshot_path) {
    TSnapshotRequest request;
    request.tablet_id = tablet_id;
    request.schema_hash = schema_hash;

    Status res = Status::OK();
    bool allow_incremental_clone; // not used
    res = SnapshotManager::instance()->make_snapshot(request, snapshot_path,
                                                     &allow_incremental_clone);
    if (!res.ok()) {
        LOG(WARNING) << "make snapshot failed. status: " << res << ", signature: " << tablet_id;
        return -1L;
    } else {
        LOG(INFO) << "make snapshot success. status: " << res << ", signature: " << tablet_id
                  << ". path: " << *snapshot_path;
    }

    return 0L;
}

} // namespace doris