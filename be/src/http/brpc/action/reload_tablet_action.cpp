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
#include "reload_tablet_action.h"

#include <boost/lexical_cast.hpp>

#include "agent/cgroups_mgr.h"
#include "olap/storage_engine.h"

namespace doris {

const std::string PATH = "path";
const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

ReloadTabletHandler::ReloadTabletHandler(ExecEnv* exev_env)
        : BaseHttpHandler("reload_tablet", exev_env) {}

void ReloadTabletHandler::handle_sync(brpc::Controller* cntl) {
    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();

    // Get path
    const std::string& path = *get_param(cntl, PATH);
    if (path.empty()) {
        std::string error_msg = std::string("parameter " + PATH + " not specified in url.");
        on_bad_req(cntl, error_msg);
        return;
    }

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

    VLOG_ROW << "get reload tablet request: " << tablet_id << "-" << schema_hash;

    reload(cntl, path, tablet_id, schema_hash);

    LOG(INFO) << "deal with reload tablet request finished! tablet id: " << tablet_id;
}

void ReloadTabletHandler::reload(brpc::Controller* cntl, const std::string& path, int64_t tablet_id,
                                 int32_t schema_hash) {
    TCloneReq clone_req;
    clone_req.__set_tablet_id(tablet_id);
    clone_req.__set_schema_hash(schema_hash);

    Status res = Status::OK();
    res = get_exec_env()->storage_engine()->load_header(path, clone_req);
    if (!res.ok()) {
        LOG(WARNING) << "load header failed. status: " << res << ", signature: " << tablet_id;
        std::string error_msg = std::string("load header failed");
        on_error(cntl, error_msg);
        return;
    } else {
        LOG(INFO) << "load header success. status: " << res << ", signature: " << tablet_id;
        std::string result_msg = std::string("load header succeed");
        on_succ(cntl, result_msg);
        return;
    }
}

} // namespace doris