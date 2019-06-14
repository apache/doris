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

#include "http/action/restore_tablet_action.h"

#include <string>
#include <sstream>
#include <unistd.h>

#include "boost/lexical_cast.hpp"

#include "agent/cgroups_mgr.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "util/file_utils.h"
#include "olap/utils.h"
#include "olap/olap_header.h"
#include "util/json_util.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/store.h"
#include "runtime/exec_env.h"

using boost::filesystem::path;

namespace doris {

const std::string TABLET_ID = "tablet_id";
const std::string SCHEMA_HASH = "schema_hash";

RestoreTabletAction::RestoreTabletAction(ExecEnv* exec_env) : _exec_env(exec_env) {
}

void RestoreTabletAction::handle(HttpRequest *req) {
    LOG(INFO) << "accept one request " << req->debug_string();
    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    Status status = _handle(req);
    std::string result = to_json(status);
    LOG(INFO) << "handle request result:" << result;
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, result);
    } else {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, result);
    }
}

Status RestoreTabletAction::_handle(HttpRequest *req) {
    // Get tablet id
    const std::string& tablet_id_str = req->param(TABLET_ID);
    if (tablet_id_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + TABLET_ID + " not specified in url.");
        return Status::InternalError(error_msg);
    }

    // Get schema hash
    const std::string& schema_hash_str = req->param(SCHEMA_HASH);
    if (schema_hash_str.empty()) {
        std::string error_msg = std::string(
                "parameter " + SCHEMA_HASH + " not specified in url.");
        return Status::InternalError(error_msg);
    }

    // valid str format
    int64_t tablet_id = std::atol(tablet_id_str.c_str());
    int32_t schema_hash = std::atoi(schema_hash_str.c_str());
    LOG(INFO) << "get restore tablet action request: " << tablet_id << "-" << schema_hash;

    OLAPTablePtr tablet =
            OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (tablet.get() != nullptr) {
        LOG(WARNING) << "find tablet. tablet_id=" << tablet_id << " schema_hash=" << schema_hash;
        return Status::InternalError("tablet already exists, can not restore.");
    }
    std::string key = std::to_string(tablet_id) + "_" + std::to_string(schema_hash);
    {
        // check tablet_id + schema_hash already is restoring
        std::lock_guard<std::mutex> l(_tablet_restore_lock);
        if (_tablet_path_map.find(key) != _tablet_path_map.end()) {
            LOG(INFO) << "tablet_id:" << tablet_id << " schema_hash:" << schema_hash << " is restoring.";
            return Status::InternalError("tablet is already restoring");
        } else {
            // set key in map and initialize value as ""
            _tablet_path_map[key] = "";
            LOG(INFO) << "start to restore tablet_id:" << tablet_id << " schema_hash:" << schema_hash;
        }
    }
    Status status = _restore(key, tablet_id, schema_hash);
    _clear_key(key);
    LOG(INFO) << "deal with restore tablet request finished! tablet id: " << tablet_id << "-" << schema_hash;
    return status;
}

Status RestoreTabletAction::_reload_tablet(
        const std::string& key, const std::string& shard_path, int64_t tablet_id, int32_t schema_hash) {
    TCloneReq clone_req;
    clone_req.__set_tablet_id(tablet_id);
    clone_req.__set_schema_hash(schema_hash);
    OLAPStatus res = OLAPStatus::OLAP_SUCCESS;
    res = _exec_env->olap_engine()->load_header(shard_path, clone_req);
    if (res != OLAPStatus::OLAP_SUCCESS) {
        LOG(WARNING) << "load header failed. status: " << res
                     << ", signature: " << tablet_id;
        // remove tablet data path in data path
        // path: /roo_path/data/shard/tablet_id
        std::string tablet_path = shard_path + "/" + std::to_string(tablet_id);
        LOG(INFO) << "remove tablet_path:" << tablet_path;
        Status s = FileUtils::remove_all(tablet_path);
        if (!s.ok()) {
            LOG(WARNING) << "remove invalid tablet schema hash path:" << tablet_path << " failed";
        }
        return Status::InternalError("command executor load header failed");
    } else {
        LOG(INFO) << "load header success. status: " << res
                  << ", signature: " << tablet_id;
        // remove tablet data path in trash
        // path: /root_path/trash/time_label, because only one tablet path under time_label
        std::string trash_tablet_schema_hash_dir = "";

        {
            // get tablet path in trash
            std::lock_guard<std::mutex> l(_tablet_restore_lock);
            trash_tablet_schema_hash_dir = _tablet_path_map[key];
        }

        boost::filesystem::path trash_tablet_schema_hash_path(trash_tablet_schema_hash_dir);
        boost::filesystem::path time_label_path = trash_tablet_schema_hash_path.parent_path().parent_path();
        LOG(INFO) << "remove time label path:" << time_label_path.string();
        Status s = FileUtils::remove_all(time_label_path.string());
        if (!s.ok()) {
            LOG(WARNING) << "remove time label path:" << time_label_path.string() << " failed";
        }
        return Status::OK();
    }
} 

Status RestoreTabletAction::_restore(const std::string& key, int64_t tablet_id, int32_t schema_hash) {
    // get latest tablet path in trash
    std::string latest_tablet_path;
    bool ret = _get_latest_tablet_path_from_trash(tablet_id, schema_hash, &latest_tablet_path);
    if (!ret) {
        LOG(WARNING) << "can not find tablet:" << tablet_id
                << ", schema hash:" << schema_hash;
        return Status::InternalError("can find tablet path in trash");
    }
    LOG(INFO) << "tablet path in trash:" << latest_tablet_path;
    std::string original_header_path = latest_tablet_path + "/" + std::to_string(tablet_id) +".hdr";
    OLAPHeader header(original_header_path);
    OLAPStatus load_status = header.load_and_init();
    if (load_status != OLAP_SUCCESS) {
        LOG(WARNING) << "header load and init error, header path:" << original_header_path;
        return Status::InternalError("load header failed");
    }
    // latest_tablet_path: /root_path/trash/time_label/tablet_id/schema_hash
    {
        // update _tablet_path_map to save tablet path in trash for delete when succeed
        std::lock_guard<std::mutex> l(_tablet_restore_lock);
        _tablet_path_map[key] = latest_tablet_path;
    }

    std::string root_path = OlapStore::get_root_path_from_schema_hash_path_in_trash(latest_tablet_path);
    OlapStore* store = OLAPEngine::get_instance()->get_store(root_path);
    std::string restore_schema_hash_path = store->get_tablet_schema_hash_path_from_header(&header);
    Status s = FileUtils::create_dir(restore_schema_hash_path);
    if (!s.ok()) {
        LOG(WARNING) << "create tablet path failed:" << restore_schema_hash_path;
        return s;
    }
    // create hard link for files in /root_path/data/shard/tablet_id/schema_hash
    std::vector<std::string> files;
    s = FileUtils::scan_dir(latest_tablet_path, &files);
    if (!s.ok()) {
        LOG(WARNING) << "scan dir failed:" << latest_tablet_path;
        return s;
    }
    for (auto& file : files) {
        std::string from = latest_tablet_path + "/" + file;
        std::string to = restore_schema_hash_path + "/" + file;
        int link_ret = link(from.c_str(), to.c_str());
        if (link_ret != 0) {
            LOG(WARNING) << "link from:" << from
                    << " to:" << to  << " failed, link ret:" << link_ret;
            std::string restore_tablet_path = store->get_tablet_path_from_header(&header);
            LOG(WARNING) << "remove tablet_path:" << restore_tablet_path;
            Status s = FileUtils::remove_all(restore_tablet_path);
            if (!s.ok()) {
                LOG(WARNING) << "remove invalid tablet path:" << restore_tablet_path << " failed";
            }
            return Status::InternalError("create link path failed");
        }
    }
    std::string restore_shard_path = store->get_shard_path_from_header(std::to_string(header.shard()));
    Status status = _reload_tablet(key, restore_shard_path, tablet_id, schema_hash);
    return status;
}

bool RestoreTabletAction::_get_latest_tablet_path_from_trash(
        int64_t tablet_id, int32_t schema_hash, std::string* path) {
    std::vector<std::string> tablet_paths;
    std::vector<OlapStore*> stores = OLAPEngine::get_instance()->get_stores();
    for (auto& store : stores) {
        store->find_tablet_in_trash(tablet_id, &tablet_paths);
    }
    if (tablet_paths.empty()) {
        LOG(WARNING) << "can not find tablet_id:" << tablet_id << ", schema_hash:" << schema_hash;
        return false;
    }
    std::vector<std::string> schema_hash_paths;
    for (auto& tablet_path : tablet_paths) {
        std::string schema_hash_path = tablet_path + "/" + std::to_string(schema_hash);
        bool exist = FileUtils::check_exist(schema_hash_path);
        if (exist) {
            schema_hash_paths.emplace_back(std::move(schema_hash_path));
        }
    }
    if (schema_hash_paths.size() == 0) {
        LOG(WARNING) << "can not find tablet_id:" << tablet_id
                << ", schema_hash:" << schema_hash;;
        return false;
    } else if (schema_hash_paths.size() == 1) {
        *path = schema_hash_paths[0];
        return true;
    } else {
        int start_index = 0;
        uint64_t max_timestamp = 0;
        uint64_t max_counter = 0;
        *path = schema_hash_paths[start_index];
        if (!_get_timestamp_and_count_from_schema_hash_path(
                schema_hash_paths[start_index], &max_timestamp, &max_counter)) {
            LOG(WARNING) << "schema hash paths are invalid, path:" << schema_hash_paths[start_index];
            return false;
        }
        // find latest path
        for (int i = start_index + 1; i < schema_hash_paths.size(); i++) {
            uint64_t current_timestamp = 0;
            uint64_t current_counter = 0;
            if (!_get_timestamp_and_count_from_schema_hash_path(
                    schema_hash_paths[i], &current_timestamp, &current_counter)) {
                LOG(WARNING) << "schema hash path:" << schema_hash_paths[i] << " is invalid";
                continue;
            }
            if (current_timestamp > max_timestamp) {
                *path = schema_hash_paths[i];
                max_timestamp = current_timestamp;
                max_counter = current_counter;
            } else if (current_timestamp == max_timestamp) {
                if (current_counter > max_counter) {
                    *path = schema_hash_paths[i];
                    max_counter = current_counter;
                }
            }
        }
        return true;
    }
}

bool RestoreTabletAction::_get_timestamp_and_count_from_schema_hash_path(
        const std::string& schema_hash_dir, uint64_t* timestamp, uint64_t* counter) {
    path schema_hash_path(schema_hash_dir);
    path time_label_path = schema_hash_path.parent_path().parent_path();
    std::string time_label = time_label_path.filename().string();
    std::vector<std::string> parts;
    doris::split_string<char>(time_label, '.', &parts);
    if (parts.size() != 2) {
        LOG(WARNING) << "invalid time label:" << time_label;
        return false;
    }
    *timestamp = std::stoul(parts[0]);
    *counter = std::stoul(parts[1]);
    return true;
}

void RestoreTabletAction::_clear_key(const std::string& key) {
    std::lock_guard<std::mutex> l(_tablet_restore_lock);
    _tablet_path_map.erase(key);
}

} // end namespace doris
