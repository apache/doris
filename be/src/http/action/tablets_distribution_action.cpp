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

#include "http/action/tablets_distribution_action.h"

#include <string>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "service/backend_options.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletsDistributionAction::TabletsDistributionAction() {
    _host = BackendOptions::get_localhost();
}

void TabletsDistributionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());

    std::string req_group_method = req->param("group_by");
    if (req_group_method == "partition") {
        std::string req_partition_id = req->param("partition_id");
        uint64_t partition_id = 0;
        if (req_partition_id != "") {
            try {
                partition_id = std::stoull(req_partition_id);
            } catch (const std::exception& e) {
                LOG(WARNING) << "invalid argument. partition_id:" << req_partition_id;
                Status status = Status::InternalError(
                        strings::Substitute("invalid argument: partition_id"));
                std::string status_result = to_json(status);
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
                return;
            }
        }
        HttpChannel::send_reply(
                req, HttpStatus::OK,
                get_tablets_distribution_group_by_partition(partition_id).ToString());
        return;
    }
    LOG(WARNING) << "invalid argument. group_by:" << req_group_method;
    Status status = Status::InternalError(strings::Substitute("invalid argument: group_by"));
    std::string status_result = to_json(status);
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status_result);
}

EasyJson TabletsDistributionAction::get_tablets_distribution_group_by_partition(
        uint64_t partition_id) {
    std::map<int64_t, std::map<DataDir*, int64_t>> tablets_num_on_disk;
    std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>> tablets_info_on_disk;
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    tablet_manager->get_tablets_distribution_on_different_disks(tablets_num_on_disk,
                                                                tablets_info_on_disk);

    EasyJson tablets_distribution_ej;
    tablets_distribution_ej["msg"] = "OK";
    tablets_distribution_ej["code"] = 0;
    EasyJson data = tablets_distribution_ej.Set("data", EasyJson::kObject);
    data["host"] = _host;
    EasyJson tablets_distribution = data.Set("tablets_distribution", EasyJson::kArray);
    int64_t tablet_total_number = 0;
    std::map<int64_t, std::map<DataDir*, int64_t>>::iterator partition_iter =
            tablets_num_on_disk.begin();
    for (; partition_iter != tablets_num_on_disk.end(); ++partition_iter) {
        if (partition_id != 0 && partition_id != partition_iter->first) {
            continue;
        }
        EasyJson partition = tablets_distribution.PushBack(EasyJson::kObject);
        partition["partition_id"] = partition_iter->first;
        EasyJson disks = partition.Set("disks", EasyJson::kArray);
        std::map<DataDir*, int64_t>::iterator disk_iter = (partition_iter->second).begin();
        for (; disk_iter != (partition_iter->second).end(); ++disk_iter) {
            EasyJson disk = disks.PushBack(EasyJson::kObject);
            disk["disk_path"] = disk_iter->first->path();
            disk["tablets_num"] = disk_iter->second;
            tablet_total_number += disk_iter->second;
            if (partition_id != 0) {
                EasyJson tablets = disk.Set("tablets", EasyJson::kArray);
                for (int64_t i = 0;
                     i < tablets_info_on_disk[partition_iter->first][disk_iter->first].size();
                     i++) {
                    EasyJson tablet = tablets.PushBack(EasyJson::kObject);
                    tablet["tablet_id"] =
                            tablets_info_on_disk[partition_iter->first][disk_iter->first][i]
                                    .tablet_id;
                    tablet["schema_hash"] =
                            tablets_info_on_disk[partition_iter->first][disk_iter->first][i]
                                    .schema_hash;
                    tablet["tablet_size"] =
                            tablets_info_on_disk[partition_iter->first][disk_iter->first][i]
                                    .tablet_size;
                }
            }
        }
    }
    tablets_distribution_ej["count"] = tablet_total_number;
    return tablets_distribution_ej;
}

} // namespace doris
