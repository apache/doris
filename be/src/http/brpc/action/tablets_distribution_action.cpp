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

#include "tablets_distribution_action.h"

#include <brpc/http_method.h>
#include <glog/logging.h>

#include "olap/data_dir.h"
#include "olap/storage_engine.h"

namespace doris {
TabletsDistributionHandler::TabletsDistributionHandler()
        : BaseHttpHandler("tablets_distribution") {}

EasyJson TabletsDistributionHandler::get_tablets_distribution_group_by_partition(
        brpc::Controller* cntl, uint64_t partition_id) {
    std::map<int64_t, std::map<DataDir*, int64_t>> tablets_num_on_disk;
    std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>> tablets_info_on_disk;
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    tablet_manager->get_tablets_distribution_on_different_disks(tablets_num_on_disk,
                                                                tablets_info_on_disk);

    EasyJson tablets_distribution_ej;
    tablets_distribution_ej["msg"] = "OK";
    tablets_distribution_ej["code"] = 0;
    EasyJson data = tablets_distribution_ej.Set("data", EasyJson::kObject);
    data["host"] = get_localhost(cntl).c_str();
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

void TabletsDistributionHandler::handle_sync(brpc::Controller* cntl) {
    const std::string* req_group_method = get_param(cntl, "group_by");
    if (req_group_method == nullptr) {
        on_bad_req(cntl, "invalid null param: group_by");
        return;
    }
    if (*req_group_method == "partition") {
        const std::string* req_partition_id = get_param(cntl, "partition_id");
        if (req_partition_id == nullptr) {
            on_bad_req(cntl, "invalid null param: partition_id");
            return;
        }
        uint64_t partition_id = 0;
        if (*req_partition_id != "") {
            try {
                partition_id = std::stoull(*req_partition_id);
            } catch (const std::exception& e) {
                LOG(WARNING) << "invalid argument. partition_id:" << *req_partition_id;
                Status status = Status::InternalError("invalid argument: {}", *req_partition_id);
                std::string status_result = status.to_json();
                on_error_json(cntl, status_result);
                return;
            }
        }
        on_succ_json(cntl,
                     get_tablets_distribution_group_by_partition(cntl, partition_id).ToString());
        return;
    }
    LOG(WARNING) << "invalid argument. group_by:" << *req_group_method;
    Status status = Status::InternalError(strings::Substitute("invalid argument: group_by"));
    std::string status_result = status.to_json();
    on_error_json(cntl, status_result);
}

bool TabletsDistributionHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}

} // namespace doris