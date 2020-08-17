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

#include "http/action/tablets_info_action.h"

#include <string>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "service/backend_options.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletsInfoAction::TabletsInfoAction() {
    _host = BackendOptions::get_localhost();
}

void TabletsInfoAction::handle(HttpRequest *req) {
    const std::string& tablet_num_to_return = req->param("limit");
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, get_tablets_info(tablet_num_to_return).ToString());
}

EasyJson TabletsInfoAction::get_tablets_info(string tablet_num_to_return) {
    std::vector<TabletInfo> tablets_info;
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    tablet_manager->obtain_all_tablets(tablets_info);

    int64_t number;
    std::string msg;
    if (tablet_num_to_return == "") {
        number = 0;
        msg = "Parameter Missing";
    } else if (tablet_num_to_return == "all") {
        number = tablets_info.size();
        msg = "OK";
    } else if (std::all_of(tablet_num_to_return.begin(), tablet_num_to_return.end(), ::isdigit)) {
        int64_t tablet_num = std::atol(tablet_num_to_return.c_str());
        number = tablet_num < tablets_info.size() ? tablet_num : tablets_info.size();
        msg = "OK";
    } else {
        number = 0;
        msg = "Parameter Error";
    }

    EasyJson tablets_info_ej;
    tablets_info_ej["msg"] = msg;
    tablets_info_ej["code"] = 0;
    EasyJson data = tablets_info_ej.Set("data", EasyJson::kObject);
    data["host"] = _host;
    EasyJson tablets = data.Set("tablets", EasyJson::kArray);
    for (int64_t i = 0; i < number; i++) {
        EasyJson tablet = tablets.PushBack(EasyJson::kObject);
        tablet["tablet_id"] = tablets_info[i].tablet_id;
        tablet["schema_hash"] = tablets_info[i].schema_hash;
    }
    tablets_info_ej["count"] = number;
    return tablets_info_ej;
}
} // namespace doris

