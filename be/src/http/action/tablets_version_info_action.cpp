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

#include "http/action/tablets_version_info_action.h"

#include <string>
#include <vector>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/olap_common.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "service/backend_options.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletsVersionInfoAction::TabletsVersionInfoAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                                   TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}

void TabletsVersionInfoAction::handle(HttpRequest* req) {
    int64_t count = 0;
    int64_t number = std::numeric_limits<std::int64_t>::max();
    std::vector<TabletInfo> tablets_info;
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    tablet_manager->obtain_specific_quantity_tablets(tablets_info, number);

    EasyJson over_limit_version_info;
    over_limit_version_info["msg"] = "OK";
    over_limit_version_info["code"] = 0;
    EasyJson data = over_limit_version_info.Set("data", EasyJson::kObject);
    data["host"] = BackendOptions::get_localhost();
    EasyJson tablets = data.Set("tablets", EasyJson::kArray);
    for (TabletInfo tablet_info : tablets_info) {
        const TTabletId tablet_id = tablet_info.tablet_id;
        TabletSharedPtr _tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (_tablet->version_count() > config::max_tablet_version_num) {
            count++;
            EasyJson tablet = tablets.PushBack(EasyJson::kObject);
            tablet["tablet_id"] = tablet_id;
        }
    }
    over_limit_version_info["count"] = count;
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, over_limit_version_info.ToString());
}

} // namespace doris