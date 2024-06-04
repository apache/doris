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

#include "http/action/check_tablet_segment_action.h"

#include <glog/logging.h>
#include <stdint.h>

#include <ostream>
#include <set>
#include <string>

#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "service/backend_options.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

CheckTabletSegmentAction::CheckTabletSegmentAction(ExecEnv* exec_env, StorageEngine& engine,
                                                   TPrivilegeHier::type hier,
                                                   TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type),
          _engine(engine),
          _host(BackendOptions::get_localhost()) {}

void CheckTabletSegmentAction::handle(HttpRequest* req) {
    bool repair = false;
    std::string is_repair = req->param("repair");
    if (is_repair == "true") {
        repair = true;
    } else if (!is_repair.empty() && is_repair != "false") {
        EasyJson result_ej;
        result_ej["status"] = "Fail";
        result_ej["msg"] = "Parameter 'repair' must be set to 'true' or 'false'";
        req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
        HttpChannel::send_reply(req, HttpStatus::OK, result_ej.ToString());
        return;
    }

    LOG(INFO) << "start to check tablet segment.";
    std::set<int64_t> bad_tablets = _engine.tablet_manager()->check_all_tablet_segment(repair);
    LOG(INFO) << "finish to check tablet segment.";

    EasyJson result_ej;
    result_ej["status"] = "Success";
    result_ej["msg"] = "Succeed to check all tablet segment";
    result_ej["num"] = bad_tablets.size();
    EasyJson tablets = result_ej.Set("bad_tablets", EasyJson::kArray);
    for (int64_t tablet_id : bad_tablets) {
        tablets.PushBack<int64_t>(tablet_id);
    }
    result_ej["set_bad"] = repair ? "true" : "false";
    result_ej["host"] = _host;
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, result_ej.ToString());
}

} // namespace doris
