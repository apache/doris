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

#include "http/brpc/action/check_tablet_segment_action.h"

#include <brpc/http_method.h>

#include "olap/storage_engine.h"
#include "util/easy_json.h"
namespace doris {
CheckTabletSegmentHandler::CheckTabletSegmentHandler() : BaseHttpHandler("check_tablet_segement") {}

void CheckTabletSegmentHandler::handle_sync(brpc::Controller* cntl) {
    bool repaired = false;
    const std::string* repaired_ptr = get_param(cntl, "repair");
    if (repaired_ptr == nullptr || repaired_ptr->empty()) {
        EasyJson result_ej;
        result_ej["status"] = "Fail";
        result_ej["msg"] = "Parameter 'repair' must be set to 'true' or 'false'";
        on_succ(cntl, result_ej.ToString());
        return;
    }
    if (*repaired_ptr == "true") {
        repaired = true;
    }
    LOG(INFO) << "start to check tablet segment.";
    std::set<int64_t> bad_tablets =
            StorageEngine::instance()->tablet_manager()->check_all_tablet_segment(repaired);
    LOG(INFO) << "finish to check tablet segment.";

    EasyJson result_ej;
    result_ej["status"] = "Success";
    result_ej["msg"] = "Succeed to check all tablet segment";
    result_ej["num"] = bad_tablets.size();
    EasyJson tablets = result_ej.Set("bad_tablets", EasyJson::kArray);
    for (int64_t tablet_id : bad_tablets) {
        tablets.PushBack<int64_t>(tablet_id);
    }
    result_ej["set_bad"] = repaired ? "true" : "false";
    result_ej["host"] = get_localhost(cntl).c_str();
    on_succ(cntl, result_ej.ToString());
}

bool CheckTabletSegmentHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_POST;
}

} // namespace doris