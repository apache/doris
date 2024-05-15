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

#include "http/action/calc_tablet_file_crc.h"

#include <exception>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/stopwatch.hpp"

namespace doris {
using namespace ErrorCode;

CalcTabletFileCrcAction::CalcTabletFileCrcAction(ExecEnv* exec_env, StorageEngine& engine,
                                                 TPrivilegeHier::type hier,
                                                 TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _engine(engine) {}

// for viewing the compaction status
Status CalcTabletFileCrcAction::_handle_calc_crc(HttpRequest* req, uint32_t* crc_value) {
    uint64_t tablet_id = 0;
    const auto& req_tablet_id = req->param(TABLET_ID_KEY);
    if (req_tablet_id.empty()) {
        return Status::InternalError("tablet id can not be empty!");
    }

    try {
        tablet_id = std::stoull(req_tablet_id);
    } catch (const std::exception& e) {
        return Status::InternalError("convert tablet id or failed, {}", e.what());
    }

    TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    auto st = tablet->clac_local_file_crc(crc_value);
    if (!st.ok()) {
        return st;
    }
    return Status::OK();
}

void CalcTabletFileCrcAction::handle(HttpRequest* req) {
    uint32_t crc_value;

    MonotonicStopWatch timer;
    timer.start();
    Status st = _handle_calc_crc(req, &crc_value);
    timer.stop();
    LOG(INFO) << "Calc tablet file crc finished, status = " << st << ", crc_value = " << crc_value
              << ", use time = " << timer.elapsed_time() / 1000000 << "ms";

    if (!st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK, st.to_json());
    } else {
        rapidjson::StringBuffer s;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
        writer.StartObject();
        writer.Key("crc_value");
        writer.String(std::to_string(crc_value).data());
        writer.Key("used_time_ms");
        writer.String(std::to_string(timer.elapsed_time() / 1000000).data());
        writer.EndObject();
        HttpChannel::send_reply(req, HttpStatus::OK, s.GetString());
    }
}

} // end namespace doris
