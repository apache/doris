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

#include "http/action/calc_file_crc_action.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <exception>
#include <string>

#include "cloud/cloud_storage_engine.h"
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

CalcFileCrcAction::CalcFileCrcAction(ExecEnv* exec_env, BaseStorageEngine& engine,
                                     TPrivilegeHier::type hier, TPrivilegeType::type ptype)
        : HttpHandlerWithAuth(exec_env, hier, ptype), _engine(engine) {}

// calculate the crc value of the files in the tablet
Status CalcFileCrcAction::_handle_calc_crc(HttpRequest* req, uint32_t* crc_value,
                                           int64_t* start_version, int64_t* end_version,
                                           int32_t* rowset_count, int64_t* file_count) {
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

    BaseTabletSPtr tablet = nullptr;

    if (auto cloudEngine = dynamic_cast<CloudStorageEngine*>(&_engine)) {
        tablet = DORIS_TRY(cloudEngine->get_tablet(tablet_id));
        // sync all rowsets
        RETURN_IF_ERROR(std::dynamic_pointer_cast<CloudTablet>(tablet)->sync_rowsets(-1));
    } else if (auto storageEngine = dynamic_cast<StorageEngine*>(&_engine)) {
        auto tabletPtr = storageEngine->tablet_manager()->get_tablet(tablet_id);
        tablet = std::dynamic_pointer_cast<Tablet>(tabletPtr);
    } else {
        return Status::InternalError("convert _engine failed");
    }

    if (tablet == nullptr) {
        return Status::NotFound("failed to get tablet {}", tablet_id);
    }

    const auto& req_start_version = req->param(PARAM_START_VERSION);
    const auto& req_end_version = req->param(PARAM_END_VERSION);

    *start_version = 0;
    *end_version = tablet->max_version_unlocked();

    if (!req_start_version.empty()) {
        try {
            *start_version = std::stoll(req_start_version);
        } catch (const std::exception& e) {
            return Status::InternalError("convert start version failed, {}", e.what());
        }
    }
    if (!req_end_version.empty()) {
        try {
            *end_version =
                    std::min(*end_version, static_cast<int64_t>(std::stoll(req_end_version)));
        } catch (const std::exception& e) {
            return Status::InternalError("convert end version failed, {}", e.what());
        }
    }

    auto st = tablet->calc_file_crc(crc_value, *start_version, *end_version, rowset_count,
                                    file_count);
    if (!st.ok()) {
        return st;
    }
    return Status::OK();
}

void CalcFileCrcAction::handle(HttpRequest* req) {
    uint32_t crc_value = 0;
    int64_t start_version = 0;
    int64_t end_version = 0;
    int32_t rowset_count = 0;
    int64_t file_count = 0;

    MonotonicStopWatch timer;
    timer.start();
    Status st = _handle_calc_crc(req, &crc_value, &start_version, &end_version, &rowset_count,
                                 &file_count);
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
        writer.Key("start_version");
        writer.String(std::to_string(start_version).data());
        writer.Key("end_version");
        writer.String(std::to_string(end_version).data());
        writer.Key("rowset_count");
        writer.String(std::to_string(rowset_count).data());
        writer.Key("file_count");
        writer.String(std::to_string(file_count).data());
        writer.EndObject();
        HttpChannel::send_reply(req, HttpStatus::OK, s.GetString());
    }
}

} // end namespace doris
