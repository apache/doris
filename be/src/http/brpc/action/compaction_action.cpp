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

#include "compaction_action.h"

#include <brpc/http_method.h>

#include <future>

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/storage_engine.h"

namespace doris {
using namespace ErrorCode;

const static std::string& SHOW_INFO_TYPE = "show";
const static std::string& RUN_COMPACTION_TYPE = "run";
const static std::string& RUN_STATUS_TYPE = "run_status";

const static std::string& PARAM_COMPACTION_TYPE = "compact_type";
const static std::string& PARAM_COMPACTION_BASE = "base";
const static std::string& PARAM_COMPACTION_CUMULATIVE = "cumulative";

CompactionHandler::CompactionHandler() : BaseHttpHandler("compaction") {}

void CompactionHandler::handle_sync(brpc::Controller* cntl) {
    const std::string& type = cntl->http_request().unresolved_path();
    if (type == SHOW_INFO_TYPE) {
        std::string json_result;
        Status st = _handle_show_compaction(cntl, &json_result);
        if (!st.ok()) {
            on_succ_json(cntl, st.to_json());
        } else {
            on_succ_json(cntl, json_result);
        }
    } else if (type == RUN_COMPACTION_TYPE) {
        std::string json_result;
        Status st = _handle_run_compaction(cntl, &json_result);
        if (!st.ok()) {
            on_succ_json(cntl, st.to_json());
        } else {
            on_succ_json(cntl, json_result);
        }
    } else if (type == RUN_STATUS_TYPE) {
        std::string json_result;
        Status st = _handle_run_status_compaction(cntl, &json_result);
        if (!st.ok()) {
            on_succ_json(cntl, st.to_json());
        } else {
            on_succ_json(cntl, json_result);
        }
    } else {
        on_bad_req(cntl, "unknown type");
    }
}

bool CompactionHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET || method == brpc::HTTP_METHOD_POST;
}

Status CompactionHandler::_handle_show_compaction(brpc::Controller* cntl,
                                                  std::string* json_result) {
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(cntl, &tablet_id), "check param failed");

    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    tablet->get_compaction_status(json_result);
    return Status::OK();
}

Status CompactionHandler::_check_param(brpc::Controller* cntl, uint64_t* tablet_id) {
    const std::string* req_tablet_id = get_param(cntl, TABLET_ID_KEY);
    if (req_tablet_id == nullptr || req_tablet_id->empty()) {
        return Status::OK();
    }
    try {
        *tablet_id = std::stoull(*req_tablet_id);
    } catch (const std::exception& e) {
        return Status::InternalError("convert tablet_id failed, {}", e.what());
    }
    return Status::OK();
}

Status CompactionHandler::_handle_run_compaction(brpc::Controller* cntl, std::string* json_result) {
    // 1. param check
    // check req_tablet_id is not empty
    uint64_t tablet_id = 0;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(cntl, &tablet_id), "check param failed");

    // check compaction_type equals 'base' or 'cumulative'
    const std::string* compaction_type_ptr = get_param(cntl, PARAM_COMPACTION_TYPE);
    if (compaction_type_ptr == nullptr) {
        return Status::NotSupported("compact type is not specified");
    }
    const std::string compaction_type = *compaction_type_ptr;
    if (compaction_type != PARAM_COMPACTION_BASE &&
        compaction_type != PARAM_COMPACTION_CUMULATIVE) {
        return Status::NotSupported("The compaction type '{}' is not supported", compaction_type);
    }

    // 2. fetch the tablet by tablet_id
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::NotFound("Tablet not found. tablet_id={}", tablet_id);
    }

    // 3. execute compaction task
    std::packaged_task<Status()> task([this, tablet, compaction_type]() {
        return _execute_compaction_callback(tablet, compaction_type);
    });
    std::future<Status> future_obj = task.get_future();
    std::thread(std::move(task)).detach();

    // 4. wait for result for 2 seconds by async
    std::future_status status = future_obj.wait_for(std::chrono::seconds(2));
    if (status == std::future_status::ready) {
        // fetch execute result
        Status olap_status = future_obj.get();
        if (!olap_status.ok()) {
            return olap_status;
        }
    } else {
        LOG(INFO) << "Manual compaction task is timeout for waiting "
                  << (status == std::future_status::timeout);
    }

    LOG(INFO) << "Manual compaction task is successfully triggered";
    *json_result =
            "{\"status\": \"Success\", \"msg\": \"compaction task is successfully triggered.\"}";

    return Status::OK();
}

Status CompactionHandler::_handle_run_status_compaction(brpc::Controller* cntl,
                                                        std::string* json_result) {
    uint64_t tablet_id = 0;

    // check req_tablet_id is not empty
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_param(cntl, &tablet_id), "check param failed");

    if (tablet_id == 0) {
        // overall compaction status
        RETURN_IF_ERROR(StorageEngine::instance()->get_compaction_status_json(json_result));
        return Status::OK();
    } else {
        // fetch the tablet by tablet_id
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << "invalid argument.tablet_id:" << tablet_id;
            return Status::InternalError("fail to get {}", tablet_id);
        }

        std::string json_template = R"({
            "status" : "Success",
            "run_status" : $0,
            "msg" : "$1",
            "tablet_id" : $2,
            "compact_type" : "$3"
        })";

        std::string msg = "compaction task for this tablet is not running";
        std::string compaction_type;
        bool run_status = false;

        {
            // use try lock to check this tablet is running cumulative compaction
            std::unique_lock<std::mutex> lock_cumulative(tablet->get_cumulative_compaction_lock(),
                                                         std::try_to_lock);
            if (!lock_cumulative.owns_lock()) {
                msg = "compaction task for this tablet is running";
                compaction_type = "cumulative";
                run_status = true;
                *json_result = strings::Substitute(json_template, run_status, msg, tablet_id,
                                                   compaction_type);
                return Status::OK();
            }
        }

        {
            // use try lock to check this tablet is running base compaction
            std::unique_lock<std::mutex> lock_base(tablet->get_base_compaction_lock(),
                                                   std::try_to_lock);
            if (!lock_base.owns_lock()) {
                msg = "compaction task for this tablet is running";
                compaction_type = "base";
                run_status = true;
                *json_result = strings::Substitute(json_template, run_status, msg, tablet_id,
                                                   compaction_type);
                return Status::OK();
            }
        }
        // not running any compaction
        *json_result =
                strings::Substitute(json_template, run_status, msg, tablet_id, compaction_type);
        return Status::OK();
    }
}

Status CompactionHandler::_execute_compaction_callback(TabletSharedPtr tablet,
                                                       const std::string& compaction_type) {
    MonotonicStopWatch timer;
    timer.start();

    std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy();
    if (tablet->get_cumulative_compaction_policy() == nullptr) {
        tablet->set_cumulative_compaction_policy(cumulative_compaction_policy);
    }
    Status res = Status::OK();
    if (compaction_type == PARAM_COMPACTION_BASE) {
        BaseCompaction base_compaction(tablet);
        res = base_compaction.compact();
        if (!res) {
            if (res.is<BE_NO_SUITABLE_VERSION>()) {
                // Ignore this error code.
                VLOG_NOTICE << "failed to init base compaction due to no suitable version, tablet="
                            << tablet->full_name();
            } else {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                LOG(WARNING) << "failed to init base compaction. res=" << res
                             << ", tablet=" << tablet->full_name();
            }
        }
    } else if (compaction_type == PARAM_COMPACTION_CUMULATIVE) {
        CumulativeCompaction cumulative_compaction(tablet);
        res = cumulative_compaction.compact();
        if (!res) {
            if (res.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
                // Ignore this error code.
                VLOG_NOTICE << "failed to init cumulative compaction due to no suitable version,"
                            << "tablet=" << tablet->full_name();
            } else {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                             << ", table=" << tablet->full_name();
            }
        }
    }

    timer.stop();
    LOG(INFO) << "Manual compaction task finish, status=" << res
              << ", compaction_use_time=" << timer.elapsed_time() / 1000000 << "ms";
    return res;
}

} // namespace doris
