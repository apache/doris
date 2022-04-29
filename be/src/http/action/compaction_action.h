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

#pragma once

#include "common/status.h"
#include "http/http_handler.h"
#include "olap/base_compaction.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"

namespace doris {

enum CompactionActionType {
    SHOW_INFO = 1,
    RUN_COMPACTION = 2,
    RUN_COMPACTION_STATUS = 3,
};

const std::string PARAM_COMPACTION_TYPE = "compact_type";
const std::string PARAM_COMPACTION_BASE = "base";
const std::string PARAM_COMPACTION_CUMULATIVE = "cumulative";

/// This action is used for viewing the compaction status.
/// See compaction-action.md for details.
class CompactionAction : public HttpHandler {
public:
    CompactionAction(CompactionActionType type) : _type(type) {
        _compaction_mem_tracker =
                type == RUN_COMPACTION ? MemTracker::create_tracker(-1, "ManualCompaction", nullptr,
                                                                    MemTrackerLevel::TASK)
                                       : nullptr;
    }

    virtual ~CompactionAction() {}

    void handle(HttpRequest* req) override;

private:
    Status _handle_show_compaction(HttpRequest* req, std::string* json_result);

    /// execute compaction request to run compaction task
    /// param compact_type in req to distinguish the task type, base or cumulative
    Status _handle_run_compaction(HttpRequest* req, std::string* json_result);

    /// thread callback function for the tablet to do compaction
    Status _execute_compaction_callback(TabletSharedPtr tablet, const std::string& compaction_type);

    /// fetch compaction running status
    Status _handle_run_status_compaction(HttpRequest* req, std::string* json_result);

    /// check param and fetch tablet_id from req
    Status _check_param(HttpRequest* req, uint64_t* tablet_id);

    std::shared_ptr<CumulativeCompactionPolicy> _create_cumulative_compaction_policy();

private:
    CompactionActionType _type;

    /// running check mutex
    static std::mutex _compaction_running_mutex;
    /// whether there is manual compaction running
    static bool _is_compaction_running;
    /// memory tracker
    std::shared_ptr<MemTracker> _compaction_mem_tracker;
};

} // end namespace doris
