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

#include <gen_cpp/FrontendService_types.h>

#include <cstddef>
#include <memory>
#include <string>

#include "cloud/cloud_tablet_mgr.h"
#include "common/status.h"
#include "http/http_handler_with_auth.h"
#include "http/http_request.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
namespace doris {

struct CompactionScoreResult {
    int64_t tablet_id;
    size_t compaction_score;
};

inline bool operator>(const CompactionScoreResult& lhs, const CompactionScoreResult& rhs) {
    return lhs.compaction_score > rhs.compaction_score;
}

struct CompactionScoresAccessor {
    virtual ~CompactionScoresAccessor() = default;

    virtual std::vector<CompactionScoreResult> get_all_tablet_compaction_scores() = 0;
};

// topn, sync
class CompactionScoreAction : public HttpHandlerWithAuth {
public:
    explicit CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                   TPrivilegeType::type type, TabletManager* tablet_mgr);

    explicit CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                   TPrivilegeType::type type, CloudTabletMgr& tablet_mgr);

    void handle(HttpRequest* req) override;

private:
    Status _handle(size_t top_n, bool sync_meta, std::string* result);

    std::unique_ptr<CompactionScoresAccessor> _accessor;
};

} // namespace doris
