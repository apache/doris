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

#include "service/http/action/compaction_action.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include <string>

#include "service/http/http_request.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/uid_util.h"

namespace doris {

// Test fixture: creates a minimal StorageEngine with TabletManager initialized.
// No real tablets are added, so get_tablet() returns nullptr for any id.
// Tests use -fno-access-control to call private methods directly.
class CompactionActionTest : public testing::Test {
public:
    void SetUp() override {
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _storage_engine = std::make_unique<StorageEngine>(options);
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }

    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
        _storage_engine.reset();
    }

protected:
    CompactionAction _make_run_action() {
        return {CompactionActionType::RUN_COMPACTION, nullptr, *_storage_engine,
                TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN};
    }

    CompactionAction _make_status_action() {
        return {CompactionActionType::RUN_COMPACTION_STATUS, nullptr, *_storage_engine,
                TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN};
    }

    CompactionAction _make_show_action() {
        return {CompactionActionType::SHOW_INFO, nullptr, *_storage_engine, TPrivilegeHier::GLOBAL,
                TPrivilegeType::ADMIN};
    }

    evhttp_request* _evhttp_req = nullptr;
    std::unique_ptr<StorageEngine> _storage_engine;
};

// ==================== _handle_run_compaction tests ====================

TEST_F(CompactionActionTest, RunCompactionMissingBothIds) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("can not be empty at the same time") != std::string::npos);
}

TEST_F(CompactionActionTest, RunCompactionBothIdsSet) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "12345";
    req._params[TABLE_ID_KEY] = "67890";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("can not be set at the same time") != std::string::npos);
}

TEST_F(CompactionActionTest, RunCompactionInvalidType) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "12345";
    req._params[PARAM_COMPACTION_TYPE] = "invalid_type";

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);
}

TEST_F(CompactionActionTest, RunCompactionInvalidRemoteParam) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "12345";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;
    req._params[PARAM_COMPACTION_REMOTE] = "invalid_value";

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);
}

TEST_F(CompactionActionTest, RunCompactionInvalidTabletId) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "not_a_number";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("convert") != std::string::npos);
}

TEST_F(CompactionActionTest, RunFullCompactionTabletNotFound) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(CompactionActionTest, RunBaseCompactionTabletNotFound) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_BASE;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(CompactionActionTest, RunCumulativeCompactionTabletNotFound) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_CUMULATIVE;

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

// ==================== force parameter tests ====================

TEST_F(CompactionActionTest, RunCompactionInvalidForceParam) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "12345";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;
    req._params[PARAM_COMPACTION_FORCE] = "invalid_value";

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("not supported") != std::string::npos);
}

TEST_F(CompactionActionTest, RunFullCompactionForceTrue) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;
    req._params[PARAM_COMPACTION_FORCE] = "true";

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    // tablet not found, but force param was parsed successfully
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(CompactionActionTest, RunFullCompactionForceFalse) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;
    req._params[PARAM_COMPACTION_FORCE] = "false";

    std::string json_result;
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

TEST_F(CompactionActionTest, RunCompactionForceWithTableId) {
    auto action = _make_run_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLE_ID_KEY] = "99999";
    req._params[PARAM_COMPACTION_TYPE] = PARAM_COMPACTION_FULL;
    req._params[PARAM_COMPACTION_FORCE] = "true";

    std::string json_result;
    // table_id path with no matching tablets returns success (empty loop)
    Status st = action._handle_run_compaction(&req, &json_result);
    EXPECT_TRUE(st.ok());
}

// ==================== _handle_show_compaction tests ====================

TEST_F(CompactionActionTest, ShowCompactionMissingTabletId) {
    auto action = _make_show_action();
    HttpRequest req(_evhttp_req);

    std::string json_result;
    Status st = action._handle_show_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
}

TEST_F(CompactionActionTest, ShowCompactionTabletNotFound) {
    auto action = _make_show_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";

    std::string json_result;
    Status st = action._handle_show_compaction(&req, &json_result);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
}

// ==================== _handle_run_status_compaction tests ====================

TEST_F(CompactionActionTest, RunStatusCompactionOverall) {
    auto action = _make_status_action();
    HttpRequest req(_evhttp_req);
    // tablet_id not set → returns overall compaction status
    std::string json_result;
    Status st = action._handle_run_status_compaction(&req, &json_result);
    EXPECT_TRUE(st.ok());
}

TEST_F(CompactionActionTest, RunStatusCompactionTabletNotFound) {
    auto action = _make_status_action();
    HttpRequest req(_evhttp_req);
    req._params[TABLET_ID_KEY] = "99999";

    std::string json_result;
    Status st = action._handle_run_status_compaction(&req, &json_result);
    EXPECT_FALSE(st.ok());
}

} // namespace doris
