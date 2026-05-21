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

#include "service/internal_service.h"

#include <functional>
#include <string>

#include "gen_cpp/Status_types.h"
#include "load/channel/load_stream_mgr.h"
#include "runtime/exec_env.h"

namespace doris {

namespace {

class TestClosure final : public google::protobuf::Closure {
public:
    explicit TestClosure(std::function<void()> callback) : _callback(std::move(callback)) {}

    void Run() override {
        if (_callback) {
            _callback();
        }
    }

private:
    std::function<void()> _callback;
};

} // namespace

TEST(InternalServiceTest, TestSyncTabletMetaNotSupported) {
    ExecEnv exec_env;
    exec_env._load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
    PInternalService service(&exec_env);

    PSyncTabletMetaRequest request;
    request.add_tablet_ids(10001);
    PSyncTabletMetaResponse response;
    bool done = false;
    TestClosure closure([&]() { done = true; });

    service.sync_tablet_meta(nullptr, &request, &response, &closure);

    EXPECT_TRUE(done);
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(TStatusCode::NOT_IMPLEMENTED_ERROR, response.status().status_code());
    ASSERT_GT(response.status().error_msgs_size(), 0);
    EXPECT_NE(std::string::npos,
              response.status().error_msgs(0).find("sync_tablet_meta only supports cloud mode"))
            << response.status().DebugString();
}

} // namespace doris
