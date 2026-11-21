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

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

#include "load/channel/load_channel_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace doris {

class GracefulLoadChannelMgrTest : public testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        if (exec_env->fragment_mgr() == nullptr) {
            _fragment_mgr = std::make_unique<FragmentMgr>(exec_env);
            exec_env->_fragment_mgr = _fragment_mgr.get();
        }
        _mgr = std::make_unique<LoadChannelMgr>();
        // Drive timeout cleanup explicitly so the interleavings are deterministic.
        _mgr->_load_state_channels = std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
    }

    void TearDown() override {
        _mgr->stop();
        _mgr.reset();
        if (_fragment_mgr != nullptr) {
            _fragment_mgr->stop();
            ExecEnv::GetInstance()->_fragment_mgr = nullptr;
            _fragment_mgr.reset();
        }
    }

    std::shared_ptr<LoadChannel> create_channel() {
        auto channel = std::make_shared<LoadChannel>(_load_id, 1, true, "127.0.0.1", 0, false, -1);
        std::lock_guard<std::mutex> lock(_mgr->_lock);
        _mgr->_load_channels[_load_id] = channel;
        return channel;
    }

    UniqueId _load_id {1, 2};
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    std::unique_ptr<LoadChannelMgr> _mgr;
};

TEST_F(GracefulLoadChannelMgrTest, GracefulShutdownCountsReceiverUntilFinish) {
    EXPECT_EQ(_mgr->num_active_channels(), 0);
    auto channel = create_channel();
    EXPECT_EQ(_mgr->num_active_channels(), 1);
    _mgr->_finish_load_channel(channel->load_id());
    EXPECT_EQ(_mgr->num_active_channels(), 0);
}

TEST_F(GracefulLoadChannelMgrTest, GracefulShutdownCensusDoesNotWaitForManagerLock) {
    // Model an open() holding the manager lock while QueryContext cleanup stalls.
    std::unique_lock<std::mutex> manager_lock(_mgr->_lock);
    std::promise<size_t> census;
    auto result = census.get_future();
    std::thread polling([&] { census.set_value(_mgr->num_active_channels()); });
    auto status = result.wait_for(std::chrono::seconds(5));
    manager_lock.unlock();
    polling.join();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(result.get(), 1);
    EXPECT_EQ(_mgr->num_active_channels(), 0);
}

} // namespace doris
