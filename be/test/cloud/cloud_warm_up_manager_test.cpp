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

#include "cloud/cloud_warm_up_manager.h"

#include <bthread/condition_variable.h>
#include <gtest/gtest.h>

#include <any>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

using namespace std::chrono_literals;

namespace {

bool wait_until(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

} // namespace

class CloudWarmUpManagerTest : public testing::Test {
public:
    void SetUp() override {
        _origin_thread_pool_size = config::warm_up_manager_thread_pool_size;
        SyncPoint::get_instance()->clear_all_call_backs();
        SyncPoint::get_instance()->enable_processing();
    }

    void TearDown() override {
        SyncPoint::get_instance()->disable_processing();
        SyncPoint::get_instance()->clear_all_call_backs();
        config::warm_up_manager_thread_pool_size = _origin_thread_pool_size;
    }

    std::unique_ptr<RowsetMeta> create_rowset_meta(int64_t tablet_id) {
        auto rs_meta = std::make_unique<RowsetMeta>();
        rs_meta->set_tablet_id(tablet_id);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_rowset_state(VISIBLE);
        rs_meta->set_version({1, 1});
        rs_meta->set_num_segments(1);
        return rs_meta;
    }

protected:
    CloudStorageEngine _engine {EngineOptions {}};
    int32_t _origin_thread_pool_size = 0;
};

TEST_F(CloudWarmUpManagerTest, NonPositiveTimeoutQueuesBackgroundCopyAndReturns) {
    config::warm_up_manager_thread_pool_size = 1;
    CloudWarmUpManager manager(_engine);

    std::mutex blocker_mtx;
    std::condition_variable blocker_cv;
    bool blocker_started = false;
    bool release_blocker = false;

    ASSERT_TRUE(manager._thread_pool_token
                        ->submit_func([&] {
                            std::unique_lock lock(blocker_mtx);
                            blocker_started = true;
                            blocker_cv.notify_all();
                            blocker_cv.wait(lock, [&] { return release_blocker; });
                        })
                        .ok());

    {
        std::unique_lock lock(blocker_mtx);
        ASSERT_TRUE(blocker_cv.wait_for(lock, 5s, [&] { return blocker_started; }));
    }

    const int64_t expected_tablet_id = 10001;
    auto expected_rowset_id = _engine.next_rowset_id();

    std::mutex observed_mtx;
    std::condition_variable observed_cv;
    bool observed = false;
    int64_t observed_tablet_id = 0;
    RowsetId observed_rowset_id;
    int64_t observed_timeout_ms = 0;

    SyncPoint::CallbackGuard warmup_enter_guard;
    SyncPoint::get_instance()->set_call_back(
            "CloudWarmUpManager::_warm_up_rowset.enter",
            [&](std::vector<std::any>&& args) {
                auto* rs_meta = try_any_cast<RowsetMeta*>(args[0]);
                auto* timeout_ms = try_any_cast<int64_t*>(args[1]);
                {
                    std::lock_guard lock(observed_mtx);
                    observed_tablet_id = rs_meta->tablet_id();
                    observed_rowset_id = rs_meta->rowset_id();
                    observed_timeout_ms = *timeout_ms;
                    observed = true;
                }
                observed_cv.notify_all();
            },
            &warmup_enter_guard);

    auto rs_meta = create_rowset_meta(expected_tablet_id);
    rs_meta->set_rowset_id(expected_rowset_id);

    std::atomic<bool> returned = false;
    std::thread caller([&] {
        manager.warm_up_rowset(*rs_meta, /*table_id=*/0, /*sync_wait_timeout_ms=*/-1);
        returned = true;
    });

    bool returned_quickly = wait_until([&] { return returned.load(); }, 1s);
    EXPECT_TRUE(returned_quickly);
    if (!returned_quickly) {
        std::lock_guard lock(blocker_mtx);
        release_blocker = true;
    }
    blocker_cv.notify_all();
    caller.join();
    ASSERT_TRUE(returned_quickly);

    rs_meta->set_tablet_id(20002);
    rs_meta->set_rowset_id(_engine.next_rowset_id());
    rs_meta.reset();

    {
        std::lock_guard lock(blocker_mtx);
        release_blocker = true;
    }
    blocker_cv.notify_all();

    bool observed_in_time = false;
    {
        std::unique_lock lock(observed_mtx);
        observed_in_time = observed_cv.wait_for(lock, 5s, [&] { return observed; });
    }
    ASSERT_TRUE(observed_in_time);
    EXPECT_EQ(expected_tablet_id, observed_tablet_id);
    EXPECT_EQ(expected_rowset_id, observed_rowset_id);
    EXPECT_EQ(-1, observed_timeout_ms);
}

TEST_F(CloudWarmUpManagerTest, NonPositiveTimeoutSkipsWarmupWhenAsyncRowsetMetaInitFails) {
    CloudWarmUpManager manager(_engine);
    auto rs_meta = create_rowset_meta(10002);

    std::mutex observed_mtx;
    std::condition_variable observed_cv;
    bool init_attempted = false;
    bool warmup_entered = false;

    SyncPoint::CallbackGuard init_guard;
    SyncPoint::get_instance()->set_call_back(
            "CloudWarmUpManager::warm_up_rowset.async_init_from_pb",
            [&](std::vector<std::any>&& args) {
                auto* init_succeed = try_any_cast<bool*>(args[0]);
                *init_succeed = false;
                {
                    std::lock_guard lock(observed_mtx);
                    init_attempted = true;
                }
                observed_cv.notify_all();
            },
            &init_guard);

    SyncPoint::CallbackGuard warmup_enter_guard;
    SyncPoint::get_instance()->set_call_back(
            "CloudWarmUpManager::_warm_up_rowset.enter",
            [&](std::vector<std::any>&&) {
                std::lock_guard lock(observed_mtx);
                warmup_entered = true;
                observed_cv.notify_all();
            },
            &warmup_enter_guard);

    manager.warm_up_rowset(*rs_meta, /*table_id=*/0, /*sync_wait_timeout_ms=*/-1);

    {
        std::unique_lock lock(observed_mtx);
        ASSERT_TRUE(observed_cv.wait_for(lock, 5s, [&] { return init_attempted; }));
        EXPECT_FALSE(observed_cv.wait_for(lock, 200ms, [&] { return warmup_entered; }));
    }
}

TEST_F(CloudWarmUpManagerTest, PositiveTimeoutIgnoresSpuriousWakeupUntilWorkerFinishes) {
    CloudWarmUpManager manager(_engine);
    auto rs_meta = create_rowset_meta(10003);

    std::mutex worker_mtx;
    std::condition_variable worker_cv;
    bool worker_entered = false;
    bool release_worker = false;

    SyncPoint::CallbackGuard warmup_enter_guard;
    SyncPoint::get_instance()->set_call_back(
            "CloudWarmUpManager::_warm_up_rowset.enter",
            [&](std::vector<std::any>&& args) {
                {
                    std::lock_guard lock(worker_mtx);
                    worker_entered = true;
                }
                worker_cv.notify_all();

                std::unique_lock lock(worker_mtx);
                worker_cv.wait(lock, [&] { return release_worker; });
            },
            &warmup_enter_guard);

    std::thread spurious_notify_thread;
    std::atomic<bool> spurious_notify_started = false;
    std::atomic<bool> spurious_notify_sent = false;

    SyncPoint::CallbackGuard before_wait_guard;
    SyncPoint::get_instance()->set_call_back(
            "CloudWarmUpManager::warm_up_rowset.before_wait",
            [&](std::vector<std::any>&& args) {
                auto* cv = try_any_cast<bthread::ConditionVariable*>(args[0]);
                bool expected = false;
                if (spurious_notify_started.compare_exchange_strong(expected, true)) {
                    spurious_notify_thread = std::thread([&, cv] {
                        std::this_thread::sleep_for(50ms);
                        cv->notify_one();
                        spurious_notify_sent = true;
                    });
                }
            },
            &before_wait_guard);

    std::atomic<bool> returned = false;
    std::thread caller([&] {
        manager.warm_up_rowset(*rs_meta, /*table_id=*/0, /*sync_wait_timeout_ms=*/1000);
        returned = true;
    });

    bool worker_entered_in_time = false;
    {
        std::unique_lock lock(worker_mtx);
        worker_entered_in_time = worker_cv.wait_for(lock, 5s, [&] { return worker_entered; });
    }

    bool spurious_notify_sent_in_time = wait_until([&] { return spurious_notify_sent.load(); }, 5s);
    bool spurious_notify_started_in_time =
            wait_until([&] { return spurious_notify_started.load(); }, 1s);
    if (worker_entered_in_time && spurious_notify_sent_in_time && spurious_notify_started_in_time) {
        std::this_thread::sleep_for(100ms);
        EXPECT_FALSE(returned.load());
    }

    {
        std::lock_guard lock(worker_mtx);
        release_worker = true;
    }
    worker_cv.notify_all();

    caller.join();
    if (spurious_notify_thread.joinable()) {
        spurious_notify_thread.join();
    }
    ASSERT_TRUE(worker_entered_in_time);
    ASSERT_TRUE(spurious_notify_sent_in_time);
    ASSERT_TRUE(spurious_notify_started_in_time);
    EXPECT_TRUE(returned.load());
}

} // namespace doris
