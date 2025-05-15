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

#include "runtime/thread_context.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

class ThreadContextTest : public testing::Test {
public:
    ThreadContextTest() = default;
    ~ThreadContextTest() override = default;

    void SetUp() override {
        tracker1 = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-ThreadContextTest1");
        tracker2 = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-ThreadContextTest2");
        tracker3 = std::make_unique<MemTracker>("UT-ThreadContextTest3");
        rc1 = ResourceContext::create_shared();
        rc1->memory_context()->set_mem_tracker(tracker1);

        query_id1.hi = 1;
        query_id1.lo = 1;
        rc1->task_controller()->set_task_id(query_id1);
        rc2 = ResourceContext::create_shared();
        rc2->memory_context()->set_mem_tracker(tracker2);
        query_id2.hi = 2;
        query_id2.lo = 2;
        rc2->task_controller()->set_task_id(query_id2);
    }

protected:
    std::shared_ptr<MemTrackerLimiter> tracker1;
    std::shared_ptr<MemTrackerLimiter> tracker2;
    std::shared_ptr<MemTracker> tracker3;
    TUniqueId query_id1;
    TUniqueId query_id2;
    std::shared_ptr<ResourceContext> rc1;
    std::shared_ptr<ResourceContext> rc2;
    std::shared_ptr<WorkloadGroup> workload_group;
};

TEST_F(ThreadContextTest, SingleScoped) {
    // AttachTask
    EXPECT_FALSE(thread_context()->is_attach_task());
    {
        auto scoped = AttachTask(rc1);
        EXPECT_TRUE(thread_context()->is_attach_task());
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest1");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id1);
    }
    EXPECT_FALSE(thread_context()->is_attach_task());
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(), "BE-UT");
    EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), TUniqueId());

    // SwitchResourceContext
    {
        auto scoped = AttachTask(rc1);
        auto scoped2 = SwitchResourceContext(rc2);
        EXPECT_TRUE(thread_context()->is_attach_task());
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest2");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id2);
    }
    EXPECT_FALSE(thread_context()->is_attach_task());
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(), "BE-UT");
    EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), TUniqueId());

    // SwitchThreadMemTrackerLimiter
    {
        auto scoped = SwitchThreadMemTrackerLimiter(tracker1);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest1");
    }
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(), "BE-UT");

    // AddThreadMemTrackerConsumer
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label(), "");
    {
        auto scoped = AddThreadMemTrackerConsumer(tracker3);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label(),
                  "UT-ThreadContextTest3");
    }
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label(), "");

    // ScopedPeakMem
    int64_t peak_mem = 0;
    {
        auto scoped = ScopedPeakMem(&peak_mem);
        thread_context()->thread_mem_tracker_mgr->consume(4 * 1024);
        EXPECT_TRUE(
                thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label().starts_with(
                        "ScopedPeakMem"));
    }
    EXPECT_EQ(peak_mem, 4 * 1024);
    EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label(), "");
}

TEST_F(ThreadContextTest, MixedScoped) {
    EXPECT_FALSE(thread_context()->is_attach_task());
    int64_t peak_mem1 = 0;
    int64_t peak_mem2 = 0;
    {
        auto scoped1 = AttachTask(rc1);
        auto scoped2 = SwitchResourceContext(rc2);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest2");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id2);

        auto scoped3 = SwitchResourceContext(rc2);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest2");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id2);
        EXPECT_TRUE(thread_context()->is_attach_task());

        auto scoped4 = SwitchThreadMemTrackerLimiter(tracker1);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest1");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id2);

        auto scoped5 = AddThreadMemTrackerConsumer(tracker3);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label(),
                  "UT-ThreadContextTest3");

        auto scoped6 = ScopedPeakMem(&peak_mem1);
        thread_context()->thread_mem_tracker_mgr->consume(4 * 1024);
        EXPECT_TRUE(
                thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label().starts_with(
                        "ScopedPeakMem"));

        auto scoped7 = ScopedPeakMem(&peak_mem2);
        thread_context()->thread_mem_tracker_mgr->consume(4 * 1024);
        EXPECT_TRUE(
                thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label().starts_with(
                        "ScopedPeakMem"));

        auto scoped8 = AddThreadMemTrackerConsumer(tracker3);
        thread_context()->thread_mem_tracker_mgr->consume(4 * 1024);
        // tracker3 already exists, will not be added again.
        EXPECT_TRUE(
                thread_context()->thread_mem_tracker_mgr->last_consumer_tracker_label().starts_with(
                        "ScopedPeakMem"));

        auto scoped9 = SwitchResourceContext(rc1);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest1");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id1);

        auto scoped10 = SwitchThreadMemTrackerLimiter(tracker2);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest2");

        auto scoped11 = SwitchResourceContext(rc2);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->label(),
                  "UT-ThreadContextTest2");
        EXPECT_EQ(thread_context()->resource_ctx()->task_controller()->task_id(), query_id2);
        EXPECT_TRUE(thread_context()->is_attach_task());
    }
    EXPECT_FALSE(thread_context()->is_attach_task());
    EXPECT_EQ(peak_mem1, 4 * 1024 * 3);
    EXPECT_EQ(peak_mem2, 4 * 1024 * 2);
}

} // end namespace doris
