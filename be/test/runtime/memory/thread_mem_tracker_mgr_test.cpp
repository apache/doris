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

#include "runtime/memory/thread_mem_tracker_mgr.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "core/allocator.h"
#include "core/allocator_fwd.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime/workload_management/resource_context.h"
#include "util/defer_op.h"

namespace doris {

class ThreadMemTrackerMgrTest : public testing::Test {
public:
    ThreadMemTrackerMgrTest() = default;
    ~ThreadMemTrackerMgrTest() override = default;

    void SetUp() override { _wg_manager = std::make_unique<WorkloadGroupMgr>(); }

    void TearDown() override { _wg_manager.reset(); }

protected:
    std::unique_ptr<WorkloadGroupMgr> _wg_manager;
};

TEST_F(ThreadMemTrackerMgrTest, ConsumeMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-ConsumeMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // size1 < config::mem_tracker_consume_min_size_bytes, not consume mem tracker.
    EXPECT_EQ(t->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    // size1 + size2 > onfig::mem_tracker_consume_min_size_bytes, consume mem tracker.
    EXPECT_EQ(t->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // std::abs(-size1) < config::mem_tracker_consume_min_size_bytes, not consume mem tracker.
    EXPECT_EQ(t->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(t->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    // std::abs(-size2) > onfig::mem_tracker_consume_min_size_bytes, consume mem tracker.
    EXPECT_EQ(t->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    EXPECT_EQ(t->consumption(), -size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t->consumption(), -size2);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 2);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 10);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 100);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 1000);
    thread_context->thread_mem_tracker_mgr->consume(size2 * 10000);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 2);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 10);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 100);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 1000);
    thread_context->thread_mem_tracker_mgr->consume(-size2 * 10000);
    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), 0); // detach automatic call flush_untracked_mem.
}

TEST_F(ThreadMemTrackerMgrTest, Boundary) {
    // TODO, Boundary check may not be necessary, add some `IF` maybe increase cost time.
}

TEST_F(ThreadMemTrackerMgrTest, NestedSwitchMemTracker) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker1");
    std::shared_ptr<MemTrackerLimiter> t2 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker2");
    std::shared_ptr<MemTrackerLimiter> t3 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTracker3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    EXPECT_EQ(t1->consumption(),
              size1 + size2 + size1); // attach automatic call flush_untracked_mem.

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1); // not changed, now consume t2
    EXPECT_EQ(t2->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t2->consumption(),
              size1 + size2 + size1); // detach automatic call flush_untracked_mem.

    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2 + size1); // not changed, now consume t1

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t3);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2); // attach automatic call flush_untracked_mem.
    EXPECT_EQ(t3->consumption(), size1 + size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t3->consumption(), size1);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size1 + size2);
    EXPECT_EQ(t3->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), -size1);

    thread_context->thread_mem_tracker_mgr->consume(-t1->consumption());
    thread_context->detach_task(); // detach t1
    EXPECT_EQ(t1->consumption(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, MultiMemTracker) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-MultiMemTracker1");
    std::shared_ptr<MemTracker> t2 = std::make_shared<MemTracker>("UT-MultiMemTracker2");
    std::shared_ptr<MemTracker> t3 = std::make_shared<MemTracker>("UT-MultiMemTracker3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    EXPECT_EQ(t1->consumption(), size1 + size2);

    bool rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t2.get());
    EXPECT_EQ(rt, true);
    EXPECT_EQ(t1->consumption(), size1 + size2); // _untracked_mem = size1
    EXPECT_EQ(t2->consumption(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2);

    rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t2.get());
    EXPECT_EQ(rt, false);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2);

    rt = thread_context->thread_mem_tracker_mgr->push_consumer_tracker(t3.get());
    EXPECT_EQ(rt, true);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1); // _untracked_mem = -size1
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->pop_consumer_tracker();
    EXPECT_EQ(t1->consumption(), size1 + size2 + size1 + size2 + size2 + size1 + size2);
    EXPECT_EQ(t2->consumption(), size2 + size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->pop_consumer_tracker();
    EXPECT_EQ(t1->consumption(),
              size1 + size2 + size1 + size2 + size2 + size1 + size2 - size1 - size2);
    EXPECT_EQ(t2->consumption(), size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);

    thread_context->thread_mem_tracker_mgr->consume(-t1->consumption());
    thread_context->detach_task(); // detach t1
    EXPECT_EQ(t1->consumption(), 0);
    EXPECT_EQ(t2->consumption(), size2 + size2);
    EXPECT_EQ(t3->consumption(), size2);
}

TEST_F(ThreadMemTrackerMgrTest, ReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "UT-ReserveMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2);

    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    // std::abs(-size1 - size1) < SYNC_PROC_RESERVED_INTERVAL_BYTES, not update process_reserved_memory.
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size1 + size1);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // reserved memory used done
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    // no reserved memory, normal memory consumption
    EXPECT_EQ(t->consumption(), size1 + size2 + size3 + size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->thread_mem_tracker_mgr->consume(-size3);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    thread_context->thread_mem_tracker_mgr->consume(-size2);
    EXPECT_EQ(t->consumption(), size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // ThreadMemTrackerMgr _reserved_mem = size3 + size1
    // ThreadMemTrackerMgr _untracked_mem = -size1
    thread_context->thread_mem_tracker_mgr->consume(size3);
    // ThreadMemTrackerMgr _reserved_mem = size1
    // ThreadMemTrackerMgr _untracked_mem = -size1 + size3
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size1); // size3 + size1 - size3

    thread_context->thread_mem_tracker_mgr->consume(-size3);
    // ThreadMemTrackerMgr _reserved_mem = size1 + size3
    // ThreadMemTrackerMgr _untracked_mem = 0, std::abs(-size3) > SYNC_PROC_RESERVED_INTERVAL_BYTES,
    // so update process_reserved_memory.
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size1 + size3);

    thread_context->thread_mem_tracker_mgr->consume(size1);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size1);
    // ThreadMemTrackerMgr _reserved_mem = size1 + size3 - size1 - size2 - size1 = size3 - size2 - size1
    // ThreadMemTrackerMgr _untracked_mem = size1
    EXPECT_EQ(t->consumption(), size1 + size2 + size3);
    // size1 + size3 - (size1 + size2)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->shrink_reserved();
    // size1 + size2 + size3 - _reserved_mem, size1 + size2 + size3 - (size3 - size2 - size1)
    EXPECT_EQ(t->consumption(), size1 + size2 + size1 + size2);
    // size3 - size2 - (_reserved_mem + _untracked_mem) = 0, size3 - size2 - ((size3 - size2 - size1) + (size1)) = 0
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), size1 + size2 + size1 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, TransfersReservationBetweenAsyncTasks) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-TransferReservation");
    auto resource_context = ResourceContext::create_shared();
    resource_context->memory_context()->set_mem_tracker(tracker);
    ThreadContext producer;
    ThreadContext consumer;
    producer.attach_task(resource_context);
    consumer.attach_task(resource_context);
    constexpr int64_t reservation = 4 * 1024 * 1024;

    ASSERT_TRUE(producer.thread_mem_tracker_mgr->try_reserve(reservation).ok());
    auto token = producer.thread_mem_tracker_mgr->take_reserved_memory();
    EXPECT_EQ(producer.thread_mem_tracker_mgr->reserved_mem(), 0);
    EXPECT_EQ(token.bytes(), reservation);

    consumer.thread_mem_tracker_mgr->adopt_reserved_memory(std::move(token));
    EXPECT_EQ(consumer.thread_mem_tracker_mgr->reserved_mem(), reservation);
    consumer.thread_mem_tracker_mgr->consume(reservation);
    EXPECT_EQ(consumer.thread_mem_tracker_mgr->reserved_mem(), 0);

    ASSERT_TRUE(producer.thread_mem_tracker_mgr->try_reserve(reservation).ok());
    {
        auto abandoned = producer.thread_mem_tracker_mgr->take_reserved_memory();
        EXPECT_EQ(abandoned.bytes(), reservation);
    }
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), 0);

    producer.detach_task();
    consumer.detach_task();
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, NestedReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedReserveMemory");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);

    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3);

    thread_context->thread_mem_tracker_mgr->consume(size2);
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2
    // ThreadMemTrackerMgr _untracked_mem = 0, size2 > SYNC_PROC_RESERVED_INTERVAL_BYTES,
    // update process_reserved_memory.
    EXPECT_EQ(t->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size2);
    EXPECT_TRUE(st.ok()) << st.to_string();
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2 + size2
    // ThreadMemTrackerMgr _untracked_mem = 0
    EXPECT_EQ(t->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size3); // size3 - size2 + size2

    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    thread_context->thread_mem_tracker_mgr->consume(size3);
    thread_context->thread_mem_tracker_mgr->consume(size2);
    thread_context->thread_mem_tracker_mgr->consume(size3);
    // ThreadMemTrackerMgr _reserved_mem = size3 - size2
    // ThreadMemTrackerMgr _untracked_mem = 0
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->shrink_reserved();
    // size3 + size2 + size3 + size3 - _reserved_mem, size3 + size2 + size3 + size3 - (size3 - size2)
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size2);
    // size3 - size2 - (_reserved_mem + _untracked_mem) = 0, size3 - size2 - ((size3 - size2 - size1) + (size1)) = 0
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

    thread_context->detach_task();
    EXPECT_EQ(t->consumption(), size3 + size2 + size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, NestedSwitchMemTrackerReserveMemory) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t1 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory1");
    std::shared_ptr<MemTrackerLimiter> t2 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory2");
    std::shared_ptr<MemTrackerLimiter> t3 = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-NestedSwitchMemTrackerReserveMemory3");
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t1);

    int64_t size1 = 4 * 1024;
    int64_t size2 = 4 * 1024 * 1024;
    int64_t size3 = size2 * 2;

    thread_context->attach_task(rc);
    auto st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    thread_context->thread_mem_tracker_mgr->consume(size2);
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t2);
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2 + size3);

    thread_context->thread_mem_tracker_mgr->consume(size2 + size3); // reserved memory used done
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->attach_limiter_tracker(t3);
    st = thread_context->thread_mem_tracker_mgr->try_reserve(size3);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2 + size3);

    thread_context->thread_mem_tracker_mgr->consume(-size2);
    thread_context->thread_mem_tracker_mgr->consume(-size1);
    // ThreadMemTrackerMgr _reserved_mem = size3 + size2 + size1
    // ThreadMemTrackerMgr _untracked_mem = -size1
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), size3);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(),
              size3 - size2 + size3 + size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size3);
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(t3->consumption(), -size1 - size2); // size3 - _reserved_mem
    //  size3 - size2 + size3 + size2 - (_reserved_mem + _untracked_mem)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->thread_mem_tracker_mgr->detach_limiter_tracker(); // detach
    EXPECT_EQ(t1->consumption(), size3);
    // not changed, reserved memory used done.
    EXPECT_EQ(t2->consumption(), size3 + size2);
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), size3 - size2);

    thread_context->detach_task();
    EXPECT_EQ(t1->consumption(), size2); // size3 - _reserved_mem
    // size3 - size2 - (_reserved_mem + _untracked_mem)
    EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, ReserveMemoryFailed) {
    std::unique_ptr<ThreadContext> thread_context = std::make_unique<ThreadContext>();
    std::shared_ptr<MemTrackerLimiter> t = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::OTHER, "UT-ReserveMemory", 1024);
    std::shared_ptr<ResourceContext> rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(t);
    {
        WorkloadGroupInfo wg_info {.id = 1, .memory_limit = 2048, .memory_high_watermark = 100};
        auto wg = _wg_manager->get_or_create_workload_group(wg_info);
        rc->set_workload_group(wg);
        thread_context->attach_task(rc);

        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);

        auto st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024);

        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_EQ(st.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(t->consumption(), 1024);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 1024);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 1024);

        t->set_limit(1024L * 1024);
        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(t->consumption(), 2048);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 2048);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 2048);

        st = thread_context->thread_mem_tracker_mgr->try_reserve(1024);
        EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(t->consumption(), 2048);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 2048);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 2048);

        thread_context->thread_mem_tracker_mgr->shrink_reserved();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);

        thread_context->detach_task();
        EXPECT_EQ(t->consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
        EXPECT_EQ(doris::GlobalMemoryArbitrator::process_reserved_memory(), 0);
    }
}

TEST_F(ThreadMemTrackerMgrTest, AllocationSkipsWorkloadGroupTotal) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 1024);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 101, .memory_limit = 2048, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    wg->_total_mem_used = 2048;
    SCOPED_ATTACH_TASK(rc);
    tracker->consume(512);
    Defer release {[&]() { tracker->release(512); }};

    Allocator<false, false, false> allocator;
    std::string error;
    EXPECT_FALSE(allocator.memory_tracker_exceed(256, &error));
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(256);
    EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
    EXPECT_EQ(tracker->consumption(), 512);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, DisabledAllocationCheckStillChecksQueryReservation) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 1024);
    tracker->set_enable_check_limit(false);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    SCOPED_ATTACH_TASK(rc);

    Allocator<false, false, false> allocator;
    std::string error;
    EXPECT_FALSE(allocator.memory_tracker_exceed(2048, &error));
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(2048);
    EXPECT_EQ(st.code(), ErrorCode::QUERY_MEMORY_EXCEEDED) << st.to_string();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, LocalAllocationChecksRespectLimitBoundaries) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 1024);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    SCOPED_ATTACH_TASK(rc);
    tracker->consume(768);
    Defer release {[&]() { tracker->release(768); }};

    for (int64_t bytes : {-1, 0, 255, 256}) {
        EXPECT_TRUE(tracker->check_limit(bytes).ok()) << bytes;
        EXPECT_TRUE(tracker->check_memory_limit(bytes, MemoryLimit::CheckScope::CHECK_TASK).ok())
                << bytes;
        EXPECT_FALSE(tracker->exceeds_memory_limit(bytes, MemoryLimit::CheckScope::CHECK_TASK))
                << bytes;
    }
    EXPECT_EQ(tracker->check_limit(257).code(), ErrorCode::MEM_LIMIT_EXCEEDED);
    EXPECT_EQ(tracker->check_memory_limit(257, MemoryLimit::CheckScope::CHECK_TASK).code(),
              ErrorCode::MEM_LIMIT_EXCEEDED);
    EXPECT_TRUE(tracker->exceeds_memory_limit(257, MemoryLimit::CheckScope::CHECK_TASK));
    EXPECT_EQ(tracker->consumption(), 768);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, LocalAllocationChecksAllowUnlimitedTasks) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", -1);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    SCOPED_ATTACH_TASK(rc);
    tracker->consume(768);
    Defer release {[&]() { tracker->release(768); }};

    for (int64_t limit : {-1, 0}) {
        tracker->set_limit(limit);
        EXPECT_TRUE(tracker->check_limit(2048).ok()) << limit;
        EXPECT_TRUE(tracker->check_memory_limit(2048, MemoryLimit::CheckScope::CHECK_TASK).ok())
                << limit;
        EXPECT_FALSE(tracker->exceeds_memory_limit(2048, MemoryLimit::CheckScope::CHECK_TASK))
                << limit;
    }
    EXPECT_EQ(tracker->consumption(), 768);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, ProcessOnlyReservationStillAccountsTaskAndGroup) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 32);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 102, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    SCOPED_ATTACH_TASK(rc);
    const auto original_process_reserved = GlobalMemoryArbitrator::process_reserved_memory();

    auto* mgr = thread_context()->thread_mem_tracker_mgr.get();
    auto st = mgr->try_reserve(128, ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(tracker->consumption(), 128);
    EXPECT_EQ(tracker->reserved_consumption(), 128);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 128);
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), original_process_reserved + 128);

    mgr->shrink_reserved();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), original_process_reserved);
}

TEST_F(ThreadMemTrackerMgrTest, SelectedAncestorChecksContinuePastDisabledQuery) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 32);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 103, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    SCOPED_ATTACH_TASK(rc);
    using Checks = MemoryLimit::CheckScope;

    auto st = tracker->check_memory_limit(128, Checks::CHECK_TASK_AND_WORKLOAD_GROUP);
    EXPECT_EQ(st.code(), ErrorCode::MEM_LIMIT_EXCEEDED) << st.to_string();
    tracker->set_enable_check_limit(false);
    EXPECT_TRUE(tracker->exceeds_memory_limit(128, Checks::CHECK_TASK_AND_WORKLOAD_GROUP));
    EXPECT_FALSE(tracker->exceeds_memory_limit(128, Checks::CHECK_TASK));
    st = tracker->check_memory_limit(128, Checks::CHECK_TASK_AND_WORKLOAD_GROUP);
    EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
    st = wg->check_memory_limit(128, Checks::CHECK_WORKLOAD_GROUP);
    EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();

    auto write_tracker = tracker->write_tracker();
    EXPECT_TRUE(write_tracker->check_limit(128).ok());
    st = write_tracker->check_memory_limit(128, Checks::CHECK_TASK_AND_WORKLOAD_GROUP);
    EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
    // The process root uses physical memory, not the sum of child counters.
    EXPECT_TRUE(MemoryLimit::process_memory_limit()->exceeds_memory_limit(MemInfo::mem_limit(),
                                                                          Checks::CHECK_PROCESS));
    st = MemoryLimit::process_memory_limit()->check_memory_limit(MemInfo::mem_limit(),
                                                                 Checks::CHECK_PROCESS);
    EXPECT_EQ(st.code(), ErrorCode::PROCESS_MEMORY_EXCEEDED) << st.to_string();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, ProcessReservationFailureRollsBackDescendants) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query");
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 104, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    SCOPED_ATTACH_TASK(rc);
    const auto original_process_reserved = GlobalMemoryArbitrator::process_reserved_memory();
    auto* mgr = thread_context()->thread_mem_tracker_mgr.get();

    // Skip the small WG budget so the process is the failing ancestor.
    auto st = mgr->try_reserve(MemInfo::soft_mem_limit(),
                               ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
    EXPECT_EQ(st.code(), ErrorCode::PROCESS_MEMORY_EXCEEDED) << st.to_string();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
    EXPECT_EQ(GlobalMemoryArbitrator::process_reserved_memory(), original_process_reserved);
    EXPECT_EQ(mgr->reserved_mem(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, SiblingQueriesShareWorkloadGroupReservationBudget) {
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 105, .memory_limit = 256, .memory_high_watermark = 100});
    auto first = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "first", 256);
    auto second = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "second", 256);
    auto first_rc = ResourceContext::create_shared();
    auto second_rc = ResourceContext::create_shared();
    first_rc->memory_context()->set_mem_tracker(first);
    first_rc->set_workload_group(wg);
    second_rc->memory_context()->set_mem_tracker(second);
    second_rc->set_workload_group(wg);

    SCOPED_ATTACH_TASK(first_rc);
    ASSERT_TRUE(thread_context()->thread_mem_tracker_mgr->try_reserve(192).ok());
    {
        SCOPED_SWITCH_RESOURCE_CONTEXT(second_rc);
        auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(128);
        EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(second->consumption(), 0);
        EXPECT_EQ(second->reserved_consumption(), 0);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 192);
    }
    thread_context()->thread_mem_tracker_mgr->shrink_reserved();
    EXPECT_EQ(first->consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
}

TEST_F(ThreadMemTrackerMgrTest, TemporaryLimiterKeepsAttachedTaskGroupForReservation) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 256);
    auto cache = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE, "cache");
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 106, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    SCOPED_ATTACH_TASK(rc);
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(cache);
        auto* mgr = thread_context()->thread_mem_tracker_mgr.get();
        auto st = mgr->try_reserve(128);
        EXPECT_EQ(st.code(), ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED) << st.to_string();
        EXPECT_EQ(cache->consumption(), 0);
        EXPECT_EQ(cache->reserved_consumption(), 0);
        ASSERT_TRUE(mgr->try_reserve(32).ok());
        EXPECT_EQ(cache->consumption(), 32);
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 32);
    }
    EXPECT_EQ(cache->consumption(), 0);
    EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
    EXPECT_EQ(cache->memory_limit_parent(), MemoryLimit::process_memory_limit());
}

TEST_F(ThreadMemTrackerMgrTest, WorkloadGroupCanBeBoundBeforeTrackerAndRemoved) {
    auto rc = ResourceContext::create_shared();
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 107, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 32);
    rc->memory_context()->set_mem_tracker(tracker);
    EXPECT_EQ(tracker->memory_limit_parent(), wg);
    EXPECT_EQ(tracker->write_tracker()->memory_limit_parent(), wg);

    rc->set_workload_group(nullptr);
    EXPECT_EQ(tracker->memory_limit_parent(), MemoryLimit::process_memory_limit());
    EXPECT_EQ(tracker->write_tracker()->memory_limit_parent(), MemoryLimit::process_memory_limit());
}

TEST_F(ThreadMemTrackerMgrTest, TrackerOnlyAttachmentPreservesSharedQueryParent) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "query", 32);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 108, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    {
        SCOPED_ATTACH_TASK(tracker);
        EXPECT_EQ(tracker->memory_limit_parent(), wg);
        EXPECT_EQ(tracker->write_tracker()->memory_limit_parent(), wg);
        // This attachment has no thread WG, so legacy reserve checks process
        // directly even though the shared tracker belongs to a WG.
        ASSERT_TRUE(
                thread_context()
                        ->thread_mem_tracker_mgr
                        ->try_reserve(128, ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS)
                        .ok());
        EXPECT_EQ(wg->wg_refresh_interval_memory_growth(), 0);
    }
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(tracker->memory_limit_parent(), wg);
}

TEST_F(ThreadMemTrackerMgrTest, WorkloadGroupBindingSupportsTrackerWithoutWriteSibling) {
    auto tracker = std::make_shared<MemTrackerLimiter>(MemTrackerLimiter::Type::QUERY, "query", 32);
    auto rc = ResourceContext::create_shared();
    rc->memory_context()->set_mem_tracker(tracker);
    auto wg = _wg_manager->get_or_create_workload_group(
            {.id = 109, .memory_limit = 64, .memory_high_watermark = 100});
    rc->set_workload_group(wg);
    EXPECT_EQ(tracker->memory_limit_parent(), wg);
    EXPECT_EQ(tracker->write_tracker(), nullptr);
    rc->set_workload_group(nullptr);
    EXPECT_EQ(tracker->memory_limit_parent(), MemoryLimit::process_memory_limit());
}

} // end namespace doris
