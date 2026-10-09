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

#include "util/jni_scan_heap_gate.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "common/config.h"

namespace doris {

namespace {
constexpr int64_t MB = 1024 * 1024;
} // namespace

// A 500 MB budget unless a test changes it.
class JniScanHeapGateTest : public testing::Test {
protected:
    using Admission = JniScanHeapGate::Admission;

    void SetUp() override {
        _saved_max_wait_ms = config::jni_scanner_heap_max_wait_ms;
        config::jni_scanner_heap_max_wait_ms = 60000;
    }

    void TearDown() override { config::jni_scanner_heap_max_wait_ms = _saved_max_wait_ms; }

    // For a reader whose scan never stops.
    std::unique_ptr<Admission> request(int64_t mb) {
        return _gate.request(mb * MB, []() { return false; });
    }

    std::unique_ptr<Admission> request(int64_t mb, const std::shared_ptr<std::atomic<bool>>& stop) {
        return _gate.request(mb * MB, [stop]() { return stop->load(); });
    }

    static bool done_soon(const Admission& admission) {
        for (int i = 0; i < 500 && !admission.future().is_done(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return admission.future().is_done();
    }

    std::atomic<int64_t> _budget_mb {500};
    JniScanHeapGate _gate {[this]() { return _budget_mb.load() * MB; }};

private:
    int64_t _saved_max_wait_ms = 0;
};

TEST_F(JniScanHeapGateTest, AdmitsWhileWhatTheReadersDeclaredFitsTheBudget) {
    auto first = request(200);
    auto second = request(200);
    EXPECT_TRUE(first->admitted());
    EXPECT_TRUE(second->admitted());
    EXPECT_EQ(first->wait_ns(), 0);
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);

    // 400 + 200 > 500: the reader waits, and nothing blocks while it does.
    auto third = request(200);
    EXPECT_TRUE(third->waiting());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_TRUE(third->waiting());
    EXPECT_FALSE(third->future().is_ready());

    // A reader closes and its share comes back: 200 + 200 fits.
    first.reset();
    EXPECT_TRUE(done_soon(*third));
    EXPECT_TRUE(third->admitted());
    EXPECT_GE(third->wait_ns(), 300LL * 1000 * 1000);
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, ReadersAreAdmittedInTheOrderTheyCame) {
    auto holder = request(400);

    // A large reader that does not fit waits, and a small one that would fit waits behind it rather
    // than passing it: otherwise a stream of small readers could keep the large one out for ever.
    auto large = request(300);
    auto small = request(50);
    EXPECT_TRUE(large->waiting());
    EXPECT_TRUE(small->waiting());
    EXPECT_EQ(_gate.waiters(), 2);

    // 300 + 50 fits once the holder is gone, in that order.
    holder.reset();
    EXPECT_TRUE(done_soon(*large));
    EXPECT_TRUE(done_soon(*small));
    EXPECT_TRUE(large->admitted());
    EXPECT_TRUE(small->admitted());
    EXPECT_EQ(_gate.admitted_bytes(), 350 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, AReaderLargerThanTheBudgetRunsAlone) {
    // Nobody holds a share, so nobody would give one back for it: waiting cannot help.
    auto large = request(800);
    EXPECT_TRUE(large->admitted());
    EXPECT_EQ(_gate.admitted_bytes(), 800 * MB);

    // While it runs, even the smallest reader waits.
    auto small = request(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_TRUE(small->waiting());

    large.reset();
    EXPECT_TRUE(done_soon(*small));
    EXPECT_TRUE(small->admitted());
    EXPECT_EQ(_gate.holders(), 1);
}

TEST_F(JniScanHeapGateTest, ABiggerBudgetAppliesToTheReadersAlreadyWaiting) {
    auto holder = request(400);
    auto waiting = request(200);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_TRUE(waiting->waiting());

    // Nobody gives a share back; the gate sees the new budget the next time it looks.
    _budget_mb = 600;
    EXPECT_TRUE(done_soon(*waiting));
    EXPECT_TRUE(waiting->admitted());
    EXPECT_EQ(_gate.admitted_bytes(), 600 * MB);
}

TEST_F(JniScanHeapGateTest, AReaderWhoseScanStopsLeavesWithoutAShare) {
    auto holder = request(500);

    // Its query is cancelled while it waits. Admitting it would open a Java scanner above the budget
    // for a query with nothing left to read - all of that query's waiting readers at once.
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    auto stopped = request(100, cancelled);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(stopped->waiting());
    cancelled->store(true);
    EXPECT_TRUE(done_soon(*stopped));
    EXPECT_FALSE(stopped->waiting());
    EXPECT_FALSE(stopped->admitted());
    EXPECT_GE(stopped->wait_ns(), 200LL * 1000 * 1000);
    EXPECT_EQ(_gate.holders(), 1);
    EXPECT_EQ(_gate.admitted_bytes(), 500 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, AReaderWhoseScanHasStoppedTakesNothingEvenWhenItFits) {
    auto stopped = std::make_shared<std::atomic<bool>>(true);
    auto admission = request(100, stopped);
    EXPECT_FALSE(admission->waiting());
    EXPECT_FALSE(admission->admitted());
    EXPECT_TRUE(admission->future().is_done());
    EXPECT_EQ(_gate.holders(), 0);
    EXPECT_EQ(_gate.admitted_bytes(), 0);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, AStoppedReaderFirstInLineLetsTheNextOneIn) {
    auto holder = request(300);

    // 300 + 400 > 500: the large reader waits first in line, and a small one behind it although
    // 300 + 100 would fit.
    auto large_stopped = std::make_shared<std::atomic<bool>>(false);
    auto large = request(400, large_stopped);
    auto small = request(100);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_TRUE(small->waiting());

    // The large reader's scan stops: it leaves the line, and the small one, first now, fits while
    // the holder still holds its share.
    large_stopped->store(true);
    EXPECT_TRUE(done_soon(*large));
    EXPECT_TRUE(done_soon(*small));
    EXPECT_FALSE(large->admitted());
    EXPECT_TRUE(small->admitted());
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 400 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

TEST_F(JniScanHeapGateTest, AdmitsAReaderThatWaitedLongerThanTheLongestWait) {
    config::jni_scanner_heap_max_wait_ms = 200;
    auto holder = request(500);

    auto late = request(100);
    EXPECT_TRUE(late->waiting());
    EXPECT_TRUE(done_soon(*late));
    EXPECT_TRUE(late->admitted());
    EXPECT_GE(late->wait_ns(), 200LL * 1000 * 1000);
    EXPECT_EQ(_gate.admitted_bytes(), 600 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
    EXPECT_EQ(_gate.admitted_after_wait_limit(), 1);
}

TEST_F(JniScanHeapGateTest, ReadersThatWaitedTooLongTogetherAreAllAdmittedAboveTheBudget) {
    // What the longest wait costs: readers that came together run out of it together, and each is
    // admitted whatever the account says. It is there for a holder that cannot finish until a
    // waiter does (a join whose build side waits for shares the probe side keeps until the build is
    // done); with a holder that merely runs long, the readers behind it open above the budget at once.
    config::jni_scanner_heap_max_wait_ms = 300;
    auto holder = request(500);
    std::vector<std::unique_ptr<Admission>> waiters;
    for (int i = 0; i < 4; ++i) {
        waiters.push_back(request(200));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(_gate.waiters(), 4);
    EXPECT_EQ(_gate.admitted_after_wait_limit(), 0);

    for (const auto& waiter : waiters) {
        EXPECT_TRUE(done_soon(*waiter));
        EXPECT_TRUE(waiter->admitted());
    }
    EXPECT_EQ(_gate.admitted_after_wait_limit(), 4);
    EXPECT_EQ(_gate.holders(), 5);
    EXPECT_EQ(_gate.admitted_bytes(), 1300 * MB);
}

TEST_F(JniScanHeapGateTest, FuturesAreCompletedOnTheGatesOwnThread) {
    auto holder = request(500);
    auto waiter = request(100);
    ASSERT_TRUE(waiter->waiting());

    // The scheduler resumes a parked scan from this callback, taking its context's locks. A thread
    // that gives a share back can be holding locks of its own - one closing scanners holds its
    // context's - so the callback must not run on it.
    std::atomic<bool> called {false};
    std::atomic<bool> admitted_when_called {false};
    std::thread::id callback_thread;
    waiter->future().add_callback([&](const Void&, const Status&) {
        callback_thread = std::this_thread::get_id();
        admitted_when_called.store(waiter->admitted());
        called.store(true);
    });
    holder.reset();
    EXPECT_TRUE(done_soon(*waiter));
    for (int i = 0; i < 500 && !called.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(called.load());
    EXPECT_TRUE(admitted_when_called.load());
    EXPECT_NE(callback_thread, std::this_thread::get_id());
}

TEST_F(JniScanHeapGateTest, DestroyingAnAdmissionGivesItsShareBackOrLeavesTheLine) {
    {
        auto admitted = request(300);
        EXPECT_EQ(_gate.admitted_bytes(), 300 * MB);
    }
    // Its Java scanner closed: the share comes back.
    EXPECT_EQ(_gate.holders(), 0);
    EXPECT_EQ(_gate.admitted_bytes(), 0);

    auto holder = request(300);
    auto first = request(400);
    auto second = request(200);
    ASSERT_TRUE(first->waiting());
    ASSERT_TRUE(second->waiting());
    // A reader closed before its turn leaves the line, and the next one in line now fits; nobody
    // will run the closed reader again, so its future stays undone.
    auto first_future = first->future();
    first.reset();
    EXPECT_TRUE(done_soon(*second));
    EXPECT_TRUE(second->admitted());
    EXPECT_FALSE(first_future.is_ready());
    EXPECT_EQ(_gate.holders(), 2);
    EXPECT_EQ(_gate.admitted_bytes(), 500 * MB);
    EXPECT_EQ(_gate.waiters(), 0);
}

} // namespace doris
